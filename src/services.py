from api import _Neo4jRepository, _RedisRepository, _MongoRepository
import uuid
from datetime import datetime, timezone


NEO4J_USER_NODE_NAME = "User"
NEO4J_CONTACT_REL_NAME = "AreConnected"
NEO4J_MSG_COUNT_PROP = "message_count"  

REDIS_MESSAGES_HASH_NAME = "messages_hash"
REDIS_QUEUE_PREFIX = "queue"

MONGO_MSGS_COLLECTION_NAME = "messages"
MONGO_SNAPSHOTS_COLLECTION_NAME = "snapshots"


class _ContactService:
    _neo4j_repo: _Neo4jRepository

    def __init__(self, neo4j_repo: _Neo4jRepository):
        self._neo4j_repo = neo4j_repo   
        self._make_unique_constraint("phone")

    def create_user(self, name: str, phone: str) -> str:
        return self._neo4j_repo.create_node(NEO4J_USER_NODE_NAME, {"name": name, "phone": phone})
    
    def _make_unique_constraint(self, prop_name: str):
        self._neo4j_repo.create_node_unique_constraint(NEO4J_USER_NODE_NAME, prop_name)

    def search_one_user(self, filters: dict) -> dict | None:
        users = self._neo4j_repo.read_nodes(NEO4J_USER_NODE_NAME, filters)
        if users:
            return users[0]
        else:
            return None
    
    def search_users(self, filters: dict | None = None) -> list[dict | None]:
        return self._neo4j_repo.read_nodes(NEO4J_USER_NODE_NAME, filters)
    
    def delete_users(self, filter: dict) -> bool:
        return self._neo4j_repo.delete_nodes(NEO4J_USER_NODE_NAME, filter)

    def _create_contact(self, from_user_props: dict, to_user_props: dict) -> list[dict]:
        """
        !!! This method is not safe against duplicate or self contacts. Use carefully.
        """
        return self._neo4j_repo.create_relation(
            from_label=NEO4J_USER_NODE_NAME,
            to_label=NEO4J_USER_NODE_NAME,
            rel_type=NEO4J_CONTACT_REL_NAME,
            from_props=from_user_props,
            to_props=to_user_props,
            rel_props={NEO4J_MSG_COUNT_PROP: 0}
        )

    def create_contact_by_ids(self, from_user_id: str, to_user_id: str) -> dict:
        if from_user_id == to_user_id:
            raise ValueError("Cannot create self-contact.")
        if self._neo4j_repo.is_connected(
            NEO4J_USER_NODE_NAME,
            NEO4J_USER_NODE_NAME,
            NEO4J_CONTACT_REL_NAME,
            from_user_id,
            to_user_id
        ):
            print(f"Contact already exists between {from_user_id} and {to_user_id}.")
            return {}
        
        return self._neo4j_repo.create_relation_by_id(
            from_label=NEO4J_USER_NODE_NAME,
            to_label=NEO4J_USER_NODE_NAME,
            rel_type=NEO4J_CONTACT_REL_NAME,
            from_id=from_user_id,
            to_id=to_user_id,
            rel_props={NEO4J_MSG_COUNT_PROP: 0}
        )
    
    def get_contacts_of_user(self, user_id: str) -> list[dict]:
        return self._neo4j_repo.get_neighbors(
            label=NEO4J_USER_NODE_NAME,
            node_id=user_id
        )
    
    # next methods are too complex for a generic version in the repo
    # so they are implemented directly in the service
    def get_contacts_ordered_by_messages(
        self,
        node_id: str,
        label: str = NEO4J_USER_NODE_NAME,
        rel_type: str = NEO4J_CONTACT_REL_NAME,
        msg_prop: str = NEO4J_MSG_COUNT_PROP,
    ) -> list[dict]:
        
        query = f"""
        MATCH (n:{label})-[r:{rel_type}]-(m:{label})
        WHERE elementId(n) = $node_id
        RETURN m, r.{msg_prop} AS messages
        ORDER BY messages DESC
        """

        records, _, _ = self._neo4j_repo._client.run_query(
            query,
            self._neo4j_repo._db_name,
            node_id=node_id
        )

        result = []
        for record in records:
            node = record["m"]
            data = dict(node._properties)
            data["id"] = node.element_id
            data["message_count"] = record["messages"]
            result.append(data)

        return result

    def shortest_path_max_messages(
        self,
        from_id: str,
        to_id: str,
        label: str = NEO4J_USER_NODE_NAME,
        rel_type: str = NEO4J_CONTACT_REL_NAME,
        msg_prop: str = NEO4J_MSG_COUNT_PROP,
        max_jumps: int = 10
    ) -> list[dict]:
        query = f"""
        MATCH (n:{label}), (m:{label})
        WHERE elementId(n) = $from_id AND elementId(m) = $to_id

        MATCH path = (n)-[:{rel_type}*1..{max_jumps}]-(m)
        WHERE all(x IN nodes(path) WHERE single(y IN nodes(path) WHERE y = x))

        WITH path,
            reduce(
            total = 0,
            r IN relationships(path) |
            total + coalesce(r.{msg_prop}, 0)
            ) AS total_messages

        RETURN path, total_messages
        ORDER BY total_messages DESC
        LIMIT 1
        """

        records, _, _ = self._neo4j_repo._client.run_query(
            query,
            self._neo4j_repo._db_name,
            from_id=from_id,
            to_id=to_id
        )

        if not records:
            return []

        path = records[0]["path"]
        nodes = []

        for node in path.nodes:
            data = dict(node._properties)
            data["id"] = node.element_id
            nodes.append(data)

        return nodes



class _MessageService:
    _redis_repo: _RedisRepository
    _neo4j_repo: _Neo4jRepository
    _mongo_repo: _MongoRepository

    def __init__(self, redis_repo: _RedisRepository, neo4j_repo: _Neo4jRepository, mongo_repo: _MongoRepository):
        self._redis_repo = redis_repo
        self._neo4j_repo = neo4j_repo
        self._mongo_repo = mongo_repo

    def _ids_to_redis_queue_name(self, from_user_id: str, to_user_id: str) -> str:
        from_phone = self._neo4j_repo.read_node_by_id(NEO4J_USER_NODE_NAME, from_user_id).get("phone")
        to_phone = self._neo4j_repo.read_node_by_id(NEO4J_USER_NODE_NAME, to_user_id).get("phone")
        return f"{REDIS_QUEUE_PREFIX}:{from_phone}:{to_phone}"

    def send_message(self, from_user_id: str, to_user_id: str, message: str) -> str:
        if from_user_id == to_user_id:
            raise ValueError("No self-messaging allowed.")
        
        if self._neo4j_repo.is_connected(NEO4J_USER_NODE_NAME, NEO4J_USER_NODE_NAME, NEO4J_CONTACT_REL_NAME, from_user_id, to_user_id) is False:
            raise ValueError("Users are not connected.")

        queue = self._ids_to_redis_queue_name(from_user_id, to_user_id)
        message_id = str(uuid.uuid4())

        self._redis_repo.insert_hash(REDIS_MESSAGES_HASH_NAME, message_id, message)
        self._redis_repo.push_list(queue, message_id)

        self._neo4j_repo._increment_property(
            NEO4J_MSG_COUNT_PROP, 
            NEO4J_USER_NODE_NAME, 
            NEO4J_USER_NODE_NAME, 
            from_user_id, 
            to_user_id, 
            NEO4J_CONTACT_REL_NAME
        )

        from_phone = self._neo4j_repo.read_node_by_id(
            NEO4J_USER_NODE_NAME, from_user_id
        ).get("phone")

        to_phone = self._neo4j_repo.read_node_by_id(
            NEO4J_USER_NODE_NAME, to_user_id
        ).get("phone")

        self._mongo_repo.insert_one(MONGO_MSGS_COLLECTION_NAME, {
            "message_id": message_id,
            "from_phone": from_phone,
            "to_phone": to_phone,
            "message": message,
            "timestamp": datetime.now(timezone.utc),
        })

        return message_id


    def consume_message(self, from_user_id: str, to_user_id: str, timeout: int = 5) -> str:
        if self._neo4j_repo.is_connected(NEO4J_USER_NODE_NAME, NEO4J_USER_NODE_NAME, NEO4J_CONTACT_REL_NAME, from_user_id, to_user_id) is False:
            raise ValueError("Users are not connected.")
        
        queue = self._ids_to_redis_queue_name(from_user_id, to_user_id)

        message_id = self._redis_repo.pop_list(queue, timeout=timeout)
        if message_id is None:
            return None 

        message = self._redis_repo.find_hash(REDIS_MESSAGES_HASH_NAME, message_id)
        
        if message is None:
            return None
        else:
            self._redis_repo.delete_hash(REDIS_MESSAGES_HASH_NAME, message_id)
            return message.decode()
        

    def message_history_by_connection(
        self,
        from_user_id: str,
        to_user_id: str,
        start_time: datetime | None = None
    ) -> list[dict]:
        
        from_phone = self._neo4j_repo.read_node_by_id(
            NEO4J_USER_NODE_NAME, from_user_id
        ).get("phone")

        to_phone = self._neo4j_repo.read_node_by_id(
            NEO4J_USER_NODE_NAME, to_user_id
        ).get("phone")


        filter_query = {
            "$or": [
                {"from_phone": from_phone, "to_phone": to_phone},
                {"from_phone": to_phone, "to_phone": from_phone}
            ]
        }
      
        if start_time:
            filter_query["timestamp"] = {"$gte": start_time}

        messages = self._mongo_repo.find_many(
            collection=MONGO_MSGS_COLLECTION_NAME,
            filter=filter_query,
            sort=[("timestamp", 1)]
        )

        return [
            {
                "timestamp": msg["timestamp"],
                "from_phone": msg["from_phone"],
                "to_phone": msg["to_phone"],
                "message": msg["message"]
            }
            for msg in messages
        ]
    
    def message_history_by_user(
        self,
        user_id: str,
        start_time: datetime | None = None
    ) -> list[dict]:
        
        phone = self._neo4j_repo.read_node_by_id(
            NEO4J_USER_NODE_NAME, user_id
        ).get("phone")


        filter_query = {
            "$or": [
                {"from_phone": phone},
                {"to_phone": phone}
            ]
        }

        if start_time:
            filter_query["timestamp"] = {"$gte": start_time}

        messages = self._mongo_repo.find_many(
            collection=MONGO_MSGS_COLLECTION_NAME,
            filter=filter_query,
            sort=[("timestamp", 1)]  # orden ascendente por tiempo
        )

        return [
            {
                "timestamp": msg["timestamp"],
                "from_phone": msg["from_phone"],
                "to_phone": msg["to_phone"],
                "message": msg["message"]
            }
            for msg in messages
        ]



class _SnapshotService:  
    _mongo_repo: _MongoRepository
    _neo4j_repo: _Neo4jRepository
    _redis_repo: _RedisRepository
    

    def __init__(self, mongo_repo: _MongoRepository, neo4j_repo: _Neo4jRepository, redis_repo: _RedisRepository):
        self._mongo_repo = mongo_repo
        self._neo4j_repo = neo4j_repo
        self._redis_repo = redis_repo

    def _get_next_version(self) -> int:
        last_snapshot = self._mongo_repo.find_one_sorted(
            collection=MONGO_SNAPSHOTS_COLLECTION_NAME,
            filter={},
            sort=[("version", -1)]
        )

        if not last_snapshot:
            return 1

        return last_snapshot["version"] + 1
    
    def delete_snapshot(self, version: int) -> bool:
        return self._mongo_repo.delete_one(
            collection=MONGO_SNAPSHOTS_COLLECTION_NAME,
            filter={"version": version}
        )
    
    def delete_all_snapshots(self) -> int:
        return self._mongo_repo.delete_many(
            collection=MONGO_SNAPSHOTS_COLLECTION_NAME,
            filter={}
        )
    
    def create_snapshot(self):
        timestamp = datetime.now(timezone.utc)

        # Neo4j
        nodes = self._neo4j_repo.read_nodes(label=NEO4J_USER_NODE_NAME) 
        relationships = self._neo4j_repo.read_relations(relation_type=NEO4J_CONTACT_REL_NAME)

        total_users = len(nodes)
        total_contacts = len(relationships)
        total_msgs = sum(rel.get(NEO4J_MSG_COUNT_PROP, 0) for rel in relationships)

        # Redis
        queues = {}
        msg_hash = {}

        queues_list = self._redis_repo.keys(f"{REDIS_QUEUE_PREFIX}:*")
        for queue in queues_list:
            q = self._redis_repo.read_list(queue)
            queues[queue] = [msg_id.decode() for msg_id in q]
        hash_raw = self._redis_repo.find_hash_all(REDIS_MESSAGES_HASH_NAME)
        for key, value in hash_raw.items():
            msg_hash[key.decode()] = value.decode()
        # Mongo
        version = self._get_next_version()
        messages = self._mongo_repo.find_many(MONGO_MSGS_COLLECTION_NAME)

        # Store snapshot in MongoDB
        snapshot = {
            "version": version,
            "timestamp": timestamp,
            "summary": {
                "total_users": total_users,
                "total_contacts": total_contacts,
                "total_messages": total_msgs
            },
            "neo4j": {
                "nodes": nodes,
                "relationships": relationships
            },
            "redis": {
                "queues": queues,
                "hash": msg_hash
            },
            "mongo": {
                "messages": messages,
            }
        }

        self._mongo_repo.insert_one(MONGO_SNAPSHOTS_COLLECTION_NAME, snapshot)


    def list_snapshots(self) -> list[tuple] | None:
        snapshots = self._mongo_repo.find_many(collection=MONGO_SNAPSHOTS_COLLECTION_NAME)
        if snapshots is None:
            return None
        snapshot_list = []
        for snapshot in snapshots:
            snapshot_list.append((f"version {snapshot['version']}", snapshot["timestamp"].strftime("%Y-%m-%d %H:%M:%S UTC"), snapshot["summary"]))
        return snapshot_list
    
    def restore_from_snapshot(self, version: int):
        snapshot = self._mongo_repo.find_one(
            collection=MONGO_SNAPSHOTS_COLLECTION_NAME,
            filter={"version": version}
        )
        if not snapshot:
            raise ValueError(f"Snapshot version {version} not found.")
        
        # Clear current databases
        self._neo4j_repo.clear_database()
        self._redis_repo.clear_database()
        self._mongo_repo.clear_collection(MONGO_MSGS_COLLECTION_NAME)

        # Restore Neo4j
        for node in snapshot["neo4j"]["nodes"]:
            node_props = {k: v for k, v in node.items() if k not in ("id", "node_type")}
            self._neo4j_repo.create_node(NEO4J_USER_NODE_NAME, node_props)    

        for rel in snapshot["neo4j"]["relationships"]:
            from_tlfn = rel["from_node"]["phone"]
            to_tlfn = rel["to_node"]["phone"]
            props = {k: v for k, v in rel.items() if k not in ("id", "type", "from_node", "to_node")}
            self._neo4j_repo.create_relation(
                from_label=NEO4J_USER_NODE_NAME,
                from_props={"phone": from_tlfn},
                to_label=NEO4J_USER_NODE_NAME,
                to_props={"phone": to_tlfn},
                rel_type=NEO4J_CONTACT_REL_NAME,
                rel_props=props
            )

        for queue_name, msg_ids in snapshot["redis"]["queues"].items():
            for msg_id in msg_ids:
                self._redis_repo.push_list(queue_name, msg_id)

        for key, value in snapshot["redis"]["hash"].items():
            self._redis_repo.insert_hash(REDIS_MESSAGES_HASH_NAME, key, value)

        for msg in snapshot["mongo"]["messages"]:
            msg_copy = {k: v for k, v in msg.items() if k != "_id"}
            self._mongo_repo.insert_one(MONGO_MSGS_COLLECTION_NAME, msg_copy)

class Application:
    _neo4j_repo: _Neo4jRepository
    _redis_repo: _RedisRepository
    _mongo_repo: _MongoRepository
    contactService: _ContactService
    messageService: _MessageService
    snapshotService: _SnapshotService

    def __init__(
        self,
        neo4j_repo: _Neo4jRepository,
        redis_repo: _RedisRepository,
        mongo_repo: _MongoRepository
    ):
        self._neo4j_repo = neo4j_repo
        self._redis_repo = redis_repo
        self._mongo_repo = mongo_repo
        self.contactService = _ContactService(neo4j_repo)
        self.messageService = _MessageService(redis_repo, neo4j_repo, mongo_repo)
        self.snapshotService = _SnapshotService(mongo_repo, neo4j_repo, redis_repo)

    def clear_databases(self):
        self._neo4j_repo.clear_database()
        self._redis_repo.clear_database()
        self._mongo_repo.clear_collection(MONGO_MSGS_COLLECTION_NAME)

    def close(self):
        self._neo4j_repo.close()
        self._redis_repo.close()
        self._mongo_repo.close()
        