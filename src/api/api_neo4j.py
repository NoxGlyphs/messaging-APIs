from neo4j import GraphDatabase

class _Neo4jClient:
    def __init__(self, uri: str = "neo4j://localhost:7687", auth: tuple = ("neo4j", "neo4j")):
        self._driver = GraphDatabase.driver(uri, auth=auth)
        if not self.verify():
            raise ConnectionError(f"Could not connect to Neo4j at {uri}")

    def verify(self) -> bool:
        try:
            self._driver.verify_connectivity()
            return True
        except Exception:
            return False

    def run_query(self, query: str, db_name: str = "neo4j", **params: dict):
        return self._driver.execute_query(query, database_=db_name, **params)

    def close(self):
        self._driver.close()


class _Neo4jRepository:
    def __init__(self, client: _Neo4jClient, db_name: str = "neo4j"):
        self._client = client
        self._db_name = db_name

    def clear_database(self):
        query = "MATCH (n) DETACH DELETE n"
        self._client.run_query(query, self._db_name)

    def close(self):
        self._client.close()


    # NODES CRUD

    def create_node(self, label: str, properties: dict) -> dict:
        if not properties:
            raise ValueError("Properties required to create a node")
        props = ", ".join(f"{k}: ${k}" for k in properties)
        query = f"CREATE (n:{label} {{ {props} }}) RETURN n"
        result = self._client.run_query(query, self._db_name, **properties)

        records = result[0]
        node = records[0]["n"]
        data = dict(node._properties)
        data["id"] = node.element_id
        data["node_type"] = label
        return data

    def read_nodes(self, label: str, filters: dict | None = None) -> list[dict]:
        filters = filters or {}
        if filters:
            cond = " AND ".join(f"n.{k} = ${k}" for k in filters)
            query = f"MATCH (n:{label}) WHERE {cond} RETURN n"
        else:
            query = f"MATCH (n:{label}) RETURN n"
        records, _, _ = self._client.run_query(query, self._db_name, **filters)
        nodes = []
        for record in records:
            node = record["n"]
            data = dict(node._properties)
            data["id"] = node.element_id
            data["node_type"] = label
            nodes.append(data)
        return nodes
    
    def read_node_by_id(self, label: str, node_id: str) -> dict:
        query = f"MATCH (n:{label}) WHERE elementId(n) = $node_id RETURN n"
        records, _, _ = self._client.run_query(query, self._db_name, node_id=node_id)
        if not records:
            return {}
        node = records[0]["n"]
        data = dict(node._properties)
        data["id"] = node.element_id
        data["node_type"] = label
        return data

    def update_nodes(self, label: str, match_props: dict = {}, update_props: dict = {}) -> int:
        if not update_props:
            raise ValueError("Update properties required to update nodes")
        
        match_str = ""
        if match_props:
            match_str = "WHERE " + " AND ".join(f"n.{k} = ${k}" for k in match_props)

        update_str = ", ".join(f"n.{k} = ${k}_new" for k in update_props)
        params = {**match_props, **{f"{k}_new": v for k, v in update_props.items()}}
        query = f"MATCH (n:{label}) {match_str} SET {update_str}"

        _, summary, _ = self._client.run_query(query, self._db_name, **params)
        return summary.counters.properties_set

    def delete_nodes(self, label: str, filters: dict) -> int:
        if not filters:
            raise ValueError("Filters required to delete nodes")
        cond = " AND ".join(f"n.{k} = ${k}" for k in filters)
        query = f"MATCH (n:{label}) WHERE {cond} DETACH DELETE n"
        _, summary, _ = self._client.run_query(query, self._db_name, **filters)
        return summary.counters.nodes_deleted
    
    # constraints
    def create_node_unique_constraint(
        self,
        label: str,
        property_name: str,
        constraint_name: str | None = None
    ) -> None:

        constraint_name = constraint_name or f"{property_name}_unique"

        query = f"""
        CREATE CONSTRAINT {constraint_name} IF NOT EXISTS
        FOR (n:{label})
        REQUIRE n.{property_name} IS UNIQUE
        """

        self._client.run_query(query)


    # RELATIONSHIPS CRUD

    def create_relation(
        self,
        from_label: str, 
        from_props: dict, 
        to_label: str, 
        to_props: dict, 
        rel_type: str, 
        rel_props: dict | None = None
    ) -> list[dict]:
        
        from_cond = " AND ".join(f"a.{k} = ${'from_' + k}" for k in from_props)
        to_cond = " AND ".join(f"b.{k} = ${'to_' + k}" for k in to_props)

        params = {f"from_{k}": v for k, v in from_props.items()}
        params.update({f"to_{k}": v for k, v in to_props.items()})
        params['props'] = rel_props or {}

        query = f"""
        MATCH (a:{from_label}), (b:{to_label})
        WHERE {from_cond} AND {to_cond}
        CREATE (a)-[r:{rel_type} $props]->(b)
        RETURN r
        """
        records, _, _ = self._client.run_query(query, self._db_name, **params)

        relations = []
        for record in records:
            r = record["r"]
            relation_data = dict(r._properties)
            relation_data["id"] = r.element_id
            relation_data["type"] = rel_type
            relations.append(relation_data)

        return relations
    
    def create_relation_by_id(
        self,
        from_label: str,
        from_id: str,
        to_label: str,
        to_id: str,
        rel_type: str,
        rel_props: dict | None = None
    ) -> dict:

        query = f"""
        MATCH (a:{from_label}), (b:{to_label})
        WHERE elementId(a) = $from_id AND elementId(b) = $to_id
        CREATE (a)-[r:{rel_type} $props]->(b)
        RETURN r
        """

        params = {
            "from_id": from_id,
            "to_id": to_id,
            "props": rel_props or {}
        }

        records, _, _ = self._client.run_query(query, self._db_name, **params)
        if not records:
            return {}

        r = records[0]["r"]
        data = dict(r._properties)
        data["id"] = r.element_id
        data["type"] = rel_type
        return data


    def read_relations(
        self,
        from_props: dict | None = None,
        to_props: dict | None = None,
        relation_type: str | None = None,
        relation_props: dict | None = None
    ) -> list[dict]:

        matches = []
        params = {}

        if from_props:
            matches.append(" AND ".join(f"a.{k} = ${'from_' + k}" for k in from_props))
            params.update({f"from_{k}": v for k, v in from_props.items()})

        if to_props:
            matches.append(" AND ".join(f"b.{k} = ${'to_' + k}" for k in to_props))
            params.update({f"to_{k}": v for k, v in to_props.items()})

        if relation_props:
            matches.append(" AND ".join(f"r.{k} = ${'rel_' + k}" for k in relation_props))
            params.update({f"rel_{k}": v for k, v in relation_props.items()})

        match_str = ""
        if matches:
            match_str = "WHERE " + " AND ".join(matches)

        type_filter = f":{relation_type}" if relation_type else ""

        query = f"""
        MATCH (a)-[r{type_filter}]->(b)
        {match_str}
        RETURN a, r, b, type(r) AS rel_type
        """

        records, _, _ = self._client.run_query(query, self._db_name, **params)
        
        relations = []
        for record in records:
            r = record["r"]
            relation_data = dict(r._properties)
            relation_data["id"] = r.element_id
            relation_data["type"] = record["rel_type"]
            relation_data["from_node"] = dict(record["a"]._properties)
            relation_data["from_node"]["id"] = record["a"].element_id
            relation_data["to_node"] = dict(record["b"]._properties)
            relation_data["to_node"]["id"] = record["b"].element_id
            relations.append(relation_data)
        
        return relations


    def update_relations(self, rel_type: str, match_props: dict, update_props: dict) -> int:
        match_str = " AND ".join(f"r.{k} = ${k}" for k in match_props)
        update_str = ", ".join(f"r.{k} = ${k}_new" for k in update_props)
        params = {**match_props, **{f"{k}_new": v for k, v in update_props.items()}}
        query = f"MATCH ()-[r:{rel_type}]->() WHERE {match_str} SET {update_str}"
        _, summary, _ = self._client.run_query(query, self._db_name, **params)
        return summary.counters.properties_set

    def delete_relations(self, rel_type: str, filters: dict) -> int:
        if not filters:
            raise ValueError("Filters required to delete relations")
        cond = " AND ".join(f"r.{k} = ${k}" for k in filters)
        query = f"MATCH ()-[r:{rel_type}]->() WHERE {cond} DELETE r"
        _, summary, _ = self._client.run_query(query, self._db_name, **filters)
        return summary.counters.relationships_deleted
    

    # UTILS

    def get_neighbors(self, label: str, node_id: str) -> list[dict]:
        query = f"""
        MATCH (n:{label})-[r]-(m:{label})
        WHERE elementId(n) = $node_id
        RETURN m, r
        """

        records, _, _ = self._client.run_query(
            query,
            self._db_name,
            node_id=node_id
        )

        neighbors = []
        for record in records:
            node = record["m"]
            data = dict(node._properties)
            data["id"] = node.element_id
            neighbors.append(data)

        return neighbors


    def is_connected(self, label_A: str, label_B: str, rel_type: str, node_A_id: str, node_B_id: str) -> bool:
        """
        Check if two nodes are connected. 
        Independent of direction.
        """
        query = f"""
        MATCH (a:{label_A})-[r:{rel_type}]-(b:{label_B})
        WHERE elementId(a) = $node_A_id AND elementId(b) = $node_B_id
        RETURN r
        """
        records, _, _ = self._client.run_query(
            query,
            self._db_name,
            node_A_id=node_A_id,
            node_B_id=node_B_id
        )
        return len(records) > 0
    
    def _increment_property(self, property_name: str, node_A_label: str, node_B_label: str, node_A_id: str, node_B_id: str, rel_type: str) -> int:
        """
        !!! This method is intended for internal use.
        Increment the message count in the relationship between two connected users. 
        Independent of direction.
        """
        query = f"""
        MATCH (a:{node_A_label})-[r:{rel_type}]-(b:{node_B_label})
        WHERE elementId(a) = $node_A_id AND elementId(b) = $node_B_id
        SET r.{property_name} = r.{property_name} + 1
        RETURN r.{property_name} AS updated_count
        """
        records, _, _ = self._client.run_query(
            query,
            self._db_name,
            node_A_id=node_A_id,
            node_B_id=node_B_id
        )

        return records[0]["updated_count"] if records else -1
    

# Factory function
def create_neo4j_repository(
    uri: str = "neo4j://localhost:7687",
    auth: tuple = ("neo4j", "neo4j"),
    db_name: str = "neo4j"
) -> '_Neo4jRepository':
    client = _Neo4jClient(uri=uri, auth=auth)
    repo = _Neo4jRepository(client=client, db_name=db_name)
    return repo