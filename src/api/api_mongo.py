import pymongo

class _MongoClient(pymongo.MongoClient):
    def __init__(self, uri: str = "mongodb://localhost:27017"):
        super().__init__(uri)
        if not self.ping():
            raise ConnectionError(f"Could not connect to MongoDB at {uri}")

    def ping(self) -> bool:
        try:
            return self.admin.command("ping")["ok"] == 1.0
        except Exception:
            return False

    # i know this is redundant, but for stating my client class responsibilities
    def close(self):
        super().close()


class _MongoRepository:
    _client: _MongoClient
    _db: pymongo.database.Database

    def __init__(self, client: _MongoClient, db_name: str = "messaging_app"):
        self._client = client
        self._db = client[db_name]

    def clear_collection(self, collection: str) -> None:
        self._db[collection].delete_many({})

    def close(self):
        self._client.close()


    # CRUD

    def insert_one(self, collection: str, document: dict) -> str:
        result = self._db[collection].insert_one(document)
        return str(result.inserted_id)

    def find_one(self, collection: str, filter: dict) -> dict:
        result = self._db[collection].find_one(filter)
        return result if result else {}

    def find_many(self, collection: str, filter: dict | None = None, sort: list[tuple[str, int]] | None = None) -> list[dict]:
        if filter is None:
            filter = {}
        if sort is None:
            return list(self._db[collection].find(filter))
        else:
            return list(self._db[collection].find(filter , sort=sort))
    
    def find_one_sorted(
        self,
        collection: str,
        filter: dict,
        sort: list[tuple[str, int]]
    ) -> dict:
        result = self._db[collection].find_one(
            filter,
            sort=sort
        )
        return result if result else {}

    def update_one(self, collection: str, filter: dict, update: dict) -> bool:
        result = self._db[collection].update_one(filter, {"$set": update})
        return result.matched_count > 0

    def delete_one(self, collection: str, filter: dict) -> bool:
        result = self._db[collection].delete_one(filter)
        return result.deleted_count > 0

    def delete_many(self, collection: str, filter: dict) -> int:
        return self._db[collection].delete_many(filter).deleted_count

def create_mongo_repository(uri: str = "mongodb://localhost:27017", db_name: str = "messaging_app") -> _MongoRepository:
    client = _MongoClient(uri)
    return _MongoRepository(client, db_name)