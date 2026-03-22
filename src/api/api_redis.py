import redis
from typing import Any


class _RedisClient(redis.Redis):
    def __init__(self, host="localhost", port=6379, db=0):
        super().__init__(host=host, port=port, db=db)
        if not self.ping():
            raise ConnectionError("Could not connect to Redis")
        
    def ping(self) -> bool:
        try:
            return super().ping()
        except Exception:
            return False

    # i know this is redundant, but for stating my client class responsibilities
    def close(self):
        super().close()


class _RedisRepository:
    _client: _RedisClient

    def __init__(self, client: _RedisClient):
        self._client = client

    def close(self):
        self._client.close()

    def clear_database(self) -> None:
        self._client.flushdb()

    def keys(self, pattern: str) -> list[str]:
        result = self._client.keys(pattern)
        return [key.decode() for key in result]


    # CRUD for HASHES

    def insert_hash(self, name: str, key: str, value: Any) -> None:
        self._client.hset(name, key, value)

    def find_hash(self, name: str, key: str) -> Any | None:
        return self._client.hget(name, key)

    def find_hash_all(self, name: str) -> dict[bytes, bytes]:
        return self._client.hgetall(name)

    def update_hash(self, name: str, key: str, value: Any) -> bool:
        if not self._client.hexists(name, key):
            return False
        self._client.hset(name, key, value)
        return True

    def delete_hash(self, name: str, key: str) -> bool:
        return self._client.hdel(name, key) > 0

    # CRUD for LISTS
    
    def push_list(self, name: str, value: Any):
        self._client.rpush(name, value)

    def pop_list(self, name: str, timeout: int = 0) -> str | None:
        result = self._client.blpop(name, timeout=timeout)
        return result[1].decode() if result else None

    def read_list(self, name: str) -> list[Any]:
        return self._client.lrange(name, 0, -1)

    def delete_from_list(self, name: str, value: Any) -> int:
        return self._client.lrem(name, 0, value)


def create_redis_repository(
    host="localhost",
    port=6379,
    db=0
) -> "_RedisRepository":
    client = _RedisClient(host=host, port=port, db=db)
    return _RedisRepository(client)
