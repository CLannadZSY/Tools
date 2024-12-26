from typing import Optional
from env import RedisConfig
import redis.asyncio as redis
from contextlib import asynccontextmanager


class RedisDB(RedisConfig):
    _instance: Optional['RedisDB'] = None
    _client: Optional[redis.Redis] = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not hasattr(self, '_initialized'):
            self._initialized = True
            self._pool = None
            self._client = None
            self._uri = ""
            self.get_client()

    def get_client(self):
        if self._client is None:
            self._create_client()

    def _create_client(self):
        if self._client is None:
            self._uri = self.generate_uri()
            self._pool = redis.ConnectionPool.from_url(self._uri)
            self._client = redis.Redis.from_pool(self._pool)

    def connect(self):
        return self._client

    @asynccontextmanager
    async def get_pipeline(self):
        """
        example:
            redis_conn = {
                'host': 'localhost',
                'port': 6379,
                'db': 0
            }
            redis_db = RedisDB(redis_conn)
            async with redis_db.get_pipeline() as redis_pipe:
                for i in range(10000):
                    ok1 = await redis_pipe.set(f"key{i}", f"value{i}")
                results = await redis_pipe.execute()
                print(results)
        :return:
        """
        async with self._client.pipeline(transaction=True) as pipe:
            yield pipe

    async def close_client(self):
        # 关闭连接池
        if self._pool is not None:
            await self._pool.disconnect()
        # 关闭客户端
        if self._client is not None:
            await self._client.aclose()
