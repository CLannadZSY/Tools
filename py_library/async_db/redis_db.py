import logging
from typing import Optional
from .env import RedisConfig
import redis.asyncio as redis
from contextlib import asynccontextmanager

logger = logging.getLogger(__name__)


class RedisDB(RedisConfig):
    _instance: Optional['RedisDB'] = None
    _client: Optional[redis.Redis] = None

    def __new__(cls, *args, **kwargs):
        """确保 RedisDB 类使用单例模式"""
        if cls._instance is None:
            logger.debug("创建 RedisDB 单例实例")
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, *args, **kwargs):
        """初始化 RedisDB 类，确保只有一个连接池和客户端实例"""
        super().__init__(*args, **kwargs)
        if not hasattr(self, '_initialized'):
            self._initialized = True
            self._pool = None  # Redis 连接池
            self._client = None  # Redis 客户端
            self._uri = ""  # Redis URI
            logger.info("初始化 RedisDB 类")
            self.get_client()

    def get_client(self):
        """获取 Redis 客户端，如果没有客户端实例则创建"""
        if self._client is None:
            logger.info("Redis 客户端尚未创建，正在创建客户端...")
            self._create_client()

    def _create_client(self):
        """创建 Redis 客户端和连接池"""
        if self._client is None:
            logger.info("正在创建 Redis 连接池和客户端")
            self._uri = self.generate_uri()
            logger.debug(f"Redis 连接 URI: {self._uri}")
            self._pool = redis.ConnectionPool.from_url(self._uri)
            self._client = redis.Redis.from_pool(self._pool)
            logger.info("Redis 客户端和连接池已成功创建")

    def connect(self):
        """返回 Redis 客户端实例"""
        logger.debug("返回 Redis 客户端实例")
        return self._client

    @asynccontextmanager
    async def get_pipeline(self):
        """
        使用 Redis 管道进行批量操作
        example:
            redis_conn = {
                'host': 'localhost',
                'port': 6379,
                'db': 0
            }
            redis_db = RedisDB(redis_conn)
            async_db with redis_db.get_pipeline() as redis_pipe:
                for i in range(10000):
                    ok1 = await redis_pipe.set(f"key{i}", f"value{i}")
                results = await redis_pipe.execute()
                print(results)
        :return: Redis 管道
        """
        logger.debug("正在创建 Redis 管道")
        async with self._client.pipeline(transaction=True) as pipe:
            yield pipe
            logger.debug("Redis 管道操作完成，正在执行管道命令...")

    async def close_client(self):
        """关闭 Redis 客户端和连接池"""
        logger.info("正在关闭 Redis 客户端和连接池")
        # 关闭连接池
        if self._pool is not None:
            await self._pool.disconnect()
            logger.info("Redis 连接池已关闭")
        # 关闭客户端
        if self._client is not None:
            await self._client.aclose()
            logger.info("Redis 客户端已关闭")
