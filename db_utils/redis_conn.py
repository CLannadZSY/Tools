"""
redis 连接池
"""
import redis


class RedisConnPool(object):

    def __new__(cls, *args, **kwargs):
        if not hasattr(RedisConnPool, '_instance'):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, config: dict):
        self._default_conf = {
            'decode_responses': True
        }
        self._redis_conf = {**self._default_conf, **config}
        self._pool = redis.ConnectionPool(**self._redis_conf)
        self._r = redis.Redis(connection_pool=self._pool)

    def connect(self):
        return self._r

    def __del__(self):
        # 超过最大连接数,使用这个关闭,但并不会阻止打开新的连接
        self._pool.disconnect()
        self._r.close()
