"""
redis 连接池
"""
import redis


class RedisConnPool(object):
    """
    example:
        REDIS_CONFIG_DEV = {
            'redis_name': {
                'host': "127.0.0.1",
                'port': 6379,
                'password': '',
                'max_connections': 100,
                'db': 0
            },
        }
        r_0 = RedisConnPool(REDIS_CONFIG_DEV['redis_name']).connect()
    """

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

    def subscribe(self, chan_sub: str):
        """订阅模式"""
        pub = self._r.pubsub()
        pub.subscribe(chan_sub)
        return pub

    def pipeline(self, transaction=True, shard_hint=None):
        """管道"""
        pipe = self._r.pipeline(transaction=transaction, shard_hint=shard_hint)
        return pipe

    def __del__(self):
        # 超过最大连接数,使用这个关闭,但并不会阻止打开新的连接
        self._pool.disconnect()
        self._r.close()


# from pypattyrn.structural.flyweight import FlyweightMeta
# from redis import Redis
#
#
# class RedisConn(Redis, metaclass=FlyweightMeta):
#     """
#     redis连接享元模式无需担心重复创建连接
#     """
#     pass
#
#
# if __name__ == '__main__':
#     redis_host = '127.0.0.1'
#     redis_port = 6379
#     redis_db = 0
#     password = ''
#     redis1 = RedisConn(host=redis_host, port=redis_port, db=redis_db, password=password)
#     redis2 = RedisConn(host=redis_host, port=redis_port, db=redis_db, password=password)
#     print(id(redis1) == id(redis2))
#     redis3 = RedisConn(host=redis_host, port=redis_port, db=1, password=password)
#     print(id(redis1) == id(redis3))
