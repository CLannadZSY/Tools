import redis
from .db_conf import REDIS_HOST, REDIS_MAX_CONN, REDIS_PASSWORD, REDIS_PORT

class RedisConnPool(object):
    # 单例模式的应用（会在数据库连接池中用到单例模式）
    # 是否需要单利模式,它的作用到底是什么

    # _instance = None
    # def __new__(cls, *args, **kwargs):
    #     if not cls._instance:
    #         # 三种写法均可, 但是差别在哪里呢? 简写?
    #         cls._instance = super().__new__(cls)
    #         # cls._instance = super(RedisConn, cls).__new__(cls)
    #         # cls._instance = super(cls, RedisConn).__new__(cls)
    #     return cls._instance

    def __new__(cls, *args, **kwargs):
        # 两种写法均可, 差别是什么?
        if not hasattr(RedisConnPool, '_instance'):
        # if not hasattr(cls, '_instance'):
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self):
        self._host = REDIS_HOST
        self._port = REDIS_PORT
        self._password = REDIS_PASSWORD
        self._max_conn = REDIS_MAX_CONN

    def _conn_redis(self, db=0, is_decode=True):
        self._pool = redis.ConnectionPool(host=self._host,
                                          port=self._port,
                                          password=self._password,
                                          db=db,
                                          max_connections=self._max_conn,
                                          decode_responses=is_decode)
        self._r = redis.Redis(connection_pool=self._pool)
        return self._r

    def __del__(self):
        # 超过最大连接数,使用这个关闭,但并不会阻止打开新的连接
        self._pool.disconnect()
        self._r.close()
        print('close')
