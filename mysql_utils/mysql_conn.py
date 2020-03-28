"""
mysql 连接池
参考文章:
    https://segmentfault.com/a/1190000017952033
"""
import pymysql
import threading
from DBUtils.PooledDB import PooledDB
from pymysql.cursors import DictCursor
from DBUtils.PersistentDB import PersistentDB


class MysqlPooledDB(object):
    """
    程序频繁的启动和关闭线程, 使用PooledDB
    """
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not hasattr(MysqlPooledDB, "_instance"):
            with MysqlPooledDB._instance_lock:
                if not hasattr(MysqlPooledDB, "_instance"):
                    MysqlPooledDB._instance = super().__new__(cls)
        return MysqlPooledDB._instance

    def __init__(self, config: dict):
        self._default_conf = {
            'creator': pymysql,
            'cursorclass': DictCursor,
            'charset': 'utf8mb4',
            'ping': 1
        }
        self._db_conf = {**self._default_conf, **config}
        self._pool = PooledDB(**self._db_conf)

    def connect(self):
        conn = self._pool.connection()
        cursor = conn.cursor()
        return conn, cursor


class MysqlPersistentDB(object):
    """
    保持常量线程数且频繁使用数据库的应用，使用PersistentDB
    """

    def __init__(self, config: dict):
        self._default_conf = {
            'creator': pymysql,
            'cursorclass': DictCursor,
            'charset': 'utf8mb4',
            'ping': 1
        }
        self._db_conf = {**self._default_conf, **config}
        self._pool = PersistentDB(**self._db_conf)

    def connect(self):
        conn = self._pool.connection()
        cursor = conn.cursor()
        return conn, cursor
