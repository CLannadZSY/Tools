import pymysql
import threading

from DBUtils.PersistentDB import PersistentDB
from DBUtils.PooledDB import PooledDB
from pymysql.cursors import DictCursor
from .db_conf import MYSQL_CONFIG

class MysqlPooledDB(object):
    _instance_lock = threading.Lock()

    def __new__(cls, *args, **kwargs):
        if not hasattr(MysqlPooledDB, "_instance"):
            with MysqlPooledDB._instance_lock:
                if not hasattr(MysqlPooledDB, "_instance"):
                    # MysqlPooledDB._instance = object.__new__(cls, *args, **kwargs)
                    MysqlPooledDB._instance = super().__new__(cls)
        return MysqlPooledDB._instance

    # TODO 还需要回去考虑怎么优化,不用的用户, 不同的端口, 不同的数据库
    def __init__(self, db='test'):
        self.pool = PooledDB(
            creator=pymysql,
            maxconnections=6,
            mincached=2,
            maxcached=5,
            # 链接池中最多共享的链接数量，0和None表示全部共享。
            # PS: 无用，因为pymysql和MySQLdb等模块的 threadsafety都为1，所有值无论设置为多少，_maxcached永远为0，所以永远是所有链接都共享。
            maxshared=3,
            blocking=True,  # 连接池中如果没有可用连接后，是否阻塞等待。True，等待；False，不等待然后报错
            maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
            setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
            # ping MySQL服务端，检查是否服务可用。
            # 0 = None = never, 1 = default = whenever it is requested, 2 = when a cursor is created, 4 = when a query is executed, 7 = always
            ping=0,
            cursorclass=DictCursor,
            host=MYSQL_CONFIG['host'],
            port=MYSQL_CONFIG['port'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            database=db,
            charset='utf8'
        )

    def connect(self):
        conn = self.pool.connection()
        cursor = conn.cursor()
        return conn, cursor


class MysqlPersistentDB(object):
    def __init__(self, db='test'):
        self.pool = PersistentDB(
            creator=pymysql,
            maxusage=None,  # 一个链接最多被重复使用的次数，None表示无限制
            setsession=[],  # 开始会话前执行的命令列表。如：["set datestyle to ...", "set time zone ..."]
            # ping MySQL服务端，检查是否服务可用。# 如：0 = None = never, 1 = default = whenever it is requested, 2 = when a cursor is created, 4 = when a query is executed, 7 = always
            ping=0,
            # 如果为False时， conn.close() 实际上被忽略，供下次使用，再线程关闭时，才会自动关闭链接。
            # 如果为True时， conn.close()则关闭链接，那么再次调用pool.connection时就会报错，因为已经真的关闭了连接（pool.steady_connection()可以获取一个新的链接）
            closeable=False,
            threadlocal=None,  # 本线程独享值得对象，用于保存链接对象，如果链接对象被重置
            cursorclass=DictCursor,
            host=MYSQL_CONFIG['host'],
            port=MYSQL_CONFIG['port'],
            user=MYSQL_CONFIG['user'],
            password=MYSQL_CONFIG['password'],
            database=db,
            charset='utf8'
        )

    def connect(self):
        conn = self.pool.connection()
        cursor = conn.cursor()
        return conn, cursor
