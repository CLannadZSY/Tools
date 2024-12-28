from asyncmy.cursors import DictCursor


class MysqlConfig:
    host: str = "localhost"
    port: int = 3306
    user: str = ""
    password: str = ""
    db: str = ""
    charset: str = 'utf8mb4'
    echo: bool = False  # 如果为 True，则在执行 SQL 语句时打印 SQL 查询，帮助调试和日志记录。
    minsize: int = 1
    maxsize: int = 100
    pool_recycle: int = 3600
    max_allowed_packet = 16 * 1024 * 1024  # 客户端能够处理的最大数据包大小 16M
    autocommit: bool = True
    cursor_cls = DictCursor

    def __init__(self, config: dict = None):
        # 使用字典参数覆盖默认配置
        self._config = config or {}
        if config:
            for key, value in config.items():
                if hasattr(self, key):
                    setattr(self, key, value)

    def __setattr__(self, key, value):
        setattr(self.__class__, key, value)


class RedisConfig:
    host: str = "localhost"
    port: int = 6379
    username: str = ""
    password: str = ""
    db: int = 0
    decode_responses = True

    def __init__(self, config: dict = None):
        self._config = config or {}
        if config:
            for key, value in config.items():
                if hasattr(self, key):
                    setattr(self, key, value)

    def __setattr__(self, key, value):
        setattr(self.__class__, key, value)

    def generate_uri(self) -> str:
        """
        生成 Redis 的 URI，包含用户名、密码、主机、端口和数据库。
        返回的格式为: redis://[username:password@]host:port/db
        """
        auth_part = f"{self.username}:{self.password}"
        uri = f"redis://{auth_part}@{self.host}:{self.port}/{self.db}"
        if self.decode_responses:
            uri += f"?decode_responses={self.decode_responses}"
        return uri
