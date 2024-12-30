import logging
import asyncmy
from enum import Enum
from .env import MysqlConfig
from dataclasses import dataclass
from contextlib import asynccontextmanager
from typing import Optional, Union, Dict, Tuple, List

logger = logging.getLogger(__name__)


# 枚举类：查询结果获取模式（如何获取查询结果）
class FetchMode(Enum):
    FETCHONE = "fetchone"  # 获取单个结果
    FETCHALL = "fetchall"  # 获取所有结果


# 枚举类：插入SQL模式
class InsertModeSql(Enum):
    INSERT_DEFAULT = 'INSERT INTO `{table_name}` ({columns_field}) VALUES ({value_field})'
    INSERT_IGNORE = 'INSERT IGNORE INTO `{table_name}` ({columns_field}) VALUES ({value_field})'
    INSERT_REPLACE = 'REPLACE INTO `{table_name}` ({columns_field}) VALUES ({value_field})'
    INSERT_UPDATE = 'INSERT INTO `{table_name}` ({columns_field}) VALUES ({value_field}) ON DUPLICATE KEY UPDATE '


# 数据类：表示MySQL操作结果
@dataclass
class MysqlResult:
    affect_count: int = 0  # 受影响的行数
    datas: Optional[Union[List[Dict], Dict, Tuple]] = None  # 查询结果
    error: Optional[str] = None  # 错误信息


# MysqlDB类，用于管理数据库连接和执行SQL操作
class MysqlDB(MysqlConfig):
    _instance: Optional['MysqlDB'] = None  # 单例实例

    def __new__(cls, *args, **kwargs):
        # 确保只创建一个MysqlDB实例（单例模式）
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not hasattr(self, '_initialized'):
            self._pool = None
            self._initialized = True

    def to_dict(self):
        """
        将实例属性转换为字典。
        排除私有属性和可调用的方法。
        """
        logger.debug("将MysqlDB实例属性转换为字典")
        attr = {key: getattr(self, key) for key in dir(self) if not key.startswith('_') and not callable(getattr(self, key))}
        attr['cursor_cls'] = self.cursor_cls  # 添加cursor类
        return attr

    async def _create_pool(self, **kwargs):
        """如果连接池不存在，则创建连接池。"""
        if self._pool is None:
            logger.info("正在创建MySQL连接池")
            self._pool = await asyncmy.create_pool(
                **self.to_dict(),
                **kwargs
            )

    @asynccontextmanager
    async def get_connection(self):
        """从连接池中获取连接。"""
        if self._pool is None:
            await self._create_pool()
        async with self._pool.acquire() as conn:
            yield conn

    async def _execute_sql(
            self,
            sql: str,
            datas: Optional[Union[Tuple, List, Dict]] = None,
            fetch_mode: FetchMode = None,
            is_many=False
    ) -> MysqlResult:
        """
        执行通用SQL命令（单个查询或批量操作）。
        :param sql: SQL查询语句
        :param datas: 查询参数
        :param fetch_mode: 查询返回模式(FetchMode.FETCHONE, FetchMode.FETCHALL)
        :param is_many: 是否为executemany操作
        :return: 包含查询结果的MysqlResult对象
        """
        result = None
        try:
            logger.debug(f"正在执行SQL: {sql}")
            async with self.get_connection() as conn:
                async with conn.cursor() as cursor:
                    if is_many:
                        affect_count = await cursor.executemany(sql, datas)
                    else:
                        affect_count = await cursor.execute(sql, datas)

                    if fetch_mode is not None:
                        result = await getattr(cursor, fetch_mode.value)()

                    return MysqlResult(affect_count=affect_count, datas=result, error=None)
        except Exception as e:
            logger.error(f"执行SQL时出错: {e}")
            return MysqlResult(affect_count=0, datas=None, error=str(e))

    async def query(self, sql: str, args: Optional[Union[Tuple, List, Dict]] = None, fetch_mode: FetchMode = None) -> MysqlResult:
        """执行查询操作（SELECT）。"""
        return await self._execute_sql(sql, args, fetch_mode)

    async def insert(self, sql: str, args: Optional[Union[Tuple, Dict]] = None) -> MysqlResult:
        """执行插入操作（INSERT）。"""
        return await self._execute_sql(sql, args)

    async def insert_many(self, sql: str, args: Optional[Union[Tuple, List, Dict]] = None) -> MysqlResult:
        """执行批量插入操作"""
        return await self._execute_sql(sql, args, is_many=True)

    async def insert_smart(self, table_name: str, datas: Union[Dict, List[Dict]]) -> MysqlResult:
        """
        根据数据自动生成插入SQL语句，支持单条和批量插入。
            sql格式: insert into <table_name> (field, ..., field) values (%s, ..., %s)
            datas: List[Dict] -> new_datas: List[Tuple]
            避免了 RE_INSERT_VALUES.match(query) 卡死, 作者迟迟不修复
            # 修改成这样, 应该就可以避免卡死了, 需要大量的 sql 进行测试才行
            RE_INSERT_VALUES = re.compile(
                r"\s*((?:INSERT|REPLACE)\b.+\bVALUES?\s*)"
                + r"(\(\s*(?:%\([^\)]+\)s|\%s)\s*(?:,\s*(?:%\([^\)]+\)s|\%s)\s*)*\))"
                + r"(\s*(?:ON DUPLICATE.*)?);?\s*\Z",
                re.IGNORECASE | re.DOTALL,
            )
        :param table_name: 表名
        :param datas: 插入的数据（单条或批量）
        :return: MysqlResult对象，包含插入操作的结果
        """
        logger.info(f"为表 {table_name} 生成插入SQL，数据条数: {len(datas)}")
        sql, new_datas = self.make_insert_sql(table_name, datas)
        is_many = False if isinstance(new_datas, Dict) else True
        return await self._execute_sql(sql, new_datas, is_many=is_many)

    async def update(self, sql: str, args: Optional[Union[Tuple, List, Dict]] = None) -> MysqlResult:
        """执行更新操作（UPDATE）。"""
        return await self._execute_sql(sql, args)

    async def delete(self, sql: str, args: Optional[Union[Tuple, List, Dict]] = None) -> MysqlResult:
        """执行删除操作（DELETE）。"""
        return await self._execute_sql(sql, args)

    async def close(self):
        """关闭连接池。"""
        logger.info("正在关闭MySQL连接池")
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()

    @staticmethod
    def make_insert_sql(
            table_name: str,
            datas: Union[Dict[str, Union[str, int, float, bool, None]], List[Dict[str, Union[str, int, float, bool, None]]]],
            update_columns: Optional[Union[List[str], Tuple[str, ...]]] = (),
            insert_mode: InsertModeSql = InsertModeSql.INSERT_DEFAULT,
    ) -> tuple[str, List[tuple]]:
        """
        生成MySQL插入或更新SQL语句，支持单条插入和批量插入。
        :param table_name: 表名
        :param datas: 数据（字典或字典列表）
        :param update_columns: 需要更新的列（仅在指定时有效）
        :param insert_mode
            默认: insert into
                 replace into
                 insert ignore
                 insert into ... duplicate key update
        :return: 生成的SQL查询和插入数据
        """
        logger.debug(f"正在为表 `{table_name}` 生成插入SQL，插入模式: {insert_mode}")
        insert_sql_mode = insert_mode.name
        insert_sql_template = insert_mode.value

        if isinstance(datas, dict):
            datas = [datas]  # 确保datas是一个列表

        # 基本SQL模板（列和占位符）
        columns = list(datas[0].keys())
        columns_field = ', '.join(map(lambda x: f"`{x}`", columns))
        value_field = ', '.join(['%s'] * len(columns))

        sql = insert_sql_template.format(
            table_name=table_name,
            columns_field=columns_field,
            value_field=value_field
        )

        # 处理“INSERT ... ON DUPLICATE KEY UPDATE”逻辑
        if insert_sql_mode == InsertModeSql.INSERT_UPDATE.name:
            if not update_columns:
                update_columns = columns  # 默认使用所有列
            update_columns_field = ", ".join([f"`{key}`=VALUES(`{key}`)" for key in update_columns])
            sql += update_columns_field

        new_dats = [tuple(record.values()) for record in datas]
        logger.debug(f"生成的SQL: {sql} 数据: {new_dats}")
        return sql, new_dats
