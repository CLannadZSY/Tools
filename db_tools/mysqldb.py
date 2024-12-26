import logging
import asyncmy
from enum import Enum
from env import MysqlConfig
from dataclasses import dataclass
from contextlib import asynccontextmanager
from typing import Optional, Union, Dict, Tuple, List

logger = logging.getLogger(__name__)


class FetchMode(Enum):
    FETCHONE = "fetchone"
    FETCHALL = "fetchall"


class InsertModeSql(Enum):
    INSERT_DEFAULT = 'INSERT INTO `{table_name}` ({columns_field}) VALUES ({value_field})'
    INSERT_IGNORE = 'INSERT IGNORE INTO `{table_name}` ({columns_field}) VALUES ({value_field})'
    INSERT_REPLACE = 'REPLACE INTO `{table_name}` ({columns_field}) VALUES ({value_field})'
    INSERT_UPDATE = 'INSERT INTO `{table_name}` ({columns_field}) VALUES ({value_field}) ON DUPLICATE KEY UPDATE '


@dataclass
class MysqlResult:
    affect_count: int = 0
    datas: Optional[Union[List[Dict], Dict, Tuple]] = None
    error: Optional[str] = None


class MysqlDB(MysqlConfig):
    _instance: Optional['MysqlDB'] = None

    def __new__(cls, *args, **kwargs):
        # 使用单例模式，确保只创建一个连接池实例
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
        返回类的属性字典，包含所有实例属性，包括从父类继承的。
        """
        # 获取所有属性（包括父类属性），排除特殊属性（例如 `_instance`）
        attr = {key: getattr(self, key) for key in dir(self) if not key.startswith('_') and not callable(getattr(self, key))}
        attr['cursor_cls'] = self.cursor_cls
        return attr

    async def _create_pool(self, **kwargs):

        if self._pool is None:
            self._pool = await asyncmy.create_pool(
                **self.to_dict(),
                **kwargs
            )

    @asynccontextmanager
    async def get_connection(self):
        if self._pool is None:
            await self._create_pool()
        async with self._pool.acquire() as conn:
            yield conn

    async def _execute_sql(
            self,
            sql: str,
            args: Optional[Union[Tuple, List, Dict]] = None,
            fetch_mode: FetchMode = None,
            is_many=False
    ) -> MysqlResult:
        """
        通用的数据库执行方法，用于 execute 和 executemany。
        :param sql: SQL 查询语句
        :param args: 查询参数
        :param fetch_mode: 查询返回的模式 (FetchMode.FETCHONE, FetchMode.FETCHALL)
        :param is_many: 是否是 executemany 操作
        :return: 查询结果或执行结果
        """
        result = None

        try:
            async with self.get_connection() as conn:
                async with conn.cursor() as cursor:
                    if is_many:
                        affect_count = await cursor.executemany(sql, args)
                    else:
                        affect_count = await cursor.execute(sql, args)

                    if fetch_mode is not None:
                        result = await getattr(cursor, fetch_mode.value)()

                    return MysqlResult(affect_count=affect_count, datas=result, error=None)
        except Exception as e:
            return MysqlResult(affect_count=0, datas=None, error=str(e))

    async def query(self, sql: str, args: Optional[Union[Tuple, List, Dict]] = None, fetch_mode: FetchMode = None) -> MysqlResult:
        return await self._execute_sql(sql, args, fetch_mode)

    async def insert(self, sql: str, args: Optional[Union[Tuple, Dict]] = None) -> MysqlResult:
        return await self._execute_sql(sql, args)

    async def insert_many(self, sql: str, args: Optional[Union[Tuple, List, Dict]] = None) -> MysqlResult:
        return await self._execute_sql(sql, args, is_many=True)

    async def insert_smart(self, table_name: str, datas: Union[Dict, List[Dict]]) -> MysqlResult:
        """
        根据数据, 自动生成 sql
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

        :param table_name:
        :param datas:
        :return:
        """
        sql, new_datas = self.make_insert_sql(table_name, datas)
        is_many = False if isinstance(new_datas, Dict) else True
        return await self._execute_sql(sql, new_datas, is_many=is_many)

    async def update(self, sql: str, args: Optional[Union[Tuple, List, Dict]] = None) -> MysqlResult:
        return await self._execute_sql(sql, args)

    async def delete(self, sql: str, args: Optional[Union[Tuple, List, Dict]] = None) -> MysqlResult:
        return await self._execute_sql(sql, args)

    async def close(self):
        print('close')
        # 关闭连接池
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
        生成 MySQL 插入或更新 SQL 语句，支持单条插入和批量插入。
        :param table_name: 表名
        :param datas: 数据，单条为字典，批量为字典列表
        :param update_columns: 需要更新的列（当指定时，auto_update无效）
        :param insert_mode: 支持：replace into, insert ignore, duplicate key update
        :return: 生成的 SQL 语句
        """
        insert_sql_mode = insert_mode.name
        insert_sql_template = insert_mode.value

        if isinstance(datas, dict):
            datas = [datas]

        # 基础SQL模板
        columns = list(datas[0].keys())
        columns_field = ', '.join(map(lambda x: f"`{x}`", columns))
        value_field = ', '.join(['%s'] * len(columns))

        sql = insert_sql_template.format(
            table_name=table_name,
            columns_field=columns_field,
            value_field=value_field
        )

        # 如果没有传入指定的更新列, 则使用传入的数据, 默认更新所有列
        if insert_sql_mode == InsertModeSql.INSERT_UPDATE.name:
            if not update_columns:
                update_columns = columns
            update_columns_field = ", ".join([f"`{key}`=VALUES(`{key}`)" for key in update_columns])
            sql += update_columns_field

        new_dats = [tuple(record.values()) for record in datas]
        return sql, new_dats

    # # 同步执行 SQL 操作
    # def _run_sync(self, coroutine):
    #     loop = asyncio.get_event_loop()
    #     return loop.run_until_complete(coroutine)
    #
    # # 同步查询操作
    # def query_sync(self, sql: str, args: Optional[Union[Tuple, List, Dict]] = None, fetch_mode: FetchMode = FetchMode.FETCHALL) -> MysqlResult:
    #     return self._run_sync(self.query(sql, args, fetch_mode))
    #
    # # 同步插入操作
    # def insert_sync(self, sql: str, args: Optional[Union[Tuple, List, Dict]] = None) -> MysqlResult:
    #     return self._run_sync(self.insert(sql, args))
    #
    # # 同步批量插入操作
    # def insert_many_sync(self, sql: str, args: Optional[Union[Tuple, List, Dict]] = None) -> MysqlResult:
    #     return self._run_sync(self.insert_many(sql, args))
    #
    # # 同步更新操作
    # def update_sync(self, sql: str, args: Optional[Union[Tuple, List, Dict]] = None) -> MysqlResult:
    #     return self._run_sync(self.update(sql, args))
    #
    # # 同步删除操作
    # def delete_sync(self, sql: str, args: Optional[Union[Tuple, List, Dict]] = None) -> MysqlResult:
    #     return self._run_sync(self.delete(sql, args))
    #
    # # 同步关闭连接池
    # def close_sync(self):
    #     self._run_sync(self.close())
