import asyncio
import logging
from contextlib import asynccontextmanager
from enum import Enum
from typing import Optional, Union, Dict, Tuple, List, Any, TypeVar, Set, AsyncGenerator

import asyncmy
from asyncmy.cursors import Cursor
from asyncmy import Connection, Pool
from asyncmy.errors import MySQLError
from sqlalchemy.orm import declarative_base

from .env import MysqlConfig

logger = logging.getLogger(__name__)
BaseT = declarative_base()
T = TypeVar('T', bound=BaseT)


class FetchMode(Enum):
    FETCHONE = "fetchone"
    FETCHALL = "fetchall"
    FETCHMANY = "fetchmany"


class InsertModeSql(Enum):
    INSERT_DEFAULT = 'INSERT INTO `{table_name}` ({columns_field}) VALUES ({value_field})'
    INSERT_IGNORE = 'INSERT IGNORE INTO `{table_name}` ({columns_field}) VALUES ({value_field})'
    INSERT_REPLACE = 'REPLACE INTO `{table_name}` ({columns_field}) VALUES ({value_field})'
    INSERT_UPDATE = 'INSERT INTO `{table_name}` ({columns_field}) VALUES ({value_field}) AS new_values ON DUPLICATE KEY UPDATE '


class MysqlDB(MysqlConfig):
    _instance: Optional['MysqlDB'] = None
    _lock: asyncio.Lock = asyncio.Lock()
    _pool: Optional[Pool] = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if not hasattr(self, '_initialized'):
            self._initialized = True

    async def ensure_pool(self) -> None:
        """确保连接池已初始化"""
        if self._pool is None:
            await self._create_pool()

    async def _create_pool(self) -> None:
        async with self._lock:
            if self._pool is None:
                logger.info("Creating MySQL connection pool")
                try:
                    self._pool = await asyncmy.create_pool(
                        **self._get_connection_config(),
                    )
                except MySQLError as e:
                    logger.error(f"Failed to create connection pool: {e}")
                    raise e

    def _get_connection_config(self) -> Dict:
        """
        将实例属性转换为字典。
        排除私有属性和可调用的方法。
        """
        logger.debug("将MysqlDB实例属性转换为字典")
        attr = {key: getattr(self, key) for key in dir(self) if not key.startswith('_') and not callable(getattr(self, key))}
        attr['cursor_cls'] = self.cursor_cls  # 添加cursor类
        return attr

    @asynccontextmanager
    async def transactional(self) -> AsyncGenerator[Tuple[Connection, 'Cursor'], None]:
        """事务管理上下文管理器"""
        await self.ensure_pool()
        async with self._pool.acquire() as conn:
            async with conn.cursor() as cursor:
                try:
                    yield conn, cursor
                    await conn.commit()
                except Exception as e:
                    await conn.rollback()
                    logger.error(f"Transaction rolled back: {e}")
                    raise e

    async def _execute(
            self,
            sql: str,
            params: Optional[Union[Tuple, Dict, List[Tuple]]] = None,
            is_many: bool = False,
    ) -> int:
        """
        执行SQL语句
        :param sql: SQL语句
        :param params: 参数
        :param is_many: 是否批量操作
        :return: 影响的行数
        """
        async with self.transactional() as (conn, cursor):
            try:
                if is_many:
                    await cursor.executemany(sql, params or [])
                else:
                    await cursor.execute(sql, params or ())
                return cursor.rowcount
            except MySQLError as e:
                logger.error(f"SQL execution failed: {sql[:200]} - {e}")
                raise

    @staticmethod
    def make_insert_sql(
            table_model: Union[T, Any],
            datas: Union[Dict[str, Union[str, int, float, bool, None]], List[Dict[str, Union[str, int, float, bool, None]]]],
            update_columns: Optional[Union[List[str], Tuple[str, ...]]] = (),
            insert_mode: InsertModeSql = InsertModeSql.INSERT_DEFAULT,
    ) -> tuple[str, List[tuple]]:
        """
        生成MySQL插入或更新SQL语句，支持单条插入和批量插入。
        :param table_model: 表模型
        :param datas: 数据（字典或字典列表）
        :param update_columns: 需要更新的列（仅在指定时有效）
        :param insert_mode
            默认: insert into
                 replace into
                 insert ignore
                 insert into ... duplicate key update
        :return: 生成的SQL查询和插入数据
        """

        if not datas:
            raise ValueError("No data provided for insertion")

        logger.debug(f"正在为表 `{table_model}` 生成插入SQL，插入模式: {insert_mode}")
        insert_sql_mode = insert_mode.name
        insert_sql_template = insert_mode.value

        if isinstance(datas, dict):
            datas = [datas]  # 确保datas是一个列表

        # 去除主键
        valid_columns = set(x.name for x in table_model.__table__.columns if not x.primary_key)
        # 获取 datas 中实际存在的列
        datas_columns = valid_columns.intersection(datas[0].keys())

        new_datas = [tuple(item[col] for col in datas_columns) for item in datas]
        # 基本SQL模板（列和占位符
        columns_field = ', '.join(map(lambda x: f"`{x}`", datas_columns))
        value_field = ', '.join(['%s'] * len(datas_columns))

        sql = insert_sql_template.format(
            table_name=table_model.__tablename__,
            columns_field=columns_field,
            value_field=value_field
        )

        # 处理“INSERT ... ON DUPLICATE KEY UPDATE”逻辑
        if insert_sql_mode == InsertModeSql.INSERT_UPDATE.name:
            if not update_columns:
                update_columns = datas_columns  # 默认使用所有列
            update_columns_field = ", ".join([f"`{key}`=new_values.`{key}`" for key in update_columns])
            sql += update_columns_field

        logger.debug(f"生成的SQL: {sql} 数据: {new_datas}")
        return sql, new_datas

    async def query(
            self,
            sql: str,
            args=None,
            table_model: Optional[Union[T, Any]] = None,
            fetch_mode: FetchMode = FetchMode.FETCHALL,
            fetch_size: int = 100
    ) -> Optional[Union[T, List[T], List[Dict], Dict]]:
        """执行查询操作（SELECT），返回 MysqlQueryResult to enable chaining."""
        async with self.transactional() as (conn, cursor):
            await cursor.execute(sql, args)
            try:

                if fetch_mode == FetchMode.FETCHONE:
                    result = await cursor.fetchone() or {}
                elif fetch_mode == FetchMode.FETCHALL:
                    result = await cursor.fetchall() or []
                elif fetch_mode == FetchMode.FETCHONE:
                    result = await cursor.fetchmany(fetch_size) or []
                else:
                    logger.error(f"ERROR fetch_mode: {fetch_mode}")
                    result = None

                if result and table_model is not None:
                    if isinstance(result, List):
                        result = [table_model(**x) for x in result]
                    elif isinstance(result, Dict):
                        result = table_model(**result)
                return result

            except Exception as e:
                logger.error(f"_execute: {sql}, Error: {e}")
                raise e

        return result

    async def insert(self, sql: str, args: Optional[Union[Tuple, Dict, List[Dict], List[Tuple]]] = None) -> int:
        """执行插入操作（INSERT）。"""
        is_many = True if isinstance(args, List) else False
        return await self._execute(sql, args, is_many=is_many)

    async def insert_smart(
            self,
            table_model: Union[T, Any],
            datas: Union[Dict, List[Dict]],
            update_columns: Optional[Union[Set[str], List[str], Tuple[str]]] = (),
            insert_mode: InsertModeSql = InsertModeSql.INSERT_DEFAULT,
    ) -> int:
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
        :param table_model: 表模型
        :param datas: 插入的数据（单条或批量）
        :param update_columns: 需要更新的列（仅在指定时有效）
        :param insert_mode
            默认: insert into
                 replace into
                 insert ignore
                 insert into ... duplicate key update
        :return: MysqlResult对象，包含插入操作的结果
        """
        # 如果传入model 还能校验字段是否匹配, 可以过滤掉不属于当前表的
        logger.info(f"为表 {table_model} 生成插入SQL，数据条数: {len(datas)}")
        sql, new_datas = self.make_insert_sql(table_model, datas, update_columns, insert_mode)
        is_many = False if isinstance(new_datas, Dict) else True
        return await self._execute(sql, new_datas, is_many=is_many)

    async def update(self, sql: str, args: Optional[Union[Tuple, List, Dict]] = None) -> int:
        """执行更新操作（UPDATE）。"""
        is_many = True if isinstance(args, List) else False
        return await self._execute(sql, args, is_many)

    async def delete(self, sql: str, args: Optional[Union[Tuple, List, Dict]] = None) -> int:
        """执行删除操作（DELETE）。"""
        is_many = True if isinstance(args, List) else False
        return await self._execute(sql, args, is_many)

    async def close_pool(self):
        """
        关闭连接池
        """
        if self._pool is not None:
            self._pool.close()
            await self._pool.wait_closed()
