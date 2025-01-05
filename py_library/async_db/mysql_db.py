import asyncio
import logging
from enum import Enum
import asyncmy
from asyncmy import Connection
from asyncmy.cursors import Cursor
from sqlalchemy.orm import declarative_base
from .env import MysqlConfig
from typing import Optional, Union, Dict, Tuple, List, Type, Any, TypeVar, Set

logger = logging.getLogger(__name__)

BaseT = declarative_base()
T = TypeVar('T', bound=BaseT)


# 枚举类：查询结果获取模式
class FetchMode(Enum):
    FETCHONE = "fetchone"  # 获取单个结果
    FETCHALL = "fetchall"  # 获取所有结果
    FETCHMANY = "fetchmany"  # 获取所有结果


# 枚举类：插入SQL模式
class InsertModeSql(Enum):
    INSERT_DEFAULT = 'INSERT INTO `{table_name}` ({columns_field}) VALUES ({value_field})'
    INSERT_IGNORE = 'INSERT IGNORE INTO `{table_name}` ({columns_field}) VALUES ({value_field})'
    INSERT_REPLACE = 'REPLACE INTO `{table_name}` ({columns_field}) VALUES ({value_field})'
    INSERT_UPDATE = 'INSERT INTO `{table_name}` ({columns_field}) VALUES ({value_field}) AS new_values ON DUPLICATE KEY UPDATE '


class MysqlDB(MysqlConfig):
    _instance: Optional['MysqlDB'] = None  # 单例实例

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cursor = None
        if not hasattr(self, '_initialized'):
            self._pool = None
            self._initialized = True
            asyncio.create_task(self._create_pool())

    async def _pool_ready(self):
        """Ensure that the pool is ready before continuing."""
        while self._pool is None:
            await asyncio.sleep(0.05)

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
        logger.info("正在创建MySQL连接池")
        if self._pool is None:
            self._pool = await asyncmy.create_pool(
                **self.to_dict(),
                **kwargs
            )

    async def get_connection(self):
        """从连接池中获取连接。"""
        if self._pool is None:
            await self._create_pool()
            await self._pool_ready()

        pool_conn = await self._pool.acquire()
        if pool_conn is None:
            raise RuntimeError("Failed to acquire a valid connection from the pool.")
        await pool_conn.ping()
        cursor = pool_conn.cursor()
        return pool_conn, cursor

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

    async def query(self, sql: str, args=None, table_model: Optional[Union[T, Any]] = None, fetch_mode: FetchMode = FetchMode.FETCHALL, fetch_size: int = 100) -> Optional[Union[T, List[T], List[Dict], Dict]]:
        """执行查询操作（SELECT），返回 MysqlQueryResult to enable chaining."""
        conn, cursor, _ = await self._execute_sql(sql, args)

        match fetch_mode:
            case FetchMode.FETCHONE:
                result = await cursor.fetchone()

            case FetchMode.FETCHALL:
                result = await cursor.fetchall()

            case FetchMode.FETCHMANY:
                result = await cursor.fetchmany(fetch_size)

            case _:
                logger.error(f"ERROR fetch_mode: {fetch_mode}")
                result = None

        await self.auto_commit(conn, cursor, True)

        if result and table_model is not None:
            if isinstance(result, List):
                result = [table_model(**x) for x in result]
            elif isinstance(result, Dict):
                result = table_model(**result)
        return result

    async def insert(self, sql: str, args: Optional[Union[Tuple, Dict, List[Dict], List[Tuple]]] = None, session_auto_commit: Optional[bool] = True) -> Tuple[
        Connection, Type['Cursor'], Optional[str]]:
        """执行插入操作（INSERT）。"""
        is_many = True if isinstance(args, List) else False
        conn, cursor, err = await self._execute_sql(sql, args, is_many=is_many)
        await self.auto_commit(conn, cursor, session_auto_commit)
        return conn, cursor, err

    async def insert_smart(
            self,
            table_model: Union[T, Any],
            datas: Union[Dict, List[Dict]],
            update_columns: Optional[Union[Set[str], List[str], Tuple[str]]] = (),
            insert_mode: InsertModeSql = InsertModeSql.INSERT_DEFAULT,
            session_auto_commit: Optional[bool] = True
    ):
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
        :param session_auto_commit 自动提交
        :return: MysqlResult对象，包含插入操作的结果
        """
        # 如果传入model 还能校验字段是否匹配, 可以过滤掉不属于当前表的
        logger.info(f"为表 {table_model} 生成插入SQL，数据条数: {len(datas)}")
        sql, new_datas = self.make_insert_sql(table_model, datas, update_columns, insert_mode)
        is_many = False if isinstance(new_datas, Dict) else True
        conn, cursor, err = await self._execute_sql(sql, new_datas, is_many=is_many)
        await self.auto_commit(conn, cursor, session_auto_commit)
        return conn, cursor, err

    async def update(self, sql: str, args: Optional[Union[Tuple, List, Dict]] = None, session_auto_commit: Optional[bool] = True):
        """执行更新操作（UPDATE）。"""
        return await self._execute_and_commit(sql, args, session_auto_commit)

    async def delete(self, sql: str, args: Optional[Union[Tuple, List, Dict]] = None, session_auto_commit: Optional[bool] = True):
        """执行删除操作（DELETE）。"""
        return await self._execute_and_commit(sql, args, session_auto_commit)

    async def auto_commit(self, conn: Connection, cursor: type['Cursor'], session_auto_commit: bool = False):
        # 会话自动提交为False, 覆盖默认配置, 不自动提交
        if not session_auto_commit:
            return

        # 会话自动提交为True, 或者默认配置为True, 自动提交
        if session_auto_commit or (not self.autocommit):
            try:
                await conn.commit()
            except Exception as e:
                print(e)
                await conn.rollback()
                logger.error(f"auto_commit error: {e}")
            finally:
                await self.close_conn_cursor(conn, cursor)

    async def _execute_sql(
            self,
            sql: str,
            datas=None,
            is_many=False
    ) -> Tuple[Connection, Type['Cursor'], Optional[str]]:
        """
        执行通用SQL命令（单个查询或批量操作）。
        :param sql: SQL查询语句
        :param datas: 查询参数
        :param is_many: 是否为executemany操作
        :return:
        """
        logger.debug(f"Execute Sql: {sql}")
        e = None
        conn, cursor = await self.get_connection()
        try:
            if is_many:
                cursor.affect_row_count = await cursor.executemany(sql, datas)
            else:
                cursor.affect_row_count = await cursor.execute(sql, datas)

            # 返回conn和cursor，让调用者决定是否提交
            return conn, cursor, e
        except Exception as e:
            logger.error(f"Execute Sql: {sql}, Error: {e}")
            # 发生异常时回滚
            await conn.rollback()
            e = e.__repr__()
            return conn, cursor, e

    async def _execute_and_commit(self, sql: str, args: Optional[Union[Tuple, List, Dict]] = None, session_auto_commit: Optional[bool] = True):
        """通用的执行 SQL 并提交操作（包括 INSERT, UPDATE, DELETE）。"""
        conn, cursor, err = await self._execute_sql(sql, args)
        await self.auto_commit(conn, cursor, session_auto_commit)
        return conn, cursor, err

    async def close_conn_cursor(self, conn: Connection, cursor: Type['Cursor']):
        """
        session_auto_commit = False, 必须手动调用这个方法, 否则会导致 too many connection
        """
        try:
            await cursor.close()
            if conn:
                try:
                    await conn.ensure_closed()
                    self._pool.release(conn)
                except Exception as e:
                    logger.error(f"close_conn_cursor: Failed to release connection: {e}")
        except Exception as e:
            logger.error(f"close_conn_cursor: Failed to close cursor: {e}")
