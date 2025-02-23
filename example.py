import asyncio
from models import Test1
from py_library.async_db.mysql_db import MysqlDB, FetchMode, InsertModeSql


async def query_tools():
    config = {
        "host": "127.0.0.1",
        "user": "root",
        "password": "root",
        "db": "test",
        "autocommit": True
    }

    mysql_db = MysqlDB(config)
    sql = f"SELECT id, a, b, c, d FROM {Test1.__tablename__} limit 100"

    result = await mysql_db.query(sql, table_model=Test1, fetch_mode=FetchMode.FETCHONE)
    print(result)
    result = await mysql_db.query(sql, table_model=Test1, fetch_mode=FetchMode.FETCHMANY, fetch_size=2)
    print(result)
    result = await mysql_db.query(sql, table_model=Test1, fetch_mode=FetchMode.FETCHALL)
    print(result)
    result = await mysql_db.query(sql, fetch_mode=FetchMode.FETCHALL)
    print(result)

    # insert
    sql = "insert into test.test_1 (a, b, c, d) values (%s, %s, %s, %s)"
    data = (11, 22, 33, 44,)
    for _ in range(1000):
        row = await mysql_db.insert(sql, data)
    print(row)

    sql = "insert into test.test_1 (a, b, c, d) values (%(a)s, %(b)s, %(c)s, %(d)s)"
    data = {'a': 111, 'b': 222, 'c': 333, 'd': 444}
    await mysql_db.insert(sql, data)

    # insert smart
    data = {'a': 1, 'b': 22, 'c': 33, 'd': 44}
    data = {'id': 1, 'a': 1, 'b': 2, 'c': 3, 'd': 4}
    data = {'id': 1, 'a': 1, 'd': 4}
    data = {'id': 1, 'a': 1, 'b': 2, 'c': 3, 'd': 4, 'e': 'e not exist col'}
    datas = [data]
    await mysql_db.insert_smart(Test1, datas)
    await mysql_db.insert_smart(Test1, datas, insert_mode=InsertModeSql.INSERT_IGNORE)
    await mysql_db.insert_smart(Test1, datas, insert_mode=InsertModeSql.INSERT_REPLACE)
    await mysql_db.insert_smart(Test1, datas, insert_mode=InsertModeSql.INSERT_UPDATE)  # 更新datas中的所有列
    await mysql_db.insert_smart(Test1, datas, insert_mode=InsertModeSql.INSERT_UPDATE, update_columns=['b'])  # 更新指定的列: b

    # update
    sql = "UPDATE test_1 SET b = %(b)s, c = %(c)s where id = %(id)s"
    data = {'b': 123, 'c': 456, 'id': 6}
    await mysql_db.update(sql, data)
    sql = "UPDATE test_1 SET b = %s, c = %s where id = %s"
    data = (123, 456, 6,)
    await mysql_db.update(sql, data)

    # delete
    sql = "DELETE FROM test_1 where id = %(id)s"
    data = {'id': 6}
    await mysql_db.update(sql, data)
    sql = "DELETE FROM test_1 where id = %s"
    data = (4,)
    await mysql_db.update(sql, data)

    # 多个SQL操作, 在一个事务中执行.
    async with mysql_db.transactional() as (conn, cursor):
        sql = "insert ignore into test.test_1 (a, b, c, d) values (%s, %s, %s, %s)"
        data = (22, 22, 33, 44,)
        await cursor.execute(sql, data)
        row_id = cursor.lastrowid
        print(row_id)

        # 扣除转出用户的余额
        sql1 = "UPDATE test_1 SET b = %(b)s, c = %(c)s where id = %(id)s"
        await cursor.execute(sql1, {'b': 3, 'c': 3, 'id': row_id})

    await mysql_db.close_pool()


if __name__ == '__main__':
    asyncio.run(query_tools())
