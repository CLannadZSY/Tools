# Tools

### Installation 安装

```sh
pip install Py-library
```

### Usage example [同步连接使用示例](example.py)

### 异步 `mysql`, `redis` 使用示例:

1. Mysql 使用案例
   ```python
   from py_library.async_db.mysql_db import MysqlDB, FetchMode
   
   # 配置 MySQL 连接
   config = {
       "host": "localhost",
       "user": "user",
       "password": "password",
       "db": "test_db",
       "echo": True  # 打印SQL查询，便于调试
   }
   # data: Dict
   # datas: List[Dict]
   # 创建 MysqlDB 实例
   mysql_db = MysqlDB(config)
   result = await mysql_db.insert(sql, data)
   result = await mysql_db.insert_many(sql, datas)
   result = await mysql_db.insert_smart('table_name', datas)
   result = await mysql_db.update(sql, data)
   result = await mysql_db.delete(sql, data)
    ```

2. Redis 使用案例
   ```python
   from py_library.async_db.redis_db import RedisDB
   
   # 创建 RedisDB 实例
   redis_conn = {
       "host": "localhost",
       "port": 6379,
       "db": 0
   }
   redis_db = RedisDB(redis_conn)
   client = redis_db.connect()
   
   # pipeline 使用
   async with redis_db.get_pipeline() as redis_pipe:
       for i in range(10000):
           ok1 = await redis_pipe.set(f"key{i}", f"value{i}")
       results = await redis_pipe.execute()
       print(results)
   await redis_db.close_client()
   ```

## Contributing 贡献指南

有什么问题或建议, 可以在 [issues](https://github.com/CLannadZSY/Tools/issues) 中告诉我

## Release History 版本历史

* 2020-10-20: 完成```1.0.4```版本
* 2020-03-28: 完成```1.0.0```版本

## License 授权协议

这个项目 MIT 协议， 请点击 [LICENSE](LICENSE) 了解更多细节。
