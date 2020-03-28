# Tools

### Prerequisites 项目使用条件
* [redis](https://redis.io/)
* [mysql](https://www.mysql.com/)
* [librdkafka](https://github.com/edenhill/librdkafka)

### Installation 安装

Linux:

```sh
pip install redis pymysql confluent_kafka
```

Windows:

* windows 下无法安装 ```confluent_kafka``` 模块

```sh
pip install redis pymysql
```

### Usage example 使用示例
```python
# [redis]
from redis_utils.redis_conn import RedisConnPool
from redis_utils.redis_conf import REDIS_CONFIG_DEV, REDIS_CONFIG_PROD

r_0 = RedisConnPool(REDIS_CONFIG_DEV['redis_name']).connect()
```

```python
# [mysql]
from mysql_utils.mysql_conn import MysqlPooledDB
from mysql_utils.mysql_conf import MYSQL_CONFIG_DEV, MYSQL_CONFIG_PROD

conn, cursor = MysqlPooledDB(MYSQL_CONFIG_DEV['db_name']).connect()
```

```python
# [kafka]
# 详细参考: https://github.com/confluentinc/confluent-kafka-python
 
from kafka_utils.kafka_tool import ConfluentKafkaProducer, ConfluentKafkaConsumer

producer = ConfluentKafkaProducer().producer()
consumer = ConfluentKafkaConsumer().consumer()
```


## Contributing 贡献指南

有好的建议, 可以在 ```issues```中告诉我

## Release History 版本历史

* 2020-03-28: 完成```1.0.0```版本 

## License 授权协议

这个项目 MIT 协议， 请点击 [LICENSE](LICENSE) 了解更多细节。