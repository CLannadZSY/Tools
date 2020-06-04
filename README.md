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

### Usage example [使用示例](example.py)
* 使用前, 请修改 ```*.conf.py``` 中的配置, 确保配置正确
```python
# [redis]
from redis_utils.redis_conn import RedisConnPool
from redis_utils.redis_conf import REDIS_CONFIG_DEV, REDIS_CONFIG_PROD

r_0 = RedisConnPool(REDIS_CONFIG_DEV['redis_name']).connect()
```

```python
# [redis]发布
from redis_utils.redis_conn import RedisConnPool
from redis_utils.redis_conf import REDIS_CONFIG_DEV, REDIS_CONFIG_PROD
r_0 = RedisConnPool(REDIS_CONFIG_DEV['redis_name']).connect()
chan_sub = 'channel_name'
message_content = 'Hello World'
r_0.publish(chan_sub, message_content)

# [redis]订阅
channel_sub = 'channel_name'
redis_conn = RedisConnPool(REDIS_CONFIG_PROD['redis_name'])
redis_sub = redis_conn.subscribe(channel_sub)
r_0 = redis_conn.connect()
while True:
    msg = redis_sub.listen()
    for i in msg:
        if i["type"] == "message" and i['channel'] == channel_sub:
            print(f'接收到消息: {i["data"]=}')
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

有什么问题或建议, 可以在 [issues](https://github.com/CLannadZSY/Tools/issues) 中告诉我

## Release History 版本历史

* 2020-03-28: 完成```1.0.0```版本 

## License 授权协议

这个项目 MIT 协议， 请点击 [LICENSE](LICENSE) 了解更多细节。
