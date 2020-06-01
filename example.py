"""
使用案例
"""

# [mysql]
from mysql_utils.mysql_conn import MysqlPooledDB
from mysql_utils.mysql_conf import MYSQL_CONFIG_DEV, MYSQL_CONFIG_PROD

conn, cursor = MysqlPooledDB(MYSQL_CONFIG_DEV['db_name']).connect()

# [redis]
from redis_utils.redis_conn import RedisConnPool
from redis_utils.redis_conf import REDIS_CONFIG_DEV, REDIS_CONFIG_PROD

r_0 = RedisConnPool(REDIS_CONFIG_DEV['redis_name']).connect()

# [redis]发布
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


# [kafka]
# 详细参考: https://github.com/confluentinc/confluent-kafka-python
from kafka_utils.kafka_tool import ConfluentKafkaProducer, ConfluentKafkaConsumer

producer = ConfluentKafkaProducer().producer()
consumer = ConfluentKafkaConsumer().consumer()
