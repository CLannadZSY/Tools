"""
使用案例
"""

# [mysql]
from py_library.mysql_conn import MysqlPooledDB
MYSQL_CONFIG_DEV = {
    '1bom.1bomSpider': {
        "host": "127.0.0.1",
        "port": 3306,
        "user": "username",
        "password": "password",
        "database": 'dbname'
    },
}
conn, cursor = MysqlPooledDB(MYSQL_CONFIG_DEV['db_name']).connect()

# [redis]
from py_library.redis_conn import RedisConnPool
REDIS_CONFIG_DEV = {
    'redis_name': {
        'host': "127.0.0.1",
        'port': 6379,
        'password': '',
        'max_connections': 100,
        'db': 0
    },
}
r_0 = RedisConnPool(REDIS_CONFIG_DEV['redis_name']).connect()

# [redis]发布
r_0 = RedisConnPool(REDIS_CONFIG_DEV['redis_name']).connect()
chan_sub = 'channel_name'
message_content = 'Hello World'
r_0.publish(chan_sub, message_content)

# [redis]订阅
channel_sub = 'channel_name'
redis_conn = RedisConnPool(REDIS_CONFIG_DEV['redis_name'])
redis_sub = redis_conn.subscribe(channel_sub)
r_0 = redis_conn.connect()
while True:
    msg = redis_sub.listen()
    for i in msg:
        if i["type"] == "message" and i['channel'] == channel_sub:
            print(f'接收到消息: {i["data"]=}')


# [kafka]
# 详细参考: https://github.com/confluentinc/confluent-kafka-python
from py_library.kafka_conn import ConfluentKafkaProducer, ConfluentKafkaConsumer

DEV_CONFLUENT_PRODUCER_CONFIG = {
    'bootstrap.servers': 'server_addr',
}

DEV_CONFLUENT_CONSUMER_CONFIG = {
    'bootstrap.servers': 'server_addr',
    'group.id': 'group_name',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}
producer = ConfluentKafkaProducer(DEV_CONFLUENT_PRODUCER_CONFIG).producer()
consumer = ConfluentKafkaConsumer(DEV_CONFLUENT_CONSUMER_CONFIG).consumer()
