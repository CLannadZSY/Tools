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

# [kafka]
# 详细参考: https://github.com/confluentinc/confluent-kafka-python
from kafka_utils.kafka_tool import ConfluentKafkaProducer, ConfluentKafkaConsumer

producer = ConfluentKafkaProducer().producer()
consumer = ConfluentKafkaConsumer().consumer()
