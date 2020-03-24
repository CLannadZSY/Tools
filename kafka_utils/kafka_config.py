"""
类参数列表, 及相关配置
文档:
    https://kafka-python.readthedocs.io/en/master/apidoc/KafkaProducer.html
    https://kafka-python.readthedocs.io/en/master/apidoc/KafkaConsumer.html
"""
import json

DEV_DEFAULT_TOPIC = ['asyncTask']

# 消费者配置
DEV_CONSUMER_CONFIG = {
    'group_id': 'python_test',
    'bootstrap_servers': ['kafka.t.1bom.net:49092'],
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': False,
    'request_timeout_ms': 3 * 60 * 1000,
    'auto_commit_interval_ms': 5000,
    # 'default_offset_commit_callback': lambda offsets, response: True,
    'max_poll_records': 500,
}

# 生产者配置
DEV_PRODUCER_CONFIG = {
    'bootstrap_servers': ['kafka.t.1bom.net:49092'],
    'key_serializer': None,
    # msgpack.dumps, lambda m: json.dumps(m).encode('ascii')
    'value_serializer': lambda m: json.dumps(m).encode('ascii'),
}
