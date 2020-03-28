"""
https://github.com/confluentinc/confluent-kafka-python
"""

DEV_CONFLUENT_PRODUCER_CONFIG = {
    'bootstrap.servers': 'host:port',
}

DEV_CONFLUENT_CONSUMER_CONFIG = {
    'bootstrap.servers': 'host:port',
    'group.id': 'python_test',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}
