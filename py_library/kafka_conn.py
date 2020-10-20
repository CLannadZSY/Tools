"""
封装 kafka 工具类
"""
from confluent_kafka import Producer, Consumer, KafkaError
from kafka_conf import DEV_CONFLUENT_PRODUCER_CONFIG, DEV_CONFLUENT_CONSUMER_CONFIG

class ConfluentKafkaProducer(object):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(ConfluentKafkaProducer, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, config: dict = None):
        self._producer = Producer(DEV_CONFLUENT_PRODUCER_CONFIG or config)
        # self._producer.produce('mytopic', b'value', callback=delivery_report)

    def __del__(self):
        self._producer.flush()

    def producer(self):
        return self._producer


def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))


class ConfluentKafkaConsumer(object):
    _instance = None

    def __new__(cls, *args, **kwargs):
        if not cls._instance:
            cls._instance = super(ConfluentKafkaConsumer, cls).__new__(cls, *args, **kwargs)
        return cls._instance

    def __init__(self, config: dict = None):
        self._consumer = Consumer(DEV_CONFLUENT_CONSUMER_CONFIG or config)

    def __del__(self):
        self._consumer.close()

    def consumer(self):
        return self._consumer
