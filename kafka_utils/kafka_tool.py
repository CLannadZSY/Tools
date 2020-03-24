"""
封装 kafka 工具类
"""
import sys
import logging
from kafka import KafkaConsumer, KafkaProducer
try:
    from kafka_config import DEV_CONSUMER_CONFIG, DEV_DEFAULT_TOPIC, DEV_PRODUCER_CONFIG
except:
    from .kafka_config import DEV_CONSUMER_CONFIG, DEV_DEFAULT_TOPIC, DEV_PRODUCER_CONFIG

log = logging.Logger(__name__)
log.addHandler(logging.StreamHandler(sys.stdout))


# logger = logging.getLogger('kafka')
# logger.setLevel(logging.DEBUG)


class ConsumerTool:
    """
    消费者类
    """

    def __init__(self, *topics, **configs):
        # self._consumer = KafkaConsumer(*DEV_DEFAULT_TOPIC or topics, **DEV_CONSUMER_CONFIG or configs)
        self._consumer = KafkaConsumer(**DEV_CONSUMER_CONFIG or configs)
        self.connect_status = self._consumer.bootstrap_connected()
        log.info(f'kafka 消费者连接状态: {self.connect_status}')

    def connect(self):
        """
        返回连接成功之后的对象
        :return: object
        """
        return self._consumer


    @staticmethod
    def _on_send_response(*args, **kwargs):
        """
        异步提交偏移量的回调函数
        args[0]是一个dict，key是TopicPartition，value是OffsetAndMetadata，
            表示该主题下的partition对应的offset；args[1]在提交成功是True，提交失败时是一个Exception类。
        :param args: args[0] --> {TopicPartition:OffsetAndMetadata}  args[1] --> Exception
        :param kwargs:
        :return: None
        """
        # log.info(args)
        if isinstance(args[1], Exception):
            log.error(f'偏移量提交异常. {args[1]}')
        else:
            log.info('偏移量提交成功')


class ProducerTool:
    """
    生产者类
    """

    def __init__(self, **configs):
        self._producer = KafkaProducer(**DEV_PRODUCER_CONFIG or configs)
        self.connect_status = self._producer.bootstrap_connected()
        log.info(f'kafka 生产者连接状态: {self.connect_status}')

    def connect(self):
        """
        返回连接成功之后的对象
        :return: object
        """
        return self._producer

    # 定义一个发送成功的回调函数
    @staticmethod
    def _on_send_success(record_metadata):
        log.info(f'发送成功: {record_metadata=}')

    # 定义一个发送失败的回调函数
    @staticmethod
    def _on_send_error(excp):
        log.error('发送失败', exc_info=excp)
