from abc import ABC, abstractmethod
from kafka import KafkaProducer
import json

class KafkaClient(ABC):
    @abstractmethod
    def push_message(self, topic, value):
        pass

    @abstractmethod
    def flush(self):
        pass


class ConfluentKafkaClient(KafkaClient):
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=['localhost:29092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

    def push_message(self, topic, value):
        self.producer.send(topic=topic, value=value)

    def flush(self):
        self.producer.flush()