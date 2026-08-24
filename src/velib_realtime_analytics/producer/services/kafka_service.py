from velib_realtime_analytics.producer.clients.kafka_client import KafkaClient


class KafkaService:
    def __init__(self, client : KafkaClient):
        self.kafka_client = client

    def produce_all_messages(self, topic : str, messages : list[dict]):
        for message in messages:
            self.kafka_client.push_message(topic=topic,value=message)
        self.kafka_client.flush()