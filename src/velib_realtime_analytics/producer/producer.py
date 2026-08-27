import time

from velib_realtime_analytics.producer.clients.http_client import HttpClient
from velib_realtime_analytics.producer.clients.kafka_client import KafkaClient
from velib_realtime_analytics.producer.config.config_store import ConfigStore, ProducerConfig
from velib_realtime_analytics.producer.services.http_service import HttpService
from velib_realtime_analytics.producer.services.kafka_service import KafkaService


class Producer:
    def __init__(self, store: "ConfigStore", http_client : HttpClient, kafka_client : KafkaClient) -> None:
        self.store = store
        self.http_service = HttpService(client=http_client)
        self.kafka_service = KafkaService(client=kafka_client)

    def run(self,cfg: ProducerConfig):
        all_stations = self.http_service.fetch_all_stations(cfg.api_url)
        self.kafka_service.produce_all_messages(cfg.kafka_topic,all_stations)

    def loop(self):
        while True:
            cfg = self.store.get()
            self.run(cfg)
            print("message poussé à ", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            time.sleep(cfg.poll_interval)
