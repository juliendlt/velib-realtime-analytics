import time

from velib_realtime_analytics.producer.clients.http_client import RequestsHttpClient
from velib_realtime_analytics.producer.clients.kafka_client import ConfluentKafkaClient
from velib_realtime_analytics.producer.services.http_service import HttpService
from velib_realtime_analytics.producer.services.kafka_service import KafkaService


class Producer:
    def __init__(self) -> None:
        # TODO : A mettre en argument les clients
        self.http_client = RequestsHttpClient()
        self.kafka_client = ConfluentKafkaClient()

        self.http_service = HttpService(client=self.http_client)
        self.kafka_service = KafkaService(client=self.kafka_client)

    def run(self):
        all_stations = self.http_service.fetch_all_stations("https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records")
        self.kafka_service.produce_all_messages("velib-paris",all_stations)


    def loop(self):
        while True:
            self.run()
            print("message poussé à ", time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
            time.sleep(10)
Producer().loop()