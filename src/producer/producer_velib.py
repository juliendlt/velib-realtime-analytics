import requests
from producer.base_kafka_producer import BaseKafkaProducer


class VelibProducer(BaseKafkaProducer):

    def __init__(self, bootstrap_servers: list[str]):
        self.url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
        super().__init__(bootstrap_servers, interval=60, topic="velib-paris")

    def get_data(self):
        return requests.get(self.url, timeout=30).json()

    def get_key(self, record : dict) -> str:
        return record["stationcode"]


if __name__ == "__main__":
    velib_producer = VelibProducer(["localhost:29092"])
    velib_producer.loop()