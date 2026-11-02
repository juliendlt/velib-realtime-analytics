import requests
from producer.base_kafka_producer import BaseKafkaProducer


class WeatherProducer(BaseKafkaProducer):

    def __init__(self, bootstrap_servers: list[str],interval: int=10, topic: str="weather-paris"):
        self.url = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"
        super().__init__(bootstrap_servers, interval, topic)

    def get_data(self):
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            "latitude": 48.8566,
            "longitude": 2.3522,
            "current": "temperature_2m,precipitation,rain,wind_speed_10m,weather_code",
            "timezone": "Europe/Paris",
        }
        r = requests.get(url, params=params, timeout=10, verify=False)  # TODO : Supprimer le SSL bypass
        r.raise_for_status()
        return [r.json()["current"]]

    def get_key(self, record : dict) -> str:
        return "1"


if __name__ == "__main__":
    weather_producer = WeatherProducer(["localhost:29092"])
    weather_producer.loop()