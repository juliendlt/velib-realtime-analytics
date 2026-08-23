from velib_realtime_analytics.producer.clients.http_client import RequestsHttpClient
from velib_realtime_analytics.producer.services.http_service import HttpService

http_client = RequestsHttpClient()
http_service = HttpService(client=http_client)
all_stations = http_service.fetch_all_stations("https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/records")


