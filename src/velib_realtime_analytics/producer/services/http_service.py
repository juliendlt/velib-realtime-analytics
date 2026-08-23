from velib_realtime_analytics.producer.clients.http_client import HttpClient


class HttpService:
    def __init__(self, client : HttpClient):
        self.client = client

    def fetch_all_stations(self, base_url : str, limit :int  = 100) -> list:
        all_records : list = []
        offset : int = 0
        total_count : int | None = None

        while total_count is None or offset < total_count:
            resp = self.client.get(base_url, params={"limit": limit, "offset": offset})
            resp.raise_for_status()
            data = resp.json()
            total_count = data["total_count"]
            all_records.extend(data["results"])
            offset += limit

        if len(all_records) != total_count:
            raise Exception("Erreur lors de la pagination, certaines données sont manquantes")

        return all_records
