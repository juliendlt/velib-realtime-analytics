import os
import requests
from producer.base_kafka_producer import BaseKafkaProducer


class DisruptionProducer(BaseKafkaProducer):

    def __init__(self, bootstrap_servers: list[str], interval: int = 120, topic: str = "idfm-disruptions"):
        # interval 120s = 720 appels/jour, sous le quota de 1000/jour des nouveaux comptes PRIM
        self.url = "https://prim.iledefrance-mobilites.fr/marketplace/disruptions_bulk/disruptions/v2"
        self.api_key = "Pis7cU8ZUy8mzuzDyAhz2vXRK8WRiNai"
        self.modes = ("Metro", "RapidTransit")  # métro + RER
        super().__init__(bootstrap_servers, interval, topic)

    def get_data(self):
        r = requests.get(self.url, headers={"apiKey": self.api_key}, timeout=30, verify=False)  # TODO : Supprimer le SSL bypass
        r.raise_for_status()
        data = r.json()

        disruptions = {d["id"]: d for d in data.get("disruptions", [])}

        records = []
        for line in data.get("lines", []):
            if line.get("mode") not in self.modes:
                continue

            ids = {i for obj in line.get("impactedObjects", []) for i in obj.get("disruptionIds", [])}
            for did in ids:
                d = disruptions.get(did)
                if d is None:
                    continue
                periods = d.get("applicationPeriods", [{}])
                records.append({
                    "disruption_id": did,
                    "line_id": line.get("id"),
                    "line": line.get("shortName"),
                    "mode": line.get("mode"),
                    "cause": d.get("cause"),        # PERTURBATION / TRAVAUX
                    "severity": d.get("severity"),  # BLOQUANTE / PERTURBEE / INFORMATION
                    "title": d.get("title"),
                    "message": d.get("message"),
                    "begin": periods[0].get("begin"),
                    "end": periods[0].get("end"),
                    "last_update": d.get("lastUpdate"),
                })

        return records

    def get_key(self, record: dict) -> str:
        return record["line_id"]


if __name__ == "__main__":
    disruption_producer = DisruptionProducer(["localhost:29092"])
    disruption_producer.loop()