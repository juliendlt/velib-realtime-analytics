import os
import requests
from producer.base_kafka_producer import BaseKafkaProducer


class DisruptionProducer(BaseKafkaProducer):

    def __init__(self, bootstrap_servers: list[str], interval: int = 120, topic: str = "idfm-disruptions"):
        # 2 appels (métro + RER) toutes les 120s = 1440 appels/jour -> vérifie ton quota Navitia sur PRIM
        self.base_url = "https://prim.iledefrance-mobilites.fr/marketplace/v2/navitia/line_reports/physical_modes/physical_mode%3A{mode}/line_reports"
        self.api_key = 'Pis7cU8ZUy8mzuzDyAhz2vXRK8WRiNai'
        self.modes = ("Metro", "RapidTransit")
        super().__init__(bootstrap_servers, interval, topic)

    def get_data(self):
        records = []

        for mode in self.modes:
            r = requests.get(
                self.base_url.format(mode=mode),
                headers={"apiKey": self.api_key},
                params={"count": 200},
                timeout=30,
                verify=False,  # TODO : Supprimer le SSL bypass
            )
            r.raise_for_status()
            data = r.json()

            # lignes liées à chaque disruption (seul endroit où l'info existe pour les impacts au niveau gare)
            lines_by_dis = {}
            for lr in data.get("line_reports", []):
                line = lr.get("line", {})
                for link in line.get("links", []):
                    if link.get("type") == "disruption":
                        lines_by_dis.setdefault(link["id"], []).append(
                            {"line_id": line.get("id"), "line": line.get("code")}
                        )

            # disruption brute Navitia + 2 champs ajoutés, aucun dédoublonnage
            for d in data.get("disruptions", []):
                d["physical_mode"] = mode
                d["linked_lines"] = lines_by_dis.get(d["id"], [])
                records.append(d)

        return records

    def get_key(self, record: dict) -> str:
        return record["id"]


if __name__ == "__main__":
    disruption_producer = DisruptionProducer(["localhost:29092"])
    disruption_producer.loop()