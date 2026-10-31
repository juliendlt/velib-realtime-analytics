import time
import json
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

URL = "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/velib-disponibilite-en-temps-reel/exports/json"

producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

last_seen = {}  # stationcode -> duedate


def loop():
    while True:
        try:
            stations = requests.get(URL, timeout=30).json()
            now = datetime.now(timezone.utc)
            sent = 0
            lags = []

            for s in stations:
                code = s["stationcode"]
                duedate = s.get("duedate")

                if duedate:
                    lags.append((now - datetime.fromisoformat(duedate)).total_seconds() / 60)

                if last_seen.get(code) == duedate:
                    continue  # pas de changement, on n'envoie pas
                last_seen[code] = duedate

                s["ingested_at"] = now.isoformat()
                producer.send("velib-paris", key=code, value=s)
                sent += 1

            producer.flush()

            lags.sort()
            median = lags[len(lags) // 2] if lags else 0
            print(
                f"{now:%Y-%m-%d %H:%M:%S} UTC - {sent}/{len(stations)} stations envoyées "
                f"- retard médian {median:.1f} min, max {lags[-1] if lags else 0:.0f} min"
            )
        except Exception as e:
            print(f"Erreur : {e}")

        time.sleep(60)


if __name__ == "__main__":
    loop()