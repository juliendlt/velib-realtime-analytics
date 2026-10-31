import time
import json
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer

BASE = "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole"
MAX_LAG_MIN = 24 * 60  # au-delà d'1 jour : station considérée comme morte

producer = KafkaProducer(
    bootstrap_servers=["localhost:29092"],
    key_serializer=lambda k: k.encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


def get(path):
    return requests.get(f"{BASE}/{path}", timeout=30).json()["data"]["stations"]


# Infos statiques chargées une fois : station_id -> {nom, coords, capacité}
info = {
    s["station_id"]: {
        "stationcode": s.get("stationCode"),
        "name": s["name"],
        "lat": s["lat"],
        "lon": s["lon"],
        "capacity": s["capacity"],
    }
    for s in get("station_information.json")
}

last_seen = {}  # station_id -> last_reported


def loop():
    while True:
        try:
            stations = get("station_status.json")
            now = datetime.now(timezone.utc)
            sent, dead, lags = 0, 0, []

            for s in stations:
                sid = s["station_id"]
                reported = datetime.fromtimestamp(s["last_reported"], timezone.utc)
                lag = (now - reported).total_seconds() / 60

                if lag > MAX_LAG_MIN:
                    dead += 1
                    continue
                lags.append(lag)

                if last_seen.get(sid) == s["last_reported"]:
                    continue
                last_seen[sid] = s["last_reported"]

                types = {k: v for t in s.get("num_bikes_available_types", []) for k, v in t.items()}
                msg = {
                    **info.get(sid, {}),
                    "station_id": sid,
                    "numbikesavailable": s["num_bikes_available"],
                    "mechanical": types.get("mechanical", 0),
                    "ebike": types.get("ebike", 0),
                    "numdocksavailable": s["num_docks_available"],
                    "is_installed": s["is_installed"],
                    "is_renting": s["is_renting"],
                    "is_returning": s["is_returning"],
                    "duedate": reported.isoformat(),
                    "ingested_at": now.isoformat(),
                }
                producer.send("velib-paris", key=str(sid), value=msg)
                sent += 1

            producer.flush()

            lags.sort()
            median = lags[len(lags) // 2] if lags else 0
            print(
                f"{now:%Y-%m-%d %H:%M:%S} UTC - {sent}/{len(stations)} envoyées "
                f"- {dead} mortes - retard médian {median:.1f} min"
            )
        except Exception as e:
            print(f"Erreur : {e}")

        time.sleep(60)


if __name__ == "__main__":
    loop()