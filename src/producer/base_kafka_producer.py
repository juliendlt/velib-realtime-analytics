import time
import json
from datetime import datetime, timezone
from kafka import KafkaProducer
from abc import ABC, abstractmethod


class BaseKafkaProducer(ABC):
    def __init__(self, bootstrap_servers : list[str],interval : int, topic : str):
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        )
        self.topic = topic
        self.interval = interval


    @abstractmethod
    def get_data(self) :
        pass

    @abstractmethod
    def get_key(self, record) -> str:
        pass

    def loop(self):
        while True:
            try:
                data = self.get_data()
                now = datetime.now(timezone.utc)

                for record in data:
                    code = self.get_key(record)
                    record["ingested_at"] = now.isoformat()
                    self.producer.send(self.topic, key=code, value=record)

                self.producer.flush()
                print(
                    f"[{self.__class__.__name__}][{self.topic}][{now:%Y-%m-%d %H:%M:%S} UTC] - {len(data)} lignes envoyées "
                )

            except Exception as e:
                print(f"Erreur : {e}")

            time.sleep(60)