from pydantic import BaseModel
import yaml
import threading
import logging

logger = logging.getLogger(__name__)

class ProducerConfig(BaseModel):
    api_url:str
    kafka_topic: str
    poll_interval: float| int

class ConfigStore:
    def __init__(self, path: str):
        self.path = path
        self._lock = threading.Lock()
        self._config = self._load()

    def _load(self):
        with open(self.path, "r") as f:
            raw = yaml.safe_load(f)
        return ProducerConfig(**raw)

    def get(self) -> ProducerConfig:
        # lecture simple, pas besoin de lock (juste une lecture de référence)
        return self._config

    def reload(self):
        try:
            new_config = self._load()
        except Exception as e:
            print(f"Config invalide, on garde l'ancienne : {e}")
            return
        with self._lock:
            self._config = new_config  # swap atomique
        print("Config rechargée avec succès")

    @property
    def path(self):
        return self._path

    @path.setter
    def path(self, value):
        self._path = value