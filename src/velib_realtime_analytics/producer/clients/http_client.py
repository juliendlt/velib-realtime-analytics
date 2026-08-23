import requests
from abc import ABC, abstractmethod

class HttpClient(ABC):
    @abstractmethod
    def get(self, url : str, params :dict | None = None):
        pass



class RequestsHttpClient(HttpClient):
    def get(self, url : str, params: dict | None = None) -> requests.Response:
        return requests.get(url=url, params=params)