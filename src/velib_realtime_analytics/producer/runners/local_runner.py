from velib_realtime_analytics.producer.clients.http_client import RequestsHttpClient
from velib_realtime_analytics.producer.clients.kafka_client import ConfluentKafkaClient
from velib_realtime_analytics.producer.config.config_store import ConfigStore
from velib_realtime_analytics.producer.producer import Producer
import threading

from velib_realtime_analytics.producer.watcher.watcher import watch_config


def main():
    http_client = RequestsHttpClient()
    kafka_client = ConfluentKafkaClient()
    store = ConfigStore("C:/Dev/perso/velib-realtime-analytics/config.yaml") #TODO FIx
    producer = Producer(store=store,http_client=http_client, kafka_client=kafka_client)
    stop_event = threading.Event()

    watcher_thread = threading.Thread(
        target=watch_config, args=(store, stop_event), daemon=True
    )
    watcher_thread.start()
    try :
        producer.loop()
    except KeyboardInterrupt:
        stop_event.set()
    except Exception as e :
        print("une erreur est survenue")
        raise e


if __name__ == "__main__":
    main()
