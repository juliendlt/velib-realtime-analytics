# watcher.py
import time, os, threading

from velib_realtime_analytics.producer.config.config_store import ConfigStore


def watch_config(store: "ConfigStore", stop_event: threading.Event, interval=2.0):
    last_mtime = os.path.getmtime(store.path)
    while not stop_event.is_set():
        time.sleep(interval)
        try:
            mtime = os.path.getmtime(store.path)
            if mtime != last_mtime:
                last_mtime = mtime
                store.reload()
        except FileNotFoundError:
            continue