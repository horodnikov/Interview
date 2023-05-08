import sys
import time
import logging
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler


def on_created(event):
    exec(open("script2.py").read())
    print("created")


def on_deleted(event):
    exec(open("script2.py").read())
    print("deleted")


def on_modified(event):
    print("modified")


def on_moved(event):
    print("moved")


if __name__ == "__main__":
    event_handler = FileSystemEventHandler()
    event_handler.on_created = on_created
    event_handler.on_deleted = on_deleted
    event_handler.on_modified = on_modified
    event_handler.on_moved = on_moved

    path_payments = "./payments"
    path_bets = "./bets"
    observer_1 = Observer()
    observer_1.schedule(event_handler, path_payments, recursive=True)
    observer_1.start()
    observer_2 = Observer()
    observer_2.schedule(event_handler, path_bets, recursive=True)
    observer_2.start()

    try:
        print("Monitoring")
        while True:
            time.sleep(1)
    finally:
        observer_1.stop()
        observer_2.stop()
        observer_1.join()
        observer_2.join()
