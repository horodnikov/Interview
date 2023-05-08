import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import script2
import threading


class OnMyWatch:
    def __init__(self):
        self.observer = Observer()

    def run(self, watch_directory):
        event_handler = Handler()
        self.observer.schedule(event_handler, watch_directory, recursive=True)
        self.observer.start()
        try:
            while not stop_event.is_set():  # Check the event state
                time.sleep(5)
        except KeyboardInterrupt:
            self.observer.stop()
            print("Observer Stopped")

        self.observer.join()


class Handler(FileSystemEventHandler):
    def on_any_event(self, event):
        if event.is_directory:
            return None

        t = threading.Thread(target=self.process_event, args=(event,))
        t.start()

    @staticmethod
    def process_event(event):
        global stop_event
        if event.event_type == 'created':
            script2.run()
            print("Watchdog received created event - % s." % event.src_path)
            stop_event.set()  # Set the event
        elif event.event_type == 'modified':
            script2.run()
            print("Watchdog received modified event - % s." % event.src_path)
            stop_event.set()  # Set the event
        elif event.event_type == 'deleted':
            script2.run()
            print("Watchdog received deleted event - % s." % event.src_path)
            stop_event.set()  # Set the event


if __name__ == '__main__':
    path_payments = "./payments"
    path_bets = "./bets"
    watch_payments = OnMyWatch()
    watch_bets = OnMyWatch()

    stop_event = threading.Event()  # Create a global event object

    t1 = threading.Thread(target=watch_payments.run, args=(path_payments,))
    t2 = threading.Thread(target=watch_bets.run, args=(path_bets,))

    t1.start()
    t2.start()

    t1.join()
    t2.join()
