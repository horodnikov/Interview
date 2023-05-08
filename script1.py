import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import script2
import multiprocessing


class OnMyWatch:
    def __init__(self):
        self.observer = Observer()

    def run(self, watch_directory):
        event_handler = Handler()
        self.observer.schedule(event_handler, watch_directory, recursive=True)
        self.observer.start()
        try:
            while True:
                time.sleep(5)
        except KeyboardInterrupt:
            self.observer.stop()
            print("Observer Stopped")

        self.observer.join()


class Handler(FileSystemEventHandler):
    @staticmethod
    def on_any_event(event):
        if event.is_directory:
            return None

        elif event.event_type == 'created':
            script2.run()
            print("Watchdog received created event - % s." % event.src_path)
        elif event.event_type == 'modified':
            script2.run()
            print("Watchdog received modified event - % s." % event.src_path)
        elif event.event_type == 'deleted':
            script2.run()
            print("Watchdog received deleted event - % s." % event.src_path)


def run_on_my_watch(watch_directory):
    watch = OnMyWatch()
    watch.run(watch_directory)


if __name__ == '__main__':
    path_payments = "./payments"
    path_bets = "./bets"
    m1 = multiprocessing.Process(target=run_on_my_watch, args=(path_payments,))
    m2 = multiprocessing.Process(target=run_on_my_watch, args=(path_bets,))
    m1.start()
    m2.start()

    m1.join()  # Wait for process m1 to finish
    m2.join()  # Wait for process m2 to finish