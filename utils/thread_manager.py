import threading
from typing import Callable


class ThreadManager:
    def __init__(self):
        self.threads: dict[str, threading.Thread] = {}
        self.stop_events: dict[str, threading.Event] = {}

    def add_thread(self, target_func: Callable, name: str, args: tuple = (), daemon: bool = False):
        _stop_event = threading.Event()
        self.stop_events[name] = _stop_event
        _thread = threading.Thread(
            target=target_func, name=name, args=(_stop_event, *args), daemon=daemon)
        _thread.start()
        self.threads[name] = _thread

    def stop_thread(self, name: str):
        if name in self.threads:
            self.stop_events[name].set()  # Signal the thread to stop
            self.threads[name].join()     # Wait for the thread to finish
            del self.threads[name]
            del self.stop_events[name]

    def stop_all_threads(self):
        for name in list(self.threads.keys()):
            self.stop_thread(name)

    def exit(self):
        for name in self.threads:
            self.stop_events[name].set()
        for name in list(self.threads.keys()):
            self.threads[name].join()
            del self.threads[name]
            del self.stop_events[name]

    def __del__(self):
        self.exit()
