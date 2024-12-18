import logging
from threading import Timer as ThreadTimer


class Timer:
    def __init__(self, name: str, duration: float, callback, data, auto_start: bool = True, renewable: bool = True):
        self.name: str = name
        self.duration = duration
        self.callback = callback
        self.data = data
        self.renewable: bool = renewable
        self.started = False
        self.cancelled = False
        self.timer: ThreadTimer = None
        if auto_start:
            self.start()

    def start(self):
        if not self.started:
            self.timer = ThreadTimer(self.duration, self.timeout)
            self.timer.start()
            self.started = True

    def cancel(self):
        if self.timer and not self.cancelled:
            self.cancelled = True
            self.timer.cancel()

    def restart(self):
        self.cancel()
        self.timer = ThreadTimer(self.duration, self.timeout)
        self.timer.start()
        self.cancelled = False

    def reschedule(self, duration: float):
        self.duration = duration
        self.restart()

    def timeout(self):
        try:
            logging.info(f"Timer '{self.name}' timed out. Running callback {self.callback} with data {self.data}")
            self.callback(self.data)
        except BaseException as exception:
            logging.exception(exception)
        if self.renewable:
            self.timer = ThreadTimer(self.duration, self.timeout)
            self.timer.start()

    def __json__(self):
        return {
            "name": self.name,
            "duration": self.duration,
            "data": self.data,
            "renewable": self.renewable,
            "started": self.started,
            "cancelled": self.cancelled
        }
