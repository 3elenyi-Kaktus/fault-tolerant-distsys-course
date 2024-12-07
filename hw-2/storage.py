import logging
from typing import Optional

from log import Entry, Event


class Storage:
    def __init__(self) -> None:
        self.storage: dict[str, int] = {}

    def apply(self, entry: Entry) -> Optional[int]:
        logging.info(f"Apply entry: {entry}")
        match entry.event:
            case Event.NOOP:
                return
            case Event.GET:
                return self.get(entry.key)
            case Event.POST:
                return self.set(entry.key, entry.value)
            case Event.PUT:
                return self.set(entry.key, entry.value)
            case Event.DELETE:
                return self.delete(entry.key)
            case _:
                raise RuntimeError(f"Unknown event {entry.event}")

    def get(self, key: str) -> int:
        return self.storage.get(key, None)

    def set(self, key: str, value: int) -> None:
        self.storage[key] = value

    def delete(self, key: str) -> None:
        del self.storage[key]

    def __json__(self):
        return {"storage": self.storage}
