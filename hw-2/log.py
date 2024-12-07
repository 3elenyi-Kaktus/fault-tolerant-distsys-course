import logging
from enum import IntEnum
from typing import Optional

from dataclasses_json import dataclass_json
from dataclasses import dataclass


class Event(IntEnum):
    NOOP = 0
    GET = 1
    POST = 2
    PUT = 3
    DELETE = 4


@dataclass_json
@dataclass
class Entry:
    term: int
    event: Event = Event.NOOP
    key: Optional[str] = None
    value: Optional[int] = None

    def __json__(self):
        return {"term": self.term, "event": self.event, "key": self.key, "value": self.value}


class Log:
    def __init__(self):
        self.entries: list[Entry] = [Entry(term=0)]

    def add_entry(self, entry: Entry, prev_entry_idx: int, prev_entry_term: int) -> bool:
        if len(self.entries) <= prev_entry_idx or self.entries[prev_entry_idx].term != prev_entry_term:
            logging.info(f"Previous entry doesn't exist or isn't consistent with provided")
            return False
        if entry is None:
            return True
        if len(self.entries) == prev_entry_idx + 1:
            logging.info(f"Add new entry")
            self.entries += [entry]
        elif self.entries[prev_entry_idx + 1].term != entry.term:
            logging.info(f"Current entry is not consistent with provided. Remove it with all subsequent entries")
            self.entries = self.entries[:prev_entry_idx + 1] + [entry]
        return True

    def size(self) -> int:
        return len(self.entries)

    def __getitem__(self, key: int):
        return self.entries[key]

    def __json__(self):
        return {"entries": self.entries}