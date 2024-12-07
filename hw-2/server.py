import random
import threading
from time import sleep
from typing import Optional


def _default(self, obj):
    return getattr(obj.__class__, "__json__", _default.default)(obj)


from json import JSONEncoder

_default.default = JSONEncoder().default
JSONEncoder.default = _default

import json
import logging
import socket
from dataclasses import dataclass
from enum import IntEnum, Enum
from threading import Thread
from dataclasses_json import dataclass_json

from timer import Timer
from storage import Storage
from log import Log, Entry, Event
from models import RaftRequest, Operation

HEARTBEAT_TIMEOUT = 10
ELECTION_TIMEOUT = 15 + random.randint(0, 100) / 10

SERVERS = {
    2: ("127.0.0.2", 32000),
    3: ("127.0.0.3", 32000),
    4: ("127.0.0.4", 32000),
    # 5: ("127.0.0.5", 32000)
}


class State(IntEnum):
    FOLLOWER = 0
    CANDIDATE = 1
    LEADER = 2


@dataclass_json
@dataclass
class AppendEntry:
    entry: Optional[Entry]
    prev_entry_idx: int
    prev_entry_term: int
    commit_idx: int

    def __json__(self):
        return {"entry": self.entry, "prev_entry_idx": self.prev_entry_idx, "prev_entry_term": self.prev_entry_term,
                "commit_idx": self.commit_idx}


@dataclass_json
@dataclass
class AppendEntryResponse:
    success: bool

    def __json__(self):
        return {"success": self.success}


@dataclass_json
@dataclass
class RequestVote:
    last_entry_idx: int
    last_entry_term: int

    def __json__(self):
        return {"last_entry_idx": self.last_entry_idx, "last_entry_term": self.last_entry_term}


@dataclass_json
@dataclass
class RequestVoteResponse:
    vote_granted: bool

    def __json__(self):
        return {"vote_granted": self.vote_granted}


class MessageType(str, Enum):
    APPEND_ENTRY = "appendEntry"
    APPEND_ENTRY_RESPONSE = "appendEntryResponse"
    REQUEST_VOTE = "requestVote"
    REQUEST_VOTE_RESPONSE = "requestVoteResponse"


@dataclass_json
@dataclass
class RPC:
    sender: int
    term: int
    message_type: MessageType
    message: AppendEntry | AppendEntryResponse | RequestVote | RequestVoteResponse

    def __json__(self):
        return {"sender": self.sender, "term": self.term, "message_type": self.message_type, "message": self.message}


class Server:
    def __init__(self, id_: int) -> None:
        self.address: tuple[str, int] = SERVERS[id_]
        self.id: int = id_

        self.state: State = State.FOLLOWER
        self.term: int = 0
        self.log: Log = Log()
        self.commit_index: int = 0
        self.storage: Storage = Storage()
        self.leader_id: int = None

        self.voted_for: int = None
        self.approves: set[int] = set()
        self.election_timer: Timer = Timer('Election', ELECTION_TIMEOUT, self.startElection)

        self.next_index: dict[int, int] = {}
        self.match_index: dict[int, int] = {}
        self.heartbeat_timer: Timer = Timer('Heartbeat', HEARTBEAT_TIMEOUT, self.heartbeatRepair, False)
        self.lock = threading.Lock()

        Thread(target=self.poll_rpcs).start()

        logging.info(f"Server started at {self.address}")

    def broadcast(self, msg: bytes) -> None:
        logging.info(f"BROADCAST -> {msg.decode('utf-8')}")
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for server_id, address in SERVERS.items():
            if server_id == self.id:
                continue
            n = sock.sendto(msg, address)
            if n != len(msg):
                logging.critical(f"Datagram split: {n} sent instead of {len(msg)}")

    @staticmethod
    def sendTo(address: tuple[str, int], msg: bytes) -> None:
        logging.info(f"SEND -> {msg.decode('utf-8')}")
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        n = sock.sendto(msg, address)
        if n != len(msg):
            logging.critical(f"Datagram split: {n} sent instead of {len(msg)}")

    def fallback(self, term: int, leader_id: int) -> None:
        self.state = State.FOLLOWER
        self.leader_id = leader_id
        self.term = term
        self.voted_for = None

        self.election_timer.restart()
        self.heartbeat_timer.cancel()

    def poll_rpcs(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            sock.bind(self.address)
            sock.settimeout(1)
            while True:
                try:
                    message, address = sock.recvfrom(4096)
                except socket.timeout:
                    logging.info("")
                    continue
                self.election_timer.restart()
                logging.info(f"RECEIVE <- {message.decode('utf-8')}")
                rpc: RPC = RPC.from_json(message)

                if rpc.sender == self.id or rpc.term < self.term:
                    logging.info(f"Old term ({rpc.term} < {self.term}), ignore")
                    continue
                self.__getattribute__(rpc.message_type)(rpc.message, rpc.sender, rpc.term)
        except BaseException as exception:
            logging.exception(exception)

    def isLeader(self):
        return self.state == State.LEADER

    def serve_client(self, request: RaftRequest, operation) -> Optional[int]:
        entry: Entry = Entry(self.term, Event(operation), request.key, request.value)
        with self.lock:
            log_size: int = self.log.size()
            res = self.log.add_entry(entry, log_size - 1, self.log[log_size - 1].term)
            if not res:
                logging.critical(f"Failed to add entry: {entry}")
            message = RPC(self.id, self.term, MessageType.APPEND_ENTRY,
                          AppendEntry(entry, log_size - 1, self.log[log_size - 1].term, self.commit_index))
        self.broadcast(json.dumps(message).encode('utf-8'))
        while self.commit_index < log_size:
            sleep(1)
        if operation == 1:
            return self.log[log_size].value

    def startElection(self) -> None:
        if self.state == State.LEADER:
            logging.info(f"Already leader, do not start election")
            return
        self.state = State.CANDIDATE
        self.term += 1
        self.voted_for = self.id
        self.approves = {self.id}
        log_size: int = self.log.size()
        message = RPC(self.id, self.term, MessageType.REQUEST_VOTE, RequestVote(log_size - 1, self.log[log_size - 1].term))
        self.broadcast(json.dumps(message).encode('utf-8'))

    def heartbeatRepair(self) -> None:
        if not self.state == State.LEADER:
            logging.info(f"Not a leader, heartbeat cancelled")
            return
        for server_id, server in SERVERS.items():
            if server_id == self.id:
                continue
            index = self.next_index[server_id]
            entry = None
            if index < self.log.size():
                entry = self.log[index]
            message = RPC(self.id, self.term, MessageType.APPEND_ENTRY,
                          AppendEntry(entry, index - 1, self.log[index - 1].term, self.commit_index))
            self.sendTo(SERVERS[server_id], json.dumps(message).encode('utf-8'))

    def requestVote(self, request: RequestVote, sender: int, term: int) -> None:
        with self.lock:
            message = RPC(self.id, self.term, MessageType.REQUEST_VOTE_RESPONSE, RequestVoteResponse(False))
            if term > self.term:
                logging.info(f"New term ({term} > {self.term}), fallback to follower")
                self.fallback(term, sender)

            if (term >= self.term and
                    (request.last_entry_term > self.log[-1].term or
                     request.last_entry_term == self.log[-1].term and request.last_entry_idx >= self.log.size() - 1) and
                    self.voted_for is None):
                logging.info(f"Voting for {sender} to become new leader")
                self.voted_for = sender
                message = RPC(self.id, self.term, MessageType.REQUEST_VOTE_RESPONSE, RequestVoteResponse(True))

        self.sendTo(SERVERS[sender], json.dumps(message).encode('utf-8'))

    def requestVoteResponse(self, response: RequestVoteResponse, sender: int, term: int) -> None:
        if term > self.term:
            logging.info(f"New term ({term} > {self.term}), fallback to follower")
            self.fallback(term, sender)
            return
        if not response.vote_granted or self.state != State.CANDIDATE:
            return
        with self.lock:
            self.approves.add(sender)
            if len(self.approves) >= (len(SERVERS) + 1) / 2. and not self.state == State.LEADER:
                self.transformToLeader()
                logging.info(f"Selected as leader")
                new_term_base_entry: Entry = Entry(self.term)
                log_size: int = self.log.size()
                res = self.log.add_entry(new_term_base_entry, log_size - 1, self.log[log_size - 1].term)
                if not res:
                    logging.critical(f"Failed to add entry: {new_term_base_entry}")
                message = RPC(self.id, self.term, MessageType.APPEND_ENTRY,
                              AppendEntry(new_term_base_entry, log_size - 1, self.log[log_size - 1].term, self.commit_index))
                self.broadcast(json.dumps(message).encode('utf-8'))

    def transformToLeader(self):
        self.state = State.LEADER
        self.leader_id = self.id
        log_size: int = self.log.size()
        self.next_index = {x: log_size for x in SERVERS.keys()}
        self.match_index = {x: 0 for x in SERVERS.keys()}

        self.election_timer.cancel()
        self.heartbeat_timer.restart()

    def appendEntry(self, request: AppendEntry, sender: int, term: int) -> None:
        if term > self.term:
            logging.info(f"New term ({term} > {self.term}), fallback to follower")
            self.fallback(term, sender)

        with self.lock:
            result = self.log.add_entry(request.entry, request.prev_entry_idx, request.prev_entry_term)
            if result:
                message = RPC(self.id, self.term, MessageType.APPEND_ENTRY_RESPONSE, AppendEntryResponse(True))
                if request.commit_idx > self.commit_index:
                    newl = min(request.commit_idx, self.log.size() - 1)
                    logging.info(f"Commiting entries from {self.commit_index + 1} to {newl}")
                    for entry in self.log[self.commit_index + 1: newl + 1]:
                        res = self.storage.apply(entry)
                        if res:
                            entry.value = res
                    self.commit_index = newl
            else:
                message = RPC(self.id, self.term, MessageType.APPEND_ENTRY_RESPONSE, AppendEntryResponse(False))
        self.sendTo(SERVERS[sender], json.dumps(message).encode('utf-8'))

    def commitEntries(self):
        book = {}
        for server_id, commited_entries in self.match_index.items():
            if commited_entries in book.keys():
                book[commited_entries] += 1
            else:
                book[commited_entries] = 1
        ranked = sorted(book.items(), key=lambda x: x[1], reverse=True)
        logging.info(f"LOL ___ {ranked}")
        for commits, count in ranked:
            if count >= (len(SERVERS) + 1) / 2. and commits > self.commit_index and self.log[commits].term == self.term:
                logging.info(f'Commiting entries from {self.commit_index + 1} to {commits} on master')
                for entry in self.log[self.commit_index + 1: commits + 1]:
                    res = self.storage.apply(entry)
                    if res:
                        entry.value = res
                self.commit_index = commits

    def appendEntryResponse(self, response: AppendEntryResponse, sender: int, term: int) -> None:
        if term > self.term:
            logging.info(f"New term ({term} > {self.term}), fallback to follower")
            self.fallback(term, sender)
            return
        with self.lock:
            if response.success:
                logging.info(f"Successfully written data on replica")
                self.next_index[sender] = min(self.next_index[sender] + 1, self.log.size())
                self.match_index[sender] = min(self.match_index[sender] + 1, self.log.size() - 1)
                self.commitEntries()
            else:
                self.next_index[sender] -= 1
                index = self.next_index[sender]
                message = RPC(self.id, self.term, MessageType.APPEND_ENTRY,
                              AppendEntry(self.log[index], index - 1, self.log[index - 1].term, self.commit_index))
                self.sendTo(SERVERS[sender], json.dumps(message).encode('utf-8'))

    def __json__(self):
        return {
            "address": self.address,
            "id": self.id,
            "state": self.state.name,
            "term": self.term,
            "log": self.log,
            "commit_index": self.commit_index,
            "storage": self.storage,
            "leader_id": self.leader_id,
            "voted_for": self.voted_for,
            "approves": list(self.approves),
            "election_timer": self.election_timer,
            "next_index": self.next_index,
            "match_index": self.match_index,
            "heartbeat_timer": self.heartbeat_timer
        }
