import copy
import json
import logging
import socket
from enum import IntEnum
from threading import Thread, Lock
from time import sleep
from typing import Any, Callable, Optional

SERVERS = {
    2: ("127.0.0.2", 32000),
    3: ("127.0.0.3", 32000),
    4: ("127.0.0.4", 32000),
}


class Timestamps:
    def __init__(self, server_ids: list[int]) -> None:
        self.timestamps: dict[int, int] = {}
        for server_id in server_ids:
            self.timestamps[server_id] = 0

    def __getitem__(self, server_id: int) -> int:
        return self.timestamps[server_id]

    def __setitem__(self, server_id: int, timestamp: int) -> None:
        self.timestamps[server_id] = timestamp

    def __lt__(self, other):
        for timestamp in self.timestamps.keys():
            if self[timestamp] > other[timestamp]:
                return False
        return True

    def __gt__(self, other):
        for timestamp in self.timestamps.keys():
            if self[timestamp] < other[timestamp]:
                return False
        return True

    def __eq__(self, other):
        for timestamp in self.timestamps.keys():
            if self[timestamp] != other[timestamp]:
                return False
        return True

    def concurrent(self, other) -> bool:
        return not self < other and not self > other and not self == other

    def __json__(self):
        return self.timestamps


class MessageType(IntEnum):
    EVENT = 0
    SYNC = 1


class Message:
    def __init__(self, type_: MessageType, sender: int, timestamps: Timestamps, data: Any) -> None:
        self.type: MessageType = type_
        self.id: tuple[int, int] = sender, timestamps[sender]
        self.sender: int = sender
        self.timestamps: Timestamps = timestamps
        self.data: Any = data

    def encode(self) -> bytes:
        return json.dumps({
            'type': self.type.value,
            'id': self.id,
            'timestamps': self.timestamps,
            'data': self.data
        }).encode('utf-8')

    @staticmethod
    def decode(buffer: bytes):
        message = json.loads(buffer.decode('utf-8'))
        type_ = MessageType(message['type'])
        id_ = message['id']
        timestamps = message['timestamps']
        data = message['data']
        return Message(type_, id_, timestamps, data)

    def __json__(self):
        return {
            'type': self.type.value,
            'id': self.id,
            'sender': self.sender,
            'timestamps': self.timestamps,
            'data': self.data
        }


class ReliableCausalBroadcast:
    def __init__(self, id_: int, delivery_callback: Callable):
        self.id: int = id_
        self.servers: dict[int, tuple[str, int]] = SERVERS
        self.ct: int = 0
        self.pending: list[tuple[int, int]] = []
        self.delivered: list[tuple[int, int]] = []
        self.acks: dict[tuple[int, int], set[int]] = {}
        self.mapping: dict[tuple[int, int], Message] = {}
        self.timestamps: Timestamps = Timestamps(list(self.servers.keys()))
        self.delivery_callback: Callable = delivery_callback
        self.lock = Lock()
        Thread(target=self._pollMessages).start()

    def _broadcast(self, message: Message):
        logging.info(f"BROADCAST -> {json.dumps(message)}")
        message = message.encode()
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        for server_id, address in self.servers.items():
            if server_id == self.id:
                continue
            n = sock.sendto(message, address)
            if n != len(message):
                logging.critical(f"Datagram split: {n} sent instead of {len(message)}")

    def broadcastMessage(self, message_type: MessageType, data: Any) -> tuple[int, int]:
        with self.lock:
            self.ct += 1
            tmp: Timestamps = copy.deepcopy(self.timestamps)
            tmp[self.id] = self.ct
            message = Message(message_type, self.id, tmp, data)
        self._broadcast(message)
        return message.id

    def _pollMessages(self):
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
            sock.bind(self.servers[self.id])
            sock.settimeout(1)
            while True:
                try:
                    message, address = sock.recvfrom(4096)
                except socket.timeout:
                    logging.info("")
                    continue
                message = Message.decode(message)
                logging.info(f"RECEIVE <- {json.dumps(message)}")
                if message.type == MessageType.EVENT:
                    self._processMessage(message)
                elif message.type == MessageType.SYNC:
                    self.delivery_callback(message)
        except BaseException as exception:
            logging.exception(exception)

    def _processMessage(self, message: Message):
        if message.id not in self.mapping.keys():
            self.mapping[message.id] = message
            self.acks[message.id] = {message.sender}
        else:
            self.acks[message.id].add(message.sender)
        if message.id not in self.pending and message.id not in self.delivered:
            self.pending += [message.id]
            if message.sender != self.id:
                self._broadcast(message)
        self._deliver()

    def _deliver(self):
        delivered = False
        for message_id in self.pending:
            message: Message = self.mapping[message_id]
            if len(self.acks[message_id]) < (len(self.servers) + 1) / 2. or self.timestamps[message.sender] + 1 != \
                    message.timestamps[message.sender]:
                continue
            deliverable = True
            for server_id in self.servers.keys():
                if server_id != message.sender and self.timestamps[server_id] < message.timestamps[server_id]:
                    deliverable = False
                    break
            if not deliverable:
                continue

            self.delivery_callback(message)

            delivered = True
            self.delivered += [message_id]
            self.pending.remove(message_id)
            with self.lock:
                self.timestamps[message.sender] += 1
            break
        if delivered:
            self._deliver()

    # def on_timer(self, timer_name: str):
    #     pass

    def __json__(self):
        return {
            'id': self.id,
            'ct': self.ct,
            'pending': self.pending,
            'delivered': self.delivered,
            'acks': self.acks,
            'mapping': self.mapping,
            'timestamps': self.timestamps,
        }


class Storage:
    def __init__(self):
        self.inserts: dict[str, tuple[int, Timestamps, int]] = {}
        self.removes: dict[str, tuple[int, Timestamps]] = {}
        self.lock = Lock()

    def get(self, key: str) -> Optional[int]:
        last_insert = self.inserts.get(key, None)
        if not last_insert:
            return None
        last_remove = self.removes.get(key, None)
        if not last_remove:
            return last_insert[2]
        if last_insert[1] < last_remove[1] or last_insert[1].concurrent(last_remove[1]) and last_insert[0] < last_remove[0]:
            return None
        return last_insert[2]

    def put(self, key: str, value: int, sender: int, timestamps: Timestamps):
        last_insert = self.inserts.get(key, None)
        if last_insert:
            current_sender, current_timestamps, _ = last_insert
            if current_timestamps > timestamps or current_timestamps.concurrent(timestamps) and current_sender > sender:
                return
        with self.lock:
            self.inserts[key] = (sender, timestamps, value)

    def delete(self, key: str, sender: int, timestamps: Timestamps):
        last_remove = self.removes.get(key, None)
        if last_remove:
            current_sender, current_timestamps = last_remove
            if current_timestamps > timestamps or current_timestamps.concurrent(timestamps) and current_sender > sender:
                return
        with self.lock:
            self.removes[key] = (sender, timestamps)

    def to_json(self) -> str:
        with self.lock:
            return json.dumps({
                "inserts": {k: (v[0], v[1].timestamps, v[2]) for k, v in self.inserts.items()},
                "removes": {k: (v[0], v[1].timestamps) for k, v in self.removes.items()},
            })

    def from_json(self, data: str):
        data = json.loads(data)
        self.inserts = data["inserts"]
        self.removes = data["removes"]
        for k, v in self.inserts.items():
            ts = Timestamps([])
            ts.timestamps = v[1]
            self.inserts[k] = (v[0], ts, v[2])
        for k, v in self.removes.items():
            ts = Timestamps([])
            ts.timestamps = v[1]
            self.removes[k] = (v[0], ts)

    def __json__(self):
        return {
            "inserts": self.inserts,
            "removes": self.removes,
        }

class Server:
    def __init__(self, server_id: int) -> None:
        self.id: int = server_id
        self.network = ReliableCausalBroadcast(self.id, self.on_message_delivery)
        self.storage: Storage = Storage()

        Thread(target=self.syncer).start()


    def syncer(self):
        while True:
            self.network._broadcast(Message(MessageType.SYNC, self.id, {self.id: 0}, self.storage.to_json()))
            sleep(10)

    def on_get(self, key: str) -> Optional[int]:
        return self.storage.get(key)

    def on_patch(self, pairs: list[tuple[str, Optional[int]]]) -> None:
        message_id: tuple[int, int] = self.network.broadcastMessage(MessageType.EVENT, pairs)
        # while not message_id in self.network.delivered:
        #     sleep(1)

    def on_message_delivery(self, message: Message):
        match message.type:
            case MessageType.EVENT:
                for key, value in message.data.items():
                    if value is None:
                        self.storage.delete(key, message.sender, message.timestamps)
                    else:
                        self.storage.put(key, value, message.sender, message.timestamps)
            case MessageType.SYNC:
                storage: Storage = Storage()
                storage.from_json(message.data)
                self.merge_storage(storage)

    def merge_storage(self, storage: Storage):
        with self.storage.lock:
            for key, value in storage.inserts.items():
                self.storage.put(key, value[2], value[0], value[1])
            for key, value in storage.removes.items():
                self.storage.delete(key, value[0], value[1])

    def __json__(self):
        return {
            "id": self.id,
            "network": self.network,
            "storage": self.storage,
        }