import json
import logging
import os
import select
import sys
import typing
from collections import defaultdict

try:
    DEBUG = bool(int(os.getenv("DEBUG", 0)))
except ValueError:
    DEBUG = False

if DEBUG:
    level = logging.DEBUG
    format = "%(asctime)s [%(levelname)s] (%(filename)s:%(lineno)s) %(message)s"
else:
    level = logging.INFO
    format = "%(asctime)s [%(levelname)s] %(message)s"

logging.basicConfig(
    stream=sys.stderr,
    datefmt="%Y-%m-%d %T",
    format=format,
    level=level,
)

log = logging.getLogger(__name__)

_BodyT = typing.TypeVar("_BodyT", bound="Body")


class Body(typing.TypedDict):
    type: str
    msg_id: typing.NotRequired[int]
    in_reply_to: typing.NotRequired[int]


class InitMessageBody(Body):
    node_id: str
    node_ids: list[str]


class Message(typing.TypedDict, typing.Generic[_BodyT]):
    src: str
    dest: str
    body: _BodyT


Handler = typing.Callable[["Node", Message[_BodyT]], typing.Any]
Callback = typing.Callable[["Node", Message[_BodyT]], typing.Any]


class Node:

    __slots__ = (
        "_handlers",
        "_callbacks",
        "_state",
        "_before_message",
        "_msg_id",
        "_id",
        "_nodes",
        "_topology",
    )

    def __init__(self) -> None:
        self._handlers: dict[str, Handler[typing.Any]] = {}
        self._callbacks: dict[int, Callback[typing.Any]] = {}
        self._before_message: list[typing.Callable[["Node"], typing.Any]] = []
        self._state: dict[str, typing.Any] = {}
        self._msg_id = 0

    def __iter__(self) -> "Node":
        return self

    def __next__(self) -> Message[typing.Any]:
        while True:
            try:
                for f in self._before_message:
                    f(self)

                if select.select([sys.stdin], [], [], 0) != ([sys.stdin], [], []):
                    continue

                line = sys.stdin.readline()
                return json.loads(line)
            except EOFError:
                raise StopIteration()
            except json.JSONDecodeError:
                log.debug("Failed to decode json, skipping line")

    def _initialize(self, node_id: str, node_ids: list[str]) -> None:
        self._id = node_id
        self._nodes = node_ids
        self._topology = self._make_topology(self._nodes)

    @staticmethod
    def _make_topology(nodes: list[str]) -> dict[str, list[str]]:
        topology: dict[str, list[str]] = defaultdict(list)
        for i, node in enumerate(nodes):
            if i > 0:
                topology[node].append(nodes[i - 1])

            if i + 1 < len(nodes):
                topology[node].append(nodes[i + 1])

        return topology

    def _write(self, message: Message[typing.Any]) -> None:
        json.dump(message, sys.stdout)
        sys.stdout.write("\n")
        sys.stdout.flush()
        log.debug("Sent {} message to {}".format(message["body"]["type"], message["dest"]))

    def send_to(self, dest: str, body: Body, callback: typing.Optional[Callback] = None) -> None:
        message = Message(src=self.id, dest=dest, body=body)
        if callback is not None and "msg_id" in body:
            self._callbacks[body["msg_id"]] = callback

        self._write(message)

    def reply_to(self, request: Message[typing.Any], body: Body) -> None:
        assert "msg_id" in request["body"]
        body["in_reply_to"] = request["body"]["msg_id"]
        self.send_to(request["src"], body)

    def next_msg_id(self) -> int:
        self._msg_id += 1
        return self._msg_id

    @property
    def id(self) -> str:
        return self._id

    @property
    def nodes(self) -> list[str]:
        return self._nodes

    @property
    def topology(self) -> dict[str, list[str]]:
        return self._topology

    @topology.setter
    def topology(self, value: dict[str, list[str]]) -> None:
        self._topology = value

    @property
    def neighbours(self) -> list[str]:
        return self.topology[self.id]

    @property
    def state(self) -> dict[str, typing.Any]:
        return self._state

    def run(self) -> None:
        try:
            for message in self:
                message_type = message["body"]["type"]
                log.debug(f"Received {message_type} message from {message['src']}")

                if message_type == "init":
                    self._initialize(message["body"]["node_id"], message["body"]["node_ids"])
                    log.debug(f"Initialized! ID: {self.id}, Nodes: {', '.join(self._nodes)}")
                    self.reply_to(message, Body(type="init_ok"))
                else:
                    if "in_reply_to" in message["body"]:
                        handler = self._callbacks.pop(message["body"]["in_reply_to"], None)
                    else:
                        handler = self._handlers.get(message_type)

                    if handler is None:
                        log.warning(f"No handler is set for {message_type} message")
                        continue

                    handler(self, message)
        except KeyboardInterrupt:
            log.info("Aborted on interrupt")
        except Exception as e:
            log.critical(e, exc_info=True)

    def before_message(
        self, f: typing.Callable[["Node"], typing.Any],
    ) -> typing.Callable[["Node"], typing.Any]:
        """Call this function before any message handler."""
        self._before_message.append(f)
        return f

    def handles(self, message_type: str) -> typing.Callable[[Handler[_BodyT]], Handler[_BodyT]]:
        """Mark function as an RPC handler."""

        def decorator(f: Handler[_BodyT]) -> Handler[_BodyT]:
            if message_type in ("init", "error"):
                raise ValueError(f"Cannot set handler for {message_type} message")
            elif message_type in self._handlers:
                raise KeyError(f"Handler for {message_type} message is already set")

            self._handlers[message_type] = f
            return f

        return decorator


class SeqKVReadBody(Body):
    key: str


class SeqKVWriteBody(SeqKVReadBody):
    value: typing.Any


class SeqKVService:

    __slots__ = ("_node")

    def __init__(self, node: Node) -> None:
        self._node = node

    def read(self, key: str, callback: typing.Optional[Callback[typing.Any]] = None) -> None:
        self._node.send_to(
            dest="seq-kv",
            body=SeqKVReadBody(type="read", msg_id=self._node.next_msg_id(), key=key),
            callback=callback,
        )

    def write(
        self,
        key: str,
        value: typing.Any,
        callback: typing.Optional[Callback[typing.Any]] = None,
    ) -> None:
        self._node.send_to(
            dest="seq-kv",
            body=SeqKVWriteBody(
                type="write", msg_id=self._node.next_msg_id(), key=key, value=value
            ),
            callback=callback
        )
