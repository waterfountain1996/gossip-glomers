import json
import logging
import os
import sys
import typing

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


Handler = typing.Callable[["Node", Message[_BodyT]], Message[_BodyT]]


class Node:

    __slots__ = ("_handlers", "_id", "_nodes")

    def __init__(self) -> None:
        self._handlers: dict[str, Handler[typing.Any]] = {}

    def _initialize(self, node_id: str, node_ids: list[str]) -> None:
        self._id = node_id
        self._nodes = node_ids

    def _write(self, reply: Message[typing.Any]) -> None:
        json.dump(reply, sys.stdout)
        sys.stdout.write("\n")
        sys.stdout.flush()
        log.debug("Replied with {} message".format(reply["body"]["type"]))

    @property
    def node_id(self) -> str:
        return self._id

    def run(self) -> None:
        try:
            for line in sys.stdin:
                message: Message[typing.Any] = json.loads(line)
                assert "msg_id" in message["body"]

                message_type = message["body"]["type"]
                log.debug(f"Received {message_type} message")

                if message_type == "init":
                    self._initialize(message["body"]["node_id"], message["body"]["node_ids"])
                    log.debug(f"Initialized! ID: {self.node_id}, Nodes: {', '.join(self._nodes)}")
                    reply = Message(
                        src=self.node_id,
                        dest=message["src"],
                        body=Body(
                            type="init_ok",
                            in_reply_to=message["body"]["msg_id"],
                            msg_id=message["body"]["msg_id"] + 1,
                        )
                    )
                else:
                    handler = self._handlers.get(message_type)
                    if handler is None:
                        raise ValueError(f"No handler is set for {message_type} message")

                    reply = handler(self, message)

                self._write(reply)
        except KeyboardInterrupt:
            log.info("Aborted on interrupt")
        except Exception as e:
            log.critical(e, exc_info=True)

    def add_handler(self, message_type: str, handler: Handler[typing.Any]) -> None:
        if message_type in ("init", "error"):
            raise ValueError(f"Cannot set handler for {message_type} message")
        elif message_type in self._handlers:
            raise KeyError(f"Handler for {message_type} message is already set")

        self._handlers[message_type] = handler
