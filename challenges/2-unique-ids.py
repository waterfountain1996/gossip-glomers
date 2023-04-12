#!/usr/bin/env python3.11

import logging
import time
import typing
from datetime import datetime

from common import Body, Node, Message

log = logging.getLogger(__name__)

# Epoch in milliseconds
EPOCH = int(datetime(2023, 1, 1, 0, 0, 0, 0).timestamp()) * 1000

# Per-node sequence counter
SEQUENCE = 1


class GenerateMessageBody(Body):
    id: typing.NotRequired[int] 


def generate_unique_id(node_id: str) -> int:
    global SEQUENCE

    ts = int(time.time()) + EPOCH

    # n0 -> 1, n1 -> 2, ...
    node_number = int(node_id[1:]) + 1

    snowflake: str = "".join(
        [
            format(ts, "042b")[-42:],
            format(node_number, "010b"),
            format((SEQUENCE % (2 ** 12)), "012b"),
        ]
    )
    SEQUENCE += 1
    return int(snowflake, base=2)


def handler(node: Node, message: Message[GenerateMessageBody]) -> Message[GenerateMessageBody]:
    assert "msg_id" in message["body"]
    uid = generate_unique_id(node.node_id)
    return Message(
        src=node.node_id,
        dest=message["src"],
        body=GenerateMessageBody(
            type="generate_ok",
            msg_id=message["body"]["msg_id"] + 1,
            in_reply_to=message["body"]["msg_id"],
            id=uid,
        )
    )


def main():
    node = Node()
    node.add_handler("generate", handler)
    node.run()


if __name__ == "__main__":
    main()
