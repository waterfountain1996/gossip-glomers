#!/usr/bin/env python3.11

import logging
import time
import typing
from datetime import datetime

from common import Body, Node, Message

log = logging.getLogger(__name__)

node = Node()

# Epoch in milliseconds
node.state["epoch"] = int(datetime(2023, 1, 1, 0, 0, 0, 0).timestamp()) * 1000

# Per-node sequence counter
node.state["sequence"] = 1


class GenerateMessageBody(Body):
    id: typing.NotRequired[int] 


def generate_unique_id(node: Node) -> int:
    ts = int(time.time()) + node.state["epoch"]

    # n0 -> 1, n1 -> 2, ...
    node_number = int(node.id[1:]) + 1

    snowflake: str = "".join(
        [
            format(ts, "042b")[-42:],
            format(node_number, "010b"),
            format((node.state["sequence"] % (2 ** 12)), "012b"),
        ]
    )
    node.state["sequence"] += 1
    return int(snowflake, base=2)


@node.handles("generate")
def handler(node: Node, message: Message[GenerateMessageBody]):
    uid = generate_unique_id(node)
    node.reply_to(message, GenerateMessageBody(type="generate_ok", id=uid))


if __name__ == "__main__":
    node.run()
