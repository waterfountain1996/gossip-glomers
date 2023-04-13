#!/usr/bin/env python3.11

import logging

from common import Body, Node, Message

log = logging.getLogger(__name__)

node = Node()


class EchoMessageBody(Body):
    echo: str


@node.handles("echo")
def handler(node: Node, message: Message[EchoMessageBody]) -> None:
    node.reply_to(message, EchoMessageBody(type="echo_ok", echo=message["body"]["echo"]))


if __name__ == "__main__":
    node.run()
