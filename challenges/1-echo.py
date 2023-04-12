#!/usr/bin/env python3.11

import logging

from common import Body, Node, Message

log = logging.getLogger(__name__)


class EchoMessageBody(Body):
    echo: str


def handler(node: Node, message: Message[EchoMessageBody]) -> Message[EchoMessageBody]:
    assert "msg_id" in message["body"]
    return Message(
        src=node.node_id,
        dest=message["src"],
        body=EchoMessageBody(
            type="echo_ok",
            in_reply_to=message["body"]["msg_id"],
            echo=message["body"]["echo"],
        ),
    )


def main():
    node = Node()
    node.add_handler("echo", handler)
    node.run()


if __name__ == "__main__":
    main()
