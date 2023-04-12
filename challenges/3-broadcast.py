#!/usr/bin/env python3.11

import logging
import typing

from common import Body, Node, Message

log = logging.getLogger(__name__)

MESSAGES: list[int] = []


class BroadcastMessageBody(Body):
    message: typing.NotRequired[int]


class ReadMessageBody(Body):
    messages: typing.NotRequired[list[int]]


class TopologyMessageBody(Body):
    pass


def handle_broadcast(
    node: Node, message: Message[BroadcastMessageBody],
) -> Message[BroadcastMessageBody]:
    global MESSAGES

    assert "msg_id" in message["body"]
    assert "message" in message["body"]

    MESSAGES.append(message["body"]["message"])

    return Message(
        src=node.node_id,
        dest=message["src"],
        body=BroadcastMessageBody(
            type="broadcast_ok",
            msg_id=message["body"]["msg_id"] + 1,
            in_reply_to=message["body"]["msg_id"],
        )
    )


def handle_read(node: Node, message: Message[ReadMessageBody]) -> Message[ReadMessageBody]:
    assert "msg_id" in message["body"]

    return Message(
        src=node.node_id,
        dest=message["src"],
        body=ReadMessageBody(
            type="read_ok",
            msg_id=message["body"]["msg_id"] + 1,
            in_reply_to=message["body"]["msg_id"],
            messages=MESSAGES,
        )
    )
    

def handle_topology(
    node: Node, message: Message[TopologyMessageBody]
) -> Message[TopologyMessageBody]:
    assert "msg_id" in message["body"]
    return Message(
        src=node.node_id,
        dest=message["src"],
        body=TopologyMessageBody(
            type="topology_ok",
            msg_id=message["body"]["msg_id"] + 1,
            in_reply_to=message["body"]["msg_id"],
        )
    )


def main():
    node = Node()
    node.add_handler("broadcast", handle_broadcast)
    node.add_handler("read", handle_read)
    node.add_handler("topology", handle_topology)
    node.run()


main()
