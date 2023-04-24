#!/usr/bin/env python3.11

import logging
import typing

from common import Body, Node, Message

log = logging.getLogger(__name__)

node = Node()

# List of all messages seen by this node
node.state["messages"] = []


class BroadcastMessageBody(Body):
    message: int


class ReadMessageBody(Body):
    messages: typing.NotRequired[list[int]]


class TopologyMessageBody(Body):
    pass


@node.handles("broadcast")
def handle_broadcast(node: Node, message: Message[BroadcastMessageBody]):
    node.state["messages"].append(message["body"]["message"])
    node.reply_to(message, Body(type="broadcast_ok"))


@node.handles("read")
def handle_read(node: Node, message: Message[ReadMessageBody]):
    node.reply_to(message, ReadMessageBody(type="read_ok", messages=node.state["messages"]))
    

@node.handles("topology")
def handle_topology(node: Node, message: Message[TopologyMessageBody]):
    node.reply_to(message, TopologyMessageBody(type="topology_ok"))


if __name__ == "__main__":
    node.run()
