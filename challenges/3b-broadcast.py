#!/usr/bin/env python3.11

import logging
import time
import typing
from itertools import product

from common import Body, Node, Message

log = logging.getLogger(__name__)

node = Node()

# List of all messages seen by this node
node.state["messages"] = set()
node.state["to_propagate"] = set()
node.state["last_propagated"] = time.perf_counter()


class BroadcastMessageBody(Body):
    message: int


class ReadMessageBody(Body):
    messages: typing.NotRequired[list[int]]


class TopologyMessageBody(Body):
    topology: dict[str, list[str]]


@node.before_message
def propagate_messages(node: Node) -> None:
    now = time.perf_counter()
    if now - node.state["last_propagated"] > 1 and node.state["to_propagate"]:
        for message, dest in product(node.state["to_propagate"], node.neighbours):
            node.send_to(
                dest,
                {
                    "type": "broadcast",
                    "msg_id": node.next_msg_id(),
                    "message": message,  # type: ignore
                },
            )
        
        node.state["to_propagate"].clear()
        node.state["last_propagated"] = now



@node.handles("broadcast")
def handle_broadcast(node: Node, request: Message[BroadcastMessageBody]):
    message = request["body"]["message"]
    node.state["messages"].add(message)
    node.state["to_propagate"].add(message)
    node.reply_to(request, Body(type="broadcast_ok"))


@node.handles("read")
def handle_read(node: Node, message: Message[ReadMessageBody]):
    node.reply_to(message, ReadMessageBody(type="read_ok", messages=list(node.state["messages"])))
    

@node.handles("topology")
def handle_topology(node: Node, message: Message[TopologyMessageBody]):
    node.reply_to(message, Body(type="topology_ok"))


if __name__ == "__main__":
    node.run()
