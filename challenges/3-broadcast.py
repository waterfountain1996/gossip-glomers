#!/usr/bin/env python3.11

import logging
import time
import typing
from collections import defaultdict

from common import Body, Node, Message

log = logging.getLogger(__name__)

node = Node()

# List of all messages seen by this node
node.state["messages"] = set()
node.state["last_propagated"] = time.perf_counter()
node.state["known"] = defaultdict(set)


class BroadcastMessageBody(Body):
    message: int


class ReadMessageBody(Body):
    messages: typing.NotRequired[list[int]]


class TopologyMessageBody(Body):
    topology: dict[str, list[str]]


class GossipMessageBody(Body):
    seen: list[str]


@node.before_message
def propagate_messages(node: Node) -> None:
    now = time.perf_counter()
    if now - node.state["last_propagated"] > .1:
        for dest in node.neighbours:
            known_to: set[str] = node.state["known"][dest]
            notify_of = list(m for m in node.state["messages"] if m not in known_to)
            node.send_to(
                dest=dest,
                body=GossipMessageBody(
                    type="gossip",
                    msg_id=node.next_msg_id(),
                    seen=notify_of,
                )
            )

        node.state["last_propagated"] = now



@node.handles("broadcast")
def handle_broadcast(node: Node, request: Message[BroadcastMessageBody]):
    message = request["body"]["message"]
    node.state["messages"].add(message)
    node.reply_to(request, Body(type="broadcast_ok"))


@node.handles("read")
def handle_read(node: Node, message: Message[ReadMessageBody]):
    node.reply_to(message, ReadMessageBody(type="read_ok", messages=list(node.state["messages"])))
    

@node.handles("topology")
def handle_topology(node: Node, message: Message[TopologyMessageBody]):
    node.reply_to(message, Body(type="topology_ok"))


@node.handles("gossip")
def handle_gossip(node: Node, message: Message[GossipMessageBody]):
    seen = message["body"]["seen"]
    node.state["known"][message["src"]].update(seen)
    node.state["messages"].update(seen)


if __name__ == "__main__":
    node.run()
