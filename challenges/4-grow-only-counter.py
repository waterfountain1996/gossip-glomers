#!/usr/bin/env python3.11

import logging
from functools import partial

from common import Body, Node, Message, SeqKVService

log = logging.getLogger(__name__)

node = Node()

kv = SeqKVService(node)


class AddMessageBody(Body):
    delta: int


class ReadMessageBody(Body):
    value: int


@node.handles("add")
def increment_global_counter(node: Node, message: Message[AddMessageBody]) -> None:
    delta = message["body"]["delta"]
    node.state[node.id] = node.state.setdefault(node.id, 0) + delta
    kv.write(key=node.id, value=node.state[node.id])
    node.reply_to(message, Body(type="add_ok"))


@node.handles("read")
def read_counter_value(node: Node, message: Message[Body]) -> None:
    value = node.state.setdefault(node.id, 0)

    def callback(node: Node, response: Message[ReadMessageBody], final: bool = False) -> None:
        nonlocal value

        if response["body"]["type"] == "read_ok":
            value += response["body"]["value"]
        
        if final:
            node.reply_to(message, ReadMessageBody(type="read_ok", value=value))

    others = [o for o in node.nodes if o != node.id]
    for key in others:
        final = key == others[-1]
        kv.read(key=key, callback=partial(callback, final=final))


if __name__ == "__main__":
    node.run()
