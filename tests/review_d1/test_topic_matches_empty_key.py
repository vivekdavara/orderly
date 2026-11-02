"""Defect: topic_matches says '*' (and '#.*', '*.#') match the empty routing key; RabbitMQ splits '' into zero words,
so none of them match it there. topic_matches claims to be AMQP topic matching and routes() relies on it."""

import asyncio
import uuid

import aio_pika
import pytest
from aio_pika.abc import AbstractChannel

from orderly.topology import topic_matches


@pytest.mark.parametrize("pattern", ["*", "#.*", "*.#"])
def test_a_pattern_needing_one_word_does_not_match_the_empty_key(pattern: str) -> None:
    assert topic_matches(pattern, "") is False


async def test_topic_matches_agrees_with_the_broker_on_the_empty_key(channel: AbstractChannel) -> None:
    patterns = ["*", "#", "#.*", "*.#", ""]
    exchange = await channel.declare_exchange(f"review-{uuid.uuid4().hex[:12]}.topics", "topic", auto_delete=True)
    queues = {}
    for pattern in patterns:
        queue = await channel.declare_queue(exclusive=True)
        await queue.bind(exchange, routing_key=pattern)
        queues[pattern] = queue
    await exchange.publish(aio_pika.Message(body=b"x"), routing_key="")
    await asyncio.sleep(0.3)
    broker = {pattern: (await queue.get(no_ack=True, fail=False)) is not None for pattern, queue in queues.items()}
    await exchange.delete()
    assert {pattern: topic_matches(pattern, "") for pattern in patterns} == broker
