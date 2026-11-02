"""Defect: the dead-letter queues are quorum queues with no delivery limit of their own, so RabbitMQ 4's default
(20) applies, and with no dead-letter exchange behind them a message whose DLQ consumer dies 21 times is dropped.
The dead-letter queue is the last stop: nothing should ever silently remove a message from it."""

import asyncio

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractConnection

from orderly.topology import Topology


async def test_a_dead_lettered_message_survives_consumers_that_die_on_it(
    connection: AbstractConnection, channel: AbstractChannel, topology: Topology
) -> None:
    dlq = topology.dead_letter_queue("payment.commands")
    dlx = await channel.get_exchange(topology.dead_letters, ensure=False)
    await dlx.publish(aio_pika.Message(body=b"poison", delivery_mode=aio_pika.DeliveryMode.PERSISTENT), routing_key=dlq)

    async def take_and_die() -> bool:
        """Fetch the message without acknowledging it, then close the channel (an inspector that crashed)."""
        async with connection.channel() as doomed:
            queue = await doomed.get_queue(dlq, ensure=False)
            for _ in range(40):  # a returned message comes back asynchronously
                if await queue.get(no_ack=False, fail=False) is not None:
                    return True
                await asyncio.sleep(0.05)
            return False

    for attempt in range(1, 26):
        assert await take_and_die(), (
            f"the message was dropped from the dead-letter queue after {attempt - 1} deliveries"
        )
