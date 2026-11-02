"""Publishing a message: persistent, mandatory, confirmed.

The outbox relay (day 2) is the only caller in the services; tests use it directly.
"""

from __future__ import annotations

import os

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractIncomingMessage

from orderly.messages import Message, decode, encode
from orderly.topology import Topology

# The local node from scripts/rabbitmq.sh: RabbitMQ's default user, which it accepts only from loopback.
DEFAULT_URL = "amqp://guest:guest@127.0.0.1:5673/"


def amqp_url() -> str:
    return os.environ.get("ORDERLY_AMQP_URL", DEFAULT_URL)


def to_amqp(message: Message) -> aio_pika.Message:
    """The AMQP message: the canonical body, with the envelope's ids repeated as properties for tools and logs."""
    return aio_pika.Message(
        body=encode(message),
        content_type="application/json",
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        message_id=str(message.message_id),
        correlation_id=str(message.correlation_id),
        type=message.type,
        timestamp=message.occurred_at,
    )


async def open_channel(connection: AbstractConnection) -> AbstractChannel:
    """A channel for publishing: confirms on, and a returned (unroutable) message raised as an error.

    aio-pika only logs a returned message unless `on_return_raises` is set, so without it a `mandatory` publish that no
    queue takes would look like a success.
    """
    return await connection.channel(publisher_confirms=True, on_return_raises=True)


async def publish(channel: AbstractChannel, topology: Topology, message: Message) -> None:
    """Publish and wait for the broker's confirm, on a channel from `open_channel`.

    `mandatory` makes a message no queue is bound for come back as an error (aio_pika.exceptions.DeliveryError)
    instead of being dropped.
    """
    exchange_name, routing_key = topology.destination(message.type)
    exchange = await channel.get_exchange(exchange_name, ensure=False)
    await exchange.publish(to_amqp(message), routing_key=routing_key, mandatory=True)


def from_amqp(incoming: AbstractIncomingMessage) -> Message:
    """Decode a delivery's body (the properties are informational; the body is the truth)."""
    return decode(incoming.body)
