"""Publishing a message: persistent, mandatory, confirmed.

The outbox relay (day 2) is the only caller in the services; tests use it directly.
"""

from __future__ import annotations

import asyncio
import os
from collections.abc import Iterator
from contextlib import contextmanager
from urllib.parse import parse_qsl, urlsplit

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractIncomingMessage

from orderly.messages import Message, decode, encode
from orderly.topology import Topology

# The local node from scripts/rabbitmq.sh: RabbitMQ's default user, which it accepts only from loopback.
DEFAULT_URL = "amqp://guest:guest@127.0.0.1:5673/"


# How often a service's robust connection tries the broker again after losing it. aio-pika's default is 5 s, which
# a broker outage in the load test (day 4) showed: the broker was back 4.2 s before the services noticed.
RECONNECT_INTERVAL = 1.0


def amqp_url() -> str:
    return os.environ.get("ORDERLY_AMQP_URL", DEFAULT_URL)


def robust_url(url: str, interval: float = RECONNECT_INTERVAL) -> str:
    """`url` with aio-pika's `reconnect_interval` set to `interval`, unless the URL sets its own."""
    if any(key == "reconnect_interval" for key, _ in parse_qsl(urlsplit(url).query)):
        return url
    return f"{url}{'&' if urlsplit(url).query else '?'}reconnect_interval={interval:g}"


@contextmanager
def reconnects(connection: AbstractConnection) -> Iterator[asyncio.Event]:
    """An event set each time `connection` (a robust one) has reconnected and restored its channels.

    A worker backing off after a broker outage waits for it as well as for its backoff, so it starts again when the
    broker is back rather than when its backoff ends (up to 5 s later: day 4's load test). A plain connection never
    reconnects, so its event is never set."""
    event = asyncio.Event()

    def reconnected(*_: object) -> None:
        event.set()

    callbacks = getattr(connection, "reconnect_callbacks", None)
    if callbacks is not None:
        callbacks.add(reconnected)
    try:
        yield event
    finally:
        if callbacks is not None:
            callbacks.discard(reconnected)


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
    # aio-pika only logs a returned message on a channel without on_return_raises: refuse such a channel rather than
    # let an unroutable message look published (the day-1 review's suggestion).
    if not (channel.publisher_confirms and getattr(channel, "on_return_raises", False)):
        raise ValueError("publish needs a channel from open_channel (confirms on, returns raised)")
    exchange_name, routing_key = topology.destination(message.type)
    exchange = await channel.get_exchange(exchange_name, ensure=False)
    await exchange.publish(to_amqp(message), routing_key=routing_key, mandatory=True)


def closed_without_reason(error: BaseException) -> bool:
    """Whether `error` is aiormq's bare `Exception()`: when a connection or channel is closed without a reason, aiormq
    fails every call still waiting on it with `exception or Exception` (aiormq/base.py, FutureStore.reject_all). A relay
    and a consumer died of one when a test closed the broker connection (day 3); it means the broker went away, like
    any ConnectionError. Only that exact one, with no arguments: a subclass or a message is someone's real error."""
    return type(error) is Exception and not error.args


def from_amqp(incoming: AbstractIncomingMessage) -> Message:
    """Decode a delivery's body (the properties are informational; the body is the truth)."""
    return decode(incoming.body)
