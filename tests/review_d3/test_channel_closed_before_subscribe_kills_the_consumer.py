"""Defect: a channel the broker closes between the consumer's open and its subscription kills the consumer for good.

consumer.py `_consume_until` opens a channel, then calls `set_qos`, `get_queue` and `queue.consume` on it. If the
broker closes that channel in between (any channel-level close: a 404/406 channel error, a stale delivery tag, the
broker closing channels while it restarts), aio-pika/aiormq raise `ChannelInvalidStateError`, which is a
`RuntimeError`, not an `AMQPError`. `Consumer.run` only backs off on AMQPError, ConnectionError, OSError, TimeoutError,
CancelledError and aiormq's bare `Exception()`; `_open_channel` converts a RuntimeError only while the channel is being
opened. So the RuntimeError escapes `run`, the worker task dies, and the service never consumes from its queue again
(the order service stops taking payment replies, the payment service stops charging), until someone restarts it.

Scenario: the consumer runs on a robust connection, as in the services. The first channel it opens is closed by the
broker right after the open (forced here by a channel error on it: a passive declare of a missing queue). Expected: as
for any other broker failure, the consumer backs off, subscribes on a new channel and handles a message published
afterwards. Actual: `run` raises ChannelInvalidStateError and the message is never handled.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
import uuid
from collections.abc import AsyncIterator

import aio_pika
import psycopg
import pytest
from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractRobustConnection
from psycopg_pool import AsyncConnectionPool

from orderly import broker
from orderly.consumer import Connection, Consumer
from orderly.messages import ChargePayment, Message, new_message
from orderly.service import _configure
from orderly.topology import Topology


@pytest.fixture
async def robust() -> AsyncIterator[AbstractRobustConnection]:
    try:
        connection = await asyncio.wait_for(aio_pika.connect_robust(broker.amqp_url()), timeout=5)
    except (OSError, TimeoutError, aio_pika.exceptions.AMQPError) as error:
        pytest.skip(f"RabbitMQ unavailable: {error!r}")
    yield connection
    await asyncio.wait_for(connection.close(), timeout=10)


@pytest.fixture
async def pool(fresh_database: str) -> AsyncIterator[AsyncConnectionPool]:
    with psycopg.connect(fresh_database) as conn:
        conn.execute(
            "CREATE TABLE processed_messages (consumer text, message_id uuid, processed_at timestamptz DEFAULT now(),"
            " PRIMARY KEY (consumer, message_id))"
        )
    pool = AsyncConnectionPool(fresh_database, min_size=1, max_size=4, open=False, configure=_configure)
    await pool.open(wait=True, timeout=10)
    yield pool
    await pool.close()


async def test_a_channel_closed_by_the_broker_before_the_subscription_is_retried(
    pool: AsyncConnectionPool,
    robust: AbstractRobustConnection,
    channel: AbstractChannel,
    topology: Topology,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    real_open = broker.open_channel
    opened = 0

    async def closed_by_the_broker_once(connection: AbstractConnection) -> AbstractChannel:
        nonlocal opened
        opened += 1
        opening = await real_open(connection)
        if opened == 1:
            # A channel error: the broker closes this channel (404 NOT_FOUND), as it closes a channel for any reason.
            with contextlib.suppress(aio_pika.exceptions.ChannelClosed):
                await opening.get_queue(f"missing-{uuid.uuid4().hex}", ensure=True)
        return opening

    monkeypatch.setattr(broker, "open_channel", closed_by_the_broker_once)
    handled: list[uuid.UUID] = []

    async def handle(conn: Connection, message: Message) -> None:
        handled.append(message.message_id)

    consumer = Consumer(
        pool=pool,
        connection=robust,
        topology=topology,
        queue="payment.commands",
        name="review",
        handle=handle,
        accepts=frozenset({"payment.charge"}),
    )
    stop = asyncio.Event()
    task = asyncio.create_task(consumer.run(stop))
    try:
        message = new_message(ChargePayment(order_id=uuid.uuid4(), customer_id="c-1", amount_cents=100, currency="USD"))
        await broker.publish(channel, topology, message)
        deadline = time.monotonic() + 10
        while not handled:
            if task.done():
                pytest.fail(f"the consumer died: {task.exception()!r}")
            assert time.monotonic() < deadline, "the message wasn't handled within 10 s"
            await asyncio.sleep(0.02)
        assert handled == [message.message_id]
    finally:
        stop.set()
        with contextlib.suppress(BaseException):
            await asyncio.wait_for(task, timeout=20)
