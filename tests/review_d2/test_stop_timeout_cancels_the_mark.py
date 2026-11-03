"""Defect: a service stop can cancel the relay between publishing a batch and marking it, re-sending what was confirmed.

DESIGN.md: "A relay stop must not cancel the mark of a batch it just published ... the publish-and-mark of a batch
finishes". The relay only checks its stop event between batches, but a batch can last `publish_timeout` (10 s, the
default the order service uses) plus its claim and mark, while `service._stop` cancels any worker still running
STOP_TIMEOUT (also 10 s) after the stop. Scenario: the stop arrives while a batch is starting (here, while it opens its
channel); one of the batch's messages is confirmed at once, another's confirm never comes. The relay waits its full
10 s for the missing confirm, measured from after the claim, so the stop's 10 s run out first: the relay is cancelled
inside the batch, the claim's transaction rolls back, and the message the broker *did* confirm is left unmarked.
Consequence: it is published again after the restart, a duplicate outside the documented crash case, exactly what the
design says a stop must not do.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator
from typing import Any

import psycopg
import pytest
from aio_pika.abc import AbstractChannel, AbstractConnection
from psycopg_pool import AsyncConnectionPool

from orderly import broker, outbox, service
from orderly.messages import ChargePayment, new_message
from orderly.migrate import migrate
from orderly.outbox import Relay
from orderly.topology import Topology


@pytest.fixture
async def pool(fresh_database: str) -> AsyncIterator[AsyncConnectionPool]:
    with psycopg.connect(fresh_database, autocommit=True) as conn:
        migrate(conn, "orders")
    async with AsyncConnectionPool(fresh_database, min_size=1, max_size=4, open=False) as pool:
        yield pool


async def test_a_stop_never_cancels_the_mark_of_a_published_message(
    pool: AsyncConnectionPool,
    connection: AbstractConnection,
    channel: AbstractChannel,
    topology: Topology,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    confirmed_one, unconfirmed = (
        new_message(ChargePayment(order_id=uuid.uuid4(), customer_id="c-1", amount_cents=100, currency="USD"))
        for _ in range(2)
    )
    async with pool.connection() as conn, conn.transaction():
        await outbox.write(conn, confirmed_one)
        await outbox.write(conn, unconfirmed)

    opening = asyncio.Event()
    real_open, real_publish = broker.open_channel, broker.publish

    async def slow_open(conn: AbstractConnection) -> AbstractChannel:
        opening.set()
        await asyncio.sleep(0.05)  # a channel open is a broker round trip
        return await real_open(conn)

    async def publish(channel: AbstractChannel, topology: Topology, message: Any) -> None:
        if message.message_id == unconfirmed.message_id:
            await asyncio.sleep(3600)  # its confirm never arrives
        await real_publish(channel, topology, message)

    monkeypatch.setattr(broker, "open_channel", slow_open)
    monkeypatch.setattr(broker, "publish", publish)
    # As orders.create_app builds it: the default publish_timeout.
    relay = Relay(pool=pool, connection=connection, topology=topology, poll_interval=30.0)
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop), name="orders.relay")
    await asyncio.wait_for(opening.wait(), timeout=5)
    # What the service's lifespan does at shutdown: set stop, wait STOP_TIMEOUT, cancel what is still running.
    await asyncio.wait_for(service._stop([task], stop), timeout=service.STOP_TIMEOUT + 5)

    async with pool.connection() as conn:
        rows = await (
            await conn.execute("SELECT message_id, published_at IS NOT NULL FROM outbox ORDER BY id")
        ).fetchall()
    queue = await channel.get_queue(topology.queue("payment.commands"), ensure=False)
    delivered = await queue.get(fail=False, no_ack=True)
    assert delivered is not None, "the first message should have reached the queue (it was confirmed)"
    assert rows == [(confirmed_one.message_id, True), (unconfirmed.message_id, False)], (
        "the confirmed message was left unmarked: the stop's cancel interrupted the batch before its mark"
    )
