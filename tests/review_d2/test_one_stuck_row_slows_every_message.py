"""Defect: one row that keeps failing puts the whole relay on a 5-second backoff that ignores NOTIFY.

`Relay.run` treats a RelayError (some rows of a batch not published, the rest marked) like the broker or the database
being down: it backs off, doubling up to MAX_BACKOFF (5 s), and the backoff waits only for the stop, not for `wake`.
A single row that fails on every batch (here: an unroutable payment.charge, a broker missing a binding) makes every
batch end in a RelayError, so the backoff never resets. Scenario: the broker is otherwise fine, the relay listens with
the order service's 1 s poll interval, and a routable order.confirmed is committed (and NOTIFYed) while the relay sits
in its 5 s backoff. Consequence: every new message waits up to 5 s instead of being published on its NOTIFY (or at worst
after the 1 s poll the design promises), for as long as any one row is stuck; a backlog drains one batch per 5 s.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator

import psycopg
import pytest
from aio_pika.abc import AbstractChannel, AbstractConnection
from psycopg_pool import AsyncConnectionPool

from orderly import outbox
from orderly.messages import ChargePayment, Message, OrderConfirmed, new_message
from orderly.migrate import migrate
from orderly.outbox import Relay
from orderly.topology import Topology


@pytest.fixture
async def pool(fresh_database: str) -> AsyncIterator[AsyncConnectionPool]:
    with psycopg.connect(fresh_database, autocommit=True) as conn:
        migrate(conn, "orders")
    async with AsyncConnectionPool(fresh_database, min_size=1, max_size=4, open=False) as pool:
        yield pool


async def write(pool: AsyncConnectionPool, message: Message) -> None:
    async with pool.connection() as conn, conn.transaction():
        await outbox.write(conn, message)


async def is_published(pool: AsyncConnectionPool, message: Message) -> bool:
    async with pool.connection() as conn:
        query = "SELECT published_at IS NOT NULL FROM outbox WHERE message_id = %s"
        row = await (await conn.execute(query, (message.message_id,))).fetchone()
    return row == (True,)


async def test_a_notified_message_isnt_held_up_by_another_rows_backoff(
    fresh_database: str,
    pool: AsyncConnectionPool,
    connection: AbstractConnection,
    channel: AbstractChannel,
    topology: Topology,
) -> None:
    commands_queue = await channel.get_queue(topology.queue("payment.commands"), ensure=False)
    await commands_queue.unbind(topology.commands, "payment.charge")  # a broker missing a binding
    stuck = ChargePayment(order_id=uuid.uuid4(), customer_id="c-1", amount_cents=1, currency="USD")
    await write(pool, new_message(stuck))
    stop = asyncio.Event()
    relay = Relay(pool=pool, connection=connection, topology=topology, listen_url=fresh_database, poll_interval=1.0)
    task = asyncio.create_task(relay.run(stop))
    try:
        # Backoffs 0.1 + 0.2 + 0.4 + 0.8 + 1.6 + 3.2 = 6.3 s: after that the relay waits 5 s at a time.
        await asyncio.sleep(6.6)
        event = new_message(
            OrderConfirmed(
                order_id=uuid.uuid4(), customer_id="c-2", email="a@example.com", total_cents=1, currency="USD"
            )
        )
        await write(pool, event)
        started = time.monotonic()
        while not await is_published(pool, event) and time.monotonic() - started < 3.0:  # noqa: ASYNC110 (polls the db)
            await asyncio.sleep(0.02)
        waited = time.monotonic() - started
        assert await is_published(pool, event), f"a routable, notified message still unpublished after {waited:.1f} s"
        assert waited < 1.5, waited
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=10)
