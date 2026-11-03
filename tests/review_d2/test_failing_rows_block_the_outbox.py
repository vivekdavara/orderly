"""Defect: rows that can't be published block every row behind them once there are `batch_size` of them.

The relay claims `... WHERE published_at IS NULL ORDER BY id LIMIT batch_size`, and a row whose publish fails stays
unpublished at the head of that order. outbox.py and DESIGN.md say a failed row "is retried with the next batch; the
others in its batch are still marked", i.e. one bad row doesn't hold the others up. But once `batch_size` failing rows
have accumulated (here: payment.charge unroutable because the broker lost its binding, the scenario the suite already
uses), every batch claims exactly those rows again, fails them all, and never reaches the newer rows: an
order.confirmed event written after them is never published, for as long as the failing rows keep failing (forever
for a row that can never be published). Consequence: one stuck message type silently stops the whole outbox.
"""

from __future__ import annotations

import contextlib
import uuid
from collections.abc import AsyncIterator

import psycopg
import pytest
from aio_pika.abc import AbstractChannel, AbstractConnection
from psycopg_pool import AsyncConnectionPool

from orderly import outbox
from orderly.messages import ChargePayment, Message, OrderConfirmed, new_message
from orderly.migrate import migrate
from orderly.outbox import Relay, RelayError
from orderly.topology import Topology


@pytest.fixture
async def pool(fresh_database: str) -> AsyncIterator[AsyncConnectionPool]:
    with psycopg.connect(fresh_database, autocommit=True) as conn:
        migrate(conn, "orders")
    async with AsyncConnectionPool(fresh_database, min_size=1, max_size=4, open=False) as pool:
        yield pool


def charge(n: int) -> Message:
    return new_message(ChargePayment(order_id=uuid.uuid4(), customer_id=f"c-{n}", amount_cents=100 + n, currency="USD"))


def confirmed() -> Message:
    payload = OrderConfirmed(
        order_id=uuid.uuid4(), customer_id="c-1", email="a@example.com", total_cents=1, currency="USD"
    )
    return new_message(payload)


async def test_a_batch_full_of_failing_rows_doesnt_stop_the_rows_behind_it(
    pool: AsyncConnectionPool, connection: AbstractConnection, channel: AbstractChannel, topology: Topology
) -> None:
    commands_queue = await channel.get_queue(topology.queue("payment.commands"), ensure=False)
    await commands_queue.unbind(topology.commands, "payment.charge")  # a broker missing a binding
    event = confirmed()
    async with pool.connection() as conn, conn.transaction():
        for n in range(10):
            await outbox.write(conn, charge(n))
        await outbox.write(conn, event)
    relay = Relay(pool=pool, connection=connection, topology=topology, batch_size=10)
    for _ in range(5):
        with contextlib.suppress(RelayError):  # the unroutable rows, expected
            await relay.publish_batch()
    async with pool.connection() as conn:
        row = await (
            await conn.execute("SELECT published_at IS NOT NULL FROM outbox WHERE message_id = %s", (event.message_id,))
        ).fetchone()
    assert row == (True,), "the order.confirmed event behind 10 failing rows was never published in 5 batches"
