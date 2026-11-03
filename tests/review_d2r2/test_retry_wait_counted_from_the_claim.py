"""Defect (review round 2, recast): a failed row's wait was counted from the claim, not from the failure.

`publish_batch` recorded a failed row's next attempt as `now() + 0.5 s * 2^attempts`, and in PostgreSQL `now()` is the
start of the claim's transaction. A failure that comes late in a slow batch (here, an unroutable row in a batch whose
other message takes 1 s to confirm) was recorded as due before it was even recorded, so the very next claim took it
again: its "0.5 s, doubling" wait didn't happen.

The review's own test used a row whose confirm never comes. After its second finding, a missing confirm counts as the
broker's failure, not the row's (the row isn't charged and the relay backs off), so that scenario no longer records a
wait at all; this is the same defect with a failure that is the row's own. The original test is kept in the review
notes.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncIterator

import psycopg
import pytest
from aio_pika.abc import AbstractChannel, AbstractConnection
from psycopg_pool import AsyncConnectionPool

from orderly import outbox
from orderly.messages import ChargePayment, Message, OrderConfirmed, encode, new_message
from orderly.migrate import migrate
from orderly.outbox import Relay, RelayError
from orderly.topology import Topology


@pytest.fixture
async def pool(fresh_database: str) -> AsyncIterator[AsyncConnectionPool]:
    with psycopg.connect(fresh_database, autocommit=True) as conn:
        migrate(conn, "orders")
    async with AsyncConnectionPool(fresh_database, min_size=1, max_size=4, open=False) as pool:
        yield pool


class SlowFor(Relay):
    """A relay whose publish of one message takes a second before it goes out."""

    slow: bytes = b""

    async def _publish(self, channel: AbstractChannel, body: bytes) -> None:
        if body == self.slow:
            await asyncio.sleep(1.0)
        await super()._publish(channel, body)


async def test_a_late_failures_wait_starts_when_it_is_recorded(
    pool: AsyncConnectionPool, connection: AbstractConnection, channel: AbstractChannel, topology: Topology
) -> None:
    commands_queue = await channel.get_queue(topology.queue("payment.commands"), ensure=False)
    await commands_queue.unbind(topology.commands, "payment.charge")  # the charge is the row's own failure
    stuck = new_message(ChargePayment(order_id=uuid.uuid4(), customer_id="c-1", amount_cents=1, currency="USD"))
    slow: Message = new_message(
        OrderConfirmed(order_id=uuid.uuid4(), customer_id="c-1", email="a@example.com", total_cents=1, currency="USD")
    )
    async with pool.connection() as conn, conn.transaction():
        await outbox.write(conn, stuck)
        await outbox.write(conn, slow)
    relay = SlowFor(pool=pool, connection=connection, topology=topology)
    relay.slow = encode(slow)
    with pytest.raises(RelayError):
        await asyncio.wait_for(relay.publish_batch(), timeout=10)  # about 1 s: the slow one is published
    async with pool.connection() as conn:
        row = await (
            await conn.execute("SELECT attempts, next_attempt_at - clock_timestamp() FROM outbox WHERE id = 1")
        ).fetchone()
    assert row is not None
    attempts, left = row
    assert attempts == 1
    assert left.total_seconds() > 0.3, f"the row's 0.5 s wait had {left.total_seconds():.2f} s left when recorded"
    assert await asyncio.wait_for(relay.publish_batch(), timeout=10) == 0  # not due yet
