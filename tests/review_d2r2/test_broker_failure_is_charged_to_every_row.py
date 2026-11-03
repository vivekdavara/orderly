"""Defect: a failure of the broker, not of any row, is charged to every row, so after it ends the outbox waits a minute.

Since 3b571be a RelayError no longer backs the relay off; instead each failed row waits its own 0.5 s, doubling to 60 s.
That is right for a row that can't be published (an unroutable type) but it is also what happens when every publish
fails for a reason common to all of them, on a channel that opens fine: the broker refusing the exchange (deleted,
not yet declared after a broker reset), the channel closed under the batch, every confirm timing out under a memory
alarm. Every claimed row gets an attempt and a growing wait, batch after batch through the whole outbox, with no
relay-level backoff, and once the broker is fine again the rows keep waiting out their per-row delays (8 s, 16 s, ...
up to 60 s) although nothing is wrong with them. Before the fix the relay backed off at most MAX_BACKOFF (5 s) and then
published everything. Consequence: a broker hiccup of a minute delays every message in the outbox by up to another
minute after it ends, and attempts/last_error report every row as stuck.

Here the commands exchange is deleted (publishes fail with the channel closed, 404), until the rows' fifth failure,
then declared again; every row should be published within MAX_BACKOFF + 1 s of the broker being right.

Adapted after the fix: the review's test waited for every row to reach five charged attempts, which the fix no longer
records (a broker failure isn't charged to the rows), so it lets the failure run for 3 s instead (several relay
backoffs) and checks that no row was charged. The assertion after the recovery is the review's.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from collections.abc import AsyncIterator

import psycopg
import pytest
from aio_pika.abc import AbstractChannel, AbstractConnection
from psycopg_pool import AsyncConnectionPool

from orderly import outbox
from orderly.messages import ChargePayment, Message, new_message
from orderly.migrate import migrate
from orderly.outbox import MAX_BACKOFF, Relay
from orderly.topology import Topology


@pytest.fixture
async def pool(fresh_database: str) -> AsyncIterator[AsyncConnectionPool]:
    with psycopg.connect(fresh_database, autocommit=True) as conn:
        migrate(conn, "orders")
    async with AsyncConnectionPool(fresh_database, min_size=1, max_size=4, open=False) as pool:
        yield pool


def charge(n: int) -> Message:
    return new_message(ChargePayment(order_id=uuid.uuid4(), customer_id=f"c-{n}", amount_cents=100 + n, currency="USD"))


async def scalar(pool: AsyncConnectionPool, query: str) -> int:
    async with pool.connection() as conn:
        row = await (await conn.execute(query)).fetchone()
    assert row is not None
    return int(row[0])


async def test_rows_are_published_soon_after_a_broker_wide_failure_ends(
    pool: AsyncConnectionPool, connection: AbstractConnection, channel: AbstractChannel, topology: Topology
) -> None:
    await channel.exchange_delete(topology.commands)  # every payment.charge publish now closes the channel (404)
    async with pool.connection() as conn, conn.transaction():
        for n in range(3):
            await outbox.write(conn, charge(n))
    relay = Relay(pool=pool, connection=connection, topology=topology, poll_interval=0.2, publish_timeout=2.0)
    stop = asyncio.Event()
    task = asyncio.create_task(relay.run(stop))
    try:
        await asyncio.sleep(3.0)  # failing all along: 0.1 + 0.2 + 0.4 + 0.8 + 1.6 s of relay backoff
        assert await scalar(pool, "SELECT max(attempts) FROM outbox") == 0  # the broker's failure, not the rows'
        async with connection.channel() as setup:
            await topology.declare(setup)  # the broker is right again
        recovered = asyncio.get_running_loop().time()
        with contextlib.suppress(TimeoutError):
            async with asyncio.timeout(MAX_BACKOFF + 1):
                while await scalar(pool, "SELECT count(*) FROM outbox WHERE published_at IS NULL") > 0:  # noqa: ASYNC110
                    await asyncio.sleep(0.05)
        waited = asyncio.get_running_loop().time() - recovered
        unpublished = await scalar(pool, "SELECT count(*) FROM outbox WHERE published_at IS NULL")
        wait_left = await scalar(
            pool, "SELECT coalesce(ceil(extract(epoch FROM max(next_attempt_at) - clock_timestamp())), 0) FROM outbox"
        )
        assert unpublished == 0, (
            f"{unpublished} of 3 rows still unpublished {waited:.1f} s after the broker recovered (relay backoff is at"
            f" most {MAX_BACKOFF} s); their own waits have ~{wait_left} s left"
        )
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=15)
