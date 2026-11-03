"""How fast the outbox relay publishes, by batch size, and what publishing a batch before awaiting its confirms buys.

    .venv/bin/python scripts/measure_relay.py [--messages 5000] [--runs 3]

Part 1: N `payment.charge` rows written to a fresh outbox, then `Relay.publish_batch` until nothing is left, for batch
sizes 1, 10, 100 and 500: messages per second from the first claim to the last mark. Part 2, the broker alone: N
messages published one at a time (each confirm awaited before the next) against all N published before any confirm is
awaited. Synthetic messages; a private schema in the test database and a private topology, both removed afterwards.
"""

from __future__ import annotations

import argparse
import asyncio
import statistics
import time
import uuid

import aio_pika
import psycopg
from psycopg import conninfo, sql
from psycopg_pool import AsyncConnectionPool

from orderly import broker, outbox
from orderly.broker import amqp_url
from orderly.messages import ChargePayment, Message, new_message
from orderly.migrate import migrate
from orderly.outbox import Relay
from orderly.topology import Topology

TEST_DATABASE = "postgresql://localhost:5432/orderly_test"


def charges(count: int) -> list[Message]:
    return [
        new_message(ChargePayment(order_id=uuid.uuid4(), customer_id="c-1", amount_cents=100, currency="USD"))
        for _ in range(count)
    ]


async def relay_rate(url: str, connection: aio_pika.abc.AbstractConnection, count: int, batch_size: int) -> float:
    topology = Topology(prefix=f"measure-{uuid.uuid4().hex[:8]}.")
    async with connection.channel() as setup:
        await topology.declare(setup)
    try:
        async with AsyncConnectionPool(url, min_size=1, max_size=2, open=False) as pool:
            async with pool.connection() as conn, conn.transaction():
                await conn.execute("TRUNCATE outbox")
                for message in charges(count):
                    await outbox.write(conn, message)
            relay = Relay(pool=pool, connection=connection, topology=topology, batch_size=batch_size)
            started = time.perf_counter()
            published = 0
            while n := await relay.publish_batch():
                published += n
            elapsed = time.perf_counter() - started
            assert published == count, (published, count)
            return count / elapsed
    finally:
        async with connection.channel() as cleanup:
            await topology.delete(cleanup)


async def broker_rate(connection: aio_pika.abc.AbstractConnection, count: int, pipelined: bool) -> float:
    topology = Topology(prefix=f"measure-{uuid.uuid4().hex[:8]}.")
    async with connection.channel() as setup:
        await topology.declare(setup)
    try:
        channel = await broker.open_channel(connection)
        messages = charges(count)
        started = time.perf_counter()
        if pipelined:
            await asyncio.gather(*(broker.publish(channel, topology, m) for m in messages))
        else:
            for m in messages:
                await broker.publish(channel, topology, m)
        elapsed = time.perf_counter() - started
        await channel.close()
        return count / elapsed
    finally:
        async with connection.channel() as cleanup:
            await topology.delete(cleanup)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--messages", type=int, default=5000)
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()
    schema = f"measure_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(TEST_DATABASE, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    url = conninfo.make_conninfo(TEST_DATABASE, options=f"-c search_path={schema}")
    connection = await aio_pika.connect(amqp_url())
    try:
        with psycopg.connect(url, autocommit=True) as conn:
            migrate(conn, "orders")
        print(f"relay, {args.messages} messages, {args.runs} runs (messages/s, each run, then the median):")
        for batch_size in (1, 10, 100, 500):
            rates = [await relay_rate(url, connection, args.messages, batch_size) for _ in range(args.runs)]
            listed = ", ".join(f"{r:,.0f}" for r in rates)
            print(f"  batch {batch_size:>3}: {listed}  median {statistics.median(rates):,.0f}")
        print(f"broker alone, {args.messages} confirmed publishes:")
        for pipelined in (False, True):
            rates = [await broker_rate(connection, args.messages, pipelined) for _ in range(args.runs)]
            label = "all, then confirms " if pipelined else "one confirm at a time"
            listed = ", ".join(f"{r:,.0f}" for r in rates)
            print(f"  {label}: {listed}  median {statistics.median(rates):,.0f}")
    finally:
        await connection.close()
        with psycopg.connect(TEST_DATABASE, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


if __name__ == "__main__":
    asyncio.run(main())
