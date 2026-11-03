"""Order intake: the monolith against the order service, every order for one product, the provider's latency fixed.

    .venv/bin/python scripts/measure_intake.py [--orders 400] [--provider-ms 50] [--concurrency 1 20]

For each app and each concurrency, N orders of one unit of the same product (a hot product: the case where the
monolith's row lock during the provider call matters) from C concurrent clients; per-request latency (p50, p95, p99)
and orders per second. In process, through httpx's ASGI transport (no HTTP server or network in the path). The
monolith calls the fake provider with the latency given; the order service answers after writing the outbox (its
relay runs and publishes meanwhile). Synthetic data; a private schema and topology, both removed afterwards.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
import uuid
from pathlib import Path

import httpx
import psycopg
from psycopg import conninfo, sql

sys.path.insert(0, str(Path(__file__).parents[1] / "tests"))
import aio_pika
from helpers import running, stock

from orderly import monolith, orders
from orderly.broker import amqp_url
from orderly.fakes import FakeProvider
from orderly.service import Settings
from orderly.topology import Topology

TEST_DATABASE = "postgresql://localhost:5432/orderly_test"


def percentile(sorted_values: list[float], p: float) -> float:
    """Nearest rank: the smallest value with at least p% of the values at or below it."""
    rank = max(1, -(-len(sorted_values) * p // 100))  # ceil(n * p / 100), at least 1
    return sorted_values[int(rank) - 1]


async def run(app_name: str, count: int, concurrency: int, provider_ms: float) -> tuple[list[float], float]:
    schema = f"measure_{uuid.uuid4().hex[:8]}"
    with psycopg.connect(TEST_DATABASE, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    url = conninfo.make_conninfo(TEST_DATABASE, options=f"-c search_path={schema}")
    topology = Topology(prefix=f"measure-{uuid.uuid4().hex[:8]}.")
    try:
        if app_name == "monolith":
            app = monolith.create_app(
                Settings("monolith", url, "amqp://unused/"), provider=FakeProvider(latency=provider_ms / 1000)
            )
        else:
            app = orders.create_app(Settings("orders", url, amqp_url(), topology))
        async with running(app) as client:
            stock(url, [("HOT-1", 1000, count)])
            body = {"customer_id": "c-1", "email": "a@example.com", "lines": [{"sku": "HOT-1", "quantity": 1}]}
            # Warm up the pool's connections and the code paths, untimed (then restock).
            for _ in range(5):
                (await client.post("/orders", json=body)).raise_for_status()
            with psycopg.connect(url) as conn:
                conn.execute("UPDATE products SET stock = %s", (count,))
            queue: asyncio.Queue[int] = asyncio.Queue()
            for n in range(count):
                queue.put_nowait(n)
            latencies: list[float] = []

            async def client_loop(http: httpx.AsyncClient) -> None:
                while True:
                    try:
                        queue.get_nowait()
                    except asyncio.QueueEmpty:
                        return
                    started = time.perf_counter()
                    response = await http.post("/orders", json=body)
                    latencies.append(time.perf_counter() - started)
                    assert response.status_code == 201, response.text

            started = time.perf_counter()
            await asyncio.gather(*(client_loop(client) for _ in range(concurrency)))
            elapsed = time.perf_counter() - started
        return sorted(latencies), count / elapsed
    finally:
        if app_name == "orders":
            connection = await aio_pika.connect(amqp_url())
            async with connection, connection.channel() as cleanup:
                await topology.delete(cleanup)
        with psycopg.connect(TEST_DATABASE, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--orders", type=int, default=400)
    parser.add_argument("--provider-ms", type=float, default=50.0)
    parser.add_argument("--concurrency", type=int, nargs="+", default=[1, 20])
    args = parser.parse_args()
    print(f"{args.orders} orders of one hot product; the monolith's provider takes {args.provider_ms:g} ms")
    print(f"{'app':<9} {'clients':>7} {'p50 ms':>8} {'p95 ms':>8} {'p99 ms':>8} {'orders/s':>9}")
    for concurrency in args.concurrency:
        for app_name in ("monolith", "orders"):
            latencies, rate = await run(app_name, args.orders, concurrency, args.provider_ms)
            p50, p95, p99 = (percentile(latencies, p) * 1000 for p in (50, 95, 99))
            print(f"{app_name:<9} {concurrency:>7} {p50:>8.1f} {p95:>8.1f} {p99:>8.1f} {rate:>9.1f}")


if __name__ == "__main__":
    asyncio.run(main())
