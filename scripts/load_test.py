"""The load test (DESIGN.md "Measurement"): the same synthetic orders through the monolith and through the services.

    .venv/bin/python scripts/load_test.py services --orders 10000 --rate 100
    .venv/bin/python scripts/load_test.py monolith --orders 10000 --rate 100
    .venv/bin/python scripts/load_test.py services --orders 10000 --concurrency 64     # closed loop: as fast as it goes

Each run starts from nothing: fresh databases (`orderly_load_<service>` on the local PostgreSQL, dropped and created),
100 products with 100,000 units each, and the target's processes started here (`python -m orderly.service ...`, real
HTTP on 127.0.0.1:8112-8114) with the fake provider's latency and decline ratio fixed, on a topology of its own
(`ORDERLY_TOPOLOGY_PREFIX`), deleted afterwards. The orders are fixed by `--seed`: 1-3 distinct products of the 100, 1-3
units each. A lead-in of `--warmup` orders goes first, paced the same, and isn't counted.

Open loop (`--rate`): order i is due at start + i / rate and sent then, whatever happened to the others, so a slow
target shows up as latency instead of as a slower sender. Closed loop (`--concurrency`): that many clients, each
sending its next order when the last one is answered.

- accept: from sending the request to its response (the services answer 201 once the order and its charge command are
  committed; the monolith answers once the order is confirmed or declined);
- end to end: from the order's due time (open loop) or its send (closed loop) to its outcome. The monolith's outcome is
  its response. The services' is the `order.confirmed` or `order.cancelled` event, read off the broker by this process
  (a queue of its own bound to the events exchange, like any subscriber).

At the end it checks the books in the databases (every order settled once, payments matching outcomes, stock
matching the confirmed orders, one notification per order) and prints one table; `--json` also writes everything.
`--broker-outage START SECONDS` stops the local RabbitMQ node (scripts/rabbitmq.sh) START seconds into the timed
orders and starts it again SECONDS later. All data synthetic.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import csv
import json
import os
import random
import signal
import subprocess
import sys
import time
import uuid
from collections import Counter
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import aio_pika
import httpx
import psycopg
from aio_pika.abc import AbstractIncomingMessage
from psycopg import conninfo, sql

from orderly.broker import amqp_url
from orderly.messages import OrderCancelled, decode
from orderly.migrate import migrate
from orderly.seed import seed
from orderly.topology import QUEUES, Topology

ROOT = Path(__file__).resolve().parents[1]
ADMIN_URL = os.environ.get("ORDERLY_LOAD_ADMIN_URL", "postgresql://localhost:5432/postgres")
TARGETS = {"services": ("orders", "payments", "notifications"), "monolith": ("monolith",)}
PORTS = {"orders": 8112, "payments": 8113, "notifications": 8114, "monolith": 8112}
PRODUCTS = 100
STOCK = 100_000


def percentile(sorted_values: list[float], p: int) -> float:
    """Nearest rank, in integers: the smallest value with at least p% of the values at or below it."""
    rank = max(1, (len(sorted_values) * p + 99) // 100)
    return sorted_values[rank - 1]


def load_url(service: str) -> str:
    return conninfo.make_conninfo(ADMIN_URL, dbname=f"orderly_load_{service}")


def fresh_database(service: str) -> str:
    """Drop and create orderly_load_<service>, migrate it, and stock the catalogue if it has one."""
    name = f"orderly_load_{service}"
    with psycopg.connect(ADMIN_URL, autocommit=True) as admin:
        admin.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE)").format(sql.Identifier(name)))
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    url = load_url(service)
    with psycopg.connect(url, autocommit=True) as conn:
        migrate(conn, service)
    if service in ("orders", "monolith"):
        with psycopg.connect(url) as conn:
            seed(conn, PRODUCTS, STOCK)
    return url


def order_bodies(count: int, rng: random.Random) -> list[dict[str, Any]]:
    bodies = []
    for n in range(count):
        skus = rng.sample(range(1, PRODUCTS + 1), rng.randint(1, 3))
        lines = [{"sku": f"SKU-{s:04d}", "quantity": rng.randint(1, 3)} for s in sorted(skus)]
        bodies.append({"customer_id": f"c-{n}", "email": f"c{n}@example.com", "lines": lines})
    return bodies


@dataclass
class Sample:
    index: int
    due: float
    sent: float = 0.0
    answered: float = 0.0
    status_code: int = 0
    order_id: str = ""
    # When the outcome was known, and what it was: CONFIRMED, or CANCELLED with its reason.
    outcome_at: float = 0.0
    outcome: str = ""
    error: str = ""


@dataclass
class Run:
    target: str
    orders: int
    warmup: int
    rate: float | None
    concurrency: int | None
    provider_ms: float
    decline_ratio: float
    payment_timeout_s: float
    seed: int
    started_at: str = ""
    samples: list[Sample] = field(default_factory=list)
    send_seconds: float = 0.0
    settle_seconds: float = 0.0
    books: dict[str, Any] = field(default_factory=dict)
    outage: dict[str, float] = field(default_factory=dict)


def start_processes(run: Run, prefix: str, log_dir: Path) -> list[subprocess.Popen[bytes]]:
    env = dict(os.environ)
    env.update(
        {
            "ORDERLY_TOPOLOGY_PREFIX": prefix,
            "ORDERLY_PROVIDER_LATENCY_MS": f"{run.provider_ms:g}",
            "ORDERLY_PROVIDER_DECLINE_RATIO": f"{run.decline_ratio:g}",
            "ORDERLY_PAYMENT_TIMEOUT_S": f"{run.payment_timeout_s:g}",
        }
    )
    for service in TARGETS[run.target]:
        env[f"ORDERLY_{service.upper()}_DATABASE_URL"] = load_url(service)
    processes = []
    for service in TARGETS[run.target]:
        log = (log_dir / f"{service}.log").open("wb")
        processes.append(
            subprocess.Popen(
                [sys.executable, "-m", "orderly.service", service],
                cwd=ROOT,
                env=env,
                stdout=log,
                stderr=subprocess.STDOUT,
            )
        )
    return processes


def stop_processes(processes: list[subprocess.Popen[bytes]]) -> None:
    for process in processes:
        if process.poll() is None:
            process.send_signal(signal.SIGINT)
    for process in processes:
        try:
            process.wait(timeout=40)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()


async def wait_healthy(client: httpx.AsyncClient, services: tuple[str, ...], timeout: float = 60.0) -> None:  # noqa: ASYNC109
    deadline = time.monotonic() + timeout
    for service in services:
        while True:
            try:
                if (await client.get(f"http://127.0.0.1:{PORTS[service]}/health")).status_code == 200:
                    break
            except httpx.TransportError:
                pass
            if time.monotonic() > deadline:
                raise SystemExit(f"{service} not healthy after {timeout} s (see its log)")
            await asyncio.sleep(0.1)


class Outcomes:
    """The services' outcomes, read off the events exchange: order id -> (when, outcome)."""

    def __init__(self, topology: Topology) -> None:
        self.topology = topology
        self.queue_name = topology.queue("load.outcomes")
        self.seen: dict[str, tuple[float, str]] = {}
        self.arrived = asyncio.Event()
        self.connection: aio_pika.abc.AbstractRobustConnection | None = None
        self.queue: aio_pika.abc.AbstractQueue | None = None
        self.tag: str | None = None

    async def start(self) -> None:
        # Robust and durable: a broker outage (--broker-outage) mustn't lose the outcomes announced around it.
        self.connection = await aio_pika.connect_robust(f"{amqp_url()}?reconnect_interval=0.5")
        channel = await self.connection.channel()
        await channel.set_qos(prefetch_count=1000)
        queue = await channel.declare_queue(self.queue_name, durable=True, arguments={"x-queue-type": "quorum"})
        for key in ("order.confirmed", "order.cancelled"):
            await queue.bind(self.topology.events, routing_key=key)
        self.queue = queue
        self.tag = await queue.consume(self._on_message)

    async def _on_message(self, incoming: AbstractIncomingMessage) -> None:
        at = time.perf_counter()
        message = decode(incoming.body)
        payload = message.payload
        outcome = f"CANCELLED:{payload.reason}" if isinstance(payload, OrderCancelled) else "CONFIRMED"
        self.seen.setdefault(str(payload.order_id), (at, outcome))
        await incoming.ack()
        self.arrived.set()

    async def close(self) -> None:
        if self.connection is not None:
            if self.queue is not None and self.tag is not None:
                await self.queue.cancel(self.tag)
            async with self.connection.channel() as cleanup:
                await cleanup.queue_delete(self.queue_name)
            await self.connection.close()


async def send(client: httpx.AsyncClient, base: str, sample: Sample, body: dict[str, Any], monolith: bool) -> None:
    sample.sent = time.perf_counter()
    try:
        response = await client.post(f"{base}/orders", json=body)
    except httpx.HTTPError as error:
        sample.error = f"{type(error).__name__}: {error}"
        sample.answered = time.perf_counter()
        return
    sample.answered = time.perf_counter()
    sample.status_code = response.status_code
    if response.status_code in (201, 402):
        placed = response.json()
        sample.order_id = placed["id"]
        if monolith:
            sample.outcome_at = sample.answered
            sample.outcome = (
                "CONFIRMED" if placed["status"] == "CONFIRMED" else f"CANCELLED:{placed.get('cancel_reason')}"
            )
    else:
        sample.error = f"HTTP {response.status_code}: {response.text[:200]}"


async def outage(run: Run, start: float, seconds: float, t0: float) -> None:
    await asyncio.sleep(max(0.0, t0 + start - time.perf_counter()))
    run.outage["stopped_at"] = time.perf_counter() - t0
    stop = await asyncio.create_subprocess_exec(str(ROOT / "scripts/rabbitmq.sh"), "stop", cwd=ROOT)
    await stop.wait()
    run.outage["stop_returned_at"] = time.perf_counter() - t0
    await asyncio.sleep(seconds)
    begin = await asyncio.create_subprocess_exec(str(ROOT / "scripts/rabbitmq.sh"), "start", cwd=ROOT)
    await begin.wait()
    run.outage["started_at"] = time.perf_counter() - t0


async def drive(
    run: Run, bodies: list[dict[str, Any]], outcomes: Outcomes | None, outage_at: tuple[float, float] | None
) -> None:
    monolith = run.target == "monolith"
    base = f"http://127.0.0.1:{PORTS[TARGETS[run.target][0]]}"
    limits = httpx.Limits(max_connections=512, max_keepalive_connections=512)
    total = run.warmup + run.orders
    samples = [Sample(index=n, due=0.0) for n in range(total)]
    run.samples = samples[run.warmup :]
    async with httpx.AsyncClient(limits=limits, timeout=httpx.Timeout(120.0)) as client:
        t0 = time.perf_counter()
        timed_t0 = t0
        tasks: list[asyncio.Task[None]] = []
        chaos: asyncio.Task[None] | None = None
        if run.rate is not None:
            for n in range(total):
                due = t0 + n / run.rate
                if n == run.warmup:
                    timed_t0 = due
                    if outage_at is not None:
                        chaos = asyncio.create_task(outage(run, outage_at[0], outage_at[1], timed_t0))
                delay = due - time.perf_counter()
                if delay > 0:
                    await asyncio.sleep(delay)
                samples[n].due = due
                tasks.append(asyncio.create_task(send(client, base, samples[n], bodies[n], monolith)))
            await asyncio.gather(*tasks)
        else:
            assert run.concurrency is not None
            pending = iter(range(total))
            warm = asyncio.Event()

            async def client_loop() -> None:
                for n in pending:
                    if n == run.warmup:
                        warm.set()
                    samples[n].due = time.perf_counter()
                    await send(client, base, samples[n], bodies[n], monolith)

            async def mark() -> None:
                nonlocal timed_t0, chaos
                await warm.wait()
                timed_t0 = time.perf_counter()
                if outage_at is not None:
                    chaos = asyncio.create_task(outage(run, outage_at[0], outage_at[1], timed_t0))

            marker = asyncio.create_task(mark())
            await asyncio.gather(*(client_loop() for _ in range(run.concurrency)))
            marker.cancel()
        sent_done = time.perf_counter()
        run.send_seconds = sent_done - timed_t0
        if chaos is not None:
            await chaos
        if outcomes is not None:
            wanted = {s.order_id for s in samples if s.order_id}
            deadline = time.monotonic() + max(120.0, run.payment_timeout_s * 3)
            while not wanted <= outcomes.seen.keys():
                if time.monotonic() > deadline:
                    print(f"warning: {len(wanted - outcomes.seen.keys())} outcomes never arrived", file=sys.stderr)
                    break
                outcomes.arrived.clear()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(outcomes.arrived.wait(), timeout=1.0)
            for sample in samples:
                if sample.order_id in outcomes.seen:
                    sample.outcome_at, sample.outcome = outcomes.seen[sample.order_id]
        last = max((s.outcome_at for s in run.samples if s.outcome_at), default=sent_done)
        run.settle_seconds = last - timed_t0


async def queue_depths(topology: Topology) -> dict[str, int]:
    """Messages ready in each of the topology's queues: work, wait (retry) and dead-letter."""
    names = []
    for spec in QUEUES:
        names += [topology.queue(spec.name), topology.dead_letter_queue(spec.name)]
        names += [topology.retry_queue(spec.name, delay) for delay in topology.retry_delays]
    connection = await aio_pika.connect(amqp_url())
    async with connection, connection.channel() as channel:
        return {
            name: (await channel.declare_queue(name, passive=True)).declaration_result.message_count or 0
            for name in names
        }


async def wait_drained(run: Run, topology: Topology, timeout: float = 120.0) -> dict[str, int]:  # noqa: ASYNC109
    """Until nothing is left to do (the services only): both outboxes empty, every queue empty, every order's
    notification recorded, three times in a row half a second apart (a handler in flight holds its delivery unacked,
    which no count shows, for about a provider call). Returns the dead-letter queues' counts."""
    if run.target != "services":
        return {}
    total = run.warmup + run.orders
    deadline = time.monotonic() + timeout
    quiet = 0
    depths: dict[str, int] = {}
    while time.monotonic() < deadline:
        unpublished = 0
        for service in ("orders", "payments"):
            with psycopg.connect(load_url(service)) as conn:
                row = conn.execute("SELECT count(*) FROM outbox WHERE published_at IS NULL").fetchone()
                unpublished += row[0] if row else 1
        with psycopg.connect(load_url("notifications")) as conn:
            row = conn.execute("SELECT count(*) FROM notifications").fetchone()
            told = row[0] if row else -1
        depths = await queue_depths(topology)
        busy = {name: n for name, n in depths.items() if n and not name.endswith(".dlq")}
        quiet = quiet + 1 if unpublished == 0 and not busy and told >= total else 0
        if quiet == 3:
            return {name: n for name, n in depths.items() if name.endswith(".dlq")}
        await asyncio.sleep(0.5)
    print("warning: the services never went quiet", file=sys.stderr)
    return {name: n for name, n in depths.items() if name.endswith(".dlq")}


def books(run: Run) -> dict[str, Any]:
    """What the databases say after the run; `ok` is whether they agree with each other."""
    orders_db = "monolith" if run.target == "monolith" else "orders"
    payments_db = "monolith" if run.target == "monolith" else "payments"
    with psycopg.connect(load_url(orders_db)) as conn:
        statuses: dict[str, int] = dict(
            conn.execute(
                "SELECT status || coalesce(':' || cancel_reason, ''), count(*) FROM orders GROUP BY 1"
            ).fetchall()
        )
        confirmed_cents = conn.execute(
            "SELECT coalesce(sum(total_cents), 0) FROM orders WHERE status = 'CONFIRMED'"
        ).fetchone()
        confirmed_units = conn.execute(
            "SELECT coalesce(sum(l.quantity), 0) FROM order_lines l JOIN orders o ON o.id = l.order_id"
            " WHERE o.status = 'CONFIRMED'"
        ).fetchone()
        stock = conn.execute("SELECT sum(stock), count(*) FROM products").fetchone()
    with psycopg.connect(load_url(payments_db)) as conn:
        payments: dict[str, int] = dict(conn.execute("SELECT status, count(*) FROM payments GROUP BY 1").fetchall())
        charged = conn.execute(
            "SELECT coalesce(sum(amount_cents), 0) FROM payments WHERE status = 'CHARGED'"
        ).fetchone()
    notifications_db = "monolith" if run.target == "monolith" else "notifications"
    with psycopg.connect(load_url(notifications_db)) as conn:
        notifications = conn.execute("SELECT count(*), count(DISTINCT order_id) FROM notifications").fetchone()
    assert confirmed_cents and confirmed_units and stock and charged and notifications
    total = run.warmup + run.orders
    result = {
        "orders": statuses,
        "payments": payments,
        "confirmed_cents": int(confirmed_cents[0]),
        "charged_cents": int(charged[0]),
        "units_taken": PRODUCTS * STOCK - int(stock[0]),
        "confirmed_units": int(confirmed_units[0]),
        "notifications": int(notifications[0]),
        "notified_orders": int(notifications[1]),
    }
    result["ok"] = (
        sum(statuses.values()) == total
        and "AWAITING_PAYMENT" not in statuses
        and result["confirmed_cents"] == result["charged_cents"]
        and result["units_taken"] == result["confirmed_units"]
        and result["notifications"] == result["notified_orders"] == total
    )
    return result


def summarize(run: Run) -> dict[str, Any]:
    timed = run.samples
    ok = [s for s in timed if s.order_id]
    accept = sorted(s.answered - s.sent for s in ok)
    e2e = sorted(s.outcome_at - s.due for s in ok if s.outcome_at)
    summary: dict[str, Any] = {
        "accepted": len(ok),
        "errors": Counter(s.error.split(":")[0] for s in timed if s.error),
        "settled": len(e2e),
        "outcomes": Counter(s.outcome for s in ok if s.outcome),
        "sent_per_s": round(run.orders / run.send_seconds, 1) if run.send_seconds else None,
        "settled_per_s": round(len(e2e) / run.settle_seconds, 1) if run.settle_seconds else None,
    }
    for name, values in (("accept_ms", accept), ("end_to_end_ms", e2e)):
        if values:
            summary[name] = {f"p{p}": round(percentile(values, p) * 1000, 1) for p in (50, 95, 99)} | {
                "max": round(values[-1] * 1000, 1)
            }
    return summary


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("target", choices=TARGETS)
    parser.add_argument("--orders", type=int, default=10_000)
    parser.add_argument("--warmup", type=int, default=200)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--rate", type=float, help="open loop: orders per second")
    mode.add_argument("--concurrency", type=int, help="closed loop: this many clients")
    parser.add_argument("--provider-ms", type=float, default=50.0)
    parser.add_argument("--decline-ratio", type=float, default=0.1)
    parser.add_argument("--payment-timeout", type=float, default=30.0, help="the services' payment deadline, seconds")
    parser.add_argument("--seed", type=int, default=1)
    parser.add_argument("--broker-outage", type=float, nargs=2, metavar=("START", "SECONDS"))
    parser.add_argument("--json", type=Path, help="write the run's settings, summary and books here")
    parser.add_argument("--samples", type=Path, help="write every timed order here, as CSV (times in seconds)")
    parser.add_argument("--log-dir", type=Path, default=ROOT / ".load")
    args = parser.parse_args()
    if args.broker_outage and args.target != "services":
        parser.error("--broker-outage needs the services: the monolith doesn't use the broker")

    run = Run(
        target=args.target,
        orders=args.orders,
        warmup=args.warmup,
        rate=args.rate,
        concurrency=args.concurrency,
        provider_ms=args.provider_ms,
        decline_ratio=args.decline_ratio,
        payment_timeout_s=args.payment_timeout,
        seed=args.seed,
        started_at=time.strftime("%Y-%m-%dT%H:%M:%S%z"),
    )
    bodies = order_bodies(run.warmup + run.orders, random.Random(args.seed))
    for service in TARGETS[run.target]:
        fresh_database(service)
    prefix = f"load-{uuid.uuid4().hex[:8]}."
    topology = Topology(prefix=prefix)
    log_dir = args.log_dir / f"{time.strftime('%Y%m%d-%H%M%S')}-{run.target}"
    log_dir.mkdir(parents=True, exist_ok=True)
    processes = start_processes(run, prefix, log_dir)
    outcomes = None
    try:
        async with httpx.AsyncClient() as probe:
            await wait_healthy(probe, TARGETS[run.target])
        if run.target == "services":
            outcomes = Outcomes(topology)
            await outcomes.start()
        await drive(run, bodies, outcomes, tuple(args.broker_outage) if args.broker_outage else None)
        dead_lettered = await wait_drained(run, topology)
        run.books = books(run)
        if run.target == "services":
            run.books["dead_lettered"] = dead_lettered
            run.books["ok"] = run.books["ok"] and not any(dead_lettered.values())
    finally:
        stop_processes(processes)
        if outcomes is not None:
            await outcomes.close()
        if run.target == "services":
            connection = await aio_pika.connect(amqp_url())
            async with connection, connection.channel() as cleanup:
                await topology.delete(cleanup)
    summary = summarize(run)
    loop = f"open loop at {run.rate:g}/s" if run.rate is not None else f"closed loop, {run.concurrency} clients"
    print(
        f"{run.target}: {run.orders} orders (+{run.warmup} untimed), {loop}, provider {run.provider_ms:g} ms,"
        f" decline {run.decline_ratio:g}, payment timeout {run.payment_timeout_s:g} s, seed {run.seed}"
    )
    print(json.dumps(summary | {"books": run.books, "outage": run.outage}, indent=2, default=dict))
    if args.json is not None:
        args.json.parent.mkdir(parents=True, exist_ok=True)
        settings = {k: v for k, v in asdict(run).items() if k != "samples"}
        args.json.write_text(json.dumps(settings | {"summary": summary}, indent=2, default=dict) + "\n")
    if args.samples is not None:
        args.samples.parent.mkdir(parents=True, exist_ok=True)
        with args.samples.open("w", newline="") as out:
            writer = csv.writer(out)
            writer.writerow(["index", "due", "sent", "answered", "outcome_at", "status_code", "outcome", "error"])
            origin = run.samples[0].due if run.samples else 0.0
            for sample in run.samples:
                times = (sample.due, sample.sent, sample.answered, sample.outcome_at)
                writer.writerow(
                    [
                        sample.index,
                        *(f"{t - origin:.6f}" if t else "" for t in times),
                        sample.status_code,
                        sample.outcome,
                        sample.error,
                    ]
                )


if __name__ == "__main__":
    asyncio.run(main())
