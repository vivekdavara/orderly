"""What every Orderly service does around its own logic: start up, report health, shut down.

At startup a service migrates its own database (under migrate's advisory lock, so instances starting together don't
both apply a migration) and declares the RabbitMQ topology; if either fails it doesn't start. `/health` answers 200
only when its database answers, its schema is the one this code expects, and its broker connection is open; otherwise
503 with what is wrong.

Each service adds its own routes and background workers (the outbox relay, consumers) to the app this builds; a worker
gets the service's resources and a stop event, and is awaited at shutdown rather than cancelled, so it can finish what
it started (DESIGN.md "The transactional outbox": a stop never cancels a mark). The monolith has no broker.

    python -m orderly.service orders        # on 127.0.0.1:8112 (payments 8113, notifications 8114)
    python -m orderly.service monolith      # the baseline, on 8112 too: it is the alternative to the split
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import logging
import os
import re
from collections.abc import AsyncIterator, Callable, Coroutine, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import aio_pika
import psycopg
from aio_pika.abc import AbstractRobustConnection
from fastapi import APIRouter, FastAPI
from fastapi.responses import JSONResponse
from psycopg_pool import AsyncConnectionPool

from orderly.broker import amqp_url
from orderly.config import SERVICES, WITH_OUTBOX, WITHOUT_BROKER, database_url
from orderly.migrate import migrate, migrations
from orderly.topology import Topology

PORTS = {"orders": 8112, "payments": 8113, "notifications": 8114, "monolith": 8112}
# How long /health waits for the database before calling it down.
HEALTH_TIMEOUT = 2.0
# How long shutdown waits for the workers to finish after asking them to stop, before cancelling them. Longer than any
# relay batch can last (its wait for a pooled connection, Settings.pool_timeout, plus its wait for confirms,
# Relay.publish_timeout, 5 s each), so a stop never cancels a batch between publishing and marking it: at 10 s, equal
# to the confirm wait then, it did (day-2 review). A test checks the margin.
STOP_TIMEOUT = 30.0
# The longest a request, a relay batch or a handler may wait for a pooled connection. A relay batch's other
# bounded waits (the channel's open 5 s, two table-lock waits 5 s each, the confirms 5 s) come to 20 s, and
# shutdown keeps 5 s of margin on top: a longer pool timeout would let a stop cancel a batch between publish and
# mark (day 2's carry-over, refused at startup since day 3). A test checks the sum.
MAX_POOL_TIMEOUT = 5.0
# What ORDERLY_TOPOLOGY_PREFIX may be: a prefix for exchange and queue names (at most 255 bytes each), nothing odd.
TOPOLOGY_PREFIX = re.compile(r"[A-Za-z0-9._-]{0,64}")
# The module whose create_app builds each service's app.
BUILDERS = {
    "orders": "orderly.orders",
    "payments": "orderly.payments",
    "notifications": "orderly.notifications",
    "monolith": "orderly.monolith",
}

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class Settings:
    service: str
    database_url: str
    amqp_url: str
    topology: Topology = field(default_factory=Topology)
    # The database pool: its size, and how long a request waits for a connection before it is refused with 503.
    pool_size: int = 10
    pool_timeout: float = 5.0

    def __post_init__(self) -> None:
        if self.pool_size < 1:
            raise ValueError(f"pool_size must be at least 1, not {self.pool_size}")
        if not 0 < self.pool_timeout <= MAX_POOL_TIMEOUT:
            raise ValueError(
                f"pool_timeout must be above 0 and at most {MAX_POOL_TIMEOUT} s (shutdown's margin counts on it),"
                f" not {self.pool_timeout}"
            )

    @classmethod
    def from_environment(cls, service: str) -> Settings:
        """ORDERLY_POOL_SIZE and ORDERLY_POOL_TIMEOUT_S override the pool's defaults. ORDERLY_TOPOLOGY_PREFIX puts every
        exchange and queue under a prefix (the load test runs its services on a topology of its own; default none)."""
        prefix = os.environ.get("ORDERLY_TOPOLOGY_PREFIX", "")
        if not TOPOLOGY_PREFIX.fullmatch(prefix):
            raise ValueError(f"ORDERLY_TOPOLOGY_PREFIX must be letters, digits, '.', '-' or '_', not {prefix!r}")
        return cls(
            service=service,
            database_url=database_url(service),
            amqp_url=amqp_url(),
            topology=Topology(prefix=prefix),
            pool_size=int(os.environ.get("ORDERLY_POOL_SIZE", "10")),
            pool_timeout=float(os.environ.get("ORDERLY_POOL_TIMEOUT_S", "5")),
        )


@dataclass
class Resources:
    """What a running service holds; routes reach it through app.state.resources."""

    settings: Settings
    pool: AsyncConnectionPool
    # None for the monolith, which has no broker.
    broker: AbstractRobustConnection | None
    workers: list[asyncio.Task[None]] = field(default_factory=list)


# A background job of a service (the outbox relay, a consumer): runs until `stop` is set, then returns.
Worker = Callable[[Resources, asyncio.Event], Coroutine[Any, Any, None]]


def _migrate(url: str, service: str) -> list[int]:
    with psycopg.connect(url, autocommit=True) as conn:
        return migrate(conn, service)


async def _configure(conn: psycopg.AsyncConnection[Any]) -> None:
    """Pin the server settings the services' code depends on, whatever the database's or role's defaults.

    - READ COMMITTED: the services' SQL (a reservation's `UPDATE ... WHERE stock >= q`, the outbox claim) is written for
      it. Under REPEATABLE READ a reservation racing another for the same product fails with a serialization error
      instead of waiting and re-checking.
    - DateStyle ISO: psycopg parses timestamptz text only in ISO; under 'SQL, DMY' reading any order raised
      NotImplementedError, after the monolith had charged (day-2 review).
    """
    await conn.set_isolation_level(psycopg.IsolationLevel.READ_COMMITTED)
    await conn.execute("SET DateStyle = ISO")
    await conn.commit()  # the pool refuses a connection its configure callback left inside a transaction


@asynccontextmanager
async def _broker(settings: Settings) -> AsyncIterator[AbstractRobustConnection | None]:
    if settings.service in WITHOUT_BROKER:
        yield None
        return
    broker = await aio_pika.connect_robust(settings.amqp_url)
    try:
        async with broker.channel() as channel:
            await settings.topology.declare(channel)
        yield broker
    finally:
        await broker.close()


async def _stop(workers: list[asyncio.Task[None]], stop: asyncio.Event) -> None:
    """Ask the workers to stop and wait for them; cancel only those still running after STOP_TIMEOUT."""
    stop.set()
    if not workers:
        return
    _, pending = await asyncio.wait(workers, timeout=STOP_TIMEOUT)
    for task in pending:
        log.warning("worker %s still running %.0f s after stop; cancelling it", task.get_name(), STOP_TIMEOUT)
        task.cancel()
    await asyncio.gather(*workers, return_exceptions=True)


async def _outbox_report(conn: psycopg.AsyncConnection[Any]) -> dict[str, Any]:
    """What is waiting in the outbox: information, not a problem. A backlog while the broker is down is expected (the
    broker's state is reported on its own), and an instance whose rows can't be published can still take orders, so
    neither makes /health 503; `failing` (rows that failed on their own and wait for a retry) is what to alert on."""
    row = await (
        await conn.execute(
            "SELECT count(*), count(*) FILTER (WHERE attempts > 0), extract(epoch FROM now() - min(created_at))"
            " FROM outbox WHERE published_at IS NULL"
        )
    ).fetchone()
    unpublished, failing, oldest = row if row is not None else (0, 0, None)
    return {
        "unpublished": unpublished,
        "failing": failing,
        "oldest_seconds": None if oldest is None else round(float(oldest), 3),
    }


def create_app(settings: Settings, *, router: APIRouter | None = None, workers: Sequence[Worker] = ()) -> FastAPI:
    if settings.service not in SERVICES:
        raise ValueError(f"unknown service {settings.service!r}")
    expected_schema = len(migrations(settings.service))

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Synchronous psycopg in a thread: migrations run once, and migrate() is written for a plain connection.
        await asyncio.to_thread(_migrate, settings.database_url, settings.service)
        async with _broker(settings) as broker:
            # check: a connection the server has ended (a restart, an idle timeout) is replaced when it is handed out,
            # instead of failing the next /health with "terminating connection" (day-1 review, round 2).
            pool = AsyncConnectionPool(
                settings.database_url,
                min_size=1,
                max_size=settings.pool_size,
                timeout=settings.pool_timeout,
                open=False,
                check=AsyncConnectionPool.check_connection,
                configure=_configure,
            )
            try:
                # Inside the try: a pool whose first connections time out still has workers retrying them, and
                # only close() stops those.
                await pool.open(wait=True, timeout=10)
                resources = Resources(settings, pool, broker)
                app.state.resources = resources
                stop = asyncio.Event()
                for worker in workers:
                    name = f"{settings.service}.{getattr(worker, '__name__', 'worker')}"
                    resources.workers.append(asyncio.create_task(worker(resources, stop), name=name))
                try:
                    yield
                finally:
                    await _stop(resources.workers, stop)
            finally:
                await pool.close()

    app = FastAPI(title=f"Orderly {settings.service} service", lifespan=lifespan)
    if router is not None:
        app.include_router(router)

    @app.get("/health")
    async def health() -> JSONResponse:
        resources: Resources = app.state.resources
        problems: dict[str, str] = {}
        report: dict[str, Any] = {"service": settings.service, "schema_expected": expected_schema}
        try:
            async with asyncio.timeout(HEALTH_TIMEOUT), resources.pool.connection() as conn:
                row = await (await conn.execute("SELECT max(version) FROM schema_migrations")).fetchone()
                schema = row[0] if row else None
                report["schema"] = schema
                if schema != expected_schema:
                    problems["schema"] = f"database at migration {schema}, this code expects {expected_schema}"
                elif settings.service in WITH_OUTBOX:
                    report["outbox"] = await _outbox_report(conn)
        except (TimeoutError, psycopg.Error) as error:
            problems["database"] = f"{type(error).__name__}: {error}" if str(error) else type(error).__name__
        if resources.broker is None:
            pass
        elif resources.broker.is_closed:
            problems["broker"] = "connection closed"
        elif not resources.broker.connected.is_set():
            problems["broker"] = "connection lost, reconnecting"
        # A worker never returns before shutdown: one that has is a relay or consumer no longer doing its job.
        for task in resources.workers:
            if task.done():
                died = None if task.cancelled() else task.exception()
                problems[f"worker {task.get_name()}"] = "stopped" if died is None else f"died: {died!r}"
        report["status"] = "ok" if not problems else "unhealthy"
        report["problems"] = problems
        return JSONResponse(report, status_code=200 if not problems else 503)

    return app


def main() -> None:
    import uvicorn

    parser = argparse.ArgumentParser(prog="python -m orderly.service", description="Run one Orderly service.")
    parser.add_argument("service", choices=SERVICES)
    parser.add_argument("--port", type=int, help="default: 8112 orders and monolith, 8113 payments, 8114 notifications")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    settings = Settings.from_environment(args.service)
    builder = BUILDERS.get(args.service)
    app = create_app(settings) if builder is None else importlib.import_module(builder).create_app(settings)
    uvicorn.run(app, host="127.0.0.1", port=PORTS[args.service] if args.port is None else args.port)


if __name__ == "__main__":
    main()
