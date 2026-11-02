"""What every Orderly service does around its own logic: start up, report health, shut down.

At startup a service migrates its own database (under migrate's advisory lock, so instances starting together don't
both apply a migration) and declares the RabbitMQ topology; if either fails it doesn't start. `/health` answers 200
only when its database answers, its schema is the one this code expects, and its broker connection is open; otherwise
503 with what is wrong. Days 2 and 3 add each service's own routes and consumers to the app this builds.

    python -m orderly.service orders        # on 127.0.0.1:8112 (payments 8113, notifications 8114)
"""

from __future__ import annotations

import argparse
import asyncio
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import aio_pika
import psycopg
from aio_pika.abc import AbstractRobustConnection
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from psycopg_pool import AsyncConnectionPool

from orderly.broker import amqp_url
from orderly.config import SERVICES, database_url
from orderly.migrate import migrate, migrations
from orderly.topology import Topology

PORTS = {"orders": 8112, "payments": 8113, "notifications": 8114}
# How long /health waits for the database before calling it down.
HEALTH_TIMEOUT = 2.0


@dataclass(frozen=True)
class Settings:
    service: str
    database_url: str
    amqp_url: str
    topology: Topology = field(default_factory=Topology)

    @classmethod
    def from_environment(cls, service: str) -> Settings:
        return cls(service=service, database_url=database_url(service), amqp_url=amqp_url())


@dataclass
class Resources:
    """What a running service holds; routes reach it through app.state.resources."""

    settings: Settings
    pool: AsyncConnectionPool
    broker: AbstractRobustConnection


def _migrate(url: str, service: str) -> list[int]:
    with psycopg.connect(url, autocommit=True) as conn:
        return migrate(conn, service)


def create_app(settings: Settings) -> FastAPI:
    if settings.service not in SERVICES:
        raise ValueError(f"unknown service {settings.service!r}")
    expected_schema = len(migrations(settings.service))

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        # Synchronous psycopg in a thread: migrations run once, and migrate() is written for a plain connection.
        await asyncio.to_thread(_migrate, settings.database_url, settings.service)
        broker = await aio_pika.connect_robust(settings.amqp_url)
        try:
            async with broker.channel() as channel:
                await settings.topology.declare(channel)
            pool = AsyncConnectionPool(settings.database_url, min_size=1, max_size=10, open=False)
            await pool.open(wait=True, timeout=10)
            try:
                app.state.resources = Resources(settings, pool, broker)
                yield
            finally:
                await pool.close()
        finally:
            await broker.close()

    app = FastAPI(title=f"Orderly {settings.service} service", lifespan=lifespan)

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
        except (TimeoutError, psycopg.Error) as error:
            problems["database"] = f"{type(error).__name__}: {error}" if str(error) else type(error).__name__
        if resources.broker.is_closed:
            problems["broker"] = "connection closed"
        elif not resources.broker.connected.is_set():
            problems["broker"] = "connection lost, reconnecting"
        report["status"] = "ok" if not problems else "unhealthy"
        report["problems"] = problems
        return JSONResponse(report, status_code=200 if not problems else 503)

    return app


def main() -> None:
    import uvicorn

    parser = argparse.ArgumentParser(prog="python -m orderly.service", description="Run one Orderly service.")
    parser.add_argument("service", choices=SERVICES)
    parser.add_argument("--port", type=int, help="default: 8112 orders, 8113 payments, 8114 notifications")
    args = parser.parse_args()
    app = create_app(Settings.from_environment(args.service))
    uvicorn.run(app, host="127.0.0.1", port=args.port or PORTS[args.service])


if __name__ == "__main__":
    main()
