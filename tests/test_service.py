"""The service skeleton: startup (migrate, declare), /health, shutdown, against the real servers."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import psycopg
import pytest
from fastapi import FastAPI
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from orderly.broker import amqp_url
from orderly.migrate import Migration, MigrationError, migrate
from orderly.service import PORTS, Resources, Settings, create_app
from orderly.topology import Topology


@asynccontextmanager
async def running(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    """The app with its lifespan started (httpx's ASGI transport doesn't run lifespans), and a client for it."""
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://orderly.test") as client:
            yield client


def settings(fresh_database: str, topology: Topology, service: str = "orders") -> Settings:
    return Settings(service=service, database_url=fresh_database, amqp_url=amqp_url(), topology=topology)


@pytest.mark.parametrize("service", ["orders", "payments", "notifications"])
async def test_a_started_service_has_migrated_and_is_healthy(
    fresh_database: str, topology: Topology, service: str
) -> None:
    app = create_app(settings(fresh_database, topology, service))
    async with running(app) as client:
        response = await client.get("/health")
        assert response.status_code == 200, response.text
        assert response.json() == {
            "service": service,
            "schema_expected": 1,
            "schema": 1,
            "status": "ok",
            "problems": {},
        }
    with psycopg.connect(fresh_database) as conn:
        assert conn.execute("SELECT version FROM schema_migrations").fetchall() == [(1,)]


async def test_a_schema_behind_the_code_is_unhealthy(fresh_database: str, topology: Topology) -> None:
    app = create_app(settings(fresh_database, topology))
    async with running(app) as client:
        with psycopg.connect(fresh_database, autocommit=True) as conn:
            conn.execute("DELETE FROM schema_migrations")
        response = await client.get("/health")
    assert response.status_code == 503
    assert response.json()["problems"] == {"schema": "database at migration None, this code expects 1"}


async def test_a_lost_database_is_unhealthy(fresh_database: str, topology: Topology) -> None:
    app = create_app(settings(fresh_database, topology))
    async with running(app) as client:
        resources: Resources = app.state.resources
        await resources.pool.close()
        response = await client.get("/health")
    assert response.status_code == 503
    assert list(response.json()["problems"]) == ["database"]
    assert response.json()["problems"]["database"].startswith("PoolClosed")


async def test_a_lost_broker_is_unhealthy(fresh_database: str, topology: Topology) -> None:
    app = create_app(settings(fresh_database, topology))
    async with running(app) as client:
        resources: Resources = app.state.resources
        await resources.broker.close()
        response = await client.get("/health")
    assert response.status_code == 503
    assert response.json()["problems"] == {"broker": "connection closed"}


async def test_a_database_with_edited_history_stops_the_start(fresh_database: str, topology: Topology) -> None:
    with psycopg.connect(fresh_database, autocommit=True) as conn:
        migrate(conn, "orders", [Migration(1, "initial", "SELECT 1")])  # not the real 0001
    app = create_app(settings(fresh_database, topology))
    with pytest.raises(MigrationError, match="migration 1 was applied as initial"):
        async with running(app):
            pass


async def test_an_unreachable_broker_stops_the_start(fresh_database: str, topology: Topology) -> None:
    unreachable = Settings("orders", fresh_database, "amqp://guest:guest@127.0.0.1:1/", topology)
    with pytest.raises(OSError):
        async with running(create_app(unreachable)):
            pass


def test_unknown_services_and_the_port_map() -> None:
    with pytest.raises(ValueError, match="unknown service 'inventory'"):
        create_app(Settings("inventory", "postgresql://x/y", "amqp://x/"))
    assert PORTS == {"orders": 8112, "payments": 8113, "notifications": 8114}


async def test_a_pool_that_cant_fill_is_closed_not_left_retrying(
    fresh_database: str, topology: Topology, monkeypatch: pytest.MonkeyPatch
) -> None:
    closed: list[bool] = []

    class StuckPool(AsyncConnectionPool):
        # The overrides keep psycopg_pool's own signatures, `timeout` included.
        async def open(self, wait: bool = False, timeout: float = 30.0) -> None:  # noqa: ASYNC109
            await super().open(wait=False)
            raise PoolTimeout("pool initialization incomplete after 10.0 sec")

        async def close(self, timeout: float = 5.0) -> None:  # noqa: ASYNC109
            closed.append(True)
            await super().close(timeout)

    monkeypatch.setattr("orderly.service.AsyncConnectionPool", StuckPool)
    with pytest.raises(PoolTimeout):
        async with running(create_app(settings(fresh_database, topology))):
            pass
    assert closed == [True]
