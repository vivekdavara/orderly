"""The service skeleton: startup (migrate, declare), /health, shutdown, against the real servers."""

from __future__ import annotations

import asyncio

import psycopg
import psycopg.conninfo
import pytest
from fastapi import APIRouter
from helpers import running
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from orderly import service
from orderly.broker import amqp_url
from orderly.migrate import Migration, MigrationError, migrate, migrations
from orderly.service import PORTS, Resources, Settings, create_app
from orderly.topology import Topology


def settings(fresh_database: str, topology: Topology, service: str = "orders") -> Settings:
    return Settings(service=service, database_url=fresh_database, amqp_url=amqp_url(), topology=topology)


@pytest.mark.parametrize("service", ["orders", "payments", "notifications", "monolith"])
async def test_a_started_service_has_migrated_and_is_healthy(
    fresh_database: str, topology: Topology, service: str
) -> None:
    app = create_app(settings(fresh_database, topology, service))
    async with running(app) as client:
        response = await client.get("/health")
        assert response.status_code == 200, response.text
        latest = len(migrations(service))
        assert response.json() == {
            "service": service,
            "schema_expected": latest,
            "schema": latest,
            "status": "ok",
            "problems": {},
        }
    with psycopg.connect(fresh_database) as conn:
        versions = conn.execute("SELECT version FROM schema_migrations ORDER BY 1").fetchall()
    assert versions == [(v,) for v in range(1, latest + 1)]


async def test_a_schema_behind_the_code_is_unhealthy(fresh_database: str, topology: Topology) -> None:
    app = create_app(settings(fresh_database, topology))
    async with running(app) as client:
        with psycopg.connect(fresh_database, autocommit=True) as conn:
            conn.execute("DELETE FROM schema_migrations")
        response = await client.get("/health")
    assert response.status_code == 503
    assert response.json()["problems"] == {"schema": "database at migration None, this code expects 2"}


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
        assert resources.broker is not None
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
    # The monolith takes the order service's port: it is the alternative to the split, and clients point at :8112
    # either way.
    assert PORTS == {"orders": 8112, "payments": 8113, "notifications": 8114, "monolith": 8112}


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


# The rest kill mutants of service.py that the day-1 review's second round found no test noticed.


async def test_shutdown_closes_the_broker_connection_and_the_pool(fresh_database: str, topology: Topology) -> None:
    app = create_app(settings(fresh_database, topology))
    async with running(app):
        resources: Resources = app.state.resources
    assert resources.broker is not None and resources.broker.is_closed
    assert resources.pool.closed


async def test_a_broker_reconnecting_is_unhealthy(fresh_database: str, topology: Topology) -> None:
    app = create_app(settings(fresh_database, topology))
    async with running(app) as client:
        resources: Resources = app.state.resources
        assert resources.broker is not None
        resources.broker.connected.clear()  # what aio-pika's robust connection does while it reconnects
        try:
            response = await client.get("/health")
        finally:
            resources.broker.connected.set()
    assert response.status_code == 503
    assert response.json()["problems"] == {"broker": "connection lost, reconnecting"}


async def test_a_database_slower_than_the_health_timeout_is_unhealthy(
    fresh_database: str, topology: Topology, monkeypatch: pytest.MonkeyPatch
) -> None:
    assert service.HEALTH_TIMEOUT == 2.0  # DESIGN.md: "answers (within 2 s)"
    app = create_app(settings(fresh_database, topology))
    async with running(app) as client:
        monkeypatch.setattr(service, "HEALTH_TIMEOUT", 1e-6)
        response = await client.get("/health")
    assert response.status_code == 503
    assert response.json()["problems"] == {"database": "TimeoutError"}


async def test_a_schema_ahead_of_the_code_is_unhealthy_too(fresh_database: str, topology: Topology) -> None:
    """A decision, not an accident: an instance only reports healthy on exactly the schema it was written for (it
    refuses to start on a newer one too). Mixed versions during a rolling deploy would need expand/contract
    migrations and a looser rule; DESIGN.md says so."""
    app = create_app(settings(fresh_database, topology))
    async with running(app) as client:
        with psycopg.connect(fresh_database, autocommit=True) as conn:
            conn.execute("INSERT INTO schema_migrations (version, name, sha256) VALUES (3, 'next', 'x')")
        response = await client.get("/health")
    assert response.status_code == 503
    assert response.json()["problems"] == {"schema": "database at migration 3, this code expects 2"}


# Day 2: the brokerless monolith, the pinned isolation level, and background workers.


async def test_the_monolith_never_connects_to_a_broker(fresh_database: str) -> None:
    unreachable = Settings("monolith", fresh_database, "amqp://guest:guest@127.0.0.1:1/")
    app = create_app(unreachable)
    async with running(app) as client:
        response = await client.get("/health")
        assert app.state.resources.broker is None
    assert response.status_code == 200, response.text
    assert response.json()["problems"] == {}


async def test_every_pooled_transaction_is_read_committed(fresh_database: str, topology: Topology) -> None:
    # A database (here, the connection's) defaulting to REPEATABLE READ doesn't change the services' isolation.
    options = str(psycopg.conninfo.conninfo_to_dict(fresh_database)["options"])
    url = psycopg.conninfo.make_conninfo(
        fresh_database, options=f"{options} -c default_transaction_isolation=repeatable\\ read"
    )
    with psycopg.connect(url) as conn:
        assert conn.execute("SHOW transaction_isolation").fetchone() == ("repeatable read",)
    app = create_app(settings(url, topology))
    async with running(app):
        resources: Resources = app.state.resources
        async with resources.pool.connection() as conn:
            assert await (await conn.execute("SHOW transaction_isolation")).fetchone() == ("read committed",)


async def test_workers_run_until_asked_to_stop_and_are_awaited_not_cancelled(
    fresh_database: str, topology: Topology
) -> None:
    events: list[str] = []

    async def worker(resources: Resources, stop: asyncio.Event) -> None:
        events.append(f"started with {resources.settings.service}")
        await stop.wait()
        await asyncio.sleep(0.05)  # finishing what it started: a cancel here would lose the next line
        events.append("finished")

    app = create_app(settings(fresh_database, topology), workers=[worker])
    async with running(app) as client:
        assert (await client.get("/health")).status_code == 200
        [task] = app.state.resources.workers
        assert task.get_name() == "orders.worker"
    assert events == ["started with orders", "finished"]


async def test_a_worker_that_ignores_stop_is_cancelled_after_the_timeout(
    fresh_database: str, topology: Topology, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(service, "STOP_TIMEOUT", 0.1)
    cancelled: list[bool] = []

    async def stubborn(resources: Resources, stop: asyncio.Event) -> None:
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            cancelled.append(True)
            raise

    app = create_app(settings(fresh_database, topology), workers=[stubborn])
    async with running(app):
        pass
    assert cancelled == [True]


async def test_a_dead_worker_makes_the_service_unhealthy(fresh_database: str, topology: Topology) -> None:
    async def broken(resources: Resources, stop: asyncio.Event) -> None:
        raise RuntimeError("relay bug")

    app = create_app(settings(fresh_database, topology), workers=[broken])
    async with running(app) as client:
        await asyncio.sleep(0)  # let it run and die
        response = await client.get("/health")
    assert response.status_code == 503
    assert response.json()["problems"] == {"worker orders.broken": "died: RuntimeError('relay bug')"}


async def test_routes_are_added_to_the_skeleton(fresh_database: str, topology: Topology) -> None:
    router = APIRouter()

    @router.get("/ping")
    async def ping() -> dict[str, str]:
        return {"pong": "yes"}

    async with running(create_app(settings(fresh_database, topology), router=router)) as client:
        assert (await client.get("/ping")).json() == {"pong": "yes"}


def test_shutdown_waits_longer_than_any_relay_batch_can_last() -> None:
    """A relay batch waits at most pool_timeout for a connection and publish_timeout for its confirms; shutdown must
    outwait both, with room for the claim and the mark, or a stop cancels a batch between publishing and marking it
    (the day-2 review's test_stop_timeout_cancels_the_mark shows it at 10 s against 10 s)."""
    from orderly.outbox import Relay

    publish_timeout = Relay.__dataclass_fields__["publish_timeout"].default
    assert isinstance(publish_timeout, float)
    assert 2 * (Settings("orders", "", "").pool_timeout + publish_timeout) <= service.STOP_TIMEOUT
