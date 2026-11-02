"""Fixtures for the tests that need RabbitMQ or PostgreSQL.

Locally they use the servers the scripts run (scripts/rabbitmq.sh, scripts/postgres.sh) and are skipped when those
aren't up. CI sets ORDERLY_REQUIRE_SERVICES=1, which turns a missing server into a failure instead of a skip, so a
broken service container can't make the suite pass by skipping half of it.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import uuid
from collections.abc import AsyncIterator, Iterator

import aio_pika
import psycopg
import pytest
from aio_pika.abc import AbstractChannel, AbstractConnection
from psycopg import conninfo, sql

from orderly.broker import amqp_url, open_channel
from orderly.topology import Topology


def _unavailable(what: str, error: BaseException) -> None:
    message = f"{what} unavailable: {type(error).__name__}: {error}"
    if os.environ.get("ORDERLY_REQUIRE_SERVICES") == "1":
        pytest.fail(message)
    pytest.skip(message)


@pytest.fixture
async def connection() -> AsyncIterator[AbstractConnection]:
    try:
        connection = await asyncio.wait_for(aio_pika.connect(amqp_url()), timeout=5)
    except (OSError, TimeoutError, aio_pika.exceptions.AMQPError) as error:
        _unavailable(f"RabbitMQ at {amqp_url()}", error)
    async with connection:
        yield connection


@pytest.fixture
async def channel(connection: AbstractConnection) -> AsyncIterator[AbstractChannel]:
    async with await open_channel(connection) as channel:
        yield channel


@pytest.fixture
async def topology(connection: AbstractConnection) -> AsyncIterator[Topology]:
    """A private copy of the whole topology under a random prefix, deleted afterwards."""
    topology = Topology(prefix=f"test-{uuid.uuid4().hex[:12]}.")
    async with connection.channel() as setup:
        await topology.declare(setup)
    yield topology
    async with connection.channel() as cleanup:
        await topology.delete(cleanup)


def test_database_url() -> str:
    return os.environ.get("ORDERLY_TEST_DATABASE_URL", "postgresql://localhost:5432/orderly_test")


@pytest.fixture(scope="session")
def test_database() -> str:
    """The database the tests use (created if missing); each test gets its own schema in it."""
    url = test_database_url()
    try:
        psycopg.connect(url, connect_timeout=5).close()
    except psycopg.OperationalError as error:
        if "does not exist" not in str(error):
            _unavailable(f"PostgreSQL at {url}", error)
        name = conninfo.conninfo_to_dict(url)["dbname"]
        admin_url = conninfo.make_conninfo(url, dbname="postgres")
        # DuplicateDatabase: another run made it just now.
        with (
            psycopg.connect(admin_url, autocommit=True) as admin,
            contextlib.suppress(psycopg.errors.DuplicateDatabase),
        ):
            admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(str(name))))
    return url


@pytest.fixture
def fresh_database(test_database: str) -> Iterator[str]:
    """A URL whose connections see an empty database: a new schema, first on their search_path, dropped afterwards.

    A schema rather than a database because PostgreSQL 14's CREATE DATABASE forces a checkpoint: 174-993 ms here
    against about 1 ms for CREATE SCHEMA.
    """
    schema = f"t_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(test_database, autocommit=True) as conn:
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        yield conninfo.make_conninfo(test_database, options=f"-c search_path={schema}")
    finally:
        with psycopg.connect(test_database, autocommit=True) as conn:
            conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))
