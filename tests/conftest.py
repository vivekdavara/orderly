"""Fixtures for the tests that need RabbitMQ or PostgreSQL.

Locally they use the servers the scripts run (scripts/rabbitmq.sh, scripts/postgres.sh) and are skipped when those
aren't up. CI sets ORDERLY_REQUIRE_SERVICES=1, which turns a missing server into a failure instead of a skip, so a
broken service container can't make the suite pass by skipping half of it.
"""

from __future__ import annotations

import asyncio
import os
import uuid
from collections.abc import AsyncIterator

import aio_pika
import pytest
from aio_pika.abc import AbstractChannel, AbstractConnection

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
