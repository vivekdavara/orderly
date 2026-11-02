"""Defect: /health calls the database down when only a pooled connection is stale. The pool has no `check`, so after
the server ends a pooled connection (a PostgreSQL restart, an idle timeout, a failover) the next /health gets that dead
connection, its query fails, and it answers 503 "AdminShutdown" while the database answers fine. The module docstring
promises 200 when "its database answers"; an orchestrator probing /health sees a healthy service as down (once per
stale connection). Fix: AsyncConnectionPool(..., check=AsyncConnectionPool.check_connection), or retry once."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

import httpx
import psycopg
from fastapi import FastAPI

from orderly.broker import amqp_url
from orderly.service import Settings, create_app
from orderly.topology import Topology


@asynccontextmanager
async def running(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://orderly.test") as client:
            yield client


async def test_health_is_ok_after_the_server_ended_a_pooled_connection(fresh_database: str, topology: Topology) -> None:
    app = create_app(Settings("orders", fresh_database, amqp_url(), topology))
    async with running(app) as client:
        assert (await client.get("/health")).status_code == 200
        async with app.state.resources.pool.connection() as conn:
            pid = conn.info.backend_pid
        # Only that one backend (the pool holds one connection, min_size=1).
        with psycopg.connect(fresh_database, autocommit=True) as admin:
            admin.execute("SELECT pg_terminate_backend(%s)", (pid,))
            assert admin.execute("SELECT 1").fetchone() == (1,)  # the database answers
        response = await client.get("/health")
        assert response.status_code == 200, response.text
