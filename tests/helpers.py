"""Helpers the integration tests share: running an app with its lifespan, and stocking a catalogue."""

from __future__ import annotations

from collections.abc import AsyncIterator, Iterable
from contextlib import asynccontextmanager

import httpx
import psycopg
from fastapi import FastAPI


@asynccontextmanager
async def running(app: FastAPI) -> AsyncIterator[httpx.AsyncClient]:
    """The app with its lifespan started (httpx's ASGI transport doesn't run lifespans), and a client for it."""
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://orderly.test") as client:
            yield client


def stock(url: str, products: Iterable[tuple[str, int, int] | tuple[str, int, int, str]]) -> None:
    """Insert products as (sku, price_cents, stock[, currency]) into a migrated database."""
    with psycopg.connect(url) as conn:
        for sku, price, count, *currency in products:
            conn.execute(
                "INSERT INTO products (sku, name, price_cents, currency, stock) VALUES (%s, %s, %s, %s, %s)",
                (sku, f"Product {sku}", price, currency[0] if currency else "USD", count),
            )


def stock_of(url: str, sku: str) -> int:
    with psycopg.connect(url) as conn:
        row = conn.execute("SELECT stock FROM products WHERE sku = %s", (sku,)).fetchone()
    assert row is not None
    return int(row[0])


def count(url: str, table: str) -> int:
    with psycopg.connect(url) as conn:
        row = conn.execute(f"SELECT count(*) FROM {table}").fetchone()
    assert row is not None
    return int(row[0])
