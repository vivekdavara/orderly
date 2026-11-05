"""The three services running together in one process, for the tests of whole sagas (POST /orders to the e-mail).

Each service gets its own schema in the test database (the `databases` fixture) and all three share one prefixed
topology on the broker; the payment provider and the mailer are fakes the test holds, so it can read the books.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import Counter
from collections.abc import AsyncIterator, Callable
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import httpx
import psycopg
from helpers import running, stock

from orderly import notifications, orders, payments
from orderly.broker import amqp_url
from orderly.fakes import Charge, FakeMailer, FakeProvider, ProviderUnavailable, Refund
from orderly.payments import Provider
from orderly.service import Settings
from orderly.topology import Topology


@dataclass
class Shop:
    orders: httpx.AsyncClient
    payments: httpx.AsyncClient
    notifications: httpx.AsyncClient
    databases: dict[str, str]
    provider: Provider
    mailer: FakeMailer

    async def order(self, sku: str = "WIDGET-1", quantity: int = 1, customer: str = "c-42") -> dict[str, Any]:
        body = {"customer_id": customer, "email": "ada@example.com", "lines": [{"sku": sku, "quantity": quantity}]}
        response = await self.orders.post("/orders", json=body)
        assert response.status_code == 201, response.text
        placed: dict[str, Any] = response.json()
        return placed

    async def settled(self, order_ids: list[str], timeout: float = 20.0) -> list[dict[str, Any]]:  # noqa: ASYNC109
        """The orders once each is CONFIRMED or CANCELLED and its customer has been told."""
        deadline = time.monotonic() + timeout
        while True:
            found = [(await self.orders.get(f"/orders/{order_id}")).json() for order_id in order_ids]
            told = [(await self.notifications.get(f"/notifications/{order_id}")).json() for order_id in order_ids]
            if all(o["status"] != "AWAITING_PAYMENT" for o in found) and all(len(t) == 1 for t in told):
                return found
            assert time.monotonic() < deadline, f"not settled: {[o['status'] for o in found]}"
            await asyncio.sleep(0.05)

    def refund_requested(self, order_id: str) -> bool:
        with psycopg.connect(self.databases["orders"]) as conn:
            row = conn.execute("SELECT refund_requested FROM orders WHERE id = %s", (order_id,)).fetchone()
        return bool(row and row[0])


@asynccontextmanager
async def open_shop(
    databases: dict[str, str], topology: Topology, provider: Provider, payment_timeout: float = 30.0
) -> AsyncIterator[Shop]:
    """The three services running, each on its own schema, sharing the broker's (prefixed) topology."""
    mailer = FakeMailer()

    def settings(service: str) -> Settings:
        return Settings(service, databases[service], amqp_url(), topology)

    apps = [
        orders.create_app(settings("orders"), payment_timeout=payment_timeout, poll_interval=0.2, sweep_interval=0.05),
        payments.create_app(settings("payments"), provider=provider, poll_interval=0.2),
        notifications.create_app(settings("notifications"), mailer=mailer),
    ]
    async with AsyncExitStack() as stack:
        clients = [await stack.enter_async_context(running(app)) for app in apps]
        stock(databases["orders"], [("WIDGET-1", 2599, 100)])
        yield Shop(clients[0], clients[1], clients[2], databases=databases, provider=provider, mailer=mailer)


@dataclass
class Unreliable:
    """A provider whose first `failures(order)` calls for an order fail before reaching the fake: deterministic by the
    order id, unlike FakeProvider.error_ratio's per-attempt coin, so a test can promise every call recovers in time."""

    inner: FakeProvider
    failures: Callable[[uuid.UUID], int]
    calls: Counter[uuid.UUID] = field(default_factory=Counter)

    def _maybe_fail(self, order_id: uuid.UUID) -> None:
        self.calls[order_id] += 1
        if self.calls[order_id] <= self.failures(order_id):
            raise ProviderUnavailable(f"{order_id}: call {self.calls[order_id]} failed")

    async def charge(self, order_id: uuid.UUID, customer_id: str, amount_cents: int, currency: str) -> Charge:
        self._maybe_fail(order_id)
        return await self.inner.charge(order_id, customer_id, amount_cents, currency)

    async def refund(self, order_id: uuid.UUID, amount_cents: int, currency: str) -> Refund:
        self._maybe_fail(order_id)
        return await self.inner.refund(order_id, amount_cents, currency)

    async def void(self, order_id: uuid.UUID, amount_cents: int, currency: str) -> tuple[Charge, Refund] | None:
        self._maybe_fail(order_id)
        return await self.inner.void(order_id, amount_cents, currency)
