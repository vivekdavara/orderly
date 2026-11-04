"""Whole sagas through the three services at once, against the real servers: POST /orders to the e-mail."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import Counter
from collections.abc import AsyncIterator, Callable, Iterator
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

import httpx
import psycopg
import pytest
from aio_pika.abc import AbstractConnection
from helpers import running, stock, stock_of
from psycopg import conninfo, sql

from orderly import notifications, orders, payments
from orderly.broker import amqp_url
from orderly.fakes import Charge, FakeMailer, FakeProvider, ProviderUnavailable, Refund, fraction
from orderly.payments import Provider
from orderly.service import Settings
from orderly.topology import Topology


@pytest.fixture
def databases(test_database: str) -> Iterator[dict[str, str]]:
    """One empty schema per service, as if each had its own database."""
    schemas = {service: f"t_{uuid.uuid4().hex[:12]}" for service in ("orders", "payments", "notifications")}
    with psycopg.connect(test_database, autocommit=True) as conn:
        for schema in schemas.values():
            conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
    try:
        yield {s: conninfo.make_conninfo(test_database, options=f"-c search_path={n}") for s, n in schemas.items()}
    finally:
        with psycopg.connect(test_database, autocommit=True) as conn:
            for schema in schemas.values():
                conn.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


@pytest.fixture
async def short_topology(connection: AbstractConnection) -> AsyncIterator[Topology]:
    topology = Topology(prefix=f"test-{uuid.uuid4().hex[:12]}.", retry_delays=(0.1, 0.2, 0.4))
    async with connection.channel() as setup:
        await topology.declare(setup)
    yield topology
    async with connection.channel() as cleanup:
        await topology.delete(cleanup)


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


async def test_an_approved_order_is_confirmed_charged_and_announced(
    databases: dict[str, str], short_topology: Topology
) -> None:
    provider = FakeProvider()
    async with open_shop(databases, short_topology, provider) as shop:
        placed = await shop.order(quantity=2)
        assert placed["status"] == "AWAITING_PAYMENT"
        [order] = await shop.settled([placed["id"]])
        assert order["status"] == "CONFIRMED"
        payment = (await shop.payments.get(f"/payments/{placed['id']}")).json()
        assert (payment["status"], payment["amount_cents"]) == ("CHARGED", 5198)
        assert provider.net_cents() == 5198
        [email] = shop.mailer.sent
        assert email.subject == f"Order {placed['id']} confirmed" and "51.98 USD" in email.body
        assert stock_of(databases["orders"], "WIDGET-1") == 98


async def test_a_declined_order_is_cancelled_and_its_stock_returned(
    databases: dict[str, str], short_topology: Topology
) -> None:
    provider = FakeProvider(declined_customers=frozenset({"c-no"}))
    async with open_shop(databases, short_topology, provider) as shop:
        placed = await shop.order(quantity=3, customer="c-no")
        [order] = await shop.settled([placed["id"]])
        assert (order["status"], order["cancel_reason"]) == ("CANCELLED", "payment_declined")
        assert (await shop.payments.get(f"/payments/{placed['id']}")).json()["status"] == "DECLINED"
        assert provider.net_cents() == 0 and not shop.refund_requested(placed["id"])
        assert "not been charged" in shop.mailer.sent[0].body
        assert stock_of(databases["orders"], "WIDGET-1") == 100


async def test_a_payment_slower_than_the_deadline_is_cancelled_and_refunded(
    databases: dict[str, str], short_topology: Topology
) -> None:
    """The provider takes 0.5 s, the order waits 0.2 s: the order is cancelled and its refund sent while the charge is
    still being made, and the customer ends up with nothing charged whichever the payment service sees first."""
    provider = FakeProvider(latency=0.5)
    async with open_shop(databases, short_topology, provider, payment_timeout=0.2) as shop:
        placed = await shop.order()
        [order] = await shop.settled([placed["id"]])
        assert (order["status"], order["cancel_reason"]) == ("CANCELLED", "payment_timeout")
        assert shop.refund_requested(placed["id"])
        deadline = time.monotonic() + 10
        while (await shop.payments.get(f"/payments/{placed['id']}")).json().get("status") not in ("REFUNDED", "VOIDED"):
            assert time.monotonic() < deadline, "the refund never reached the payment service"
            await asyncio.sleep(0.05)
        assert provider.net_cents() == 0
        assert "will be refunded" in shop.mailer.sent[0].body
        assert stock_of(databases["orders"], "WIDGET-1") == 100


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


async def test_many_orders_with_failing_provider_calls_all_settle_and_the_books_agree(
    databases: dict[str, str], short_topology: Topology
) -> None:
    """60 orders, about a quarter declined, and for about 30% of orders the provider's first two calls fail (each
    retried through the wait queues; three retries are allowed): every order settles, each customer is told once, the
    provider holds exactly the confirmed orders' totals, and the stock is what they took. Synthetic: the outcomes are
    fixed by the order ids."""
    inner = FakeProvider(decline_ratio=0.25)
    provider = Unreliable(inner, lambda order_id: 2 if fraction(order_id, "unreliable") < 0.3 else 0)
    async with open_shop(databases, short_topology, provider) as shop:
        placed = await asyncio.gather(*(shop.order(quantity=1 + n % 2) for n in range(60)))
        final = await shop.settled([p["id"] for p in placed], timeout=60)
        confirmed = [o for o in final if o["status"] == "CONFIRMED"]
        cancelled = [o for o in final if o["status"] == "CANCELLED"]
        assert len(confirmed) + len(cancelled) == 60 and confirmed and cancelled
        assert all(o["cancel_reason"] == "payment_declined" for o in cancelled)
        assert any(provider.calls[uuid.UUID(o["id"])] == 3 for o in final), "no order exercised the retries"
        assert inner.net_cents() == sum(o["total_cents"] for o in confirmed)
        assert len(shop.mailer.sent) == 60
        assert stock_of(databases["orders"], "WIDGET-1") == 100 - sum(o["lines"][0]["quantity"] for o in confirmed)


async def test_a_charge_dead_lettered_after_its_retries_times_out_and_is_refunded(
    databases: dict[str, str], short_topology: Topology
) -> None:
    """The provider fails a charge's four attempts (the first try and three retries): the command is dead-lettered,
    the order waits out its deadline, is cancelled and asks for a refund, which voids the order at the provider."""
    inner = FakeProvider()
    provider = Unreliable(inner, lambda order_id: 4)
    async with open_shop(databases, short_topology, provider, payment_timeout=1.5) as shop:
        placed = await shop.order()
        [order] = await shop.settled([placed["id"]])
        assert (order["status"], order["cancel_reason"]) == ("CANCELLED", "payment_timeout")
        deadline = time.monotonic() + 10
        while (await shop.payments.get(f"/payments/{placed['id']}")).json().get("status") != "VOIDED":
            assert time.monotonic() < deadline, "the refund never voided the order"
            await asyncio.sleep(0.05)
        assert inner.net_cents() == 0 and uuid.UUID(placed["id"]) in inner.voided
        assert stock_of(databases["orders"], "WIDGET-1") == 100
