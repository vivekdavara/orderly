"""Failure injection (day 4): whole sagas through the three services while the broker goes away, while every message
arrives twice, and while the provider's answers are lost. Each test ends by checking the books: every order settled,
each customer told exactly once, the provider holding exactly the confirmed orders' money, the stock what they took.
Synthetic data; outcomes fixed by the order ids (FakeProvider's decline coin)."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import Counter
from dataclasses import dataclass, field
from typing import Any

import psycopg
import pytest
from aio_pika.abc import AbstractChannel
from chaos import proxy_to
from helpers import stock_of
from shop import Shop, open_shop

from orderly import consumer, outbox
from orderly.broker import amqp_url
from orderly.fakes import Charge, FakeProvider, Refund
from orderly.outbox import Relay
from orderly.topology import Topology


def with_query(url: str, query: str) -> str:
    return f"{url}{'&' if '?' in url else '?'}{query}"


async def until(condition: Any, what: str, timeout: float = 20.0) -> None:  # noqa: ASYNC109
    deadline = time.monotonic() + timeout
    while not await condition():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        await asyncio.sleep(0.05)


def check_books(shop: Shop, provider: FakeProvider, final: list[dict[str, Any]], stocked: int = 100) -> None:
    """The invariants every run must end with, whatever was injected."""
    confirmed = [o for o in final if o["status"] == "CONFIRMED"]
    assert provider.net_cents() == sum(o["total_cents"] for o in confirmed)
    told = Counter(email.subject for email in shop.mailer.sent)
    assert len(told) == len(final) and set(told.values()) == {1}, "each customer told exactly once"
    taken = sum(line["quantity"] for o in confirmed for line in o["lines"])
    assert stock_of(shop.databases["orders"], "WIDGET-1") == stocked - taken


def unpublished(url: str) -> int:
    with psycopg.connect(url) as conn:
        row = conn.execute("SELECT count(*) FROM outbox WHERE published_at IS NULL").fetchone()
    assert row is not None
    return int(row[0])


async def test_orders_taken_while_the_broker_is_down_all_settle_once_it_is_back(
    databases: dict[str, str], short_topology: Topology
) -> None:
    """20 orders settle; the broker goes away (every connection dropped, reconnects refused) for 1.5 s, during which
    30 more orders are taken (201: the request never touches the broker) and wait in the outbox; once it is back,
    all 50 settle, nothing lost or done twice."""
    provider = FakeProvider(decline_ratio=0.2)
    async with proxy_to(amqp_url()) as proxy:
        url = with_query(proxy.url(amqp_url()), "reconnect_interval=0.2")
        async with open_shop(databases, short_topology, provider, amqp=url) as shop:
            before = await asyncio.gather(*(shop.order(quantity=1 + n % 2) for n in range(20)))
            await shop.settled([p["id"] for p in before])

            await proxy.cut()
            assert proxy.dropped == 3, "each service's connection went through the proxy"
            during = await asyncio.gather(*(shop.order(quantity=1 + n % 2) for n in range(30)))
            assert all(p["status"] == "AWAITING_PAYMENT" for p in during)

            async def broker_reported_down() -> bool:
                health = await shop.orders.get("/health")
                return health.status_code == 503 and "broker" in health.json()["problems"]

            await until(broker_reported_down, "/health to report the broker")
            assert unpublished(databases["orders"]) == 30, "the charges wait in the outbox"
            await asyncio.sleep(1.5)
            # Still down: nothing moved.
            statuses = [(await shop.orders.get(f"/orders/{p['id']}")).json()["status"] for p in during]
            assert set(statuses) == {"AWAITING_PAYMENT"}
            await proxy.restore()

            final = await shop.settled([p["id"] for p in before + during], timeout=60)
            check_books(shop, provider, final)
            assert {o["status"] for o in final} == {"CONFIRMED", "CANCELLED"}
            assert all(o["cancel_reason"] == "payment_declined" for o in final if o["status"] == "CANCELLED")
            for client in (shop.orders, shop.payments, shop.notifications):
                await until(lambda c=client: _healthy(c), "every service healthy again")
            assert unpublished(databases["orders"]) == 0 and unpublished(databases["payments"]) == 0
            assert proxy.accepted > 3, "the services reconnected through the proxy"


async def _healthy(client: Any) -> bool:
    return bool((await client.get("/health")).status_code == 200)


def consumer_stats(shop: Shop) -> dict[str, Counter[str]]:
    return {consumer.name: consumer.stats for app in shop.apps for consumer in app.state.consumers}


async def test_every_message_published_twice_changes_nothing(
    databases: dict[str, str], short_topology: Topology, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every relay publishes every message twice (the same bytes, the same message id: what a relay that crashed
    between publishing and marking does): each consumer handles each message once and skips its copy, the provider is
    asked once per order, and each customer gets one e-mail."""
    original = Relay._publish

    async def twice(self: Relay, channel: AbstractChannel, body: bytes) -> None:
        await original(self, channel, body)
        await original(self, channel, body)

    monkeypatch.setattr(Relay, "_publish", twice)
    provider = FakeProvider(decline_ratio=0.25)
    async with open_shop(databases, short_topology, provider) as shop:
        placed = await asyncio.gather(*(shop.order(quantity=1 + n % 3) for n in range(40)))
        final = await shop.settled([p["id"] for p in placed])
        check_books(shop, provider, final)
        stats = consumer_stats(shop)

        async def every_copy_skipped() -> bool:
            return all(s["duplicate"] == s["handled"] for s in stats.values())

        await until(every_copy_skipped, "every copy to be consumed")
        # orders: 40 replies; payments: 40 charges; notifications: 40 announcements. Every one handled once.
        assert {name: s["handled"] for name, s in stats.items()} == {"orders": 40, "payments": 40, "notifications": 40}
        assert set(provider.attempts.values()) == {1}, "the provider was asked once per order"


async def test_replaying_every_outbox_row_again_changes_nothing(
    databases: dict[str, str], short_topology: Topology
) -> None:
    """After 30 sagas have settled, both outboxes are marked unpublished again, so the relays send the whole history
    a second time (a restored backup, or a relay whose marks were lost): every consumer skips all of it."""
    provider = FakeProvider(decline_ratio=0.3)
    async with open_shop(databases, short_topology, provider) as shop:
        placed = await asyncio.gather(*(shop.order() for _ in range(30)))
        final = await shop.settled([p["id"] for p in placed])
        stats = consumer_stats(shop)
        await until(lambda: _counted(stats, 30), "the first pass to be consumed")
        before = {name: Counter(s) for name, s in stats.items()}
        payments_before = [(await shop.payments.get(f"/payments/{p['id']}")).json() for p in placed]

        for service in ("orders", "payments"):
            with psycopg.connect(databases[service]) as conn:
                conn.execute("UPDATE outbox SET published_at = NULL")
        await until(lambda: _replayed(stats, before), "the replay to be consumed", timeout=30)

        assert {name: s["handled"] for name, s in stats.items()} == {n: s["handled"] for n, s in before.items()}
        again = [(await shop.orders.get(f"/orders/{p['id']}")).json() for p in placed]
        assert again == final
        assert [(await shop.payments.get(f"/payments/{p['id']}")).json() for p in placed] == payments_before
        check_books(shop, provider, again)
        assert set(provider.attempts.values()) == {1}


async def _counted(stats: dict[str, Counter[str]], orders: int) -> bool:
    return all(s["handled"] == orders for s in stats.values())


async def _replayed(stats: dict[str, Counter[str]], before: dict[str, Counter[str]]) -> bool:
    # Each consumer sees every message it consumed before once more, as a duplicate.
    return all(s["duplicate"] == before[name]["duplicate"] + before[name]["handled"] for name, s in stats.items())


@dataclass
class LosesAnswers:
    """A provider whose charge goes through and whose answer never comes back, for an order's first `lost` charge
    calls: the call charges the inner fake, then hangs until the payment service's handler timeout gives up on it
    (as a provider timing out after taking the money looks from the outside)."""

    inner: FakeProvider
    lost: int
    hang: float = 60.0
    calls: Counter[uuid.UUID] = field(default_factory=Counter)

    async def charge(self, order_id: uuid.UUID, customer_id: str, amount_cents: int, currency: str) -> Charge:
        self.calls[order_id] += 1
        charge = await self.inner.charge(order_id, customer_id, amount_cents, currency)
        if self.calls[order_id] <= self.lost:
            await asyncio.sleep(self.hang)
        return charge

    async def refund(self, order_id: uuid.UUID, amount_cents: int, currency: str) -> Refund:
        return await self.inner.refund(order_id, amount_cents, currency)

    async def void(self, order_id: uuid.UUID, amount_cents: int, currency: str) -> tuple[Charge, Refund] | None:
        return await self.inner.void(order_id, amount_cents, currency)


async def test_a_charge_whose_answer_was_lost_is_asked_again_and_taken_once(
    databases: dict[str, str], short_topology: Topology
) -> None:
    """The provider takes each order's money and its answer is lost (the handler times out at 0.3 s and the command
    is retried): the retry gets the provider's first answer, so each order is confirmed and charged exactly once."""
    inner = FakeProvider()
    provider = LosesAnswers(inner, lost=1)
    async with open_shop(databases, short_topology, provider, payment_handler_timeout=0.3) as shop:
        placed = await asyncio.gather(*(shop.order(quantity=1 + n % 2) for n in range(10)))
        final = await shop.settled([p["id"] for p in placed])
        assert {o["status"] for o in final} == {"CONFIRMED"}
        check_books(shop, inner, final)
        assert set(provider.calls.values()) == {2} and len(inner.charges) == 10
        assert consumer_stats(shop)["payments"]["retried"] == 10


async def test_a_charge_whose_every_answer_was_lost_is_refunded_by_the_orders_void(
    databases: dict[str, str], short_topology: Topology
) -> None:
    """Every charge call takes the money and never answers: the command's four attempts time out, the order is
    cancelled at its deadline, and its refund voids the order at the provider, which gives the money back."""
    inner = FakeProvider()
    provider = LosesAnswers(inner, lost=10**6)
    async with open_shop(databases, short_topology, provider, payment_timeout=1.0, payment_handler_timeout=0.3) as shop:
        placed = await shop.order(quantity=2)
        [order] = await shop.settled([placed["id"]])
        assert (order["status"], order["cancel_reason"]) == ("CANCELLED", "payment_timeout")

        async def refunded() -> bool:
            payment = (await shop.payments.get(f"/payments/{placed['id']}")).json()
            return bool(payment.get("status") == "REFUNDED")

        await until(refunded, "the void to refund the charge")
        assert inner.charged_cents() == 5198, "the money was taken"
        assert inner.net_cents() == 0, "and given back"
        check_books(shop, inner, [order])
        assert "will be refunded" in shop.mailer.sent[0].body


async def test_after_an_outage_the_workers_start_when_the_broker_is_back_not_when_their_backoff_ends(
    databases: dict[str, str], short_topology: Topology, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Every relay's and consumer's backoff pinned at 30 s: orders taken while the broker is away still settle within
    seconds of its return, because the robust connection's reconnect wakes them. Day 4's load test found the services
    idle for 4.2 s after the broker came back (aio-pika's 5 s reconnect interval, then the workers' own backoff)."""
    for module in (outbox, consumer):
        monkeypatch.setattr(module, "MIN_BACKOFF", 30.0)
        monkeypatch.setattr(module, "MAX_BACKOFF", 30.0)
    provider = FakeProvider()
    async with proxy_to(amqp_url()) as proxy:
        url = with_query(proxy.url(amqp_url()), "reconnect_interval=0.2")
        async with open_shop(databases, short_topology, provider, amqp=url) as shop:
            await proxy.cut()
            placed = await asyncio.gather(*(shop.order() for _ in range(10)))
            await asyncio.sleep(1.0)  # every worker has failed by now and is in its 30 s backoff
            await proxy.restore()
            restored = time.monotonic()
            final = await shop.settled([p["id"] for p in placed], timeout=10)
            assert time.monotonic() - restored < 10
            check_books(shop, provider, final)
