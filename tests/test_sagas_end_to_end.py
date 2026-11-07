"""Whole sagas through the three services at once, against the real servers: POST /orders to the e-mail."""

from __future__ import annotations

import asyncio
import time
import uuid

from aio_pika.abc import AbstractConnection
from helpers import stock_of
from shop import RefundsDown, Unreliable, open_shop

from orderly.dead_letters import Action, DeadLetter, run_pass
from orderly.fakes import FakeProvider, fraction
from orderly.topology import Topology


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


async def test_a_refund_dead_lettered_while_refunds_were_down_is_replayed_and_the_customer_refunded(
    databases: dict[str, str], short_topology: Topology, connection: AbstractConnection
) -> None:
    """The case the dead-letter tool is for (day 5): the charge outlives the order's deadline, so the order is cancelled
    and a refund sent, but the provider's refunds are down for all four attempts. The refund is dead-lettered and the
    customer of a cancelled order stays charged. Once refunds are back, replaying it refunds them."""
    inner = FakeProvider(latency=0.5)
    provider = RefundsDown(inner)
    async with open_shop(databases, short_topology, provider, payment_timeout=0.2) as shop:
        placed = await shop.order()
        [order] = await shop.settled([placed["id"]])
        assert (order["status"], order["cancel_reason"]) == ("CANCELLED", "payment_timeout")

        def keep(_: DeadLetter) -> Action:
            return "keep"

        deadline = time.monotonic() + 15
        while not (listed := await run_pass(connection, short_topology, "payment.commands", keep)).outcomes:
            assert time.monotonic() < deadline, "the refund was never dead-lettered"
            await asyncio.sleep(0.1)
        [dead] = listed.outcomes
        assert (dead.letter.type, dead.letter.order_id, dead.letter.attempt) == ("payment.refund", placed["id"], 4)
        payment = (await shop.payments.get(f"/payments/{placed['id']}")).json()
        assert payment["status"] == "CHARGED"
        assert inner.net_cents() == placed["total_cents"]  # a cancelled order, and the customer charged

        provider.down = False

        def replay(_: DeadLetter) -> Action:
            return "replay"

        replayed = await run_pass(connection, short_topology, "payment.commands", replay)
        assert [o.action for o in replayed.outcomes] == ["replayed"]
        deadline = time.monotonic() + 10
        while (await shop.payments.get(f"/payments/{placed['id']}")).json()["status"] != "REFUNDED":
            assert time.monotonic() < deadline, "the replayed refund was never applied"
            await asyncio.sleep(0.05)
        assert inner.net_cents() == 0
        assert (await run_pass(connection, short_topology, "payment.commands", keep)).outcomes == ()
        assert len(shop.mailer.sent) == 1 and "will be refunded" in shop.mailer.sent[0].body
        assert stock_of(databases["orders"], "WIDGET-1") == 100
