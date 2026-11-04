"""The order service's half of the saga against the real servers: payment replies and the deadline move orders, give
stock back, ask for refunds and announce the outcome."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import httpx
import psycopg
import pytest
from aio_pika.abc import AbstractChannel, AbstractConnection
from fastapi import FastAPI
from helpers import running, stock, stock_of

from orderly import orders
from orderly.broker import amqp_url, publish
from orderly.consumer import Consumer
from orderly.messages import (
    Message,
    OrderCancelled,
    OrderConfirmed,
    PaymentFailed,
    PaymentRefunded,
    PaymentSucceeded,
    RefundPayment,
    decode,
    new_message,
)
from orderly.service import Settings
from orderly.topology import Topology


@dataclass
class Saga:
    client: httpx.AsyncClient
    app: FastAPI
    url: str
    topology: Topology
    channel: AbstractChannel

    @property
    def consumer(self) -> Consumer:
        consumer: Consumer = self.app.state.consumers[0]
        return consumer

    async def order(self, quantity: int = 2, customer: str = "c-42") -> dict[str, Any]:
        body = {
            "customer_id": customer,
            "email": "ada@example.com",
            "lines": [{"sku": "WIDGET-1", "quantity": quantity}],
        }
        response = await self.client.post("/orders", json=body)
        assert response.status_code == 201, response.text
        order: dict[str, Any] = response.json()
        return order

    async def reply(self, payload: PaymentSucceeded | PaymentFailed | PaymentRefunded) -> Message:
        message = new_message(payload)
        await publish(self.channel, self.topology, message)
        return message

    async def taken(self, queue: str, expected: int, timeout: float = 10.0) -> list[Message]:  # noqa: ASYNC109
        """The next `expected` messages on a queue; then checks that no more arrive."""
        source = await self.channel.get_queue(self.topology.queue(queue), ensure=False)
        received: list[Message] = []
        deadline = time.monotonic() + timeout
        while len(received) < expected:
            incoming = await source.get(fail=False, no_ack=True)
            if incoming is None:
                assert time.monotonic() < deadline, f"{len(received)} of {expected} on {queue} after {timeout} s"
                await asyncio.sleep(0.01)
                continue
            received.append(decode(incoming.body))
        await asyncio.sleep(0.2)
        assert await source.get(fail=False, no_ack=True) is None, f"more on {queue} than expected"
        return received

    async def status(self, order_id: str, expected: str, timeout: float = 10.0) -> dict[str, Any]:  # noqa: ASYNC109
        deadline = time.monotonic() + timeout
        while True:
            order: dict[str, Any] = (await self.client.get(f"/orders/{order_id}")).json()
            if order["status"] == expected:
                return order
            assert time.monotonic() < deadline, f"order {order_id} still {order['status']}, not {expected}"
            await asyncio.sleep(0.02)

    def refund_requested(self, order_id: str) -> bool:
        with psycopg.connect(self.url) as conn:
            row = conn.execute("SELECT refund_requested FROM orders WHERE id = %s", (order_id,)).fetchone()
        assert row is not None
        return bool(row[0])


@pytest.fixture
async def short_topology(connection: AbstractConnection) -> AsyncIterator[Topology]:
    topology = Topology(prefix=f"test-{uuid.uuid4().hex[:12]}.", retry_delays=(0.2, 0.4))
    async with connection.channel() as setup:
        await topology.declare(setup)
    yield topology
    async with connection.channel() as cleanup:
        await topology.delete(cleanup)


async def start(
    fresh_database: str,
    topology: Topology,
    channel: AbstractChannel,
    payment_timeout: float,
    sweep_interval: float = 0.05,
) -> AsyncIterator[Saga]:
    settings = Settings("orders", fresh_database, amqp_url(), topology)
    app = orders.create_app(settings, payment_timeout=payment_timeout, poll_interval=0.2, sweep_interval=sweep_interval)
    async with running(app) as client:
        stock(fresh_database, [("WIDGET-1", 2599, 10)])
        yield Saga(client, app, fresh_database, topology, channel)


@pytest.fixture
async def saga(fresh_database: str, short_topology: Topology, channel: AbstractChannel) -> AsyncIterator[Saga]:
    async for running_saga in start(fresh_database, short_topology, channel, payment_timeout=30.0):
        yield running_saga


@pytest.fixture
async def impatient(fresh_database: str, short_topology: Topology, channel: AbstractChannel) -> AsyncIterator[Saga]:
    """An order service whose orders give up on their payment after 0.3 s."""
    async for running_saga in start(fresh_database, short_topology, channel, payment_timeout=0.3):
        yield running_saga


def succeeded(order: dict[str, Any]) -> PaymentSucceeded:
    return PaymentSucceeded(
        order_id=uuid.UUID(order["id"]), payment_id=uuid.uuid4(), amount_cents=order["total_cents"], currency="USD"
    )


async def test_a_success_confirms_the_order_and_announces_it(saga: Saga) -> None:
    order = await saga.order()
    cause = await saga.reply(succeeded(order))
    confirmed = await saga.status(order["id"], "CONFIRMED")
    assert confirmed["cancel_reason"] is None
    [announcement] = await saga.taken("notification.order-events", 1)
    assert announcement.payload == OrderConfirmed(
        order_id=uuid.UUID(order["id"]), customer_id="c-42", email="ada@example.com", total_cents=5198, currency="USD"
    )
    assert announcement.causation_id == cause.message_id
    assert stock_of(saga.url, "WIDGET-1") == 8  # sold
    [charge] = await saga.taken("payment.commands", 1)
    assert charge.type == "payment.charge"


async def test_a_decline_cancels_the_order_and_gives_its_stock_back(saga: Saga) -> None:
    order = await saga.order(quantity=3)
    assert stock_of(saga.url, "WIDGET-1") == 7
    await saga.reply(PaymentFailed(order_id=uuid.UUID(order["id"]), reason="declined"))
    cancelled = await saga.status(order["id"], "CANCELLED")
    assert cancelled["cancel_reason"] == "payment_declined"
    [announcement] = await saga.taken("notification.order-events", 1)
    assert isinstance(announcement.payload, OrderCancelled) and announcement.payload.reason == "payment_declined"
    assert stock_of(saga.url, "WIDGET-1") == 10
    assert not saga.refund_requested(order["id"])
    await saga.taken("payment.commands", 1)  # the charge, and no refund


async def test_the_deadline_cancels_the_order_and_asks_for_a_refund(impatient: Saga) -> None:
    order = await impatient.order()
    cancelled = await impatient.status(order["id"], "CANCELLED")
    assert cancelled["cancel_reason"] == "payment_timeout"
    assert stock_of(impatient.url, "WIDGET-1") == 10
    charge, refund = await impatient.taken("payment.commands", 2)
    assert (charge.type, refund.type) == ("payment.charge", "payment.refund")
    assert refund.payload == RefundPayment(
        order_id=uuid.UUID(order["id"]), amount_cents=5198, currency="USD", reason="payment_timeout"
    )
    [announcement] = await impatient.taken("notification.order-events", 1)
    assert isinstance(announcement.payload, OrderCancelled) and announcement.payload.reason == "payment_timeout"
    assert impatient.refund_requested(order["id"])


async def test_a_success_after_the_deadline_is_covered_by_its_refund(impatient: Saga) -> None:
    order = await impatient.order()
    await impatient.status(order["id"], "CANCELLED")
    await impatient.reply(succeeded(order))
    await impatient.reply(succeeded(order))  # answered twice under two ids
    deadline = time.monotonic() + 10
    while impatient.consumer.stats["handled"] < 2:
        assert time.monotonic() < deadline
        await asyncio.sleep(0.01)
    assert (await impatient.client.get(f"/orders/{order['id']}")).json()["status"] == "CANCELLED"
    # Only the deadline's refund: the payment service's charge and refund commute, so it covers the late charge.
    assert [m.type for m in await impatient.taken("payment.commands", 2)] == ["payment.charge", "payment.refund"]
    assert len(await impatient.taken("notification.order-events", 1)) == 1


async def test_a_success_after_a_decline_is_refunded_once(saga: Saga) -> None:
    order = await saga.order()
    await saga.reply(PaymentFailed(order_id=uuid.UUID(order["id"]), reason="declined"))
    await saga.status(order["id"], "CANCELLED")
    first = await saga.reply(succeeded(order))
    second = await saga.reply(succeeded(order))
    commands = await saga.taken("payment.commands", 2)
    assert [c.type for c in commands] == ["payment.charge", "payment.refund"]
    assert isinstance(commands[1].payload, RefundPayment) and commands[1].payload.reason == "order_cancelled"
    # Handled concurrently: whichever locked the order first asked for the refund, the other found it asked.
    assert commands[1].causation_id in (first.message_id, second.message_id)
    assert saga.refund_requested(order["id"])


async def test_a_reply_the_protocol_rules_out_changes_nothing(saga: Saga) -> None:
    order = await saga.order()
    order_id = uuid.UUID(order["id"])
    await saga.reply(PaymentRefunded(order_id=order_id, payment_id=uuid.uuid4(), amount_cents=0, currency="USD"))
    await saga.reply(PaymentFailed(order_id=order_id, reason="voided"))
    deadline = time.monotonic() + 10
    while saga.consumer.stats["handled"] < 2:
        assert time.monotonic() < deadline
        await asyncio.sleep(0.01)
    assert (await saga.client.get(f"/orders/{order['id']}")).json()["status"] == "AWAITING_PAYMENT"
    assert stock_of(saga.url, "WIDGET-1") == 8
    await saga.taken("notification.order-events", 0)


async def test_a_reply_for_an_unknown_order_is_dead_lettered(saga: Saga) -> None:
    await saga.reply(PaymentFailed(order_id=uuid.uuid4(), reason="declined"))
    deadline = time.monotonic() + 10
    while saga.consumer.stats["dead_lettered"] < 1:
        assert time.monotonic() < deadline
        await asyncio.sleep(0.01)
    assert len(await saga.taken("order.payment-events.dlq", 1)) == 1


async def test_successes_racing_the_deadline_end_confirmed_or_refunded(impatient: Saga) -> None:
    """Replies sent right around the deadline: each order ends confirmed with no refund, or cancelled with one (the
    success arrived after the deadline's cancellation, whose refund covers it). Stock matches the confirmed orders."""
    stock(impatient.url, [("BULK-1", 100, 1000)])
    placed = []
    for _ in range(30):
        body = {"customer_id": "c-1", "email": "a@b.c", "lines": [{"sku": "BULK-1", "quantity": 1}]}
        placed.append((await impatient.client.post("/orders", json=body)).json())
    await asyncio.sleep(0.25)  # the first orders' deadlines are near
    for order in placed:
        await impatient.reply(succeeded(order))
    deadline = time.monotonic() + 15
    while impatient.consumer.stats["handled"] < 30:
        assert time.monotonic() < deadline
        await asyncio.sleep(0.01)
    final = [(await impatient.client.get(f"/orders/{order['id']}")).json() for order in placed]
    confirmed = [o for o in final if o["status"] == "CONFIRMED"]
    cancelled = [o for o in final if o["status"] == "CANCELLED"]
    assert len(confirmed) + len(cancelled) == 30
    assert all(not impatient.refund_requested(o["id"]) for o in confirmed)
    assert all(impatient.refund_requested(o["id"]) for o in cancelled)
    assert stock_of(impatient.url, "BULK-1") == 1000 - len(confirmed)


@pytest.fixture
async def unswept(fresh_database: str, short_topology: Topology, channel: AbstractChannel) -> AsyncIterator[Saga]:
    """An order service whose own sweeper looks once at startup and then not for an hour: the test sweeps."""
    async for running_saga in start(fresh_database, short_topology, channel, payment_timeout=30.0, sweep_interval=3600):
        yield running_saga


async def test_an_order_locked_by_a_reply_does_not_hold_up_the_sweeper(unswept: Saga) -> None:
    """The day-3 review's surviving mutant: `FOR UPDATE` instead of `FOR UPDATE SKIP LOCKED` in the sweeper's claim
    is as correct (lock_order re-reads the row) but makes the sweeper wait behind a reply's lock on one order while
    others expire. With SKIP LOCKED it cancels the others at once and leaves the locked one to the next pass."""
    first, second = await unswept.order(), await unswept.order()
    with psycopg.connect(unswept.url, autocommit=True) as conn:
        conn.execute(
            "UPDATE orders SET payment_deadline = now() - interval '1 second' WHERE id IN (%s, %s)",
            (first["id"], second["id"]),
        )
    pool = unswept.app.state.resources.pool
    with psycopg.connect(unswept.url) as holder:  # a reply's handler holding the first order's row
        holder.execute("SELECT 1 FROM orders WHERE id = %s FOR UPDATE", (first["id"],))
        swept = await asyncio.wait_for(orders.sweep(pool), timeout=3)
        assert swept == 1
        assert (await unswept.client.get(f"/orders/{second['id']}")).json()["status"] == "CANCELLED"
        assert (await unswept.client.get(f"/orders/{first['id']}")).json()["status"] == "AWAITING_PAYMENT"
        holder.rollback()
    assert await asyncio.wait_for(orders.sweep(pool), timeout=3) == 1  # the next pass takes it
    assert (await unswept.client.get(f"/orders/{first['id']}")).json()["status"] == "CANCELLED"
