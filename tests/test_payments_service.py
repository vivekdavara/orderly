"""The payment service against the real servers: payment.commands in, the provider called, replies out through the
outbox to order.payment-events."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

import httpx
import psycopg
import pytest
from aio_pika.abc import AbstractChannel, AbstractConnection
from fastapi import FastAPI
from helpers import running, take

from orderly import payments
from orderly.broker import amqp_url, publish
from orderly.consumer import Consumer
from orderly.fakes import Charge, FakeProvider, ProviderUnavailable, Refund
from orderly.messages import (
    ChargePayment,
    Message,
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
class Flaky:
    """A provider whose first `failures` calls per order fail before reaching the real fake."""

    inner: FakeProvider
    failures: int = 1
    seen: dict[tuple[str, uuid.UUID], int] = field(default_factory=dict)

    def _fail(self, call: str, order_id: uuid.UUID) -> None:
        n = self.seen[(call, order_id)] = self.seen.get((call, order_id), 0) + 1
        if n <= self.failures:
            raise ProviderUnavailable(f"{call} {order_id}: attempt {n} failed")

    async def charge(self, order_id: uuid.UUID, customer_id: str, amount_cents: int, currency: str) -> Charge:
        self._fail("charge", order_id)
        return await self.inner.charge(order_id, customer_id, amount_cents, currency)

    async def refund(self, order_id: uuid.UUID, amount_cents: int, currency: str) -> Refund:
        self._fail("refund", order_id)
        return await self.inner.refund(order_id, amount_cents, currency)

    async def void(self, order_id: uuid.UUID, amount_cents: int, currency: str) -> tuple[Charge, Refund] | None:
        self._fail("void", order_id)
        return await self.inner.void(order_id, amount_cents, currency)


@dataclass
class PaymentService:
    client: httpx.AsyncClient
    app: FastAPI
    url: str
    topology: Topology
    channel: AbstractChannel
    provider: FakeProvider

    @property
    def consumer(self) -> Consumer:
        consumer: Consumer = self.app.state.consumers[0]
        return consumer

    async def send(self, message: Message) -> Message:
        await publish(self.channel, self.topology, message)
        return message

    async def replies(self, expected: int, timeout: float = 10.0) -> list[Message]:  # noqa: ASYNC109
        """The next `expected` replies on order.payment-events; then checks that no more arrive."""
        queue = await self.channel.get_queue(self.topology.queue("order.payment-events"), ensure=False)
        received: list[Message] = []
        deadline = time.monotonic() + timeout
        while len(received) < expected:
            incoming = await queue.get(fail=False, no_ack=True)
            if incoming is None:
                assert time.monotonic() < deadline, f"{len(received)} of {expected} replies after {timeout} s"
                await asyncio.sleep(0.01)
                continue
            received.append(decode(incoming.body))
        await asyncio.sleep(0.2)
        assert await queue.get(fail=False, no_ack=True) is None, "more replies than expected"
        return received

    def row(self, order_id: uuid.UUID) -> tuple[str, int, str | None, str | None] | None:
        with psycopg.connect(self.url) as conn:
            found = conn.execute(
                "SELECT status, amount_cents, charge_ref, refund_ref FROM payments WHERE order_id = %s", (order_id,)
            ).fetchone()
        return None if found is None else (found[0], found[1], found[2], found[3])


@pytest.fixture
async def short_topology(connection: AbstractConnection) -> AsyncIterator[Topology]:
    topology = Topology(prefix=f"test-{uuid.uuid4().hex[:12]}.", retry_delays=(0.2, 0.4))
    async with connection.channel() as setup:
        await topology.declare(setup)
    yield topology
    async with connection.channel() as cleanup:
        await topology.delete(cleanup)


def make_service(
    fresh_database: str, short_topology: Topology, channel: AbstractChannel, provider: payments.Provider
) -> tuple[FastAPI, Settings]:
    settings = Settings("payments", fresh_database, amqp_url(), short_topology)
    return payments.create_app(settings, provider=provider, poll_interval=0.2), settings


@pytest.fixture
async def service(
    fresh_database: str, short_topology: Topology, channel: AbstractChannel
) -> AsyncIterator[PaymentService]:
    provider = FakeProvider(declined_customers=frozenset({"c-declined"}))
    app, _ = make_service(fresh_database, short_topology, channel, provider)
    async with running(app) as client:
        yield PaymentService(client, app, fresh_database, short_topology, channel, provider)


def charge(order_id: uuid.UUID | None = None, customer: str = "c-42", amount: int = 2599) -> Message:
    order = uuid.uuid4() if order_id is None else order_id
    return new_message(ChargePayment(order_id=order, customer_id=customer, amount_cents=amount, currency="USD"))


def refund(order_id: uuid.UUID, amount: int = 2599) -> Message:
    return new_message(RefundPayment(order_id=order_id, amount_cents=amount, currency="USD", reason="payment_timeout"))


async def test_an_approved_charge_is_answered_succeeded(service: PaymentService) -> None:
    command = await service.send(charge())
    order_id = command.payload.order_id
    [answer] = await service.replies(1)
    assert isinstance(answer.payload, PaymentSucceeded)
    assert (answer.payload.amount_cents, answer.payload.currency) == (2599, "USD")
    assert answer.correlation_id == order_id and answer.causation_id == command.message_id
    assert service.row(order_id) == ("CHARGED", 2599, f"ch_{order_id.hex[:24]}", None)
    assert service.provider.net_cents(order_id) == 2599
    response = await service.client.get(f"/payments/{order_id}")
    assert response.status_code == 200 and response.json()["id"] == str(answer.payload.payment_id)
    assert (await service.client.get(f"/payments/{uuid.uuid4()}")).status_code == 404


async def test_a_declined_charge_is_answered_failed(service: PaymentService) -> None:
    command = await service.send(charge(customer="c-declined"))
    [answer] = await service.replies(1)
    assert isinstance(answer.payload, PaymentFailed) and answer.payload.reason == "declined"
    assert service.row(command.payload.order_id) == ("DECLINED", 2599, None, None)
    assert service.provider.net_cents() == 0


async def test_a_redelivered_command_is_answered_once_and_a_repeated_one_alike(service: PaymentService) -> None:
    command = await service.send(charge())
    [first] = await service.replies(1)
    await service.send(command)  # the same message again: deduplicated, no reply
    await asyncio.sleep(0.3)
    assert service.consumer.stats["duplicate"] == 1
    # A second charge command for the order (a new id): answered again with the same payment, not charged again.
    await service.send(charge(command.payload.order_id))
    [second] = await service.replies(1)
    assert isinstance(first.payload, PaymentSucceeded) and isinstance(second.payload, PaymentSucceeded)
    assert second.payload.payment_id == first.payload.payment_id
    assert service.provider.attempts[command.payload.order_id] == 1


async def test_a_refund_after_the_charge_gives_the_money_back(service: PaymentService) -> None:
    command = await service.send(charge())
    order_id = command.payload.order_id
    await service.replies(1)
    await service.send(refund(order_id))
    [answer] = await service.replies(1)
    assert isinstance(answer.payload, PaymentRefunded) and answer.payload.amount_cents == 2599
    status, _, charge_ref, refund_ref = service.row(order_id) or ("", 0, None, None)
    assert (status, charge_ref is not None, refund_ref) == ("REFUNDED", True, f"re_{order_id.hex[:24]}")
    assert service.provider.net_cents(order_id) == 0


async def test_a_refund_before_the_charge_voids_it_and_the_charge_is_never_taken(service: PaymentService) -> None:
    order_id = uuid.uuid4()
    await service.send(refund(order_id))
    [voided] = await service.replies(1)
    assert isinstance(voided.payload, PaymentRefunded) and voided.payload.amount_cents == 0
    assert service.row(order_id) == ("VOIDED", 2599, None, None)
    await service.send(charge(order_id))
    [answer] = await service.replies(1)
    assert isinstance(answer.payload, PaymentFailed) and answer.payload.reason == "voided"
    assert service.provider.attempts[order_id] == 0 and service.provider.net_cents() == 0


async def test_a_refund_finding_no_row_refunds_a_charge_whose_transaction_was_lost(service: PaymentService) -> None:
    """A charge attempt reached the provider and then died before committing (a crash, a timeout): the provider has
    the money, the service has no row. The refund that comes first must not record a void and leave the money taken."""
    order_id = uuid.uuid4()
    await service.provider.charge(order_id, "c-42", 2599, "USD")  # the lost attempt
    await service.send(refund(order_id))
    [answer] = await service.replies(1)
    assert isinstance(answer.payload, PaymentRefunded) and answer.payload.amount_cents == 2599
    assert (service.row(order_id) or ("",))[0] == "REFUNDED"
    assert service.provider.net_cents(order_id) == 0
    # And the charge's own redelivery, when it comes, is answered from the row without charging.
    await service.send(charge(order_id))
    [late] = await service.replies(1)
    assert isinstance(late.payload, PaymentFailed) and late.payload.reason == "voided"
    assert service.provider.net_cents(order_id) == 0


async def test_charge_and_refund_delivered_together_never_leave_money_taken(service: PaymentService) -> None:
    """With prefetch 8 both commands of an order run at once; the order's lock puts them one after the other, and
    whichever goes first the provider ends with nothing charged."""
    orders = [uuid.uuid4() for _ in range(20)]
    for n, order_id in enumerate(orders):
        first, second = (charge(order_id), refund(order_id))
        for command in (first, second) if n % 2 else (second, first):
            await service.send(command)
    answers = await service.replies(40, timeout=20)
    assert service.provider.net_cents() == 0
    assert {(service.row(order_id) or ("",))[0] for order_id in orders} <= {"REFUNDED", "VOIDED"}
    assert sum(isinstance(a.payload, PaymentRefunded) for a in answers) == 20


async def test_a_provider_outage_is_retried_until_it_answers(
    fresh_database: str, short_topology: Topology, channel: AbstractChannel
) -> None:
    inner = FakeProvider()
    app, _ = make_service(fresh_database, short_topology, channel, Flaky(inner, failures=2))
    async with running(app) as client:
        service = PaymentService(client, app, fresh_database, short_topology, channel, inner)
        command = await service.send(charge())
        [answer] = await service.replies(1)
        assert isinstance(answer.payload, PaymentSucceeded)
        assert service.consumer.stats["retried"] == 2
        assert inner.net_cents(command.payload.order_id) == 2599


async def test_a_command_contradicting_the_payment_is_dead_lettered(service: PaymentService) -> None:
    command = await service.send(charge())
    await service.replies(1)
    await service.send(refund(command.payload.order_id, amount=1))
    deadline = time.monotonic() + 10
    while service.consumer.stats["dead_lettered"] < 1:
        assert time.monotonic() < deadline
        await asyncio.sleep(0.01)
    [dead] = await take(service.channel, service.topology.dead_letter_queue("payment.commands"), 1)
    assert decode(dead.body).payload.TYPE == "payment.refund"
    assert service.provider.net_cents() == 2599  # nothing moved on the contradiction


async def test_a_charge_after_a_void_whose_transaction_was_lost_is_answered_voided(service: PaymentService) -> None:
    """The day-3 review: a refund's void reached the provider, then its transaction was lost (no row). The charge that
    comes next must be answered "failed (voided)", as DESIGN's table says, not dead-lettered as a conflict."""
    order_id = uuid.uuid4()
    assert await service.provider.void(order_id, 2599, "USD") is None  # the lost attempt
    await service.send(charge(order_id))
    [answer] = await service.replies(1)
    assert isinstance(answer.payload, PaymentFailed) and answer.payload.reason == "voided"
    assert service.row(order_id) == ("VOIDED", 2599, None, None)
    assert service.consumer.stats["dead_lettered"] == 0
    # The refund's own redelivery then finds the void recorded.
    await service.send(refund(order_id))
    [refunded] = await service.replies(1)
    assert isinstance(refunded.payload, PaymentRefunded) and refunded.payload.amount_cents == 0
    assert service.provider.net_cents() == 0
