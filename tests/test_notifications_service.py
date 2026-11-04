"""The notification service against the real servers: order events in, one e-mail per order and kind out."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass

import httpx
import pytest
from aio_pika.abc import AbstractChannel, AbstractConnection
from fastapi import FastAPI
from helpers import count, running

from orderly import notifications
from orderly.broker import amqp_url, publish
from orderly.consumer import Consumer
from orderly.fakes import FakeMailer
from orderly.messages import Message, OrderCancelled, OrderConfirmed, decode, new_message
from orderly.service import Settings
from orderly.topology import Topology


@dataclass
class NotificationService:
    client: httpx.AsyncClient
    app: FastAPI
    url: str
    topology: Topology
    channel: AbstractChannel
    mailer: FakeMailer

    @property
    def consumer(self) -> Consumer:
        consumer: Consumer = self.app.state.consumers[0]
        return consumer

    async def send(self, message: Message) -> Message:
        await publish(self.channel, self.topology, message)
        return message

    async def until(self, stat: str, n: int, timeout: float = 10.0) -> None:  # noqa: ASYNC109
        deadline = time.monotonic() + timeout
        while self.consumer.stats[stat] < n:
            assert time.monotonic() < deadline, (
                f"{stat} {self.consumer.stats[stat]} of {n}: {dict(self.consumer.stats)}"
            )
            await asyncio.sleep(0.01)


@pytest.fixture
async def short_topology(connection: AbstractConnection) -> AsyncIterator[Topology]:
    topology = Topology(prefix=f"test-{uuid.uuid4().hex[:12]}.", retry_delays=(0.2, 0.4))
    async with connection.channel() as setup:
        await topology.declare(setup)
    yield topology
    async with connection.channel() as cleanup:
        await topology.delete(cleanup)


@pytest.fixture
async def service(
    fresh_database: str, short_topology: Topology, channel: AbstractChannel
) -> AsyncIterator[NotificationService]:
    mailer = FakeMailer()
    settings = Settings("notifications", fresh_database, amqp_url(), short_topology)
    app = notifications.create_app(settings, mailer=mailer)
    async with running(app) as client:
        yield NotificationService(client, app, fresh_database, short_topology, channel, mailer)


def confirmed(order_id: uuid.UUID | None = None, total: int = 2599, currency: str = "USD") -> Message:
    order = uuid.uuid4() if order_id is None else order_id
    return new_message(
        OrderConfirmed(
            order_id=order, customer_id="c-42", email="ada@example.com", total_cents=total, currency=currency
        )
    )


def cancelled(order_id: uuid.UUID, reason: str = "payment_declined") -> Message:
    return new_message(
        OrderCancelled(order_id=order_id, customer_id="c-42", email="ada@example.com", reason=reason)  # type: ignore[arg-type]
    )


async def test_a_confirmation_sends_one_email_and_records_it(service: NotificationService) -> None:
    event = await service.send(confirmed(total=500, currency="JPY"))
    order_id = event.payload.order_id
    await service.until("handled", 1)
    [email] = service.mailer.sent
    assert email.recipient == "ada@example.com" and email.subject == f"Order {order_id} confirmed"
    assert "500 JPY" in email.body
    [record] = (await service.client.get(f"/notifications/{order_id}")).json()
    assert record["kind"] == "order_confirmed" and record["message_id"] == str(event.message_id)


@pytest.mark.parametrize(
    ("reason", "says"), [("payment_declined", "not been charged"), ("payment_timeout", "will be refunded")]
)
async def test_a_cancellation_says_why(service: NotificationService, reason: str, says: str) -> None:
    event = await service.send(cancelled(uuid.uuid4(), reason))
    await service.until("handled", 1)
    [email] = service.mailer.sent
    assert email.subject.endswith("cancelled") and says in email.body
    assert (await service.client.get(f"/notifications/{event.payload.order_id}")).json()[0]["kind"] == "order_cancelled"


async def test_an_event_delivered_twice_or_published_twice_sends_one_email(service: NotificationService) -> None:
    event = await service.send(confirmed())
    await service.until("handled", 1)
    await service.send(event)  # redelivered: the same id
    await service.until("duplicate", 1)
    await service.send(confirmed(event.payload.order_id))  # the same fact under another id
    await service.until("handled", 2)
    assert len(service.mailer.sent) == 1
    assert count(service.url, "notifications") == 1


async def test_a_mailer_outage_is_retried_and_the_email_sent_once(service: NotificationService) -> None:
    service.mailer.down = True
    event = await service.send(confirmed())
    await service.until("retried", 1)
    assert service.mailer.sent == [] and count(service.url, "notifications") == 0  # rolled back with the failed send
    service.mailer.down = False
    await service.until("handled", 1)
    assert len(service.mailer.sent) == 1
    assert service.mailer.attempts[event.payload.order_id] >= 2


async def test_a_mailer_down_for_good_dead_letters_the_event(service: NotificationService) -> None:
    service.mailer.down = True
    event = await service.send(confirmed())
    await service.until("dead_lettered", 1)
    assert service.mailer.attempts[event.payload.order_id] == 3  # the first try and two retries
    dlq = await service.channel.get_queue(service.topology.dead_letter_queue("notification.order-events"), ensure=False)
    dead = await dlq.get(fail=False, no_ack=True)
    assert dead is not None and decode(dead.body).message_id == event.message_id
    assert count(service.url, "notifications") == 0
