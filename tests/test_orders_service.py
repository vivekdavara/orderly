"""The order service end to end against the real servers: POST /orders → outbox → relay → payment.commands."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import psycopg
import pytest
from aio_pika.abc import AbstractChannel
from fastapi import FastAPI
from helpers import count, running, stock, stock_of

from orderly import orders, outbox
from orderly.broker import amqp_url
from orderly.messages import ChargePayment, Message, decode
from orderly.service import Resources, Settings
from orderly.topology import Topology


@dataclass
class OrderService:
    client: httpx.AsyncClient
    app: FastAPI
    url: str
    topology: Topology
    channel: AbstractChannel

    async def order(self, lines: list[tuple[str, int]], *, key: str | None = None, **body: Any) -> httpx.Response:
        payload = {
            "customer_id": "c-42",
            "email": "ada@example.com",
            "lines": [{"sku": sku, "quantity": quantity} for sku, quantity in lines],
        } | body
        return await self.client.post("/orders", json=payload, headers={} if key is None else {"Idempotency-Key": key})

    async def commands(self, expected: int, timeout: float = 10.0) -> list[Message]:  # noqa: ASYNC109
        """The next `expected` messages on payment.commands, decoded; then checks that no more arrive."""
        queue = await self.channel.get_queue(self.topology.queue("payment.commands"), ensure=False)
        received: list[Message] = []
        deadline = time.monotonic() + timeout
        while len(received) < expected:
            incoming = await queue.get(fail=False, no_ack=True)
            if incoming is None:
                assert time.monotonic() < deadline, f"{len(received)} of {expected} commands after {timeout} s"
                await asyncio.sleep(0.01)
                continue
            received.append(decode(incoming.body))
        await asyncio.sleep(0.1)
        assert await queue.get(fail=False, no_ack=True) is None, "more commands than expected"
        return received


@pytest.fixture
async def service(fresh_database: str, topology: Topology, channel: AbstractChannel) -> AsyncIterator[OrderService]:
    settings = Settings("orders", fresh_database, amqp_url(), topology)
    app = orders.create_app(settings, payment_timeout=30.0)
    async with running(app) as client:
        stock(fresh_database, [("WIDGET-1", 2599, 10), ("GADGET-2", 1000, 3)])
        yield OrderService(client, app, fresh_database, topology, channel)


def outbox_rows(url: str) -> list[tuple[Any, ...]]:
    with psycopg.connect(url) as conn:
        return conn.execute("SELECT message_type, published_at IS NOT NULL FROM outbox ORDER BY id").fetchall()


async def test_an_order_is_taken_at_once_and_its_charge_command_reaches_the_payment_queue(
    service: OrderService,
) -> None:
    response = await service.order([("WIDGET-1", 2), ("GADGET-2", 1)])
    assert response.status_code == 201, response.text
    order = response.json()
    assert order["status"] == "AWAITING_PAYMENT" and order["total_cents"] == 6198
    assert (stock_of(service.url, "WIDGET-1"), stock_of(service.url, "GADGET-2")) == (8, 2)
    [command] = await service.commands(1)
    assert command.type == "payment.charge"
    assert command.payload == ChargePayment(
        order_id=uuid.UUID(order["id"]), customer_id="c-42", amount_cents=6198, currency="USD"
    )
    assert command.correlation_id == command.payload.order_id and command.causation_id is None
    assert outbox_rows(service.url) == [("payment.charge", True)]
    assert (await service.client.get(f"/orders/{order['id']}")).json() == order


async def test_the_payment_deadline_is_the_timeout_after_the_order(service: OrderService) -> None:
    order = (await service.order([("WIDGET-1", 1)])).json()
    with psycopg.connect(service.url) as conn:
        row = conn.execute("SELECT payment_deadline - created_at FROM orders WHERE id = %s", (order["id"],)).fetchone()
    assert row == (timedelta(seconds=30),)
    await service.commands(1)


async def test_a_refused_order_writes_no_command(service: OrderService) -> None:
    assert (await service.order([("GADGET-2", 4)])).status_code == 409
    assert (await service.order([("NOPE-1", 1)])).status_code == 422
    assert count(service.url, "orders") == 0 and outbox_rows(service.url) == []
    await service.commands(0)


async def test_a_retry_replays_the_order_and_sends_no_second_command(service: OrderService) -> None:
    first = await service.order([("WIDGET-1", 1)], key="k-1")
    again = await service.order([("WIDGET-1", 1)], key="k-1")
    assert (first.status_code, again.status_code) == (201, 201)
    assert again.json() == first.json() and again.headers["idempotent-replayed"] == "true"
    assert len(await service.commands(1)) == 1
    assert (await service.order([("WIDGET-1", 2)], key="k-1")).status_code == 422


async def test_concurrent_orders_each_get_one_command(service: OrderService) -> None:
    stock(service.url, [(f"BULK-{n}", 100, 100) for n in range(5)])
    responses = await asyncio.gather(*(service.order([(f"BULK-{n % 5}", 1)]) for n in range(50)))
    assert {response.status_code for response in responses} == {201}
    commands = await service.commands(50)
    assert sorted(str(c.payload.order_id) for c in commands) == sorted(r.json()["id"] for r in responses)


async def test_an_outbox_failure_rolls_the_order_back(service: OrderService, monkeypatch: pytest.MonkeyPatch) -> None:
    async def broken(conn: Any, message: Message) -> None:
        raise psycopg.errors.DiskFull("could not extend file")

    monkeypatch.setattr(outbox, "write", broken)
    with pytest.raises(psycopg.errors.DiskFull):
        await service.order([("WIDGET-1", 1)])
    assert count(service.url, "orders") == 0 and stock_of(service.url, "WIDGET-1") == 10


async def test_orders_are_taken_while_the_broker_is_down_and_wait_in_the_outbox(service: OrderService) -> None:
    resources: Resources = service.app.state.resources
    assert resources.broker is not None
    await resources.broker.close()  # the relay can't publish now; the requests never needed the broker
    responses = [await service.order([("WIDGET-1", 1)]) for _ in range(3)]
    assert [response.status_code for response in responses] == [201] * 3
    await asyncio.sleep(0.3)
    assert outbox_rows(service.url) == [("payment.charge", False)] * 3
    health = await service.client.get("/health")
    assert health.status_code == 503 and health.json()["problems"] == {"broker": "connection closed"}


async def test_the_relay_is_a_worker_of_the_service(service: OrderService) -> None:
    resources: Resources = service.app.state.resources
    assert [task.get_name() for task in resources.workers] == ["orders.relay"]
    assert (await service.client.get("/health")).json()["problems"] == {}


async def test_the_order_view_speaks_utc(service: OrderService) -> None:
    order = (await service.order([("WIDGET-1", 1)])).json()
    created = datetime.strptime(order["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)
    assert abs(datetime.now(UTC) - created) < timedelta(seconds=30)
    await service.commands(1)


async def test_after_a_broker_drop_the_relay_reconnects_and_drains_the_outbox(
    fresh_database: str, topology: Topology, channel: AbstractChannel
) -> None:
    # A drop the client didn't ask for (unlike close()): aio-pika's robust connection reconnects after
    # reconnect_interval and restores its channels; the orders taken meanwhile wait in the outbox.
    settings = Settings("orders", fresh_database, f"{amqp_url()}?reconnect_interval=0.5", topology)
    app = orders.create_app(settings, poll_interval=0.2)
    async with running(app) as client:
        stock(fresh_database, [("WIDGET-1", 2599, 10)])
        service = OrderService(client, app, fresh_database, topology, channel)
        resources: Resources = app.state.resources
        assert resources.broker is not None
        assert (await service.order([("WIDGET-1", 1)])).status_code == 201
        await service.commands(1)
        await resources.broker.transport.connection.close(ConnectionError("simulated drop"))  # type: ignore[union-attr]
        taken = [(await service.order([("WIDGET-1", 1)])).json()["id"] for _ in range(3)]
        commands = await service.commands(3, timeout=15.0)
        assert sorted(str(c.payload.order_id) for c in commands) == sorted(taken)
        assert outbox_rows(fresh_database) == [("payment.charge", True)] * 4
        assert [task.done() for task in resources.workers] == [False]
        assert (await client.get("/health")).json()["problems"] == {}


async def test_the_service_relay_listens_so_a_command_goes_out_long_before_its_poll(
    fresh_database: str, topology: Topology, channel: AbstractChannel
) -> None:
    app = orders.create_app(Settings("orders", fresh_database, amqp_url(), topology), poll_interval=30.0)
    async with running(app) as client:
        stock(fresh_database, [("WIDGET-1", 2599, 10)])
        service = OrderService(client, app, fresh_database, topology, channel)
        await asyncio.sleep(0.3)  # the relay's first batch is done; it sleeps until a NOTIFY or 30 s
        started = time.monotonic()
        assert (await service.order([("WIDGET-1", 1)])).status_code == 201
        await service.commands(1, timeout=2.0)
        assert time.monotonic() - started < 2.0
