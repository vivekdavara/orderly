"""The order service (DESIGN.md "The split"): the monolith's order-taking half, with the charge replaced by a message.

`POST /orders` reserves the stock, inserts the order as AWAITING_PAYMENT with a payment deadline, and writes
`payment.charge` to the outbox, all in one transaction; it answers 201 without waiting for the payment. The outbox
relay, a worker of this service, publishes the command to the payment service's queue. Nothing in the request touches
the broker, so orders are still taken while RabbitMQ is down: they wait in the outbox.

Day 3 adds the saga: the consumer of the payment service's replies, and the sweeper that cancels orders whose
deadline passes.

    python -m orderly.service orders        # on 127.0.0.1:8112, against orderly_orders and RabbitMQ on 5673
"""

from __future__ import annotations

import asyncio
import os
import uuid

from fastapi import FastAPI

from orderly import order_api, ordering, outbox
from orderly.messages import ChargePayment, new_message
from orderly.ordering import Connection, OrderRequest, Reserved
from orderly.outbox import Relay
from orderly.service import Resources, Settings
from orderly.service import create_app as create_service_app

# How long an order waits for the payment service's answer before the saga cancels it (day 3).
DEFAULT_PAYMENT_TIMEOUT = 30.0


def payment_timeout_from_environment() -> float:
    return float(os.environ.get("ORDERLY_PAYMENT_TIMEOUT_S", DEFAULT_PAYMENT_TIMEOUT))


def create_app(settings: Settings, *, payment_timeout: float | None = None, poll_interval: float = 1.0) -> FastAPI:
    timeout = payment_timeout_from_environment() if payment_timeout is None else payment_timeout

    def write_for(body: OrderRequest, key: str | None) -> ordering.Write:
        async def write(conn: Connection, order_id: uuid.UUID, reserved: Reserved) -> None:
            await conn.execute(
                "INSERT INTO orders (id, customer_id, email, status, total_cents, currency, idempotency_key,"
                " request_hash, payment_deadline)"
                " VALUES (%s, %s, %s, 'AWAITING_PAYMENT', %s, %s, %s, %s, now() + make_interval(secs => %s))",
                (
                    order_id,
                    body.customer_id,
                    body.email,
                    reserved.total_cents,
                    reserved.currency,
                    key,
                    None if key is None else body.fingerprint(),
                    timeout,
                ),
            )
            await ordering.insert_lines(conn, order_id, reserved)
            # The saga's first step, in the order's own transaction: the command exists if and only if the order does.
            charge = ChargePayment(
                order_id=order_id,
                customer_id=body.customer_id,
                amount_cents=reserved.total_cents,
                currency=reserved.currency,
            )
            await outbox.write(conn, new_message(charge))

        return write

    async def relay(resources: Resources, stop: asyncio.Event) -> None:
        if resources.broker is None:
            raise RuntimeError("the order service needs a broker")
        await Relay(
            pool=resources.pool,
            connection=resources.broker,
            topology=resources.settings.topology,
            listen_url=resources.settings.database_url,
            poll_interval=poll_interval,
        ).run(stop)

    # Every order taken is 201: where its saga stands is in the body's status.
    router = order_api.router(write_for, status_code=lambda order: 201)
    return create_service_app(settings, router=router, workers=[relay])
