"""The notification service (DESIGN.md "The split"): tells the customer how their order ended.

It subscribes to `order.confirmed` and `order.cancelled` (choreography: nothing waits for it, nothing is undone if it is
slow) and sends the e-mail through the mailer. Each event is one transaction of the idempotent consumer:

1. insert the notification's row (`unique (order_id, kind)`); if a row is already there, the customer was told (an event
   published twice under two ids, which the outbox doesn't do, still sends one e-mail) and nothing else happens;
2. send the e-mail with the transaction open; a failed send rolls the row back and the event is retried with backoff;
3. commit; acknowledge.

The e-mail is the one effect outside the database, so it is at least once: a crash after the send and before the commit
sends it again on the redelivery. Every event carries what the e-mail needs, so the service never asks the order
service anything.

    python -m orderly.service notifications     # on 127.0.0.1:8114, against orderly_notifications and RabbitMQ on 5673
"""

from __future__ import annotations

import asyncio
import os
import uuid
from typing import Any, Protocol

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse

from orderly import emails
from orderly.consumer import Connection, Consumer, Refused
from orderly.fakes import Email, FakeMailer
from orderly.messages import Message, OrderCancelled, OrderConfirmed
from orderly.service import Resources, Settings
from orderly.service import create_app as create_service_app

QUEUE = "notification.order-events"
CONSUMER = "notifications"


class Mailer(Protocol):
    async def send(self, order_id: uuid.UUID, email: Email) -> None: ...


def mailer_from_environment() -> FakeMailer:
    """ORDERLY_MAILER_LATENCY_MS and ORDERLY_MAILER_FAILURE_RATIO (defaults 0)."""
    return FakeMailer(
        latency=float(os.environ.get("ORDERLY_MAILER_LATENCY_MS", "0")) / 1000,
        failure_ratio=float(os.environ.get("ORDERLY_MAILER_FAILURE_RATIO", "0")),
    )


def compose(event: OrderConfirmed | OrderCancelled) -> tuple[str, Email]:
    """The notification's kind and its e-mail."""
    if isinstance(event, OrderConfirmed):
        return "order_confirmed", emails.confirmation(event.order_id, event.email, event.total_cents, event.currency)
    return "order_cancelled", emails.cancellation(event.order_id, event.email, event.reason)


async def apply(conn: Connection, message: Message, mailer: Mailer) -> None:
    """Record and send one notification in the caller's transaction (the consumer's)."""
    event = message.payload
    if not isinstance(event, OrderConfirmed | OrderCancelled):
        raise Refused(f"the notification service doesn't take {message.type}")
    kind, email = compose(event)
    inserted = await conn.execute(
        "INSERT INTO notifications (order_id, kind, recipient, subject, body, message_id)"
        " VALUES (%s, %s, %s, %s, %s, %s) ON CONFLICT (order_id, kind) DO NOTHING",
        (event.order_id, kind, email.recipient, email.subject, email.body, message.message_id),
    )
    if inserted.rowcount == 0:
        return  # told already
    await mailer.send(event.order_id, email)


def _router() -> APIRouter:
    routes = APIRouter()

    @routes.get("/notifications/{order_id}")
    async def get_notifications(request: Request, order_id: uuid.UUID) -> JSONResponse:
        async with request.app.state.resources.pool.connection() as conn:
            rows = await (
                await conn.execute(
                    "SELECT kind, recipient, subject, body, message_id FROM notifications WHERE order_id = %s"
                    " ORDER BY id",
                    (order_id,),
                )
            ).fetchall()
        keys = ("kind", "recipient", "subject", "body", "message_id")
        found: list[dict[str, Any]] = [dict(zip(keys, (*row[:4], str(row[4])), strict=True)) for row in rows]
        return JSONResponse(found)

    return routes


def create_app(settings: Settings, *, mailer: Mailer | None = None, prefetch: int = 8) -> FastAPI:
    the_mailer: Mailer = mailer_from_environment() if mailer is None else mailer
    consumers: list[Consumer] = []

    async def consumer(resources: Resources, stop: asyncio.Event) -> None:
        if resources.broker is None:
            raise RuntimeError("the notification service needs a broker")

        async def handle(conn: Connection, message: Message) -> None:
            await apply(conn, message, the_mailer)

        worker = Consumer(
            pool=resources.pool,
            connection=resources.broker,
            topology=resources.settings.topology,
            queue=QUEUE,
            name=CONSUMER,
            handle=handle,
            accepts=frozenset({"order.confirmed", "order.cancelled"}),
            prefetch=prefetch,
        )
        consumers.append(worker)
        await worker.run(stop)

    app = create_service_app(settings, router=_router(), workers=[consumer])
    app.state.mailer = the_mailer
    app.state.consumers = consumers
    return app
