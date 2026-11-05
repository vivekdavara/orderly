"""The payment service (DESIGN.md "The split", "The saga"): charges and refunds orders through the provider.

It consumes `payment.commands` (`payment.charge`, `payment.refund`) with an idempotent consumer and answers each command
through its own outbox and relay (`payment.succeeded`, `payment.failed`, `payment.refunded`, to the order service). One
command is one transaction:

1. lock the order (a transaction-level advisory lock on its id), so two commands for one order (a charge and a refund
   delivered together) are applied one after the other, never both against "no payment yet";
2. read the order's payment row and let `payment_rules.decide` say what to do;
3. call the provider if that's charging, refunding or voiding, with the transaction open (the provider's calls are
   idempotent by order id, so a retry after a failure asks again without taking money twice);
4. write the row, and the reply `payment_rules.reply` computes from it, to the outbox; commit; acknowledge.

A refund that finds no payment voids the order at the provider rather than only recording a void: a charge whose
transaction never committed (a crash, a timeout after the provider answered) may have taken the money, and the void
refunds it and closes the order's key against any charge still in flight.

    python -m orderly.service payments      # on 127.0.0.1:8113, against orderly_payments and RabbitMQ on 5673
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any, Protocol

from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import JSONResponse

from orderly import outbox
from orderly.consumer import Connection, Consumer, Refused
from orderly.fakes import Charge, ProviderConflict, ProviderVoided, Refund, provider_from_environment
from orderly.messages import ChargePayment, Message, RefundPayment, new_message
from orderly.outbox import Relay
from orderly.payment_rules import PaymentConflict, PaymentRow, decide, reply
from orderly.service import Resources, Settings
from orderly.service import create_app as create_service_app

QUEUE = "payment.commands"
CONSUMER = "payments"
# The messages its consumer takes (contracts/payments.json says what it reads in each).
ACCEPTS = frozenset({"payment.charge", "payment.refund"})
# The class of the advisory locks this service takes on orders (the key is the order id's hash): its own number, so
# they can't meet the migrations' lock or another service's.
LOCK_CLASS = 4113


class Provider(Protocol):
    async def charge(self, order_id: uuid.UUID, customer_id: str, amount_cents: int, currency: str) -> Charge: ...
    async def refund(self, order_id: uuid.UUID, amount_cents: int, currency: str) -> Refund: ...
    async def void(self, order_id: uuid.UUID, amount_cents: int, currency: str) -> tuple[Charge, Refund] | None: ...


async def read_payment(conn: Connection, order_id: uuid.UUID) -> PaymentRow | None:
    row = await (
        await conn.execute(
            "SELECT id, order_id, status, amount_cents, currency FROM payments WHERE order_id = %s", (order_id,)
        )
    ).fetchone()
    return None if row is None else PaymentRow(row[0], row[1], row[2], row[3], row[4])


async def _insert(
    conn: Connection,
    order_id: uuid.UUID,
    status: str,
    amount_cents: int,
    currency: str,
    charge_ref: str | None = None,
    refund_ref: str | None = None,
) -> PaymentRow:
    payment_id = uuid.uuid4()
    await conn.execute(
        "INSERT INTO payments (id, order_id, status, amount_cents, currency, charge_ref, refund_ref)"
        " VALUES (%s, %s, %s, %s, %s, %s, %s)",
        (payment_id, order_id, status, amount_cents, currency, charge_ref, refund_ref),
    )
    return PaymentRow(payment_id, order_id, status, amount_cents, currency)  # type: ignore[arg-type]


async def apply(conn: Connection, message: Message, provider: Provider) -> None:
    """Apply one command in the caller's transaction (the consumer's), writing the reply to the outbox."""
    command = message.payload
    if not isinstance(command, ChargePayment | RefundPayment):
        raise Refused(f"the payment service doesn't take {message.type}")
    order_id = command.order_id
    await conn.execute("SELECT pg_advisory_xact_lock(%s, hashtext(%s))", (LOCK_CLASS, str(order_id)))
    row = await read_payment(conn, order_id)
    try:
        action = decide(row, command)
        if action == "charge":
            assert isinstance(command, ChargePayment)
            try:
                charge = await provider.charge(order_id, command.customer_id, command.amount_cents, command.currency)
            except ProviderVoided as voided:
                # A refund's void reached the provider and its transaction was lost (so no row says so yet): the
                # provider will never take this charge. Record what it says, and answer as the table does for a
                # voided or refunded payment, rather than dead-lettering the charge (day-3 review). If the void had
                # refunded a charge taken earlier, the books say so: REFUNDED, with both references (round 2).
                if voided.settled is None:
                    row = await _insert(conn, order_id, "VOIDED", command.amount_cents, command.currency)
                else:
                    taken, refund = voided.settled
                    row = await _insert(
                        conn,
                        order_id,
                        "REFUNDED",
                        taken.amount_cents,
                        taken.currency,
                        taken.reference,
                        refund.reference,
                    )
            else:
                status = "CHARGED" if charge.approved else "DECLINED"
                row = await _insert(conn, order_id, status, command.amount_cents, command.currency, charge.reference)
        elif action == "void":
            settled = await provider.void(order_id, command.amount_cents, command.currency)
            if settled is None:
                row = await _insert(conn, order_id, "VOIDED", command.amount_cents, command.currency)
            else:
                taken, refund = settled
                row = await _insert(
                    conn, order_id, "REFUNDED", taken.amount_cents, taken.currency, taken.reference, refund.reference
                )
        elif action == "refund":
            assert row is not None
            refund = await provider.refund(order_id, row.amount_cents, row.currency)
            await conn.execute(
                "UPDATE payments SET status = 'REFUNDED', refund_ref = %s, updated_at = now() WHERE id = %s",
                (refund.reference, row.id),
            )
            row = PaymentRow(row.id, row.order_id, "REFUNDED", row.amount_cents, row.currency)
    except (PaymentConflict, ProviderConflict) as error:
        # The command contradicts the books or the provider's: no retry will change that.
        raise Refused(str(error)) from error
    assert row is not None
    await outbox.write(conn, new_message(reply(row, command), causation_id=message.message_id))


def _router() -> APIRouter:
    routes = APIRouter()

    @routes.get("/payments/{order_id}")
    async def get_payment(request: Request, order_id: uuid.UUID) -> JSONResponse:
        async with request.app.state.resources.pool.connection() as conn:
            row = await (
                await conn.execute(
                    "SELECT id, status, amount_cents, currency, charge_ref, refund_ref FROM payments"
                    " WHERE order_id = %s",
                    (order_id,),
                )
            ).fetchone()
        if row is None:
            return JSONResponse({"detail": {"error": "not_found", "message": f"no payment for {order_id}"}}, 404)
        keys = ("id", "status", "amount_cents", "currency", "charge_ref", "refund_ref")
        body: dict[str, Any] = dict(zip(keys, row, strict=True))
        body["id"] = str(body["id"])
        return JSONResponse(body)

    return routes


def create_app(
    settings: Settings,
    *,
    provider: Provider | None = None,
    poll_interval: float = 1.0,
    prefetch: int = 8,
    handler_timeout: float = 10.0,
) -> FastAPI:
    """`handler_timeout` bounds each command's transaction, its provider call included (the consumer's default)."""
    the_provider: Provider = provider_from_environment() if provider is None else provider
    consumers: list[Consumer] = []

    async def relay(resources: Resources, stop: asyncio.Event) -> None:
        if resources.broker is None:
            raise RuntimeError("the payment service needs a broker")
        await Relay(
            pool=resources.pool,
            connection=resources.broker,
            topology=resources.settings.topology,
            listen_url=resources.settings.database_url,
            poll_interval=poll_interval,
        ).run(stop)

    async def consumer(resources: Resources, stop: asyncio.Event) -> None:
        if resources.broker is None:
            raise RuntimeError("the payment service needs a broker")

        async def handle(conn: Connection, message: Message) -> None:
            await apply(conn, message, the_provider)

        worker = Consumer(
            pool=resources.pool,
            connection=resources.broker,
            topology=resources.settings.topology,
            queue=QUEUE,
            name=CONSUMER,
            handle=handle,
            accepts=ACCEPTS,
            prefetch=prefetch,
            handler_timeout=handler_timeout,
        )
        consumers.append(worker)
        await worker.run(stop)

    app = create_service_app(settings, router=_router(), workers=[relay, consumer])
    app.state.provider = the_provider
    app.state.consumers = consumers
    return app
