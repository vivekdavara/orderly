"""The order service (DESIGN.md "The split"): the monolith's order-taking half, with the charge replaced by a message.

`POST /orders` reserves the stock, inserts the order as AWAITING_PAYMENT with a payment deadline, and writes
`payment.charge` to the outbox, all in one transaction; it answers 201 without waiting for the payment. The outbox
relay, a worker of this service, publishes the command to the payment service's queue. Nothing in the request touches
the broker, so orders are still taken while RabbitMQ is down: they wait in the outbox.

Day 3 adds the rest of the saga, the order service being its orchestrator (DESIGN.md "The saga"): a consumer of the
payment service's replies (`order.payment-events`) and a sweeper that gives up on orders whose payment deadline has
passed. Both read the order's row `FOR UPDATE`, ask `saga.transition` what to do, and apply its `Step` in the same
transaction: the new status, the stock given back, `payment.refund`, `order.confirmed` or `order.cancelled` written to
the outbox. Locking the row first is what makes a reply and the deadline arriving together safe: one waits for the
other and then sees the order as the other left it.

    python -m orderly.service orders        # on 127.0.0.1:8112, against orderly_orders and RabbitMQ on 5673
"""

from __future__ import annotations

import asyncio
import logging
import os
import uuid
from collections.abc import Callable
from dataclasses import dataclass

import psycopg
from fastapi import FastAPI
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from orderly import order_api, ordering, outbox
from orderly.consumer import Consumer, Refused
from orderly.messages import (
    ChargePayment,
    Message,
    OrderCancelled,
    OrderConfirmed,
    PaymentFailed,
    PaymentRefunded,
    PaymentSucceeded,
    RefundPayment,
    new_message,
)
from orderly.ordering import Connection, OrderRequest, Overloaded, Reserved
from orderly.outbox import Relay, wait_any
from orderly.saga import DeadlinePassed, OrderState, SagaInput, Step, transition
from orderly.service import Resources, Settings
from orderly.service import create_app as create_service_app

# How long an order waits for the payment service's answer before the saga cancels it (day 3).
DEFAULT_PAYMENT_TIMEOUT = 30.0
# How often the sweeper looks for orders past their deadline, and how many it cancels per pass at most.
SWEEP_INTERVAL = 1.0
SWEEP_BATCH = 100
QUEUE = "order.payment-events"
CONSUMER = "orders"
# The messages its consumer takes (contracts/orders.json says what it reads in each).
ACCEPTS = frozenset({"payment.succeeded", "payment.failed", "payment.refunded"})

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class LockedOrder:
    id: uuid.UUID
    state: OrderState
    customer_id: str
    email: str
    total_cents: int
    currency: str


async def lock_order(conn: Connection, order_id: uuid.UUID) -> LockedOrder | None:
    """The order's row, locked until the transaction ends (a reply and the sweeper take this lock first)."""
    row = await (
        await conn.execute(
            "SELECT status, refund_requested, customer_id, email, total_cents, currency FROM orders"
            " WHERE id = %s FOR UPDATE",
            (order_id,),
        )
    ).fetchone()
    if row is None:
        return None
    return LockedOrder(order_id, OrderState(row[0], row[1]), row[2], row[3], row[4], row[5])


async def release_stock(conn: Connection, order_id: uuid.UUID) -> None:
    """Give a cancelled order's stock back, in sku order like a reservation, so the two can't deadlock."""
    lines = await (
        await conn.execute("SELECT sku, quantity FROM order_lines WHERE order_id = %s ORDER BY sku", (order_id,))
    ).fetchall()
    for sku, quantity in lines:
        await conn.execute("UPDATE products SET stock = stock + %s WHERE sku = %s", (quantity, sku))


async def apply_step(conn: Connection, order: LockedOrder, step: Step, cause: uuid.UUID | None) -> None:
    """Apply a saga step to a locked order, in the caller's transaction."""
    if step.anomaly:
        log.warning("order %s: %s while %s; ignored", order.id, step.outcome, order.state.status)
    if step.status != order.state.status:
        await conn.execute(
            "UPDATE orders SET status = %s, cancel_reason = %s, updated_at = now() WHERE id = %s",
            (step.status, step.cancel_reason, order.id),
        )
    if step.release_stock:
        await release_stock(conn, order.id)
    if step.refund is not None:
        await conn.execute("UPDATE orders SET refund_requested = true, updated_at = now() WHERE id = %s", (order.id,))
        refund = RefundPayment(
            order_id=order.id, amount_cents=order.total_cents, currency=order.currency, reason=step.refund
        )
        await outbox.write(conn, new_message(refund, causation_id=cause))
    if step.announce == "order.confirmed":
        confirmed = OrderConfirmed(
            order_id=order.id,
            customer_id=order.customer_id,
            email=order.email,
            total_cents=order.total_cents,
            currency=order.currency,
        )
        await outbox.write(conn, new_message(confirmed, causation_id=cause))
    elif step.announce == "order.cancelled":
        assert step.cancel_reason is not None
        cancelled = OrderCancelled(
            order_id=order.id, customer_id=order.customer_id, email=order.email, reason=step.cancel_reason
        )
        await outbox.write(conn, new_message(cancelled, causation_id=cause))


async def handle_reply(conn: Connection, message: Message) -> None:
    """The consumer's handler: one payment reply, applied to its order."""
    reply = message.payload
    if not isinstance(reply, PaymentSucceeded | PaymentFailed | PaymentRefunded):
        raise Refused(f"the order service doesn't take {message.type}")
    order = await lock_order(conn, reply.order_id)
    if order is None:
        # Not a timing problem: orders are committed before their charge is even written. Waiting won't help.
        raise Refused(f"no order {reply.order_id}")
    event: SagaInput = reply
    await apply_step(conn, order, transition(order.state, event), cause=message.message_id)


async def sweep(pool: AsyncConnectionPool, batch: int = SWEEP_BATCH) -> int:
    """Cancel up to `batch` orders whose payment deadline has passed; return how many.

    One order per transaction: an order's stock is released in sku order, and two orders released in one transaction
    would lock products out of that order (A's skus, then B's), which can deadlock against a reservation.
    """
    done = 0
    while done < batch:
        async with pool.connection() as conn, conn.transaction():
            row = await (
                await conn.execute(
                    "SELECT id FROM orders WHERE status = 'AWAITING_PAYMENT' AND payment_deadline <= now()"
                    " ORDER BY payment_deadline LIMIT 1 FOR UPDATE SKIP LOCKED"
                )
            ).fetchone()
            if row is None:
                return done
            order = await lock_order(conn, row[0])
            assert order is not None
            await apply_step(conn, order, transition(order.state, DeadlinePassed()), cause=None)
        done += 1
    return done


async def run_sweeper(pool: AsyncConnectionPool, stop: asyncio.Event, interval: float = SWEEP_INTERVAL) -> None:
    while not stop.is_set():
        try:
            swept = await sweep(pool)
            if swept:
                log.info("sweeper: cancelled %d orders past their payment deadline", swept)
            if swept == SWEEP_BATCH:
                continue  # probably more waiting
        except (psycopg.Error, PoolTimeout, OSError) as error:
            log.warning("sweeper: %r; trying again in %.1f s", error, interval)
        await wait_any(stop, timeout=interval)


def order_writer(
    payment_timeout: float, max_awaiting: int | None = None
) -> Callable[[OrderRequest, str | None], ordering.Write]:
    """The order service's write after the reservation, for `ordering.take_order`: the order as AWAITING_PAYMENT with
    its deadline `payment_timeout` seconds away, its lines, and `payment.charge` to the outbox, in the request's
    transaction. (The contract tests run it without the app.)

    With `max_awaiting`, an order finding that many orders already awaiting payment raises `Overloaded` instead (503,
    everything rolled back): the day-4 load test showed the service taking orders faster than the payment service could
    charge them, until they timed out. The count isn't locked, so concurrent requests can overshoot it by up to the
    pool's size; it is a brake, not an exact limit."""

    def write_for(body: OrderRequest, key: str | None) -> ordering.Write:
        async def write(conn: Connection, order_id: uuid.UUID, reserved: Reserved) -> None:
            if max_awaiting is not None:
                row = await (
                    await conn.execute("SELECT count(*) FROM orders WHERE status = 'AWAITING_PAYMENT'")
                ).fetchone()
                awaiting = 0 if row is None else int(row[0])
                if awaiting >= max_awaiting:
                    raise Overloaded(f"{awaiting} orders are awaiting payment (at most {max_awaiting}); try again")
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
                    payment_timeout,
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

    return write_for


def payment_timeout_from_environment() -> float:
    return float(os.environ.get("ORDERLY_PAYMENT_TIMEOUT_S", DEFAULT_PAYMENT_TIMEOUT))


def max_awaiting_from_environment() -> int | None:
    """ORDERLY_MAX_AWAITING_PAYMENT: the cap on orders awaiting payment (unset or empty: none)."""
    value = os.environ.get("ORDERLY_MAX_AWAITING_PAYMENT", "")
    if not value:
        return None
    cap = int(value)
    if cap < 1:
        raise ValueError(f"ORDERLY_MAX_AWAITING_PAYMENT must be at least 1, not {cap}")
    return cap


def create_app(
    settings: Settings,
    *,
    payment_timeout: float | None = None,
    poll_interval: float = 1.0,
    sweep_interval: float = SWEEP_INTERVAL,
    prefetch: int = 8,
    max_awaiting: int | None = None,
) -> FastAPI:
    """`max_awaiting` caps the orders awaiting payment (default: ORDERLY_MAX_AWAITING_PAYMENT, else none)."""
    timeout = payment_timeout_from_environment() if payment_timeout is None else payment_timeout
    cap = max_awaiting_from_environment() if max_awaiting is None else max_awaiting
    consumers: list[Consumer] = []

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

    async def consumer(resources: Resources, stop: asyncio.Event) -> None:
        if resources.broker is None:
            raise RuntimeError("the order service needs a broker")
        worker = Consumer(
            pool=resources.pool,
            connection=resources.broker,
            topology=resources.settings.topology,
            queue=QUEUE,
            name=CONSUMER,
            handle=handle_reply,
            accepts=ACCEPTS,
            prefetch=prefetch,
        )
        consumers.append(worker)
        await worker.run(stop)

    async def sweeper(resources: Resources, stop: asyncio.Event) -> None:
        await run_sweeper(resources.pool, stop, sweep_interval)

    # Every order taken is 201: where its saga stands is in the body's status.
    router = order_api.router(
        order_writer(timeout, cap),
        status_code=lambda order: 201,
        unavailable={Overloaded: "too_many_awaiting_payment"},
    )
    app = create_service_app(settings, router=router, workers=[relay, consumer, sweeper])
    app.state.consumers = consumers
    return app
