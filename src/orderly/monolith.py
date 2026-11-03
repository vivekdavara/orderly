"""The monolith: the baseline the services were split from (DESIGN.md "The monolith").

`POST /orders` does everything in one request and one transaction: reserve the stock, insert the order, charge the
customer through the provider, record the payment, confirm or cancel, send the e-mail, commit. It is simple and
strongly consistent, and it has the problems the split exists to fix, which the tests pin rather than hide:

- the provider and the mailer are called with the transaction open, holding the products' row locks and a pooled
  connection for their whole latency, so a slow provider slows every order sharing a product;
- a failure after the charge (the mailer, the commit) rolls the order back but not the charge, and the client's retry
  creates a new order and charges again.

    python -m orderly.service monolith      # on 127.0.0.1:8112, against orderly_monolith
"""

from __future__ import annotations

import uuid
from typing import Annotated

from fastapi import APIRouter, FastAPI, Header, Request
from fastapi.responses import JSONResponse

from orderly import emails, ordering
from orderly.fakes import FakeMailer, FakeProvider, MailerUnavailable, ProviderUnavailable, provider_from_environment
from orderly.ordering import Connection, OrderRefused, OrderRequest, Reserved
from orderly.service import Resources, Settings
from orderly.service import create_app as create_service_app

# A declined charge is a failed request: the order exists (cancelled), and the response says payment is required.
STATUS_CODES = {"CONFIRMED": 201, "CANCELLED": 402}

IdempotencyKey = Annotated[str | None, Header(pattern=ordering.IDEMPOTENCY_KEY.pattern)]


def create_app(
    settings: Settings, *, provider: FakeProvider | None = None, mailer: FakeMailer | None = None
) -> FastAPI:
    provider = provider_from_environment() if provider is None else provider
    mailer = FakeMailer() if mailer is None else mailer

    async def charge_and_decide(
        conn: Connection, order_id: uuid.UUID, reserved: Reserved, body: OrderRequest, key: str | None
    ) -> None:
        # Inserted before the charge, so the idempotency key is claimed first: a concurrent retry with the same key
        # waits on the key's unique index here instead of charging the customer a second time.
        await conn.execute(
            "INSERT INTO orders (id, customer_id, email, status, total_cents, currency, idempotency_key, request_hash)"
            " VALUES (%s, %s, %s, 'AWAITING_PAYMENT', %s, %s, %s, %s)",
            (
                order_id,
                body.customer_id,
                body.email,
                reserved.total_cents,
                reserved.currency,
                key,
                None if key is None else body.fingerprint(),
            ),
        )
        await ordering.insert_lines(conn, order_id, reserved)
        # The baseline's flaw: the provider's whole latency with this transaction open and the stock rows locked.
        charge = await provider.charge(order_id, body.customer_id, reserved.total_cents, reserved.currency)
        await conn.execute(
            "INSERT INTO payments (id, order_id, status, amount_cents, currency, charge_ref)"
            " VALUES (%s, %s, %s, %s, %s, %s)",
            (
                uuid.uuid4(),
                order_id,
                "CHARGED" if charge.approved else "DECLINED",
                reserved.total_cents,
                reserved.currency,
                charge.reference,
            ),
        )
        if charge.approved:
            await conn.execute("UPDATE orders SET status = 'CONFIRMED', updated_at = now() WHERE id = %s", (order_id,))
            kind, email = (
                "order_confirmed",
                emails.confirmation(order_id, body.email, reserved.total_cents, reserved.currency),
            )
        else:
            await ordering.release(conn, reserved)
            await conn.execute(
                "UPDATE orders SET status = 'CANCELLED', cancel_reason = 'payment_declined', updated_at = now()"
                " WHERE id = %s",
                (order_id,),
            )
            kind, email = "order_cancelled", emails.cancellation(order_id, body.email, "payment_declined")
        # And the e-mail inside it too: if it fails, the order fails (after the customer was charged).
        await mailer.send(order_id, email)
        await conn.execute(
            "INSERT INTO notifications (order_id, kind, recipient, subject, body) VALUES (%s, %s, %s, %s, %s)",
            (order_id, kind, email.recipient, email.subject, email.body),
        )

    router = APIRouter()

    @router.post("/orders")
    async def place_order(request: Request, body: OrderRequest, idempotency_key: IdempotencyKey = None) -> JSONResponse:
        resources: Resources = request.app.state.resources

        async def write(conn: Connection, order_id: uuid.UUID, reserved: Reserved) -> None:
            await charge_and_decide(conn, order_id, reserved, body, idempotency_key)

        try:
            taken = await ordering.take_order(resources.pool, body, idempotency_key, write)
        except OrderRefused as refused:
            return JSONResponse({"detail": refused.detail}, status_code=refused.status)
        except (ProviderUnavailable, MailerUnavailable) as error:
            which = "payment_provider_unavailable" if isinstance(error, ProviderUnavailable) else "mailer_unavailable"
            return JSONResponse(
                {"detail": {"error": which, "message": str(error)}}, status_code=503, headers={"Retry-After": "1"}
            )
        headers = {"Location": f"/orders/{taken.order.id}"}
        if taken.replayed:
            headers["Idempotent-Replayed"] = "true"
        return JSONResponse(
            taken.order.model_dump(mode="json"), status_code=STATUS_CODES[taken.order.status], headers=headers
        )

    @router.get("/orders/{order_id}")
    async def get_order(request: Request, order_id: uuid.UUID) -> JSONResponse:
        resources: Resources = request.app.state.resources
        async with resources.pool.connection() as conn:
            order = await ordering.read_order(conn, order_id)
        if order is None:
            return JSONResponse({"detail": {"error": "not_found", "message": f"no order {order_id}"}}, status_code=404)
        return JSONResponse(order.model_dump(mode="json"))

    app = create_service_app(settings, router=router)
    app.state.provider = provider
    app.state.mailer = mailer
    return app
