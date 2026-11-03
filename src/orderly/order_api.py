"""The HTTP API for orders, the same for the monolith and the order service: what differs is what an order's
transaction writes (`write_for`) and what status code an order's state answers with.

    POST /orders         body: OrderRequest; optional Idempotency-Key header
                         201/402 the order (402: the monolith's decline); "Idempotent-Replayed: true" on a replay
                         409 out of stock, 422 a malformed request, an unknown sku or a reused key, 503 a dependency
    GET  /orders/{id}    200 the order, 404
"""

from __future__ import annotations

import uuid
from collections.abc import Callable, Mapping
from typing import Annotated

from fastapi import APIRouter, Header, Request
from fastapi.responses import JSONResponse

from orderly import ordering
from orderly.ordering import OrderRefused, OrderRequest, OrderView, Write

IdempotencyKey = Annotated[str | None, Header(pattern=ordering.IDEMPOTENCY_KEY.pattern)]


def router(
    write_for: Callable[[OrderRequest, str | None], Write],
    status_code: Callable[[OrderView], int],
    unavailable: Mapping[type[Exception], str] | None = None,
) -> APIRouter:
    """`write_for(request, key)` gives the write for one request; `unavailable` maps the exceptions of a dependency
    that is down (the provider, the mailer) to the error name of a 503, which tells the client to retry."""
    errors = dict(unavailable or {})
    routes = APIRouter()

    @routes.post("/orders")
    async def place_order(request: Request, body: OrderRequest, idempotency_key: IdempotencyKey = None) -> JSONResponse:
        pool = request.app.state.resources.pool
        try:
            taken = await ordering.take_order(pool, body, idempotency_key, write_for(body, idempotency_key))
        except OrderRefused as refused:
            return JSONResponse({"detail": refused.detail}, status_code=refused.status)
        except tuple(errors) as error:
            name = next(name for kind, name in errors.items() if isinstance(error, kind))
            return JSONResponse(
                {"detail": {"error": name, "message": str(error)}}, status_code=503, headers={"Retry-After": "1"}
            )
        headers = {"Location": f"/orders/{taken.order.id}"}
        if taken.replayed:
            headers["Idempotent-Replayed"] = "true"
        return JSONResponse(taken.order.model_dump(mode="json"), status_code=status_code(taken.order), headers=headers)

    @routes.get("/orders/{order_id}")
    async def get_order(request: Request, order_id: uuid.UUID) -> JSONResponse:
        async with request.app.state.resources.pool.connection() as conn:
            order = await ordering.read_order(conn, order_id)
        if order is None:
            return JSONResponse({"detail": {"error": "not_found", "message": f"no order {order_id}"}}, status_code=404)
        return JSONResponse(order.model_dump(mode="json"))

    return routes
