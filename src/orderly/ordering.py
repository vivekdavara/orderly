"""Taking an order: the part of the monolith that the order service kept (DESIGN.md "Taking an order").

The monolith and the order service both use `take_order`: validate the request, honour its idempotency key, reserve
stock line by line in sku order, and write the order. What comes next is the caller's `write`: the monolith charges
the customer with the transaction still open, the order service writes `payment.charge` to its outbox.

An Idempotency-Key makes a retry safe: the same key with the same request returns the order the first one created
(whatever has happened to it since), the same key with a different request is refused, and two requests with the same
key at once create one order (the second waits on the key's unique index, then replays the first).
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from datetime import UTC
from typing import Annotated, Any

import psycopg
from psycopg_pool import AsyncConnectionPool
from pydantic import BaseModel, ConfigDict, Field, StrictInt, field_validator

from orderly.messages import MAX_CENTS, CanonicalUUID, CustomerId, Email, UtcTime

MAX_LINES = 50
Sku = Annotated[str, Field(strict=True, pattern=r"^[A-Z0-9][A-Z0-9-]{0,31}$")]
# Visible ASCII: a key travels in an HTTP header and is compared byte for byte. Anchored, because pydantic (which
# checks the header) searches for a pattern rather than matching it whole: unanchored, "has space" passed.
IDEMPOTENCY_KEY = re.compile(r"^[\x21-\x7E]{1,200}$")
# The unique index on orders.idempotency_key, as PostgreSQL names it.
_KEY_CONSTRAINT = "orders_idempotency_key_key"


class LineRequest(BaseModel):
    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    sku: Sku
    quantity: Annotated[StrictInt, Field(ge=1, le=1000)]


class OrderRequest(BaseModel):
    """The body of POST /orders. The customer's id and e-mail use the message types, so an order the API accepts can
    always be put into `payment.charge` and `order.confirmed`; prices come from the catalogue, never the client."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    customer_id: CustomerId
    email: Email
    lines: Annotated[list[LineRequest], Field(min_length=1, max_length=MAX_LINES)]

    @field_validator("lines")
    @classmethod
    def _one_line_per_sku(cls, lines: list[LineRequest]) -> list[LineRequest]:
        skus = [line.sku for line in lines]
        repeated = sorted({sku for sku in skus if skus.count(sku) > 1})
        if repeated:
            raise ValueError(f"each sku at most once per order; repeated: {', '.join(repeated)}")
        return lines

    def fingerprint(self) -> str:
        """SHA-256 of the request's canonical JSON: what tells a retry (same request) from a reused key."""
        canonical = json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()


class LineView(BaseModel):
    sku: str
    quantity: int
    unit_price_cents: int


class OrderView(BaseModel):
    """An order as the API returns it."""

    id: CanonicalUUID
    customer_id: str
    email: str
    status: str
    total_cents: int
    currency: str
    cancel_reason: str | None
    lines: list[LineView]
    created_at: UtcTime


class OrderRefused(Exception):
    """The order can't be taken; nothing was written. `status` is the HTTP status, `detail` the response body."""

    status = 422

    def __init__(self, error: str, message: str, **extra: Any) -> None:
        super().__init__(message)
        self.detail: dict[str, Any] = {"error": error, "message": message, **extra}


class OutOfStock(OrderRefused):
    status = 409


class KeyReused(OrderRefused):
    status = 422


class Overloaded(Exception):
    """Too many orders are awaiting payment to take another (the order service's cap, day 4): nothing was written, and
    the client should try again later (503 with Retry-After, not a refusal of the order itself)."""


@dataclass(frozen=True)
class ReservedLine:
    line_no: int
    sku: str
    quantity: int
    unit_price_cents: int


@dataclass(frozen=True)
class Reserved:
    """The stock taken for an order, its lines in the request's order, and what it costs."""

    lines: tuple[ReservedLine, ...]
    total_cents: int
    currency: str


Connection = psycopg.AsyncConnection[Any]


async def reserve(conn: Connection, lines: Sequence[LineRequest]) -> Reserved:
    """Take each line's quantity from stock, in sku order, at the price on the locked row.

    Every transaction that reserves locks products in the same order, so two orders sharing products wait for each
    other instead of deadlocking. `stock >= q` is re-checked after a wait (READ COMMITTED), so stock never goes below
    zero and an order that loses the race for the last units is refused. The caller's transaction rolls everything
    back if this raises.
    """
    taken: dict[str, tuple[int, str]] = {}
    for line in sorted(lines, key=lambda line: line.sku):
        row = await (
            await conn.execute(
                "UPDATE products SET stock = stock - %s WHERE sku = %s AND stock >= %s RETURNING price_cents, currency",
                (line.quantity, line.sku, line.quantity),
            )
        ).fetchone()
        if row is None:
            found = await (await conn.execute("SELECT stock FROM products WHERE sku = %s", (line.sku,))).fetchone()
            if found is None:
                raise OrderRefused("unknown_sku", f"no product {line.sku}", sku=line.sku)
            raise OutOfStock("out_of_stock", f"{line.sku}: {line.quantity} wanted, {found[0]} in stock", sku=line.sku)
        taken[line.sku] = (int(row[0]), str(row[1]))
    currencies = sorted({currency for _, currency in taken.values()})
    if len(currencies) > 1:
        raise OrderRefused("mixed_currencies", f"an order is in one currency; these lines are in {currencies}")
    reserved = tuple(
        ReservedLine(number, line.sku, line.quantity, taken[line.sku][0]) for number, line in enumerate(lines, 1)
    )
    total = sum(line.quantity * line.unit_price_cents for line in reserved)
    if total > MAX_CENTS:
        raise OrderRefused("total_too_large", f"the total, {total} cents, is over the limit of {MAX_CENTS}")
    return Reserved(reserved, total, currencies[0])


async def release(conn: Connection, reserved: Reserved) -> None:
    """Give an order's stock back, in sku order like `reserve`."""
    for line in sorted(reserved.lines, key=lambda line: line.sku):
        await conn.execute("UPDATE products SET stock = stock + %s WHERE sku = %s", (line.quantity, line.sku))


async def insert_lines(conn: Connection, order_id: uuid.UUID, reserved: Reserved) -> None:
    async with conn.cursor() as cursor:
        await cursor.executemany(
            "INSERT INTO order_lines (order_id, line_no, sku, quantity, unit_price_cents) VALUES (%s, %s, %s, %s, %s)",
            [(order_id, line.line_no, line.sku, line.quantity, line.unit_price_cents) for line in reserved.lines],
        )


async def replay(conn: Connection, key: str, fingerprint: str) -> uuid.UUID | None:
    """The order an earlier request with this key created, or None. A different request under the key is refused."""
    row = await (
        await conn.execute("SELECT id, request_hash FROM orders WHERE idempotency_key = %s", (key,))
    ).fetchone()
    if row is None:
        return None
    if row[1] != fingerprint:
        raise KeyReused(
            "idempotency_key_reused", "this Idempotency-Key was used for a different request", order_id=str(row[0])
        )
    return uuid.UUID(str(row[0]))


async def read_order(conn: Connection, order_id: uuid.UUID) -> OrderView | None:
    row = await (
        await conn.execute(
            "SELECT id, customer_id, email, status, total_cents, currency, cancel_reason, created_at"
            " FROM orders WHERE id = %s",
            (order_id,),
        )
    ).fetchone()
    if row is None:
        return None
    lines = await (
        await conn.execute(
            "SELECT sku, quantity, unit_price_cents FROM order_lines WHERE order_id = %s ORDER BY line_no", (order_id,)
        )
    ).fetchall()
    return OrderView(
        id=row[0],
        customer_id=row[1],
        email=row[2],
        status=row[3],
        total_cents=row[4],
        currency=row[5],
        cancel_reason=row[6],
        lines=[LineView(sku=sku, quantity=quantity, unit_price_cents=price) for sku, quantity, price in lines],
        # psycopg returns timestamptz in the session's time zone; the API speaks UTC.
        created_at=row[7].astimezone(UTC),
    )


# What the caller does with a reserved order, in the same transaction: insert the `orders` row (its columns differ
# between the monolith and the order service) and its lines, then charge or write to the outbox.
Write = Callable[[Connection, uuid.UUID, Reserved], Awaitable[None]]


@dataclass(frozen=True)
class Taken:
    order: OrderView
    # True when an earlier request with the same Idempotency-Key created the order.
    replayed: bool


async def take_order(pool: AsyncConnectionPool, request: OrderRequest, key: str | None, write: Write) -> Taken:
    """Reserve, write and commit an order, or replay the one its key already made. Raises OrderRefused (nothing
    written) for an order that can't be taken."""
    fingerprint = request.fingerprint()
    # Twice at most: a unique violation on the key means a request with it committed, so the second pass replays it.
    for _ in range(2):
        try:
            async with pool.connection() as conn, conn.transaction():
                if key is not None and (earlier := await replay(conn, key, fingerprint)) is not None:
                    return Taken(await _read(conn, earlier), replayed=True)
                reserved = await reserve(conn, request.lines)
                order_id = uuid.uuid4()
                await write(conn, order_id, reserved)
                return Taken(await _read(conn, order_id), replayed=False)
        except psycopg.errors.UniqueViolation as error:
            if key is None or error.diag.constraint_name != _KEY_CONSTRAINT:
                raise
        except (OutOfStock, Overloaded):
            # A request with the same key may have taken the stock this one was waiting for (or the last place under
            # the cap): it was this request's first attempt, and the client should get its order, not a 409 or a 503
            # (re-check the key after a refusal).
            if key is not None:
                async with pool.connection() as conn, conn.transaction():
                    earlier = await replay(conn, key, fingerprint)
                    if earlier is not None:
                        return Taken(await _read(conn, earlier), replayed=True)
            raise
    raise RuntimeError(f"Idempotency-Key {key!r} conflicted twice without a committed order to replay")


async def _read(conn: Connection, order_id: uuid.UUID) -> OrderView:
    order = await read_order(conn, order_id)
    if order is None:
        raise RuntimeError(f"order {order_id} vanished inside its own transaction")
    return order
