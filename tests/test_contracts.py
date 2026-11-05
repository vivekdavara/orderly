"""Consumer-driven contract tests (day 4): `contracts/<consumer>.json` against both sides.

Each consumer owns one file: for every message it takes, an example, the state (`given`) its producer must be in to
send it, and `match`, the fields the consumer reads and what it needs of each (`exact`: the example's value; `uuid`:
any id; `order`: the order's id). Fields not in `match` are ones the consumer doesn't read. The tests check:

- **the contracts themselves**: well formed, every example a valid message (`decode`, and the published JSON Schema),
  routed by the topology to the consumer's queue and taken by its consumer, and every type a consumer takes covered;
- **the consumer**: its handler, fed the example in the stated consumer state, does what `then` says; and fed the
  example with any field it doesn't list changed, does exactly the same (so `match` is honest about what it reads);
- **the producer**: driven into `given` by its own code (the order service's write and saga steps, the payment
  service's handler), it writes a message of that type whose listed fields match.

With one codebase the shapes can't drift (every service decodes with messages.py); what this pins is the values and
the states behind them, and which fields each consumer depends on, so a producer knows what it may change.
"""

from __future__ import annotations

import json
import re
import uuid
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import jsonschema
import psycopg
import pytest
from helpers import stock
from psycopg_pool import AsyncConnectionPool

from orderly import notifications, ordering, orders, payments
from orderly.fakes import Charge, FakeMailer, FakeProvider, Refund
from orderly.messages import (
    ChargePayment,
    Message,
    PaymentFailed,
    PaymentSucceeded,
    RefundPayment,
    decode,
    encode,
    new_message,
)
from orderly.migrate import migrate
from orderly.topology import Topology

ROOT = Path(__file__).parents[1]
CONTRACTS = ROOT / "contracts"
SERVICES = {"orders": orders, "payments": payments, "notifications": notifications}
MATCHERS = {"exact", "uuid", "order"}
UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
Connection = psycopg.AsyncConnection[Any]


@dataclass(frozen=True)
class Interaction:
    consumer: str
    name: str
    producer: str
    given: str
    message: dict[str, Any]
    match: dict[str, str]
    then: str
    consumer_state: str | None = None

    @property
    def type(self) -> str:
        return str(self.message["type"])

    @property
    def order_id(self) -> uuid.UUID:
        return uuid.UUID(self.message["payload"]["order_id"])

    def decoded(self) -> Message:
        return decode(json.dumps(self.message).encode())


def load() -> list[Interaction]:
    found = []
    for path in sorted(CONTRACTS.glob("*.json")):
        contract = json.loads(path.read_text())
        assert contract["consumer"] == path.stem, f"{path.name} names consumer {contract['consumer']!r}"
        for raw in contract["interactions"]:
            found.append(Interaction(consumer=contract["consumer"], **raw))
    return found


INTERACTIONS = load()
IDS = [f"{i.consumer}: {i.name}" for i in INTERACTIONS]


def get(message: dict[str, Any], path: str) -> Any:
    value: Any = message
    for part in path.split("."):
        value = value[part]
    return value


def with_value(message: dict[str, Any], path: str, value: Any) -> dict[str, Any]:
    changed: dict[str, Any] = json.loads(json.dumps(message))
    *parents, last = path.split(".")
    target = changed
    for part in parents:
        target = target[part]
    target[last] = value
    return changed


# The contracts themselves.


def test_there_is_a_contract_for_every_consumer() -> None:
    assert {i.consumer for i in INTERACTIONS} == set(SERVICES)
    assert len(INTERACTIONS) == 11


@pytest.mark.parametrize("interaction", INTERACTIONS, ids=IDS)
def test_the_contract_is_well_formed(interaction: Interaction) -> None:
    assert interaction.producer in SERVICES and interaction.producer != interaction.consumer
    assert set(interaction.match.values()) <= MATCHERS
    for path in interaction.match:
        get(interaction.message, path)  # every listed field exists in the example
    # Every consumer deduplicates on the message id and finds the saga by the order id.
    assert interaction.match["message_id"] == "uuid" and interaction.match["payload.order_id"] == "order"


@pytest.mark.parametrize("interaction", INTERACTIONS, ids=IDS)
def test_the_example_is_a_valid_message_by_the_code_and_by_the_published_schema(interaction: Interaction) -> None:
    message = interaction.decoded()
    assert json.loads(encode(message)) == interaction.message, "the example is in canonical form"
    schema = json.loads((ROOT / "docs/message-schema.json").read_text())
    jsonschema.validate(interaction.message, schema, format_checker=jsonschema.FormatChecker())


@pytest.mark.parametrize("interaction", INTERACTIONS, ids=IDS)
def test_the_example_reaches_the_consumer_and_is_taken(interaction: Interaction) -> None:
    module = SERVICES[interaction.consumer]
    topology = Topology()
    assert topology.queue(module.QUEUE) in topology.routes(interaction.type)
    assert interaction.type in module.ACCEPTS


def test_every_type_a_consumer_takes_is_in_its_contract() -> None:
    for consumer, module in SERVICES.items():
        assert {i.type for i in INTERACTIONS if i.consumer == consumer} == module.ACCEPTS, consumer


# The consumer side: its handler, in its own database, against the example.


@dataclass
class Recorder:
    """A FakeProvider that also writes down every call made to it."""

    inner: FakeProvider = field(default_factory=FakeProvider)
    calls: list[tuple[Any, ...]] = field(default_factory=list)

    async def charge(self, order_id: uuid.UUID, customer_id: str, amount_cents: int, currency: str) -> Charge:
        self.calls.append(("charge", order_id, customer_id, amount_cents, currency))
        return await self.inner.charge(order_id, customer_id, amount_cents, currency)

    async def refund(self, order_id: uuid.UUID, amount_cents: int, currency: str) -> Refund:
        self.calls.append(("refund", order_id, amount_cents, currency))
        return await self.inner.refund(order_id, amount_cents, currency)

    async def void(self, order_id: uuid.UUID, amount_cents: int, currency: str) -> tuple[Charge, Refund] | None:
        self.calls.append(("void", order_id, amount_cents, currency))
        return await self.inner.void(order_id, amount_cents, currency)


async def outbox_after(conn: Connection, after: int) -> list[Message]:
    rows = await (await conn.execute("SELECT body FROM outbox WHERE id > %s ORDER BY id", (after,))).fetchall()
    return [decode(bytes(row[0])) for row in rows]


async def last_outbox_id(conn: Connection) -> int:
    row = await (await conn.execute("SELECT coalesce(max(id), 0) FROM outbox")).fetchone()
    assert row is not None
    return int(row[0])


def without_ids(message: Message, order_id: uuid.UUID) -> tuple[str, dict[str, Any]]:
    """A message as its type and payload, with every id but the order's replaced: they are fresh on every run."""
    payload = message.payload.model_dump(mode="json")
    return message.type, {
        k: "<id>" if isinstance(v, str) and UUID.fullmatch(v) and v != str(order_id) else v for k, v in payload.items()
    }


TIME = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z")


def scalars(value: Any) -> set[Any]:
    if isinstance(value, dict):
        return {v for item in value.values() for v in scalars(item)}
    return {value}


def as_written(message: Message, incoming: dict[str, Any]) -> dict[str, Any]:
    """Everything in a message the consumer wrote, envelope included, for comparing two runs: a value copied from the
    incoming message stays as it is (so changing that field shows), and an id or time that is new on every run (its
    own id, its time, a new payment's id) becomes "<fresh>". Day-4 review: comparing only the type and the payload
    missed a consumer that stamped its reply with the command's occurred_at."""
    copied = scalars(incoming)

    def seen(value: Any) -> Any:
        if value in copied or not isinstance(value, str):
            return value
        return "<fresh>" if UUID.fullmatch(value) or TIME.fullmatch(value) else value

    sent: dict[str, Any] = json.loads(encode(message))
    return {k: {pk: seen(pv) for pk, pv in v.items()} if k == "payload" else seen(v) for k, v in sent.items()}


ORDER_ROW = {"customer_id": "c-42", "email": "ada@example.com", "total_cents": 5198, "currency": "USD"}


async def insert_order(conn: Connection, order_id: uuid.UUID, status: str, reason: str | None, refund: bool) -> None:
    await conn.execute(
        "INSERT INTO orders (id, customer_id, email, total_cents, currency, status, payment_deadline, cancel_reason,"
        " refund_requested) VALUES (%s, %s, %s, %s, %s, %s, now() + interval '30 seconds', %s, %s)",
        (order_id, *ORDER_ROW.values(), status, reason, refund),
    )
    await conn.execute("INSERT INTO order_lines VALUES (%s, 1, 'WIDGET-1', 2, 2599)", (order_id,))


async def consume(interaction: Interaction, message: dict[str, Any], url: str) -> dict[str, Any]:
    """Run the consumer's handler on `message` in the interaction's consumer state, in a transaction rolled back
    afterwards; return everything it did."""
    decoded = decode(json.dumps(message).encode())
    order_id = interaction.order_id
    async with await psycopg.AsyncConnection.connect(url) as conn, conn.transaction(force_rollback=True):
        if interaction.consumer == "payments":
            provider = Recorder()
            if interaction.consumer_state == "the order's charge of 5198 USD was taken":
                setup = ChargePayment(order_id=order_id, customer_id="c-42", amount_cents=5198, currency="USD")
                await payments.apply(conn, new_message(setup), provider)
                provider.calls.clear()
            else:
                assert interaction.consumer_state is None
            after = await last_outbox_id(conn)
            await payments.apply(conn, decoded, provider)
            row = await payments.read_payment(conn, order_id)
            assert row is not None
            replies = await outbox_after(conn, after)
            return {
                "provider": provider.calls,
                "payment": (row.status, row.amount_cents, row.currency),
                "replies": [without_ids(m, order_id) for m in replies],
                "envelopes": [as_written(m, message) for m in replies],
            }
        if interaction.consumer == "orders":
            await conn.execute("INSERT INTO products VALUES ('WIDGET-1', 'Widget', 2599, 'USD', 100)")
            if interaction.consumer_state == "the order awaits its payment":
                await insert_order(conn, order_id, "AWAITING_PAYMENT", None, False)
                await conn.execute("UPDATE products SET stock = stock - 2")
            elif interaction.consumer_state == "the order was cancelled at its deadline and asked for a refund":
                await insert_order(conn, order_id, "CANCELLED", "payment_timeout", True)
            else:
                raise AssertionError(f"no consumer state {interaction.consumer_state!r} for orders")
            await orders.handle_reply(conn, decoded)
            order = await (
                await conn.execute(
                    "SELECT status, cancel_reason, refund_requested FROM orders WHERE id = %s", (order_id,)
                )
            ).fetchone()
            stock_row = await (await conn.execute("SELECT stock FROM products")).fetchone()
            assert order is not None and stock_row is not None
            written = await outbox_after(conn, 0)
            return {
                "order": tuple(order),
                "stock": stock_row[0],
                "written": [without_ids(m, order_id) for m in written],
                "envelopes": [as_written(m, message) for m in written],
            }
        assert interaction.consumer == "notifications" and interaction.consumer_state is None
        mailer = FakeMailer()
        await notifications.apply(conn, decoded, mailer)
        rows = await (await conn.execute("SELECT order_id, kind FROM notifications")).fetchall()
        return {"sent": [(e.recipient, e.subject, e.body) for e in mailer.sent], "rows": rows}


@pytest.fixture(scope="module")
def migrated(test_database: str) -> Any:
    """One schema per consumer service, migrated; every consumer test rolls its own transaction back."""
    urls = {}
    schemas = []
    for service in SERVICES:
        schema = f"t_contracts_{service}_{uuid.uuid4().hex[:8]}"
        schemas.append(schema)
        with psycopg.connect(test_database, autocommit=True) as conn:
            conn.execute(f'CREATE SCHEMA "{schema}"')
        url = psycopg.conninfo.make_conninfo(test_database, options=f"-c search_path={schema}")
        with psycopg.connect(url, autocommit=True) as conn:
            migrate(conn, service)
        urls[service] = url
    yield urls
    with psycopg.connect(test_database, autocommit=True) as conn:
        for schema in schemas:
            conn.execute(f'DROP SCHEMA "{schema}" CASCADE')


ORDER_CONFIRMED = ("order.confirmed", {"order_id": "<order>", **ORDER_ROW})


def expected_then(interaction: Interaction) -> dict[str, Any]:
    """What each interaction's `then` says, as the consumer's effects."""
    order = str(interaction.order_id)

    def named(written: tuple[str, dict[str, Any]]) -> tuple[str, dict[str, Any]]:
        kind, payload = written
        return kind, {k: order if v == "<order>" else v for k, v in payload.items()}

    refunded = {
        "provider": [("refund", interaction.order_id, 5198, "USD")],
        "payment": ("REFUNDED", 5198, "USD"),
        "replies": [
            ("payment.refunded", {"order_id": order, "payment_id": "<id>", "amount_cents": 5198, "currency": "USD"})
        ],
    }
    unchanged = {"order": ("CANCELLED", "payment_timeout", True), "stock": 100, "written": []}
    expected: dict[tuple[str, str], dict[str, Any]] = {
        ("payments", "charge an order"): {
            "provider": [("charge", interaction.order_id, "c-42", 5198, "USD")],
            "payment": ("CHARGED", 5198, "USD"),
            "replies": [
                (
                    "payment.succeeded",
                    {"order_id": order, "payment_id": "<id>", "amount_cents": 5198, "currency": "USD"},
                )
            ],
        },
        ("payments", "refund an order that timed out"): refunded,
        ("payments", "refund a success that came after a cancellation"): refunded,
        ("orders", "a charge approved"): {
            "order": ("CONFIRMED", None, False),
            "stock": 98,
            "written": [named(ORDER_CONFIRMED)],
        },
        ("orders", "a charge declined"): {
            "order": ("CANCELLED", "payment_declined", False),
            "stock": 100,
            "written": [
                (
                    "order.cancelled",
                    {
                        "order_id": order,
                        "customer_id": "c-42",
                        "email": "ada@example.com",
                        "reason": "payment_declined",
                    },
                )
            ],
        },
        ("orders", "a charge refused after a void"): unchanged,
        ("orders", "a charge refunded"): unchanged,
        ("orders", "a refund with nothing charged"): unchanged,
    }
    if interaction.consumer == "notifications":
        return {}  # checked on the e-mail's text below
    return expected[(interaction.consumer, interaction.name)]


EMAIL_TEXT = {
    "an order confirmed": ("confirmed", "51.98 USD"),
    "an order declined": ("cancelled", "not been charged"),
    "an order that timed out": ("cancelled", "will be refunded"),
}


@pytest.mark.parametrize("interaction", INTERACTIONS, ids=IDS)
async def test_the_consumer_does_what_its_contract_says(interaction: Interaction, migrated: dict[str, str]) -> None:
    effects = await consume(interaction, interaction.message, migrated[interaction.consumer])
    if interaction.consumer == "notifications":
        [(recipient, subject, body)] = effects["sent"]
        in_subject, in_body = EMAIL_TEXT[interaction.name]
        assert recipient == "ada@example.com" and in_subject in subject and in_body in body
        kind = "order_confirmed" if interaction.type == "order.confirmed" else "order_cancelled"
        assert effects["rows"] == [(interaction.order_id, kind)]
    else:
        # What `then` says is about the effects; the envelopes are for the next test.
        assert {k: v for k, v in effects.items() if k != "envelopes"} == expected_then(interaction)


# A different valid value for every field a consumer might not read; a field with none fails the next test.
ALTERNATES: dict[str, Callable[[Interaction], Any]] = {
    "occurred_at": lambda i: "2020-02-29T23:59:59.999999Z",
    "causation_id": lambda i: None if i.message["causation_id"] else "99999999-9999-4999-8999-999999999999",
    "payload.customer_id": lambda i: "c-someone-else",
    "payload.email": lambda i: "someone.else@example.com",
    "payload.amount_cents": lambda i: 777,
    "payload.total_cents": lambda i: 777,
    "payload.currency": lambda i: "EUR",
    "payload.payment_id": lambda i: "77777777-7777-4777-8777-777777777777",
    "payload.reason": lambda i: {
        "payment_timeout": "order_cancelled",
        "order_cancelled": "payment_timeout",
        "declined": "voided",
        "voided": "declined",
        "payment_declined": "payment_timeout",
    }[i.message["payload"]["reason"]],
}
# Tied to fields the contracts always list: correlation_id must be the payload's order_id, and type and version are
# what the message is.
TIED = {"type", "version", "correlation_id"}


def unlisted(interaction: Interaction) -> list[str]:
    paths = [k for k in interaction.message if k != "payload"] + [
        f"payload.{k}" for k in interaction.message["payload"]
    ]
    return [p for p in paths if p not in interaction.match and p not in TIED]


@pytest.mark.parametrize("interaction", INTERACTIONS, ids=IDS)
async def test_the_consumer_ignores_every_field_its_contract_doesnt_list(
    interaction: Interaction, migrated: dict[str, str]
) -> None:
    """If a consumer read a field its contract leaves out, a producer could change it thinking it safe: changing each
    such field to another valid value must leave everything the consumer does exactly the same."""
    baseline = await consume(interaction, interaction.message, migrated[interaction.consumer])
    fields = unlisted(interaction)
    assert set(fields) <= set(ALTERNATES), f"no alternate value for {set(fields) - set(ALTERNATES)}"
    for path in fields:
        variant = with_value(interaction.message, path, ALTERNATES[path](interaction))
        assert get(variant, path) != get(interaction.message, path)
        assert await consume(interaction, variant, migrated[interaction.consumer]) == baseline, path


# The producer side: its own code, driven into the interaction's state, writes a message that matches.


@pytest.fixture
async def pool(fresh_database: str) -> AsyncIterator[AsyncConnectionPool]:
    async with AsyncConnectionPool(fresh_database, min_size=1, max_size=2, open=False) as opened:
        yield opened


async def placed_order(pool: AsyncConnectionPool, url: str) -> uuid.UUID:
    """An order of 2 x WIDGET-1 at 2599 USD by c-42 (ada@example.com), through the order service's own write."""
    with psycopg.connect(url, autocommit=True) as conn:
        migrate(conn, "orders")
    stock(url, [("WIDGET-1", 2599, 100)])
    request = ordering.OrderRequest.model_validate(
        {"customer_id": "c-42", "email": "ada@example.com", "lines": [{"sku": "WIDGET-1", "quantity": 2}]}
    )
    taken = await ordering.take_order(pool, request, None, orders.order_writer(30.0)(request, None))
    return taken.order.id


async def reply(pool: AsyncConnectionPool, message: Message) -> None:
    async with pool.connection() as conn, conn.transaction():
        await orders.handle_reply(conn, message)


async def produce_orders(given: str, pool: AsyncConnectionPool, url: str) -> tuple[uuid.UUID, list[Message]]:
    """The order service in the state `given`: the messages its last step wrote."""
    order_id = await placed_order(pool, url)
    async with pool.connection() as conn:
        after = await last_outbox_id(conn)
    base = "an order of 2 x WIDGET-1 at 2599 USD by customer c-42 (ada@example.com)"
    succeeded = PaymentSucceeded(order_id=order_id, payment_id=uuid.uuid4(), amount_cents=5198, currency="USD")
    declined = PaymentFailed(order_id=order_id, reason="declined")
    if given == base:
        after = 0
    elif given == base + ", whose charge was approved":
        await reply(pool, new_message(succeeded))
    elif given == base + ", whose charge was declined":
        await reply(pool, new_message(declined))
    elif given == base + ", whose payment deadline passed":
        async with pool.connection() as conn:
            await conn.execute("UPDATE orders SET payment_deadline = now() - interval '1 second'")
        assert await orders.sweep(pool) == 1
    elif given == base + ", whose charge was declined, then a success arrived for it":
        await reply(pool, new_message(declined))
        async with pool.connection() as conn:
            after = await last_outbox_id(conn)
        await reply(pool, new_message(succeeded))
    else:
        raise AssertionError(f"the order service has no state {given!r}")
    async with pool.connection() as conn:
        return order_id, await outbox_after(conn, after)


async def produce_payments(given: str, url: str) -> tuple[uuid.UUID, list[Message]]:
    """The payment service's handler in the state `given`: the replies its last command wrote."""
    with psycopg.connect(url, autocommit=True) as conn:
        migrate(conn, "payments")
    order_id = uuid.uuid4()
    charge = ChargePayment(order_id=order_id, customer_id="c-42", amount_cents=5198, currency="USD")
    refund = RefundPayment(order_id=order_id, amount_cents=5198, currency="USD", reason="payment_timeout")
    steps: dict[str, tuple[FakeProvider, list[ChargePayment | RefundPayment]]] = {
        "a charge of 5198 USD for customer c-42, approved": (FakeProvider(), [charge]),
        "a charge of 5198 USD for customer c-42, declined by the provider": (
            FakeProvider(declined_customers=frozenset({"c-42"})),
            [charge],
        ),
        "a charge of 5198 USD arriving after the order's refund voided it": (FakeProvider(), [refund, charge]),
        "a refund of an approved charge of 5198 USD": (FakeProvider(), [charge, refund]),
        "a refund of 5198 USD arriving before any charge": (FakeProvider(), [refund]),
    }
    assert given in steps, f"the payment service has no state {given!r}"
    provider, commands = steps[given]
    async with await psycopg.AsyncConnection.connect(url) as conn:
        after = 0
        for command in commands:
            after = await last_outbox_id(conn)
            async with conn.transaction():
                await payments.apply(conn, new_message(command), provider)
        return order_id, await outbox_after(conn, after)


PRODUCERS: dict[str, Callable[[str, AsyncConnectionPool, str], Awaitable[tuple[uuid.UUID, list[Message]]]]] = {
    "orders": produce_orders,
    "payments": lambda given, pool, url: produce_payments(given, url),
}


@pytest.mark.parametrize("interaction", INTERACTIONS, ids=IDS)
async def test_the_producer_sends_what_the_consumer_needs(
    interaction: Interaction, pool: AsyncConnectionPool, fresh_database: str
) -> None:
    order_id, written = await PRODUCERS[interaction.producer](interaction.given, pool, fresh_database)
    [produced] = [m for m in written if m.type == interaction.type]
    sent = json.loads(encode(produced))
    for path, rule in interaction.match.items():
        value = get(sent, path)
        if rule == "exact":
            assert value == get(interaction.message, path), path
        elif rule == "uuid":
            assert isinstance(value, str) and UUID.fullmatch(value), path
        else:
            assert value == str(order_id), path
