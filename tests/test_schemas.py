"""The invariants each service's schema enforces by itself, whatever the code above it does."""

import uuid
from collections.abc import Iterator

import psycopg
import pytest
from psycopg import errors

from orderly.migrate import migrate

ORDER = uuid.UUID("0b8f6f8e-1d1f-4f4e-8a3c-7f0f2b9d5e11")


def database(url: str, service: str) -> Iterator[psycopg.Connection[tuple[object, ...]]]:
    with psycopg.connect(url, autocommit=True) as conn:
        migrate(conn, service)
        yield conn


@pytest.fixture
def orders(fresh_database: str) -> Iterator[psycopg.Connection[tuple[object, ...]]]:
    yield from database(fresh_database, "orders")


@pytest.fixture
def payments(fresh_database: str) -> Iterator[psycopg.Connection[tuple[object, ...]]]:
    yield from database(fresh_database, "payments")


@pytest.fixture
def notifications(fresh_database: str) -> Iterator[psycopg.Connection[tuple[object, ...]]]:
    yield from database(fresh_database, "notifications")


def insert_order(conn: psycopg.Connection[tuple[object, ...]], **overrides: object) -> None:
    row: dict[str, object] = {
        "id": ORDER,
        "customer_id": "c-42",
        "email": "a@example.com",
        "status": "AWAITING_PAYMENT",
        "total_cents": 2599,
        "currency": "USD",
        "idempotency_key": None,
        "request_hash": None,
        "payment_deadline": "2026-11-02T20:00:00Z",
        "cancel_reason": None,
        "refund_requested": False,
    }
    row.update(overrides)
    columns = ", ".join(row)
    placeholders = ", ".join(["%s"] * len(row))
    conn.execute(f"INSERT INTO orders ({columns}) VALUES ({placeholders})", list(row.values()))


def test_a_valid_order_and_its_lines_are_accepted(orders: psycopg.Connection[tuple[object, ...]]) -> None:
    orders.execute("INSERT INTO products VALUES ('SKU-1', 'Mug', 1299, 'USD', 5)")
    insert_order(orders)
    orders.execute("INSERT INTO order_lines VALUES (%s, 1, 'SKU-1', 2, 1299)", (ORDER,))


def test_stock_cannot_go_below_zero(orders: psycopg.Connection[tuple[object, ...]]) -> None:
    orders.execute("INSERT INTO products VALUES ('SKU-1', 'Mug', 1299, 'USD', 1)")
    with pytest.raises(errors.CheckViolation):
        orders.execute("UPDATE products SET stock = stock - 2 WHERE sku = 'SKU-1'")
    # The reservation the order service uses: a guarded update that simply matches no row when stock is short.
    reserved = orders.execute("UPDATE products SET stock = stock - 2 WHERE sku = 'SKU-1' AND stock >= 2")
    assert reserved.rowcount == 0


@pytest.mark.parametrize(
    "overrides",
    [
        {"status": "SHIPPED"},
        {"status": "CANCELLED"},  # without a reason
        {"cancel_reason": "payment_declined"},  # a reason on an order that isn't cancelled
        {"status": "CANCELLED", "cancel_reason": "changed_mind"},
        {"idempotency_key": "key-1"},  # without its request hash
        {"request_hash": "0" * 64},  # a hash without a key
        {"idempotency_key": "key-1", "request_hash": "XYZ"},
        {"refund_requested": True},  # only a cancelled order asks for a refund
        {"total_cents": 0},
        {"currency": "usd"},
        {"customer_id": "c 42"},
        {"email": "not-an-address"},
    ],
)
def test_orders_that_break_an_invariant_are_refused(
    orders: psycopg.Connection[tuple[object, ...]], overrides: dict[str, object]
) -> None:
    with pytest.raises(errors.CheckViolation):
        insert_order(orders, **overrides)


def test_a_cancelled_order_with_its_reason_and_refund_is_valid(orders: psycopg.Connection[tuple[object, ...]]) -> None:
    insert_order(orders, status="CANCELLED", cancel_reason="payment_timeout", refund_requested=True)


def test_an_idempotency_key_belongs_to_one_order(orders: psycopg.Connection[tuple[object, ...]]) -> None:
    insert_order(orders, idempotency_key="key-1", request_hash="a" * 64)
    with pytest.raises(errors.UniqueViolation):
        insert_order(orders, id=uuid.uuid4(), idempotency_key="key-1", request_hash="a" * 64)


def test_an_order_names_each_product_once(orders: psycopg.Connection[tuple[object, ...]]) -> None:
    orders.execute("INSERT INTO products VALUES ('SKU-1', 'Mug', 1299, 'USD', 5)")
    insert_order(orders)
    orders.execute("INSERT INTO order_lines VALUES (%s, 1, 'SKU-1', 1, 1299)", (ORDER,))
    with pytest.raises(errors.UniqueViolation):
        orders.execute("INSERT INTO order_lines VALUES (%s, 2, 'SKU-1', 1, 1299)", (ORDER,))
    with pytest.raises(errors.ForeignKeyViolation):
        orders.execute("INSERT INTO order_lines VALUES (%s, 3, 'SKU-404', 1, 1299)", (ORDER,))


def test_the_outbox_keeps_message_ids_unique(orders: psycopg.Connection[tuple[object, ...]]) -> None:
    message_id = uuid.uuid4()
    orders.execute(
        "INSERT INTO outbox (message_id, message_type, body) VALUES (%s, 'payment.charge', '{}')", (message_id,)
    )
    with pytest.raises(errors.UniqueViolation):
        orders.execute(
            "INSERT INTO outbox (message_id, message_type, body) VALUES (%s, 'payment.charge', '{}')", (message_id,)
        )


def test_a_consumer_records_a_message_once(orders: psycopg.Connection[tuple[object, ...]]) -> None:
    message_id = uuid.uuid4()
    record = "INSERT INTO processed_messages (consumer, message_id) VALUES (%s, %s) ON CONFLICT DO NOTHING"
    assert orders.execute(record, ("order.payment-events", message_id)).rowcount == 1
    assert orders.execute(record, ("order.payment-events", message_id)).rowcount == 0
    # Another consumer of the same message has its own record.
    assert orders.execute(record, ("audit", message_id)).rowcount == 1


@pytest.mark.parametrize(
    ("status", "charge_ref", "refund_ref", "valid"),
    [
        ("CHARGED", "ch_1", None, True),
        ("CHARGED", None, None, False),
        ("CHARGED", "ch_1", "re_1", False),
        ("DECLINED", None, None, True),
        ("DECLINED", "ch_1", None, False),
        ("REFUNDED", "ch_1", "re_1", True),
        ("REFUNDED", "ch_1", None, False),
        ("VOIDED", None, None, True),
        ("VOIDED", None, "re_1", False),
        ("PENDING", None, None, False),
    ],
)
def test_a_payments_provider_references_match_its_status(
    payments: psycopg.Connection[tuple[object, ...]],
    status: str,
    charge_ref: str | None,
    refund_ref: str | None,
    valid: bool,
) -> None:
    insert = (
        "INSERT INTO payments (id, order_id, status, amount_cents, currency, charge_ref, refund_ref)"
        " VALUES (%s, %s, %s, 2599, 'USD', %s, %s)"
    )
    values = (uuid.uuid4(), ORDER, status, charge_ref, refund_ref)
    if valid:
        payments.execute(insert, values)
    else:
        with pytest.raises(errors.CheckViolation):
            payments.execute(insert, values)


def test_one_payment_per_order(payments: psycopg.Connection[tuple[object, ...]]) -> None:
    insert = "INSERT INTO payments (id, order_id, status, amount_cents, currency) VALUES (%s, %s, 'VOIDED', 1, 'USD')"
    payments.execute(insert, (uuid.uuid4(), ORDER))
    with pytest.raises(errors.UniqueViolation):
        payments.execute(insert, (uuid.uuid4(), ORDER))


def test_one_notification_of_each_kind_per_order(notifications: psycopg.Connection[tuple[object, ...]]) -> None:
    insert = (
        "INSERT INTO notifications (order_id, kind, recipient, subject, body, message_id)"
        " VALUES (%s, %s, 'a@example.com', 's', 'b', %s)"
    )
    notifications.execute(insert, (ORDER, "order_confirmed", uuid.uuid4()))
    with pytest.raises(errors.UniqueViolation):
        notifications.execute(insert, (ORDER, "order_confirmed", uuid.uuid4()))
    with pytest.raises(errors.CheckViolation):
        notifications.execute(insert, (ORDER, "order_shipped", uuid.uuid4()))
