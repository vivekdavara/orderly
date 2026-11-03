"""The monolith against a real PostgreSQL: the whole order in one request and one transaction.

Besides what it does right (stock never oversold, idempotent retries, no deadlocks), these tests pin the two problems
the split exists to fix (DESIGN.md "The monolith"): a failure after the charge leaves the charge, and the provider's
latency is paid with the products' rows locked.
"""

from __future__ import annotations

import asyncio
import random
import time
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any

import httpx
import psycopg
import psycopg.conninfo
import pytest
from fastapi import FastAPI
from helpers import count, running, stock, stock_of

from orderly import monolith
from orderly.fakes import FakeMailer, FakeProvider
from orderly.service import Settings


@dataclass
class Monolith:
    client: httpx.AsyncClient
    app: FastAPI
    url: str
    provider: FakeProvider
    mailer: FakeMailer

    async def order(
        self, lines: list[tuple[str, int]], *, key: str | None = None, customer: str = "c-42", **extra: Any
    ) -> httpx.Response:
        body = {
            "customer_id": customer,
            "email": "ada@example.com",
            "lines": [{"sku": sku, "quantity": quantity} for sku, quantity in lines],
            **extra,
        }
        headers = {} if key is None else {"Idempotency-Key": key}
        return await self.client.post("/orders", json=body, headers=headers)


@pytest.fixture
async def shop(fresh_database: str) -> AsyncIterator[Monolith]:
    provider = FakeProvider(declined_customers=frozenset({"c-broke"}))
    mailer = FakeMailer()
    settings = Settings("monolith", fresh_database, "amqp://unused/")
    app = monolith.create_app(settings, provider=provider, mailer=mailer)
    async with running(app) as client:
        stock(
            fresh_database,
            [("WIDGET-1", 2599, 10), ("GADGET-2", 1000, 3), ("EURO-1", 500, 5, "EUR"), ("HUGE-1", 10**12, 5)],
        )
        yield Monolith(client, app, fresh_database, provider, mailer)


def rows(url: str, query: str) -> list[tuple[Any, ...]]:
    with psycopg.connect(url) as conn:
        return conn.execute(query).fetchall()


async def test_an_approved_order_is_confirmed_charged_and_mailed_in_one_request(shop: Monolith) -> None:
    response = await shop.order([("WIDGET-1", 2), ("GADGET-2", 1)])
    assert response.status_code == 201, response.text
    order = response.json()
    assert response.headers["location"] == f"/orders/{order['id']}"
    assert "idempotent-replayed" not in response.headers
    assert order["status"] == "CONFIRMED" and order["cancel_reason"] is None
    assert (order["total_cents"], order["currency"]) == (2 * 2599 + 1000, "USD")
    assert order["lines"] == [
        {"sku": "WIDGET-1", "quantity": 2, "unit_price_cents": 2599},
        {"sku": "GADGET-2", "quantity": 1, "unit_price_cents": 1000},
    ]
    assert (stock_of(shop.url, "WIDGET-1"), stock_of(shop.url, "GADGET-2")) == (8, 2)
    assert rows(shop.url, "SELECT status, amount_cents, charge_ref IS NOT NULL FROM payments") == [
        ("CHARGED", 6198, True)
    ]
    assert rows(shop.url, "SELECT kind, recipient FROM notifications") == [("order_confirmed", "ada@example.com")]
    assert shop.provider.charged_cents() == 6198
    [email] = shop.mailer.sent
    assert "charged 61.98 USD" in email.body
    assert (await shop.client.get(f"/orders/{order['id']}")).json() == order


async def test_a_declined_order_is_cancelled_with_its_stock_back_and_answers_402(shop: Monolith) -> None:
    response = await shop.order([("WIDGET-1", 4)], customer="c-broke")
    assert response.status_code == 402, response.text
    assert (response.json()["status"], response.json()["cancel_reason"]) == ("CANCELLED", "payment_declined")
    assert stock_of(shop.url, "WIDGET-1") == 10
    assert rows(shop.url, "SELECT status, charge_ref FROM payments") == [("DECLINED", None)]
    assert rows(shop.url, "SELECT kind FROM notifications") == [("order_cancelled",)]
    assert shop.provider.charged_cents() == 0


@pytest.mark.parametrize(
    ("lines", "status", "error"),
    [
        ([("WIDGET-1", 11)], 409, "out_of_stock"),
        ([("WIDGET-1", 1), ("GADGET-2", 4)], 409, "out_of_stock"),
        ([("NOPE-1", 1)], 422, "unknown_sku"),
        ([("WIDGET-1", 1), ("EURO-1", 1)], 422, "mixed_currencies"),
        ([("HUGE-1", 2)], 422, "total_too_large"),
    ],
)
async def test_an_order_that_cant_be_taken_writes_nothing_and_charges_nothing(
    shop: Monolith, lines: list[tuple[str, int]], status: int, error: str
) -> None:
    response = await shop.order(lines)
    assert response.status_code == status, response.text
    assert response.json()["detail"]["error"] == error
    assert (count(shop.url, "orders"), count(shop.url, "payments"), count(shop.url, "notifications")) == (0, 0, 0)
    assert (stock_of(shop.url, "WIDGET-1"), stock_of(shop.url, "GADGET-2")) == (10, 3)
    assert shop.provider.attempts == {}


async def test_out_of_stock_names_the_line(shop: Monolith) -> None:
    detail = (await shop.order([("GADGET-2", 4)])).json()["detail"]
    assert detail == {"error": "out_of_stock", "message": "GADGET-2: 4 wanted, 3 in stock", "sku": "GADGET-2"}


async def test_a_malformed_request_or_key_is_refused_before_anything_happens(shop: Monolith) -> None:
    assert (await shop.order([("WIDGET-1", 1)], email_verified=True)).status_code == 422
    assert (await shop.order([("WIDGET-1", 1)], key="has space")).status_code == 422
    assert (await shop.order([("WIDGET-1", 1)], key="k" * 201)).status_code == 422
    bad = await shop.client.post("/orders", content=b"{not json", headers={"content-type": "application/json"})
    assert bad.status_code == 422
    assert count(shop.url, "orders") == 0


async def test_unknown_and_malformed_order_ids(shop: Monolith) -> None:
    assert (await shop.client.get(f"/orders/{uuid.uuid4()}")).status_code == 404
    assert (await shop.client.get("/orders/not-a-uuid")).status_code == 422


async def test_a_retry_with_the_same_key_replays_the_order_without_charging_again(shop: Monolith) -> None:
    first = await shop.order([("WIDGET-1", 2)], key="retry-1")
    again = await shop.order([("WIDGET-1", 2)], key="retry-1")
    assert (first.status_code, again.status_code) == (201, 201)
    assert again.json() == first.json()
    assert again.headers["idempotent-replayed"] == "true"
    assert stock_of(shop.url, "WIDGET-1") == 8
    assert shop.provider.charged_cents() == 2 * 2599 and len(shop.provider.attempts) == 1
    assert len(shop.mailer.sent) == 1


async def test_a_replayed_decline_answers_402_again(shop: Monolith) -> None:
    first = await shop.order([("WIDGET-1", 1)], key="d-1", customer="c-broke")
    again = await shop.order([("WIDGET-1", 1)], key="d-1", customer="c-broke")
    assert (first.status_code, again.status_code) == (402, 402)
    assert again.json()["id"] == first.json()["id"]


async def test_a_key_reused_for_another_request_is_refused(shop: Monolith) -> None:
    first = await shop.order([("WIDGET-1", 2)], key="k-1")
    other = await shop.order([("WIDGET-1", 3)], key="k-1")
    assert other.status_code == 422
    assert other.json()["detail"] == {
        "error": "idempotency_key_reused",
        "message": "this Idempotency-Key was used for a different request",
        "order_id": first.json()["id"],
    }
    assert stock_of(shop.url, "WIDGET-1") == 8


async def test_concurrent_requests_with_one_key_make_one_order_and_one_charge(shop: Monolith) -> None:
    shop.provider.latency = 0.2  # all ten are in flight while the first holds the key
    responses = await asyncio.gather(*(shop.order([("WIDGET-1", 1)], key="same") for _ in range(10)))
    assert {response.status_code for response in responses} == {201}
    assert len({response.json()["id"] for response in responses}) == 1
    assert sum(response.headers.get("idempotent-replayed") == "true" for response in responses) == 9
    assert count(shop.url, "orders") == 1 and stock_of(shop.url, "WIDGET-1") == 9
    assert len(shop.provider.attempts) == 1 and shop.provider.charged_cents() == 2599


async def test_a_retry_that_waited_for_the_last_unit_gets_the_order_not_a_409(shop: Monolith) -> None:
    # GADGET-2 has 3. The first request takes all 3 and holds the rows through the provider's latency; its retry
    # (same key, same body) waits on the row lock, then finds no stock. It is the same request: it must get the order.
    shop.provider.latency = 0.2
    first, retry = await asyncio.gather(
        shop.order([("GADGET-2", 3)], key="last"), shop.order([("GADGET-2", 3)], key="last")
    )
    assert (first.status_code, retry.status_code) == (201, 201), (first.text, retry.text)
    assert first.json()["id"] == retry.json()["id"]
    assert stock_of(shop.url, "GADGET-2") == 0 and shop.provider.charged_cents() == 3000


async def test_orders_racing_for_the_last_units_never_oversell(shop: Monolith) -> None:
    shop.provider.latency = 0.01
    responses = await asyncio.gather(*(shop.order([("GADGET-2", 1)]) for _ in range(12)))
    codes = sorted(response.status_code for response in responses)
    assert codes == [201] * 3 + [409] * 9
    assert stock_of(shop.url, "GADGET-2") == 0
    assert count(shop.url, "orders") == 3 and shop.provider.charged_cents() == 3000


async def test_orders_locking_the_same_products_in_any_order_never_deadlock(fresh_database: str) -> None:
    # Each order lists the same four products in a random order; reserve() locks them in sku order, so no two
    # transactions wait on each other in a cycle. (Locking in the request's order deadlocks within a few orders.)
    provider = FakeProvider(latency=0.005)
    app = monolith.create_app(Settings("monolith", fresh_database, "amqp://unused/"), provider=provider)
    skus = ["A-1", "B-1", "C-1", "D-1"]
    async with running(app) as client:
        stock(fresh_database, [(sku, 100, 1000) for sku in skus])
        shuffled = random.Random(7)

        async def one(n: int) -> int:
            lines = skus[:]
            shuffled.shuffle(lines)
            body = {
                "customer_id": f"c-{n}",
                "email": "a@example.com",
                "lines": [{"sku": s, "quantity": 1} for s in lines],
            }
            return (await client.post("/orders", json=body)).status_code

        codes = await asyncio.gather(*(one(n) for n in range(40)))
    assert codes == [201] * 40
    assert {sku: stock_of(fresh_database, sku) for sku in skus} == dict.fromkeys(skus, 960)


async def test_a_provider_that_fails_rolls_the_order_back_and_a_retry_succeeds(shop: Monolith) -> None:
    shop.provider.error_ratio = 1.0
    failed = await shop.order([("WIDGET-1", 1)], key="p-1")
    assert failed.status_code == 503 and failed.headers["retry-after"] == "1"
    assert failed.json()["detail"]["error"] == "payment_provider_unavailable"
    assert count(shop.url, "orders") == 0 and stock_of(shop.url, "WIDGET-1") == 10
    shop.provider.error_ratio = 0.0
    retried = await shop.order([("WIDGET-1", 1)], key="p-1")
    assert retried.status_code == 201 and "idempotent-replayed" not in retried.headers
    assert shop.provider.charged_cents() == 2599


async def test_the_baselines_flaw_a_failure_after_the_charge_keeps_the_charge_and_the_retry_charges_again(
    shop: Monolith,
) -> None:
    """Pinned, not fixed: this is what the split is for. The mailer is down, so the order's transaction rolls back
    after the provider approved the charge. The client retries with the same key: the first attempt left no order to
    replay, so the retry is a new order with a new id, and the provider (idempotent by order id) charges again."""
    shop.mailer.down = True
    failed = await shop.order([("WIDGET-1", 1)], key="m-1")
    assert failed.status_code == 503 and failed.json()["detail"]["error"] == "mailer_unavailable"
    assert count(shop.url, "orders") == 0 and stock_of(shop.url, "WIDGET-1") == 10
    assert shop.provider.charged_cents() == 2599  # charged, with no order to show for it
    shop.mailer.down = False
    retried = await shop.order([("WIDGET-1", 1)], key="m-1")
    assert retried.status_code == 201
    assert count(shop.url, "orders") == 1
    assert shop.provider.charged_cents() == 2 * 2599  # charged twice for one order


async def test_the_baselines_flaw_the_providers_latency_is_paid_with_the_rows_locked(shop: Monolith) -> None:
    """Two orders for the same product take two provider latencies end to end, because the second waits for the
    first's row lock; two orders for different products overlap."""
    shop.provider.latency = 0.3
    started = time.perf_counter()
    await asyncio.gather(shop.order([("WIDGET-1", 1)]), shop.order([("WIDGET-1", 1)]))
    same_product = time.perf_counter() - started
    started = time.perf_counter()
    await asyncio.gather(shop.order([("WIDGET-1", 1)]), shop.order([("GADGET-2", 1)]))
    different_products = time.perf_counter() - started
    assert same_product >= 0.6, same_product
    assert different_products < 0.6, different_products


async def test_a_request_that_cant_get_a_connection_in_time_is_a_503_not_a_500(fresh_database: str) -> None:
    """The monolith holds a pooled connection through the provider's latency: with one connection and a 0.5 s
    provider, a second order waits for the pool, and after its timeout is told to retry."""
    provider = FakeProvider(latency=0.5)
    settings = Settings("monolith", fresh_database, "amqp://unused/", pool_size=1, pool_timeout=0.2)
    async with running(monolith.create_app(settings, provider=provider)) as client:
        stock(fresh_database, [("A-1", 100, 10), ("B-1", 100, 10)])

        async def order(sku: str) -> httpx.Response:
            body = {"customer_id": "c-1", "email": "a@example.com", "lines": [{"sku": sku, "quantity": 1}]}
            return await client.post("/orders", json=body)

        first, second = await asyncio.gather(order("A-1"), order("B-1"))
        assert first.status_code == 201, first.text
        assert second.status_code == 503 and second.headers["retry-after"] == "1"
        assert second.json()["detail"]["error"] == "database_busy"
        slow_read = asyncio.create_task(order("A-1"))
        await asyncio.sleep(0.05)  # holding the only connection for 0.5 s
        busy = await client.get(f"/orders/{first.json()['id']}")
        assert busy.status_code == 503 and busy.json()["detail"]["error"] == "database_busy"
        assert (await slow_read).status_code == 201
    assert count(fresh_database, "orders") == 2


async def test_racing_orders_behave_the_same_under_a_repeatable_read_default(fresh_database: str) -> None:
    """Why every pooled connection is pinned to READ COMMITTED: with the database defaulting to REPEATABLE READ, an
    order that waited for another's row lock would fail with a serialization error (a 500) instead of re-checking
    the stock and getting its 201 or 409."""
    options = str(psycopg.conninfo.conninfo_to_dict(fresh_database)["options"])
    url = psycopg.conninfo.make_conninfo(
        fresh_database, options=f"{options} -c default_transaction_isolation=repeatable\\ read"
    )
    provider = FakeProvider(latency=0.01)
    async with running(monolith.create_app(Settings("monolith", url, "amqp://unused/"), provider=provider)) as client:
        stock(url, [("GADGET-2", 1000, 3)])
        body = {"customer_id": "c-1", "email": "a@example.com", "lines": [{"sku": "GADGET-2", "quantity": 1}]}
        responses = await asyncio.gather(*(client.post("/orders", json=body) for _ in range(12)))
    assert sorted(response.status_code for response in responses) == [201] * 3 + [409] * 9
    assert stock_of(url, "GADGET-2") == 0


async def test_a_total_of_exactly_the_limit_is_accepted(shop: Monolith) -> None:
    # 10^12 cents: the largest total the schema and the messages accept; one more is refused (above).
    response = await shop.order([("HUGE-1", 1)])
    assert response.status_code == 201, response.text
    assert response.json()["total_cents"] == 10**12
