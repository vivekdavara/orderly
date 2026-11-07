"""The load test's drain and books (scripts/load_test.py) against real services: day 4's round-2 review left five
surviving mutants in `wait_drained`, `books`, `accepted_all` and the books' final verdict, which no test covered
because they read the load databases and the broker. Here they read the in-process shop's (tests/shop.py), through a
patched `load_url`, so the books are checked against databases the real services wrote."""

from __future__ import annotations

import random
import sys
import uuid
from collections.abc import AsyncIterator
from pathlib import Path
from typing import Any

import httpx
import psycopg
import pytest
from shop import Shop, open_shop

from orderly.fakes import FakeProvider
from orderly.topology import QUEUES, Topology

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))
import load_test


def services_run(sent: int, accepted: int) -> load_test.Run:
    """A run of the services that sent `sent` orders (no warm-up) of which the target accepted `accepted`."""
    run = load_test.Run("services", sent, 0, 10.0, None, 0.0, 0.0, 30.0, 1)
    run.accepted_all = accepted
    return run


@pytest.fixture
async def shop(
    databases: dict[str, str], short_topology: Topology, monkeypatch: pytest.MonkeyPatch
) -> AsyncIterator[Shop]:
    """The three services with three orders settled (two confirmed, one declined), and the load test pointed at their
    databases: one product (WIDGET-1, 100 units), as the shop stocks it."""
    monkeypatch.setattr(load_test, "load_url", lambda service: databases[service])
    monkeypatch.setattr(load_test, "PRODUCTS", 1)
    monkeypatch.setattr(load_test, "STOCK", 100)
    provider = FakeProvider(declined_customers=frozenset({"c-declined"}))
    async with open_shop(databases, short_topology, provider) as shop:
        placed = [await shop.order(quantity=2), await shop.order(quantity=1), await shop.order(customer="c-declined")]
        await shop.settled([p["id"] for p in placed])
        yield shop


async def test_the_books_agree_when_every_accepted_order_settled_though_some_sent_were_refused(shop: Shop) -> None:
    """Five sent, two refused by the cap (never written), three accepted and settled: the books agree. Counting every
    order sent (M13) makes them disagree."""
    result = load_test.books(services_run(sent=5, accepted=3))
    assert result["ok"], result
    assert result["orders"] == {"CONFIRMED": 2, "CANCELLED:payment_declined": 1}
    assert (result["confirmed_cents"], result["charged_cents"]) == (3 * 2599, 3 * 2599)
    assert (result["units_taken"], result["confirmed_units"]) == (3, 3)
    assert result["notifications"] == result["notified_orders"] == 3 and result["order_mismatches"] == 0


async def test_the_books_disagree_when_an_accepted_order_is_missing(shop: Shop) -> None:
    assert not load_test.books(services_run(sent=5, accepted=4))["ok"]


async def test_the_books_disagree_when_one_order_is_wrong_even_if_the_sums_match(shop: Shop) -> None:
    """A confirmed order's payment marked REFUNDED and the declined order's marked CHARGED for the same amount: every
    sum still matches, the order-by-order check doesn't."""
    with psycopg.connect(shop.databases["orders"]) as conn:
        rows = conn.execute("SELECT id, status, total_cents FROM orders ORDER BY created_at").fetchall()
    declined = next(r for r in rows if r[1] == "CANCELLED")
    confirmed = next(r for r in rows if r[1] == "CONFIRMED" and r[2] == declined[2])  # the same amount
    with psycopg.connect(shop.databases["payments"]) as conn:
        conn.execute("UPDATE payments SET status = 'DECLINED', charge_ref = NULL WHERE order_id = %s", (confirmed[0],))
        conn.execute("UPDATE payments SET status = 'CHARGED', charge_ref = 'ch_x' WHERE order_id = %s", (declined[0],))
    result = load_test.books(services_run(sent=3, accepted=3))
    assert result["confirmed_cents"] == result["charged_cents"]
    assert not result["ok"] and result["order_mismatches"] == 2


async def test_the_drain_goes_quiet_counting_the_accepted_orders(shop: Shop, short_topology: Topology) -> None:
    """Every accepted order told, nothing queued: quiet. Waiting for every order sent (M12) never goes quiet."""
    quiet, dead = await load_test.wait_drained(services_run(sent=5, accepted=3), short_topology, timeout=10)
    assert quiet
    assert dead == {short_topology.dead_letter_queue(spec.name): 0 for spec in QUEUES}


async def test_a_drain_that_times_out_says_so(shop: Shop, short_topology: Topology) -> None:
    """One accepted order never told: the drain times out and reports it (M15 reported a timeout as quiet)."""
    quiet, _ = await load_test.wait_drained(services_run(sent=4, accepted=4), short_topology, timeout=2)
    assert not quiet


@pytest.mark.parametrize(
    ("ok", "quiet", "dead", "expected"),
    [
        (True, True, {"a.dlq": 0}, True),
        (True, False, {"a.dlq": 0}, False),  # M14: the drain's timeout ignored
        (True, True, {"a.dlq": 1}, False),
        (False, True, {"a.dlq": 0}, False),
    ],
)
def test_the_services_books_count_only_after_a_quiet_drain_with_nothing_dead_lettered(
    ok: bool, quiet: bool, dead: dict[str, int], expected: bool
) -> None:
    result: dict[str, Any] = {"ok": ok}
    load_test.settle_books(result, quiet, dead)
    assert result == {"ok": expected, "quiet": quiet, "dead_lettered": dead}


class Answers:
    """A fake HTTP client for `drive`: the target accepts every order but each `refuse_every`-th, which the cap
    refuses."""

    def __init__(self, refuse_every: int) -> None:
        self.refuse_every = refuse_every
        self.calls = 0

    def __call__(self, **_: Any) -> Answers:
        return self

    async def __aenter__(self) -> Answers:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def post(self, url: str, json: dict[str, Any]) -> httpx.Response:
        self.calls += 1
        if self.calls % self.refuse_every == 0:
            return httpx.Response(503, json={"error": "too_many_awaiting_payment"})
        return httpx.Response(201, json={"id": str(uuid.uuid4()), "status": "CONFIRMED"})


@pytest.mark.parametrize("loop", ["open", "closed"])
async def test_the_accepted_count_includes_the_warm_up_and_excludes_refusals(
    loop: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The books must account for every order written, the untimed warm-up included (M11 counted the timed orders
    only), and for none the cap refused."""
    answers = Answers(refuse_every=4)
    monkeypatch.setattr(httpx, "AsyncClient", answers)
    rate, concurrency = (1000.0, None) if loop == "open" else (None, 3)
    run = load_test.Run("monolith", 12, 8, rate, concurrency, 0.0, 0.0, 30.0, 1)
    await load_test.drive(run, load_test.order_bodies(20, random.Random(1)), None, None)
    assert answers.calls == 20
    assert run.accepted_all == 20 - 20 // 4
    assert len(run.samples) == 12 and sum(s.outcome == "REFUSED" for s in run.samples) == 3
