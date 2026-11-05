"""The load test's own arithmetic and inputs (scripts/load_test.py): the numbers in the README come from these."""

from __future__ import annotations

import random
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parents[1] / "scripts"))
import load_test

from orderly.ordering import OrderRequest


def test_percentiles_are_nearest_rank_in_integers() -> None:
    values = [float(n) for n in range(1, 1001)]
    # In floats, ceil(99.0 / 100 * 1000) is fine but ceil(0.999 * 1000) isn't; integers are always the rank.
    assert [load_test.percentile(values, p) for p in (50, 95, 99, 100)] == [500.0, 950.0, 990.0, 1000.0]
    assert load_test.percentile([7.0], 50) == 7.0
    assert load_test.percentile([1.0, 2.0, 3.0], 1) == 1.0  # never rank 0


def test_the_orders_are_fixed_by_the_seed_and_all_valid() -> None:
    first = load_test.order_bodies(500, random.Random(1))
    assert first == load_test.order_bodies(500, random.Random(1))
    assert first != load_test.order_bodies(500, random.Random(2))
    for body in first:
        request = OrderRequest.model_validate(body)
        assert 1 <= len(request.lines) <= 3
        assert all(1 <= line.quantity <= 3 for line in request.lines)
    skus = {line["sku"] for body in first for line in body["lines"]}
    assert skus <= {f"SKU-{n:04d}" for n in range(1, load_test.PRODUCTS + 1)} and len(skus) > 90


@pytest.mark.parametrize("target", ["services", "monolith"])
def test_the_summary_counts_only_the_timed_orders(target: str) -> None:
    run = load_test.Run(target, 3, 1, 10.0, None, 50.0, 0.1, 30.0, 1)
    run.samples = [
        load_test.Sample(
            1, due=1.0, sent=1.0, answered=1.004, status_code=201, order_id="a", outcome_at=1.06, outcome="CONFIRMED"
        ),
        load_test.Sample(
            2,
            due=1.1,
            sent=1.1,
            answered=1.105,
            status_code=201,
            order_id="b",
            outcome_at=1.2,
            outcome="CANCELLED:payment_declined",
        ),
        load_test.Sample(3, due=1.2, sent=1.2, answered=1.3, error="ConnectError: refused"),
    ]
    run.send_seconds, run.settle_seconds = 0.3, 0.2
    summary = load_test.summarize(run)
    assert (summary["accepted"], summary["settled"], summary["errors"]) == (2, 2, {"ConnectError": 1})
    assert summary["outcomes"] == {"CONFIRMED": 1, "CANCELLED:payment_declined": 1}
    assert summary["end_to_end_ms"] == {"p50": 60.0, "p95": 100.0, "p99": 100.0, "max": 100.0}
    assert summary["settled_per_s"] == 10.0
