"""Defect: with --max-awaiting and --broker-outage together, the load test's recovery numbers count the cap's 503s as
orders that settled.

scripts/load_test.py `send` (34dfacd) records a refusal by the awaiting cap as `outcome = "REFUSED"` with
`outcome_at = answered`. `summarize` then hands `recovery` a (due, outcome_at) pair for every timed sample, refusals
included (`pairs = [(s.due - origin, s.outcome_at - origin if s.outcome_at else 0.0) for s in timed]`), and
`recovery` treats any non-zero outcome_at as an order that settled:

- `outcomes_while_stopped` counts every 503 answered while the node was down (the order service answers those from
  PostgreSQL alone), so a run whose services settled nothing during the outage reports thousands of outcomes;
- `first_outcome_after_restart_s` ("the first order to settle once the node was back: how long the services took to
  notice") becomes the first 503 after the restart: a few milliseconds, whenever the services actually reconnected.

That combination is the one the cap exists for: during an outage no charge is published, orders pile up in
AWAITING_PAYMENT until the cap is reached, and the order service turns the next ones away until the payment service
catches up after the restart. scripts/outage_recovery.py reads the same numbers from the --samples CSV, whose
outcome_at column is filled for refusals too. (summarize's own `ok`/`e2e` exclude refusals; only recovery is wrong.)

Expected: the refusals are left out of the recovery (no order settled while stopped; the first settled 4 s after the
restart). Actual: outcomes_while_stopped == 1 and first_outcome_after_restart_s == 0.21.
Fix idea: in summarize, give recovery 0.0 for a sample without an order_id (`s.outcome_at if s.order_id else 0`), and
in outage_recovery.py skip rows whose outcome is REFUSED.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import httpx

sys.path.insert(0, str(Path(__file__).parents[2] / "scripts"))
import load_test


class Refusing:
    """What the order service answers past the cap (orders.create_app's router: 503, Retry-After)."""

    async def post(self, url: str, json: dict[str, Any]) -> httpx.Response:
        body = {"detail": {"error": "too_many_awaiting_payment", "message": "3 orders are awaiting payment"}}
        return httpx.Response(503, json=body, headers={"Retry-After": "1"})


async def test_a_refusal_is_not_an_order_that_settled_in_the_recovery_numbers() -> None:
    # What send() leaves behind for a refusal: no order, an outcome, and an outcome time (its answer).
    refused = load_test.Sample(0, due=0.0)
    await load_test.send(Refusing(), "http://orders", refused, {}, monolith=False)  # type: ignore[arg-type]
    assert (refused.order_id, refused.outcome, refused.error) == ("", "REFUSED", "")
    assert refused.outcome_at == refused.answered > 0

    run = load_test.Run("services", 4, 0, 10.0, None, 50.0, 0.1, 30.0, 1, max_awaiting=3)
    # Seconds since the first timed order; the node stopped at 10 s and was back at 20 s.
    run.outage = {"stopped_at": 10.0, "stop_returned_at": 11.0, "started_at": 20.0}
    run.samples = [
        # Settled before the stop.
        load_test.Sample(
            0, due=0.0, sent=0.0, answered=0.01, status_code=201, order_id="a", outcome_at=0.05, outcome="CONFIRMED"
        ),
        # Taken during the outage (its charge waits in the outbox), settled 4 s after the node was back.
        load_test.Sample(
            1, due=12.0, sent=12.0, answered=12.01, status_code=201, order_id="b", outcome_at=24.0, outcome="CONFIRMED"
        ),
        # Turned away by the cap while the node was down, and again just after it was back (still full).
        load_test.Sample(2, due=15.0, sent=15.0, answered=15.01, status_code=503, outcome_at=15.01, outcome="REFUSED"),
        load_test.Sample(3, due=20.2, sent=20.2, answered=20.21, status_code=503, outcome_at=20.21, outcome="REFUSED"),
    ]
    run.send_seconds, run.settle_seconds = 20.21, 24.0
    summary = load_test.summarize(run)
    assert (summary["accepted"], summary["refused"], summary["settled"]) == (2, 2, 2)
    recovery = summary["recovery"]
    assert recovery["outcomes_while_stopped"] == 0, f"refusals counted as settled while stopped: {recovery}"
    assert recovery["first_outcome_after_restart_s"] == 4.0, f"a refusal counted as the first settle: {recovery}"
