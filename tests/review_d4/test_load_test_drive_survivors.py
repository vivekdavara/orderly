"""Day-4 review, mutation survivors (not a defect: this PASSES on e2c03bf). tests/test_load_test.py hands `summarize`
ready-made samples whose `due == sent` and which contain no warm-up order, so three mutants of scripts/load_test.py
survive it: end to end measured from `sent` instead of `due`, `drive` keeping the warm-ups in `run.samples`, and the
open loop recording each order's send time as its due time (coordinated omission). This drives `drive`'s open loop
against a fake HTTP client whose POST blocks the event loop for a few ms, so the sender falls behind its schedule,
and kills all three.
"""

from __future__ import annotations

import random
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import httpx
import pytest

sys.path.insert(0, str(Path(__file__).parents[2] / "scripts"))
import load_test


class FakeResponse:
    status_code = 201

    def __init__(self) -> None:
        self.body = {"id": str(uuid.uuid4()), "status": "CONFIRMED"}
        self.text = ""

    def json(self) -> dict[str, Any]:
        return self.body


class FakeClient:
    def __init__(self, **_: Any) -> None:
        pass

    async def __aenter__(self) -> FakeClient:
        return self

    async def __aexit__(self, *_: object) -> None:
        return None

    async def post(self, url: str, json: dict[str, Any]) -> FakeResponse:
        time.sleep(0.008)  # noqa: ASYNC251 (on purpose) blocks the loop: at 500/s (2 ms apart) the sender falls behind its schedule
        return FakeResponse()


async def test_the_open_loop_times_the_timed_orders_from_their_due_times(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(httpx, "AsyncClient", FakeClient)  # what load_test.drive opens
    rate = 500.0
    run = load_test.Run("monolith", 20, 5, rate, None, 50.0, 0.1, 30.0, 1)
    bodies = load_test.order_bodies(run.warmup + run.orders, random.Random(1))
    await load_test.drive(run, bodies, None, None)

    # Only the timed orders, in order.
    assert [s.index for s in run.samples] == list(range(run.warmup, run.warmup + run.orders))
    # Each due on the schedule (start + i / rate), whatever the sender managed.
    origin = run.samples[0].due
    assert [round(s.due - origin, 6) for s in run.samples] == [round(i / rate, 6) for i in range(run.orders)]
    lateness = max(s.sent - s.due for s in run.samples)
    assert lateness > 0.005, "the sender fell behind (else the next check can't tell due from sent)"
    summary = load_test.summarize(run)
    expected = max(s.outcome_at - s.due for s in run.samples)
    assert summary["end_to_end_ms"]["max"] == round(expected * 1000, 1)
