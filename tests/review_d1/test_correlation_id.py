"""Defect: DESIGN.md "Messages" says correlation_id *is* the order id on every message of that order's saga, but
decode accepts a message whose correlation_id names a different order than its payload's order_id."""

import json

import pytest

from orderly.messages import InvalidMessage, decode

ORDER = "0b8f6f8e-1d1f-4f4e-8a3c-7f0f2b9d5e11"
OTHER = "9a4a1f0e-55b5-4c1d-8a0b-2f2e1d0c9b8a"


def test_a_correlation_id_that_isnt_the_order_id_is_refused() -> None:
    body = json.dumps(
        {
            "message_id": "6f1c0c1e-6b8e-4c51-9d0e-2f1c4b0e7a10",
            "type": "payment.failed",
            "version": 1,
            "occurred_at": "2026-11-02T20:15:03.123456Z",
            "correlation_id": OTHER,
            "causation_id": None,
            "payload": {"order_id": ORDER, "reason": "declined"},
        }
    ).encode()
    with pytest.raises(InvalidMessage):
        decode(body)
