"""Defect: the Email type accepts control characters, NUL included. PostgreSQL text can't hold NUL, so such an
order.confirmed decodes fine but can never be stored by the notification service (or have been written by the order
service): a "valid" message that fails its handler on every attempt instead of being refused as undecodable."""

import json

import pytest

from orderly.messages import InvalidMessage, decode

ORDER = "0b8f6f8e-1d1f-4f4e-8a3c-7f0f2b9d5e11"


@pytest.mark.parametrize(
    "email", ["a\x00b@example.com", "a\x1cb@example.com", "a@example.com\x00", "a\x7fb@c.d", "a\x85b@c.d"]
)
def test_an_email_with_a_control_character_is_refused(email: str) -> None:
    body = json.dumps(
        {
            "message_id": "6f1c0c1e-6b8e-4c51-9d0e-2f1c4b0e7a10",
            "type": "order.confirmed",
            "version": 1,
            "occurred_at": "2026-11-02T20:15:03.123456Z",
            "correlation_id": ORDER,
            "causation_id": None,
            "payload": {"order_id": ORDER, "customer_id": "c-1", "email": email, "total_cents": 5, "currency": "USD"},
        }
    ).encode()
    with pytest.raises(InvalidMessage):
        decode(body)
