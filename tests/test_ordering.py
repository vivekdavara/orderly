"""The order request: what POST /orders accepts, and the fingerprint that tells a retry from a reused key."""

from __future__ import annotations

from typing import Any

import pytest
from pydantic import ValidationError

from orderly.ordering import IDEMPOTENCY_KEY, MAX_LINES, OrderRequest


def body(**changes: Any) -> dict[str, Any]:
    return {"customer_id": "c-42", "email": "ada@example.com", "lines": [{"sku": "WIDGET-1", "quantity": 2}]} | changes


def test_a_valid_request() -> None:
    request = OrderRequest.model_validate(body())
    assert request.lines[0].sku == "WIDGET-1" and request.lines[0].quantity == 2


@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"customer_id": "c 42"}, "customer_id"),
        ({"customer_id": ""}, "customer_id"),
        ({"email": "ada"}, "email"),
        ({"email": "ada@exam ple.com"}, "email"),
        ({"email": "ada@example.com\x00"}, "email"),
        ({"lines": []}, "at least 1"),
        ({"lines": [{"sku": f"S-{n}", "quantity": 1} for n in range(MAX_LINES + 1)]}, "at most 50"),
        ({"lines": [{"sku": "widget-1", "quantity": 1}]}, "sku"),
        ({"lines": [{"sku": "-WIDGET", "quantity": 1}]}, "sku"),
        ({"lines": [{"sku": "W" * 33, "quantity": 1}]}, "sku"),
        ({"lines": [{"sku": "WIDGET-1", "quantity": 0}]}, "greater than or equal to 1"),
        ({"lines": [{"sku": "WIDGET-1", "quantity": 1001}]}, "less than or equal to 1000"),
        ({"lines": [{"sku": "WIDGET-1", "quantity": "2"}]}, "valid integer"),
        ({"lines": [{"sku": "WIDGET-1", "quantity": 2.0}]}, "valid integer"),
        ({"lines": [{"sku": "WIDGET-1", "quantity": True}]}, "valid integer"),
        ({"lines": [{"sku": "WIDGET-1", "quantity": 2, "price_cents": 1}]}, "Extra inputs"),
        ({"total_cents": 1}, "Extra inputs"),
        (
            {"lines": [{"sku": "B", "quantity": 1}, {"sku": "A", "quantity": 1}, {"sku": "B", "quantity": 2}]},
            "repeated: B",
        ),
    ],
)
def test_requests_that_are_refused(changes: dict[str, Any], message: str) -> None:
    with pytest.raises(ValidationError, match=message):
        OrderRequest.model_validate(body(**changes))


def test_the_fingerprint_is_the_requests_content_not_its_spelling() -> None:
    first = OrderRequest.model_validate_json(
        '{"customer_id":"c-42","email":"ada@example.com","lines":[{"sku":"A","quantity":1}]}'
    )
    spaced = OrderRequest.model_validate_json(
        '{ "lines": [ {"quantity": 1, "sku": "A"} ], "email": "ada@example.com", "customer_id": "c-42" }'
    )
    assert first.fingerprint() == spaced.fingerprint()
    assert len(first.fingerprint()) == 64


@pytest.mark.parametrize(
    "changes",
    [
        {"customer_id": "c-43"},
        {"email": "bob@example.com"},
        {"lines": [{"sku": "WIDGET-1", "quantity": 3}]},
        {"lines": [{"sku": "WIDGET-2", "quantity": 2}]},
        {"lines": [{"sku": "WIDGET-1", "quantity": 2}, {"sku": "WIDGET-2", "quantity": 1}]},
    ],
)
def test_any_change_to_the_request_changes_the_fingerprint(changes: dict[str, Any]) -> None:
    assert (
        OrderRequest.model_validate(body()).fingerprint() != OrderRequest.model_validate(body(**changes)).fingerprint()
    )


def test_line_order_is_part_of_the_request() -> None:
    # Lines keep the client's order (line numbers), so the same lines reordered are a different request.
    ab = body(lines=[{"sku": "A", "quantity": 1}, {"sku": "B", "quantity": 1}])
    ba = body(lines=[{"sku": "B", "quantity": 1}, {"sku": "A", "quantity": 1}])
    assert OrderRequest.model_validate(ab).fingerprint() != OrderRequest.model_validate(ba).fingerprint()


@pytest.mark.parametrize(
    ("key", "valid"),
    [
        ("k", True),
        ("a" * 200, True),
        ("4f1c-uuid:retry/1", True),
        ("", False),
        ("a" * 201, False),
        ("a b", False),
        ("é", False),
        ("k\n", False),
    ],
)
def test_idempotency_keys_are_1_to_200_visible_ascii_characters(key: str, valid: bool) -> None:
    assert (IDEMPOTENCY_KEY.fullmatch(key) is not None) is valid
