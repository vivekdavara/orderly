"""Defect: the published JSON Schema refuses a message that decode accepts (and encode produces). The e-mail pattern
uses \\s, which the JSON Schema dialect (ECMA-262) defines to include U+FEFF (and pydantic's Rust regex and Python's
re do not). So an OrderConfirmed whose e-mail holds a U+FEFF (a BOM pasted into a form) is valid to decode but invalid
to any ECMA-based validator (ajv, a browser, Node): the schema is meant to be looser than decode, never stricter
(json_schema's docstring: "decode stays the authority and is stricter"). Node runs the schema's own pattern."""

from __future__ import annotations

import json
import shutil
import subprocess
import uuid

import pytest

from orderly.messages import InvalidMessage, decode, json_schema


def ecma_matches(pattern: str, text: str) -> bool:
    node = shutil.which("node")
    if node is None:
        pytest.skip("node not installed")
    script = "const [p, s] = JSON.parse(process.argv[1]); process.stdout.write(String(new RegExp(p, 'u').test(s)))"
    result = subprocess.run(
        [node, "-e", script, json.dumps([pattern, text])], capture_output=True, text=True, check=True, timeout=30
    )
    return result.stdout == "true"


@pytest.mark.parametrize(
    "char",
    ["\ufeff", "\u00a0", "\u0085", "\u2028", "\u3000", "\u200b", "\x9f", "\x80", "\t", "\u00e9", "\u4e2d"],
)
def test_decode_and_the_schemas_pattern_in_ecma_262_agree_on_an_email(char: str) -> None:
    """Originally: decode accepted "ann\ufeff@example.com" and the schema refused it. Now decode and the schema's own
    pattern, run by Node, give the same verdict for each character (whitespace and controls refused, letters and
    U+200B, which no dialect calls whitespace, accepted)."""
    email = f"ann{char}@example.com"
    order = str(uuid.uuid4())
    body = json.dumps(
        {
            "message_id": str(uuid.uuid4()),
            "type": "order.confirmed",
            "version": 1,
            "occurred_at": "2026-11-02T20:15:03.123456Z",
            "correlation_id": order,
            "causation_id": None,
            "payload": {"order_id": order, "customer_id": "c-1", "email": email, "total_cents": 1, "currency": "USD"},
        }
    ).encode()
    try:
        decode(body)
        accepted = True
    except InvalidMessage:
        accepted = False
    pattern = json_schema()["$defs"]["OrderConfirmed"]["properties"]["email"]["pattern"]
    assert ecma_matches(pattern, email) == accepted
    assert accepted == (char in {"\u00e9", "\u4e2d", "\u200b"})
