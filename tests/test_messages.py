import contextlib
import json
import uuid
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from jsonschema import Draft202012Validator

from orderly.messages import (
    MAX_CENTS,
    PAYLOADS,
    AnyPayload,
    ChargePayment,
    InvalidMessage,
    Message,
    OrderCancelled,
    OrderConfirmed,
    PaymentFailed,
    PaymentRefunded,
    PaymentSucceeded,
    RefundPayment,
    decode,
    encode,
    format_time,
    json_schema,
    json_schema_text,
    new_message,
)

ORDER = uuid.UUID("0b8f6f8e-1d1f-4f4e-8a3c-7f0f2b9d5e11")
MESSAGE = uuid.UUID("6f1c0c1e-6b8e-4c51-9d0e-2f1c4b0e7a10")
WHEN = datetime(2026, 11, 2, 20, 15, 3, 123456, tzinfo=UTC)


def charge(**overrides: Any) -> Message:
    fields: dict[str, Any] = {"order_id": ORDER, "customer_id": "c-42", "amount_cents": 2599, "currency": "USD"}
    fields.update(overrides)
    return new_message(ChargePayment(**fields), correlation_id=ORDER, occurred_at=WHEN, message_id=MESSAGE)


def wire(**changes: Any) -> bytes:
    """The canonical charge message as JSON, with envelope or payload fields changed (_DELETE removes one)."""
    raw = json.loads(encode(charge()))
    for key, value in changes.items():
        target = raw["payload"] if key.startswith("payload.") else raw
        name = key.removeprefix("payload.")
        if value is _DELETE:
            del target[name]
        else:
            target[name] = value
    return json.dumps(raw).encode()


_DELETE = object()


def test_the_canonical_form_is_pinned() -> None:
    assert encode(charge()) == (
        b'{"message_id":"6f1c0c1e-6b8e-4c51-9d0e-2f1c4b0e7a10","type":"payment.charge","version":1,'
        b'"occurred_at":"2026-11-02T20:15:03.123456Z","correlation_id":"0b8f6f8e-1d1f-4f4e-8a3c-7f0f2b9d5e11",'
        b'"causation_id":null,"payload":{"order_id":"0b8f6f8e-1d1f-4f4e-8a3c-7f0f2b9d5e11","customer_id":"c-42",'
        b'"amount_cents":2599,"currency":"USD"}}'
    )


def test_decode_gives_back_the_message_and_its_type() -> None:
    message = decode(encode(charge()))
    assert message == charge()
    assert (message.type, message.kind) == ("payment.charge", "command")
    assert isinstance(message.payload, ChargePayment)


def test_whitespace_and_key_order_dont_matter_on_input() -> None:
    raw = json.loads(encode(charge()))
    shuffled = {key: raw[key] for key in reversed(list(raw))}
    assert decode(json.dumps(shuffled, indent=2).encode()) == charge()


def test_every_type_has_a_kind_and_names_its_service() -> None:
    assert set(PAYLOADS) == {
        "payment.charge",
        "payment.refund",
        "payment.succeeded",
        "payment.failed",
        "payment.refunded",
        "order.confirmed",
        "order.cancelled",
    }
    commands = {name for name, model in PAYLOADS.items() if model.KIND == "command"}
    assert commands == {"payment.charge", "payment.refund"}


@pytest.mark.parametrize(
    ("body", "reason"),
    [
        (b"\xff{}", "not UTF-8"),
        (b"{", "not JSON"),
        (b"[]", "not a JSON object"),
        (b'{"a": 1, "a": 2}', "duplicate key 'a'"),
        (b'{"a": NaN}', "NaN is not allowed"),
        (b'{"a": -Infinity}', "-Infinity is not allowed"),
        (b"[" * 100_000, "nested too deeply"),
        (b"9" * 5000, "not JSON"),
        (wire(version=2), "unknown version 2"),
        (wire(version=True), "envelope.version"),
        (wire(version=1.0), "envelope.version"),
        (wire(version="1"), "envelope.version"),
        (wire(type="payment.capture"), "unknown type 'payment.capture'"),
        (wire(extra=1), "envelope.extra: Extra inputs are not permitted"),
        (wire(causation_id=_DELETE), "envelope.causation_id: Field required"),
        (wire(message_id="6F1C0C1E-6B8E-4C51-9D0E-2F1C4B0E7A10"), "envelope.message_id"),
        (wire(message_id="6f1c0c1e6b8e4c519d0e2f1c4b0e7a10"), "envelope.message_id"),
        (wire(message_id="{6f1c0c1e-6b8e-4c51-9d0e-2f1c4b0e7a10}"), "envelope.message_id"),
        (wire(occurred_at="2026-11-02T20:15:03Z"), "envelope.occurred_at"),
        (wire(occurred_at="2026-11-02T20:15:03.123456+00:00"), "envelope.occurred_at"),
        (wire(occurred_at="2026-11-02 20:15:03.123456Z"), "envelope.occurred_at"),
        (wire(occurred_at="2026-02-30T20:15:03.123456Z"), "envelope.occurred_at"),
        (wire(occurred_at="2026-11-02T20:15:03.1234567Z"), "envelope.occurred_at"),
        # A full-width digit: the pattern says [0-9], not \d, which would match it.
        (wire(occurred_at="\uff12026-11-02T20:15:03.123456Z"), "envelope.occurred_at"),
        (wire(payload=[]), "envelope.payload"),
        (wire(**{"payload.amount_cents": True}), "payload.amount_cents"),
        (wire(**{"payload.amount_cents": 2599.0}), "payload.amount_cents"),
        (wire(**{"payload.amount_cents": "2599"}), "payload.amount_cents"),
        (wire(**{"payload.amount_cents": 0}), "payload.amount_cents"),
        (wire(**{"payload.amount_cents": -1}), "payload.amount_cents"),
        (wire(**{"payload.amount_cents": MAX_CENTS + 1}), "payload.amount_cents"),
        (wire(**{"payload.currency": "usd"}), "payload.currency"),
        (wire(**{"payload.currency": "USDX"}), "payload.currency"),
        (wire(**{"payload.currency": "USD\n"}), "payload.currency"),
        (wire(**{"payload.customer_id": ""}), "payload.customer_id"),
        (wire(**{"payload.customer_id": "c 42"}), "payload.customer_id"),
        (wire(**{"payload.customer_id": "c" * 65}), "payload.customer_id"),
        (wire(**{"payload.reason": "declined"}), "payload.reason: Extra inputs"),
        (wire(**{"payload.order_id": _DELETE}), "payload.order_id: Field required"),
    ],
)
def test_malformed_messages_are_refused_with_a_reason(body: bytes, reason: str) -> None:
    with pytest.raises(InvalidMessage) as caught:
        decode(body)
    assert reason in caught.value.reason


def test_the_largest_amount_and_a_zero_refund_are_valid() -> None:
    assert decode(wire(**{"payload.amount_cents": MAX_CENTS})).payload == ChargePayment(
        order_id=ORDER, customer_id="c-42", amount_cents=MAX_CENTS, currency="USD"
    )
    refunded = PaymentRefunded(order_id=ORDER, payment_id=MESSAGE, amount_cents=0, currency="USD")
    assert decode(encode(new_message(refunded, correlation_id=ORDER))).payload == refunded


def test_times_must_be_utc_and_keep_their_microseconds() -> None:
    with pytest.raises(ValueError, match="UTC"):
        new_message(charge().payload, correlation_id=ORDER, occurred_at=datetime(2026, 11, 2, 20, 15))
    with pytest.raises(ValueError, match="UTC"):
        format_time(datetime(2026, 11, 2, 20, 15, tzinfo=timezone(timedelta(hours=-5))))
    assert format_time(datetime(2026, 11, 2, tzinfo=UTC)) == "2026-11-02T00:00:00.000000Z"
    # Years before 1000 keep four digits (strftime's %Y doesn't pad them on Linux).
    assert format_time(datetime(999, 1, 1, tzinfo=UTC)) == "0999-01-01T00:00:00.000000Z"


def test_a_message_without_a_given_time_or_id_gets_fresh_ones() -> None:
    first = new_message(charge().payload, correlation_id=ORDER)
    second = new_message(charge().payload, correlation_id=ORDER)
    assert first.message_id != second.message_id
    assert abs(first.occurred_at - datetime.now(UTC)) < timedelta(seconds=5)


uuids = st.uuids()
customer_ids = st.text(alphabet="abcXYZ019._:-", min_size=1, max_size=64)
emails = st.builds(lambda a, b: f"{a}@{b}", st.text("abc.+_", min_size=1), st.text("abc.-", min_size=1))
positive = st.integers(1, MAX_CENTS)
currencies = st.sampled_from(["USD", "EUR", "INR", "JPY"])
payloads: st.SearchStrategy[AnyPayload] = st.one_of(
    st.builds(ChargePayment, order_id=uuids, customer_id=customer_ids, amount_cents=positive, currency=currencies),
    st.builds(
        RefundPayment,
        order_id=uuids,
        amount_cents=positive,
        currency=currencies,
        reason=st.sampled_from(["payment_timeout", "order_cancelled"]),
    ),
    st.builds(PaymentSucceeded, order_id=uuids, payment_id=uuids, amount_cents=positive, currency=currencies),
    st.builds(PaymentFailed, order_id=uuids, reason=st.sampled_from(["declined", "voided"])),
    st.builds(
        PaymentRefunded, order_id=uuids, payment_id=uuids, amount_cents=st.integers(0, MAX_CENTS), currency=currencies
    ),
    st.builds(
        OrderConfirmed,
        order_id=uuids,
        customer_id=customer_ids,
        email=emails,
        total_cents=positive,
        currency=currencies,
    ),
    st.builds(
        OrderCancelled,
        order_id=uuids,
        customer_id=customer_ids,
        email=emails,
        reason=st.sampled_from(["payment_declined", "payment_timeout"]),
    ),
)
messages = st.builds(
    lambda payload, correlation, causation, when, message_id: new_message(
        payload, correlation_id=correlation, causation_id=causation, occurred_at=when, message_id=message_id
    ),
    payloads,
    uuids,
    st.none() | uuids,
    st.datetimes(timezones=st.just(UTC)),
    uuids,
)


@given(messages)
def test_every_message_survives_a_round_trip_byte_for_byte(message: Message) -> None:
    body = encode(message)
    decoded = decode(body)
    assert decoded == message
    assert encode(decoded) == body


json_values = st.recursive(
    st.none() | st.booleans() | st.integers() | st.floats(allow_nan=False) | st.text(),
    lambda inner: st.lists(inner, max_size=3) | st.dictionaries(st.text(max_size=5), inner, max_size=3),
    max_leaves=10,
)


@settings(max_examples=300)
@given(messages, st.data())
def test_a_changed_field_decodes_or_is_refused_never_anything_else(message: Message, data: st.DataObject) -> None:
    raw = json.loads(encode(message))
    target = data.draw(st.sampled_from([raw, raw["payload"]]))
    key = data.draw(st.sampled_from([*target, "unexpected"]))
    target[key] = data.draw(json_values)
    try:
        decoded = decode(json.dumps(raw).encode())
    except InvalidMessage:
        return
    # If it decoded, it is a valid message whose canonical form round-trips.
    assert decode(encode(decoded)) == decoded


@given(st.binary(max_size=200))
def test_arbitrary_bytes_are_refused_cleanly(body: bytes) -> None:
    with contextlib.suppress(InvalidMessage):
        decode(body)


SCHEMA_FILE = Path(__file__).parents[1] / "docs" / "message-schema.json"


def test_the_published_schema_is_the_codes() -> None:
    assert SCHEMA_FILE.read_text() == json_schema_text(), "run scripts/export_message_schema.py"
    Draft202012Validator.check_schema(json.loads(SCHEMA_FILE.read_text()))


@given(messages)
def test_every_encoded_message_matches_exactly_one_variant_of_the_schema(message: Message) -> None:
    validator = Draft202012Validator(json_schema())
    validator.validate(json.loads(encode(message)))  # oneOf: exactly one variant, else this raises


@pytest.mark.parametrize(
    "body",
    [
        wire(type="payment.capture"),
        wire(version=2),
        wire(version=True),
        wire(extra=1),
        wire(causation_id=_DELETE),
        wire(message_id="6F1C0C1E-6B8E-4C51-9D0E-2F1C4B0E7A10"),
        wire(occurred_at="2026-11-02T20:15:03Z"),
        wire(**{"payload.amount_cents": True}),
        wire(**{"payload.amount_cents": "2599"}),
        wire(**{"payload.amount_cents": 0}),
        wire(**{"payload.amount_cents": MAX_CENTS + 1}),
        wire(**{"payload.currency": "usd"}),
        wire(**{"payload.reason": "declined"}),
    ],
)
def test_the_schema_refuses_what_it_can_express(body: bytes) -> None:
    assert not Draft202012Validator(json_schema()).is_valid(json.loads(body))
    with pytest.raises(InvalidMessage):
        decode(body)


@pytest.mark.parametrize(
    "body",
    [
        wire(occurred_at="2026-02-30T20:15:03.123456Z"),
        wire(**{"payload.amount_cents": 2599.0}),
        wire(**{"payload.currency": "USD\n"}),
    ],
)
def test_the_decoder_is_stricter_than_the_schema_where_its_docstring_says(body: bytes) -> None:
    assert Draft202012Validator(json_schema()).is_valid(json.loads(body))
    with pytest.raises(InvalidMessage):
        decode(body)
