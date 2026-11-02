"""The message contracts: one JSON envelope around a typed payload, decoded strictly.

Every message on the broker is the UTF-8 JSON of an envelope (DESIGN.md "Messages"):

    {"message_id", "type", "version", "occurred_at", "correlation_id", "causation_id", "payload"}

`decode` turns bytes into a `Message` or raises `InvalidMessage` with a reason. It refuses anything that isn't exactly a
message of a known type: duplicate keys, NaN, unknown or missing fields, a boolean or float where an integer belongs,
a UUID or a time not in its one canonical spelling, an amount out of range. `encode` writes the one canonical form, so
`encode(decode(b)) == b` for every `b` that `encode` produced.
"""

from __future__ import annotations

import json
import re
import uuid
from datetime import UTC, datetime
from typing import Annotated, Any, ClassVar, Literal, TypeAlias, cast

from pydantic import BaseModel, ConfigDict, Field, PlainSerializer, PlainValidator, StrictInt, ValidationError

VERSION = 1
# Ten billion dollars in cents: far above any real order, far below bigint's limit.
MAX_CENTS = 10**12

_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
_TIME = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z")


class InvalidMessage(ValueError):
    """The bytes are not a valid message; `reason` says why. Such a message is never retried."""

    def __init__(self, reason: str) -> None:
        super().__init__(reason)
        self.reason = reason


def _canonical_uuid(value: object) -> uuid.UUID:
    if isinstance(value, uuid.UUID):
        return value
    if not isinstance(value, str) or not _UUID.fullmatch(value):
        raise ValueError("must be a UUID in lowercase hyphenated form")
    return uuid.UUID(value)


def _utc_micros(value: object) -> datetime:
    if isinstance(value, datetime):
        if value.utcoffset() != UTC.utcoffset(None):
            raise ValueError("must be in UTC")
        return value.astimezone(UTC)
    if not isinstance(value, str) or not _TIME.fullmatch(value):
        raise ValueError("must be a UTC time like 2026-11-02T20:15:03.123456Z")
    return datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=UTC)


def format_time(value: datetime) -> str:
    """The one spelling of a time on the wire: UTC, six fractional digits, a Z."""
    # Not strftime: its %Y doesn't pad years before 1000 on Linux (it does on macOS). isoformat always does.
    return _utc_micros(value).isoformat(timespec="microseconds").replace("+00:00", "Z")


CanonicalUUID = Annotated[
    uuid.UUID, PlainValidator(_canonical_uuid, json_schema_input_type=str), PlainSerializer(str, return_type=str)
]
UtcTime = Annotated[
    datetime, PlainValidator(_utc_micros, json_schema_input_type=str), PlainSerializer(format_time, return_type=str)
]
PositiveCents = Annotated[StrictInt, Field(gt=0, le=MAX_CENTS)]
Cents = Annotated[StrictInt, Field(ge=0, le=MAX_CENTS)]
Currency = Annotated[str, Field(strict=True, pattern=r"^[A-Z]{3}$")]
CustomerId = Annotated[str, Field(strict=True, min_length=1, max_length=64, pattern=r"^[A-Za-z0-9._:-]+$")]
Email = Annotated[str, Field(strict=True, max_length=254, pattern=r"^[^@\s]+@[^@\s]+$")]


class Payload(BaseModel):
    """A message's body. Subclasses set TYPE (the routing key) and KIND."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    TYPE: ClassVar[str]
    KIND: ClassVar[Literal["command", "event"]]


class ChargePayment(Payload):
    TYPE = "payment.charge"
    KIND = "command"
    order_id: CanonicalUUID
    customer_id: CustomerId
    amount_cents: PositiveCents
    currency: Currency


class RefundPayment(Payload):
    TYPE = "payment.refund"
    KIND = "command"
    order_id: CanonicalUUID
    # The amount the order asked to be charged. A refund can arrive before its charge (DESIGN.md "The saga"); the
    # payment service then records the payment as voided, for this amount, and never charges it.
    amount_cents: PositiveCents
    currency: Currency
    # payment_timeout: the order gave up waiting; order_cancelled: a success arrived after it did.
    reason: Literal["payment_timeout", "order_cancelled"]


class PaymentSucceeded(Payload):
    TYPE = "payment.succeeded"
    KIND = "event"
    order_id: CanonicalUUID
    payment_id: CanonicalUUID
    amount_cents: PositiveCents
    currency: Currency


class PaymentFailed(Payload):
    TYPE = "payment.failed"
    KIND = "event"
    order_id: CanonicalUUID
    # declined: the provider said no; voided: a refund for this order came first, so it is never charged.
    reason: Literal["declined", "voided"]


class PaymentRefunded(Payload):
    TYPE = "payment.refunded"
    KIND = "event"
    order_id: CanonicalUUID
    payment_id: CanonicalUUID
    # 0 when nothing had been charged (the refund voided the payment, or the charge was declined).
    amount_cents: Cents
    currency: Currency


class OrderConfirmed(Payload):
    TYPE = "order.confirmed"
    KIND = "event"
    order_id: CanonicalUUID
    customer_id: CustomerId
    email: Email
    total_cents: PositiveCents
    currency: Currency


class OrderCancelled(Payload):
    TYPE = "order.cancelled"
    KIND = "event"
    order_id: CanonicalUUID
    customer_id: CustomerId
    email: Email
    reason: Literal["payment_declined", "payment_timeout"]


PAYLOADS: dict[str, type[Payload]] = {
    cls.TYPE: cls
    for cls in (
        ChargePayment,
        RefundPayment,
        PaymentSucceeded,
        PaymentFailed,
        PaymentRefunded,
        OrderConfirmed,
        OrderCancelled,
    )
}

AnyPayload: TypeAlias = (
    ChargePayment | RefundPayment | PaymentSucceeded | PaymentFailed | PaymentRefunded | OrderConfirmed | OrderCancelled
)


class _Envelope(BaseModel):
    """The envelope with its payload still raw: the payload's model depends on `type`."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    message_id: CanonicalUUID
    type: str
    version: StrictInt
    occurred_at: UtcTime
    correlation_id: CanonicalUUID
    causation_id: CanonicalUUID | None
    payload: dict[str, Any]


class Message(BaseModel):
    """A decoded message. Build new ones with `new_message`."""

    model_config = ConfigDict(strict=True, extra="forbid", frozen=True)

    message_id: CanonicalUUID
    occurred_at: UtcTime
    correlation_id: CanonicalUUID
    causation_id: CanonicalUUID | None
    payload: AnyPayload

    @property
    def type(self) -> str:
        return self.payload.TYPE

    @property
    def kind(self) -> Literal["command", "event"]:
        return self.payload.KIND


def new_message(
    payload: AnyPayload,
    *,
    correlation_id: uuid.UUID,
    causation_id: uuid.UUID | None = None,
    occurred_at: datetime | None = None,
    message_id: uuid.UUID | None = None,
) -> Message:
    """A new message with a fresh id, stamped now (to the microsecond) unless told otherwise."""
    when = datetime.now(UTC) if occurred_at is None else occurred_at
    return Message(
        message_id=uuid.uuid4() if message_id is None else message_id,
        occurred_at=_utc_micros(when),
        correlation_id=correlation_id,
        causation_id=causation_id,
        payload=payload,
    )


def encode(message: Message) -> bytes:
    """The canonical bytes: the envelope's keys in a fixed order, the payload's in field order, no spaces."""
    envelope = {
        "message_id": str(message.message_id),
        "type": message.type,
        "version": VERSION,
        "occurred_at": format_time(message.occurred_at),
        "correlation_id": str(message.correlation_id),
        "causation_id": None if message.causation_id is None else str(message.causation_id),
        "payload": message.payload.model_dump(mode="json"),
    }
    return json.dumps(envelope, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _no_duplicate_keys(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise InvalidMessage(f"duplicate key {key!r}")
        result[key] = value
    return result


def _no_constants(name: str) -> Any:
    raise InvalidMessage(f"{name} is not allowed")


def _describe(error: ValidationError, where: str) -> str:
    first = error.errors(include_url=False)[0]
    location = ".".join(str(part) for part in first["loc"])
    return f"{where}{'.' + location if location else ''}: {first['msg']}"


def decode(body: bytes) -> Message:
    """Decode a message's bytes, or raise InvalidMessage saying what is wrong with them."""
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as error:
        raise InvalidMessage(f"not UTF-8: {error.reason} at byte {error.start}") from None
    try:
        raw = json.loads(text, object_pairs_hook=_no_duplicate_keys, parse_constant=_no_constants)
    except InvalidMessage:
        raise
    except json.JSONDecodeError as error:
        raise InvalidMessage(f"not JSON: {error.msg} at line {error.lineno} column {error.colno}") from None
    except ValueError as error:  # an integer too long for int() (sys.set_int_max_str_digits)
        raise InvalidMessage(f"not JSON: {error}") from None
    except RecursionError:
        raise InvalidMessage("not JSON: nested too deeply") from None
    if not isinstance(raw, dict):
        raise InvalidMessage("not a JSON object")
    try:
        envelope = _Envelope.model_validate(raw)
    except ValidationError as error:
        raise InvalidMessage(_describe(error, "envelope")) from None
    if envelope.version != VERSION:
        raise InvalidMessage(f"unknown version {envelope.version} (this code reads {VERSION})")
    model = PAYLOADS.get(envelope.type)
    if model is None:
        raise InvalidMessage(f"unknown type {envelope.type!r}")
    try:
        payload = model.model_validate(envelope.payload)
    except ValidationError as error:
        raise InvalidMessage(_describe(error, f"{envelope.type} payload")) from None
    return Message(
        message_id=envelope.message_id,
        occurred_at=envelope.occurred_at,
        correlation_id=envelope.correlation_id,
        causation_id=envelope.causation_id,
        payload=cast(AnyPayload, payload),
    )
