"""The fakes standing in for the outside world: a payment provider and an e-mail sender (DESIGN.md "The domain").

Both are deterministic, so tests and the load test (day 4) are repeatable: whether a call is declined or fails is a
function of the order id, the attempt number and the fake's settings, never of a random generator's state. A failure
is keyed by the attempt, so a retry of a call that failed can succeed (a transient error), while a decline is keyed by
the order alone (asking again gets the same answer).

State is in memory: restarting a process forgets its charges, like resetting a provider's sandbox.
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import uuid
from collections import Counter
from dataclasses import dataclass, field


def fraction(order_id: uuid.UUID, salt: str) -> float:
    """A number in [0, 1) fixed by the order id and the salt: the fakes' coin."""
    digest = hashlib.sha256(salt.encode() + order_id.bytes).digest()
    return int.from_bytes(digest[:8], "big") / 2**64


class ProviderUnavailable(Exception):
    """The provider didn't answer. The fake raises it before charging, so nothing was charged; a real provider's
    timeout leaves that unknown, which is why charges carry the order id as their idempotency key."""


class ProviderConflict(Exception):
    """A charge reused an order id (the idempotency key) with a different amount or currency."""


@dataclass(frozen=True)
class Charge:
    order_id: uuid.UUID
    amount_cents: int
    currency: str
    approved: bool
    # The provider's id for an approved charge; None when declined.
    reference: str | None


@dataclass(frozen=True)
class Refund:
    order_id: uuid.UUID
    amount_cents: int
    currency: str
    # The provider's id for the refund.
    reference: str


@dataclass
class FakeProvider:
    """A payment provider: `latency` seconds per call, a share of orders declined and a share of calls failing.

    `declined_customers` are always declined (tests use it to pick an outcome without searching for an order id).
    Charges are idempotent by order id: charging an order again returns the first answer and charges nothing more.
    Refunds are too: an order's charge is refunded whole, once, however often it is asked.
    """

    latency: float = 0.0
    decline_ratio: float = 0.0
    error_ratio: float = 0.0
    declined_customers: frozenset[str] = frozenset()
    charges: dict[uuid.UUID, Charge] = field(default_factory=dict)
    # Calls per order, failed ones included: the attempt number that keys an error.
    attempts: Counter[uuid.UUID] = field(default_factory=Counter)
    refunds: dict[uuid.UUID, Refund] = field(default_factory=dict)
    refund_attempts: Counter[uuid.UUID] = field(default_factory=Counter)

    async def charge(self, order_id: uuid.UUID, customer_id: str, amount_cents: int, currency: str) -> Charge:
        await asyncio.sleep(self.latency)
        self.attempts[order_id] += 1
        if fraction(order_id, f"error:{self.attempts[order_id]}") < self.error_ratio:
            raise ProviderUnavailable(f"charge for order {order_id} failed (attempt {self.attempts[order_id]})")
        previous = self.charges.get(order_id)
        if previous is not None:
            if (previous.amount_cents, previous.currency) != (amount_cents, currency):
                raise ProviderConflict(
                    f"order {order_id} was charged {previous.amount_cents} {previous.currency}, "
                    f"now asked for {amount_cents} {currency}"
                )
            return previous
        declined = customer_id in self.declined_customers or fraction(order_id, "decline") < self.decline_ratio
        charge = Charge(
            order_id=order_id,
            amount_cents=amount_cents,
            currency=currency,
            approved=not declined,
            reference=None if declined else f"ch_{order_id.hex[:24]}",
        )
        self.charges[order_id] = charge
        return charge

    async def refund(self, order_id: uuid.UUID, amount_cents: int, currency: str) -> Refund:
        """Give back an order's approved charge, which must be for exactly this amount. Idempotent by order id."""
        await asyncio.sleep(self.latency)
        self.refund_attempts[order_id] += 1
        if fraction(order_id, f"refund-error:{self.refund_attempts[order_id]}") < self.error_ratio:
            raise ProviderUnavailable(f"refund for order {order_id} failed (attempt {self.refund_attempts[order_id]})")
        charge = self.charges.get(order_id)
        if charge is None or not charge.approved:
            raise ProviderConflict(f"order {order_id} has no approved charge to refund")
        if (charge.amount_cents, charge.currency) != (amount_cents, currency):
            raise ProviderConflict(
                f"order {order_id} was charged {charge.amount_cents} {charge.currency}, "
                f"asked to refund {amount_cents} {currency}"
            )
        previous = self.refunds.get(order_id)
        if previous is not None:
            return previous
        refund = Refund(order_id, amount_cents, currency, reference=f"re_{order_id.hex[:24]}")
        self.refunds[order_id] = refund
        return refund

    def charged_cents(self) -> int:
        """What the provider has taken from customers in total: the approved charges, refunded or not."""
        return sum(charge.amount_cents for charge in self.charges.values() if charge.approved)

    def net_cents(self, order_id: uuid.UUID | None = None) -> int:
        """What customers are out of pocket, for one order or all: approved charges less their refunds."""
        charges = [c for c in self.charges.values() if c.approved and order_id in (None, c.order_id)]
        refunds = [r for r in self.refunds.values() if order_id in (None, r.order_id)]
        return sum(c.amount_cents for c in charges) - sum(r.amount_cents for r in refunds)


class MailerUnavailable(Exception):
    """The e-mail wasn't sent."""


@dataclass(frozen=True)
class Email:
    recipient: str
    subject: str
    body: str


@dataclass
class FakeMailer:
    """An e-mail sender: `latency` seconds per message, a share of attempts failing (keyed by order and attempt), or
    every attempt failing while `down` is set."""

    latency: float = 0.0
    failure_ratio: float = 0.0
    down: bool = False
    sent: list[Email] = field(default_factory=list)
    attempts: Counter[uuid.UUID] = field(default_factory=Counter)

    async def send(self, order_id: uuid.UUID, email: Email) -> None:
        await asyncio.sleep(self.latency)
        self.attempts[order_id] += 1
        if self.down or fraction(order_id, f"mail:{self.attempts[order_id]}") < self.failure_ratio:
            raise MailerUnavailable(f"could not send {email.subject!r} to {email.recipient}")
        self.sent.append(email)


def provider_from_environment() -> FakeProvider:
    """ORDERLY_PROVIDER_LATENCY_MS, ORDERLY_PROVIDER_DECLINE_RATIO and ORDERLY_PROVIDER_ERROR_RATIO (defaults 0)."""
    return FakeProvider(
        latency=float(os.environ.get("ORDERLY_PROVIDER_LATENCY_MS", "0")) / 1000,
        decline_ratio=float(os.environ.get("ORDERLY_PROVIDER_DECLINE_RATIO", "0")),
        error_ratio=float(os.environ.get("ORDERLY_PROVIDER_ERROR_RATIO", "0")),
    )
