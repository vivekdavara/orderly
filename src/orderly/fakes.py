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


@dataclass
class FakeProvider:
    """A payment provider: `latency` seconds per call, a share of orders declined and a share of calls failing.

    `declined_customers` are always declined (tests use it to pick an outcome without searching for an order id).
    Charges are idempotent by order id: charging an order again returns the first answer and charges nothing more.
    """

    latency: float = 0.0
    decline_ratio: float = 0.0
    error_ratio: float = 0.0
    declined_customers: frozenset[str] = frozenset()
    charges: dict[uuid.UUID, Charge] = field(default_factory=dict)
    # Calls per order, failed ones included: the attempt number that keys an error.
    attempts: Counter[uuid.UUID] = field(default_factory=Counter)

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

    def charged_cents(self) -> int:
        """What the provider has taken from customers in total: the approved charges."""
        return sum(charge.amount_cents for charge in self.charges.values() if charge.approved)


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
