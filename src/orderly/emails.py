"""What the customer is told: the confirmation and cancellation e-mails, composed from what the events carry.

The monolith composes them from its own tables; the notification service (day 3) from `order.confirmed` and
`order.cancelled`, which carry everything needed, so it never asks the order service.
"""

from __future__ import annotations

import uuid

from orderly.fakes import Email

# Why, and what it means for the customer's money. A timeout can't promise "not charged": the charge may have happened
# with its reply stuck, which is why the order service sends a refund with the cancellation (DESIGN.md "The saga").
REASONS = {
    "payment_declined": "your payment was declined. You have not been charged",
    "payment_timeout": "we could not confirm your payment in time. Any charge for it will be refunded",
}


def money(cents: int, currency: str) -> str:
    """Integer cents as `25.99 USD`, without a float anywhere."""
    if cents < 0:
        raise ValueError(f"negative amount {cents}")
    return f"{cents // 100}.{cents % 100:02d} {currency}"


def confirmation(order_id: uuid.UUID, recipient: str, total_cents: int, currency: str) -> Email:
    return Email(
        recipient=recipient,
        subject=f"Order {order_id} confirmed",
        body=f"Thank you. Your order {order_id} is confirmed and you have been charged {money(total_cents, currency)}.",
    )


def cancellation(order_id: uuid.UUID, recipient: str, reason: str) -> Email:
    return Email(
        recipient=recipient,
        subject=f"Order {order_id} cancelled",
        body=f"Your order {order_id} was cancelled: {REASONS[reason]}.",
    )
