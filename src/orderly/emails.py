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


# ISO 4217 currencies whose minor unit isn't a hundredth (amounts are integers of the minor unit, "cents" in the code).
# Every other currency has two decimals. The day-2 review noted 500 JPY read "5.00 JPY".
MINOR_UNITS = {
    **dict.fromkeys(
        (
            "BIF",
            "CLP",
            "DJF",
            "GNF",
            "ISK",
            "JPY",
            "KMF",
            "KRW",
            "PYG",
            "RWF",
            "UGX",
            "VND",
            "VUV",
            "XAF",
            "XOF",
            "XPF",
        ),
        0,
    ),
    **dict.fromkeys(("BHD", "IQD", "JOD", "KWD", "LYD", "OMR", "TND"), 3),
}


def money(cents: int, currency: str) -> str:
    """An integer amount of the currency's minor unit as `25.99 USD` (`500 JPY`, `1.250 KWD`), without a float."""
    if cents < 0:
        raise ValueError(f"negative amount {cents}")
    digits = MINOR_UNITS.get(currency, 2)
    if digits == 0:
        return f"{cents} {currency}"
    whole, fraction = divmod(cents, 10**digits)
    return f"{whole}.{fraction:0{digits}d} {currency}"


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
