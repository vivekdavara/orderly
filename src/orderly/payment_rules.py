"""The payment service's rules (DESIGN.md "The saga"): one payment row per order, and charge and refund commute.

`decide(row, command)` says what a command does to the order's payment row: call the provider to charge, call it to
refund, record a void, or only answer again. `reply(row, command)` is the answer, computed from the row as it stands
after the command, so a repeated command gets the same answer as the first. Both are pure, like the saga's
transition function, and the payment service (orderly.payments) applies them in one transaction per command.

The table, by the order's payment row before the command:

| Row               | payment.charge                        | payment.refund                               |
|-------------------|---------------------------------------|----------------------------------------------|
| none              | charge: CHARGED or DECLINED           | void: VOIDED for the refund's amount         |
| CHARGED           | answer `succeeded` again              | refund through the provider: REFUNDED        |
| DECLINED          | answer `failed` (declined) again      | answer `refunded` 0: nothing was charged     |
| REFUNDED / VOIDED | answer `failed` (voided), never charge| answer `refunded` again                      |

So whichever of a charge and a refund arrives first (a retry or a dead-letter replay can reorder them), an order that
was refunded is never left charged. A command whose amount or currency disagrees with the row's is refused: one order
has one amount, and moving money on a message that contradicts the books is the one thing the rules won't guess about.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Literal

from orderly.messages import ChargePayment, PaymentFailed, PaymentRefunded, PaymentSucceeded, RefundPayment

PaymentStatus = Literal["CHARGED", "DECLINED", "REFUNDED", "VOIDED"]
Action = Literal["charge", "refund", "void", "answer"]
Command = ChargePayment | RefundPayment
Reply = PaymentSucceeded | PaymentFailed | PaymentRefunded


@dataclass(frozen=True)
class PaymentRow:
    """An order's payment, as the payments table holds it."""

    id: uuid.UUID
    order_id: uuid.UUID
    status: PaymentStatus
    amount_cents: int
    currency: str


class PaymentConflict(Exception):
    """The command contradicts the order's payment (another amount or currency). It is never retried."""


def decide(row: PaymentRow | None, command: Command) -> Action:
    """What `command` does, given the order's payment row (None: no payment yet)."""
    if row is not None and (row.amount_cents, row.currency) != (command.amount_cents, command.currency):
        raise PaymentConflict(
            f"order {command.order_id}'s payment is {row.amount_cents} {row.currency}; "
            f"{command.TYPE} is for {command.amount_cents} {command.currency}"
        )
    if row is None:
        return "charge" if isinstance(command, ChargePayment) else "void"
    if isinstance(command, RefundPayment) and row.status == "CHARGED":
        return "refund"
    return "answer"


def reply(row: PaymentRow, command: Command) -> Reply:
    """The answer to `command`, from the order's payment row after the command was applied."""
    if isinstance(command, ChargePayment):
        if row.status == "CHARGED":
            return PaymentSucceeded(
                order_id=row.order_id, payment_id=row.id, amount_cents=row.amount_cents, currency=row.currency
            )
        return PaymentFailed(order_id=row.order_id, reason="declined" if row.status == "DECLINED" else "voided")
    if row.status == "CHARGED":
        # decide() sends a refund of a charged payment to the provider, which leaves it REFUNDED.
        raise ValueError(f"order {row.order_id}: a refund's reply from a payment still CHARGED")
    refunded = row.amount_cents if row.status == "REFUNDED" else 0
    return PaymentRefunded(order_id=row.order_id, payment_id=row.id, amount_cents=refunded, currency=row.currency)
