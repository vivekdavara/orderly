"""The order saga's transition function (DESIGN.md "The saga"): where an order goes on each payment reply or deadline.

It is pure and total: `transition(state, event)` has a defined answer for every order state and every input, and
touches nothing. The order service applies the `Step` it returns in one transaction (orderly.orders): the order's new
status, the stock released, the messages written to the outbox. Keeping the rules here, apart from the database and the
broker, is what lets the tests enumerate every (state, input) pair and run random sequences of replies through it.

The inputs are the payment service's three replies and the deadline passing (the sweeper's input). The rules:

- Awaiting payment: a success confirms; a decline cancels (stock released); the deadline cancels too (stock released)
  and asks for a refund, because no answer doesn't mean no charge.
- Confirmed is the pivot: nothing moves it again.
- Cancelled: a success that arrives late is refunded, once; nothing else changes anything.

Replies the payment service can't send in that state (a void or a refund before the order asked for a refund, a failure
after a success) are ignored and marked `anomaly`, so the consumer logs them: acting on a message that the protocol says
can't happen would let a bug elsewhere undo a confirmed order.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, TypeAlias

from orderly.messages import PaymentFailed, PaymentRefunded, PaymentSucceeded

Status = Literal["AWAITING_PAYMENT", "CONFIRMED", "CANCELLED"]
CancelReason = Literal["payment_declined", "payment_timeout"]
RefundReason = Literal["payment_timeout", "order_cancelled"]
Announcement = Literal["order.confirmed", "order.cancelled"]


@dataclass(frozen=True)
class OrderState:
    """What the transition needs from the order's row."""

    status: Status
    # payment.refund has been written for this order (orders.refund_requested).
    refund_requested: bool = False


@dataclass(frozen=True)
class DeadlinePassed:
    """The sweeper's input: the order's payment deadline is in the past."""


SagaInput: TypeAlias = PaymentSucceeded | PaymentFailed | PaymentRefunded | DeadlinePassed


@dataclass(frozen=True)
class Step:
    """What to do: the order's status afterwards and the effects that go with the change, all in one transaction."""

    # A name for the rule that applied, for logs and the tests' table.
    outcome: str
    status: Status
    # Set when this step cancels the order.
    cancel_reason: CancelReason | None = None
    # Give the order's stock back (only ever when an awaiting order is cancelled).
    release_stock: bool = False
    # Write payment.refund with this reason and set refund_requested.
    refund: RefundReason | None = None
    # The event to publish about the order, if its status changed.
    announce: Announcement | None = None
    # The protocol says this input can't arrive in this state: ignored, and worth a warning.
    anomaly: bool = False

    @property
    def changes_anything(self) -> bool:
        return self.release_stock or self.refund is not None or self.announce is not None


def _ignore(state: OrderState, outcome: str, *, anomaly: bool = False) -> Step:
    return Step(outcome=outcome, status=state.status, anomaly=anomaly)


def transition(state: OrderState, event: SagaInput) -> Step:
    """The step for `event` arriving while the order is in `state`. Never raises for a valid state and input."""
    if state.status == "AWAITING_PAYMENT":
        if isinstance(event, PaymentSucceeded):
            return Step("confirm", "CONFIRMED", announce="order.confirmed")
        if isinstance(event, PaymentFailed):
            if event.reason == "declined":
                return Step(
                    "cancel_declined",
                    "CANCELLED",
                    cancel_reason="payment_declined",
                    release_stock=True,
                    announce="order.cancelled",
                )
            # A void answers a refund that came before the charge, and this order hasn't asked for a refund. The
            # deadline still cancels it (and its refund is answered "refunded" again), so ignoring this is safe.
            return _ignore(state, "ignore_void_before_refund", anomaly=True)
        if isinstance(event, PaymentRefunded):
            return _ignore(state, "ignore_refund_before_request", anomaly=True)
        return Step(
            "cancel_timeout",
            "CANCELLED",
            cancel_reason="payment_timeout",
            release_stock=True,
            refund="payment_timeout",
            announce="order.cancelled",
        )
    if state.status == "CONFIRMED":
        if isinstance(event, PaymentSucceeded):
            return _ignore(state, "ignore_duplicate_success")
        if isinstance(event, DeadlinePassed):
            # A sweep that raced the confirmation: it read the order before the success committed.
            return _ignore(state, "ignore_deadline_after_confirm")
        # The pivot: a failure or a refund after a success means the payment service broke the protocol.
        return _ignore(state, "ignore_after_pivot", anomaly=True)
    # CANCELLED
    if isinstance(event, PaymentSucceeded):
        if state.refund_requested:
            # The refund already written (at the deadline, or for an earlier late success) covers this charge: the
            # payment service's charge and refund commute (DESIGN.md "The saga").
            return _ignore(state, "ignore_success_refund_already_requested")
        return Step("refund_late_success", "CANCELLED", refund="order_cancelled")
    if isinstance(event, PaymentRefunded):
        if state.refund_requested:
            return _ignore(state, "refund_confirmed")
        return _ignore(state, "ignore_refund_before_request", anomaly=True)
    if isinstance(event, PaymentFailed):
        return _ignore(state, "ignore_failure_after_cancel")
    return _ignore(state, "ignore_deadline_after_cancel")
