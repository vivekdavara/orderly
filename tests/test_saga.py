"""The saga's transition function: every (state, input) pair, and random reply sequences through it."""

from __future__ import annotations

import uuid
from dataclasses import replace

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from orderly.messages import PaymentFailed, PaymentRefunded, PaymentSucceeded
from orderly.saga import DeadlinePassed, OrderState, SagaInput, Step, transition

ORDER = uuid.UUID("0b8f6f8e-1d1f-4f4e-8a3c-7f0f2b9d5e11")
PAYMENT = uuid.UUID("6f1c0c1e-6b8e-4c51-9d0e-2f1c4b0e7a10")

INPUTS: dict[str, SagaInput] = {
    "succeeded": PaymentSucceeded(order_id=ORDER, payment_id=PAYMENT, amount_cents=2599, currency="USD"),
    "declined": PaymentFailed(order_id=ORDER, reason="declined"),
    "voided": PaymentFailed(order_id=ORDER, reason="voided"),
    "refunded": PaymentRefunded(order_id=ORDER, payment_id=PAYMENT, amount_cents=2599, currency="USD"),
    "refunded_nothing": PaymentRefunded(order_id=ORDER, payment_id=PAYMENT, amount_cents=0, currency="USD"),
    "deadline": DeadlinePassed(),
}
STATES = {
    "awaiting": OrderState("AWAITING_PAYMENT"),
    "confirmed": OrderState("CONFIRMED"),
    "cancelled": OrderState("CANCELLED"),
    "cancelled_refund_requested": OrderState("CANCELLED", refund_requested=True),
}

# The whole table: (state, input) → (outcome, status afterwards, effects). Every pair is listed, so adding a state or
# an input without deciding its rules fails test_the_table_covers_every_pair.
C, R, A = "cancel", "refund", "announce"
TABLE: dict[tuple[str, str], tuple[str, str, set[str]]] = {
    ("awaiting", "succeeded"): ("confirm", "CONFIRMED", {A}),
    ("awaiting", "declined"): ("cancel_declined", "CANCELLED", {C, A}),
    ("awaiting", "voided"): ("ignore_void_before_refund", "AWAITING_PAYMENT", set()),
    ("awaiting", "refunded"): ("ignore_refund_before_request", "AWAITING_PAYMENT", set()),
    ("awaiting", "refunded_nothing"): ("ignore_refund_before_request", "AWAITING_PAYMENT", set()),
    ("awaiting", "deadline"): ("cancel_timeout", "CANCELLED", {C, R, A}),
    ("confirmed", "succeeded"): ("ignore_duplicate_success", "CONFIRMED", set()),
    ("confirmed", "declined"): ("ignore_after_pivot", "CONFIRMED", set()),
    ("confirmed", "voided"): ("ignore_after_pivot", "CONFIRMED", set()),
    ("confirmed", "refunded"): ("ignore_after_pivot", "CONFIRMED", set()),
    ("confirmed", "refunded_nothing"): ("ignore_after_pivot", "CONFIRMED", set()),
    ("confirmed", "deadline"): ("ignore_deadline_after_confirm", "CONFIRMED", set()),
    ("cancelled", "succeeded"): ("refund_late_success", "CANCELLED", {R}),
    ("cancelled", "declined"): ("ignore_failure_after_cancel", "CANCELLED", set()),
    ("cancelled", "voided"): ("ignore_failure_after_cancel", "CANCELLED", set()),
    ("cancelled", "refunded"): ("ignore_refund_before_request", "CANCELLED", set()),
    ("cancelled", "refunded_nothing"): ("ignore_refund_before_request", "CANCELLED", set()),
    ("cancelled", "deadline"): ("ignore_deadline_after_cancel", "CANCELLED", set()),
    ("cancelled_refund_requested", "succeeded"): ("ignore_success_refund_already_requested", "CANCELLED", set()),
    ("cancelled_refund_requested", "declined"): ("ignore_failure_after_cancel", "CANCELLED", set()),
    ("cancelled_refund_requested", "voided"): ("ignore_failure_after_cancel", "CANCELLED", set()),
    ("cancelled_refund_requested", "refunded"): ("refund_confirmed", "CANCELLED", set()),
    ("cancelled_refund_requested", "refunded_nothing"): ("refund_confirmed", "CANCELLED", set()),
    ("cancelled_refund_requested", "deadline"): ("ignore_deadline_after_cancel", "CANCELLED", set()),
}
# The pairs the payment service's protocol rules out: ignored, and logged as anomalies.
ANOMALIES = {
    ("awaiting", "voided"),
    ("awaiting", "refunded"),
    ("awaiting", "refunded_nothing"),
    ("confirmed", "declined"),
    ("confirmed", "voided"),
    ("confirmed", "refunded"),
    ("confirmed", "refunded_nothing"),
    ("cancelled", "refunded"),
    ("cancelled", "refunded_nothing"),
}


def effects(step: Step) -> set[str]:
    found = set()
    if step.release_stock:
        found.add(C)
    if step.refund is not None:
        found.add(R)
    if step.announce is not None:
        found.add(A)
    return found


def test_the_table_covers_every_pair() -> None:
    assert set(TABLE) == {(state, event) for state in STATES for event in INPUTS}


@pytest.mark.parametrize(("state", "event"), sorted(TABLE))
def test_every_state_and_input_has_its_step(state: str, event: str) -> None:
    outcome, status, expected = TABLE[(state, event)]
    step = transition(STATES[state], INPUTS[event])
    assert (step.outcome, step.status, effects(step)) == (outcome, status, expected)
    assert step.anomaly == ((state, event) in ANOMALIES)
    assert step.changes_anything == bool(expected)


def test_a_cancellation_says_why_and_announces_it() -> None:
    declined = transition(STATES["awaiting"], INPUTS["declined"])
    assert (declined.cancel_reason, declined.refund, declined.announce) == (
        "payment_declined",
        None,
        "order.cancelled",
    )
    timed_out = transition(STATES["awaiting"], INPUTS["deadline"])
    assert (timed_out.cancel_reason, timed_out.refund, timed_out.announce) == (
        "payment_timeout",
        "payment_timeout",
        "order.cancelled",
    )
    late = transition(STATES["cancelled"], INPUTS["succeeded"])
    assert (late.cancel_reason, late.refund, late.announce) == (None, "order_cancelled", None)
    assert transition(STATES["awaiting"], INPUTS["succeeded"]).announce == "order.confirmed"


@pytest.mark.parametrize(("state", "event"), sorted(TABLE))
def test_only_a_step_out_of_awaiting_changes_the_status_or_releases_stock(state: str, event: str) -> None:
    before = STATES[state]
    step = transition(before, INPUTS[event])
    if before.status != "AWAITING_PAYMENT":
        assert step.status == before.status
        assert not step.release_stock and step.announce is None and step.cancel_reason is None
    assert step.release_stock == (step.status == "CANCELLED" and before.status == "AWAITING_PAYMENT")
    assert (step.cancel_reason is not None) == step.release_stock


# Random reply sequences, as retries, duplicates and dead-letter replays can deliver them: any order, any repeats.
events = st.sampled_from(sorted(INPUTS))


def run(sequence: list[str]) -> tuple[OrderState, list[Step]]:
    state = STATES["awaiting"]
    steps = []
    for name in sequence:
        step = transition(state, INPUTS[name])
        steps.append(step)
        state = OrderState(step.status, state.refund_requested or step.refund is not None)
    return state, steps


@settings(max_examples=500)
@given(st.lists(events, max_size=12))
def test_any_sequence_releases_announces_and_refunds_at_most_once(sequence: list[str]) -> None:
    final, steps = run(sequence)
    assert sum(step.release_stock for step in steps) <= 1
    assert sum(step.announce is not None for step in steps) <= 1
    assert sum(step.refund is not None for step in steps) <= 1
    # The announcement, if any, matches where the order ended.
    announced = [step.announce for step in steps if step.announce is not None]
    if announced:
        assert announced == [{"CONFIRMED": "order.confirmed", "CANCELLED": "order.cancelled"}[final.status]]


@settings(max_examples=500)
@given(st.lists(events, max_size=12))
def test_a_customer_charged_for_a_cancelled_order_gets_a_refund(sequence: list[str]) -> None:
    """The saga's safety property: an order that ends cancelled after any success asked for a refund, and a confirmed
    order never did."""
    final, steps = run(sequence)
    refunded = any(step.refund is not None for step in steps)
    if final.status == "CANCELLED" and "succeeded" in sequence:
        assert refunded
    if final.status == "CONFIRMED":
        assert not refunded
    # A success before anything else confirms, and the order stays confirmed whatever follows.
    if sequence[:1] == ["succeeded"]:
        assert final.status == "CONFIRMED"


@given(st.lists(events, max_size=12), events)
def test_a_duplicate_of_the_last_input_changes_nothing(sequence: list[str], last: str) -> None:
    """Redelivery under a new message id (a charge answered twice) must not do anything twice."""
    final, _ = run([*sequence, last])
    again = transition(final, INPUTS[last])
    assert not again.changes_anything
    assert again.status == final.status


def test_states_are_values() -> None:
    state = STATES["cancelled"]
    assert replace(state, refund_requested=True) == STATES["cancelled_refund_requested"]
