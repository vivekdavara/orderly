"""The payment service's rules: the table for every row and command, and that charge and refund commute."""

from __future__ import annotations

import uuid
from dataclasses import replace

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from orderly.messages import ChargePayment, PaymentFailed, PaymentRefunded, PaymentSucceeded, RefundPayment
from orderly.payment_rules import Command, PaymentConflict, PaymentRow, decide, reply

ORDER = uuid.UUID("0b8f6f8e-1d1f-4f4e-8a3c-7f0f2b9d5e11")
PAYMENT = uuid.UUID("6f1c0c1e-6b8e-4c51-9d0e-2f1c4b0e7a10")
CHARGE = ChargePayment(order_id=ORDER, customer_id="c-42", amount_cents=2599, currency="USD")
REFUND = RefundPayment(order_id=ORDER, amount_cents=2599, currency="USD", reason="payment_timeout")


def row(status: str) -> PaymentRow:
    return PaymentRow(PAYMENT, ORDER, status, 2599, "USD")  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("status", "command", "action", "answer"),
    [
        ("CHARGED", CHARGE, "answer", ("payment.succeeded", 2599)),
        ("DECLINED", CHARGE, "answer", ("payment.failed", "declined")),
        ("REFUNDED", CHARGE, "answer", ("payment.failed", "voided")),
        ("VOIDED", CHARGE, "answer", ("payment.failed", "voided")),
        ("CHARGED", REFUND, "refund", None),
        ("DECLINED", REFUND, "answer", ("payment.refunded", 0)),
        ("REFUNDED", REFUND, "answer", ("payment.refunded", 2599)),
        ("VOIDED", REFUND, "answer", ("payment.refunded", 0)),
    ],
)
def test_the_table_for_an_existing_payment(
    status: str, command: Command, action: str, answer: tuple[str, object] | None
) -> None:
    assert decide(row(status), command) == action
    if answer is None:
        return
    response = reply(row(status), command)
    detail = response.reason if isinstance(response, PaymentFailed) else response.amount_cents
    assert (response.TYPE, detail) == answer
    assert response.order_id == ORDER
    if isinstance(response, PaymentSucceeded | PaymentRefunded):
        assert response.payment_id == PAYMENT and response.currency == "USD"


def test_with_no_payment_a_charge_charges_and_a_refund_voids() -> None:
    assert decide(None, CHARGE) == "charge"
    assert decide(None, REFUND) == "void"


def test_a_refunds_reply_from_a_charged_payment_is_a_bug() -> None:
    with pytest.raises(ValueError, match="still CHARGED"):
        reply(row("CHARGED"), REFUND)


@pytest.mark.parametrize("status", [None, "CHARGED", "DECLINED", "REFUNDED", "VOIDED"])
@pytest.mark.parametrize(
    "command",
    [
        ChargePayment(order_id=ORDER, customer_id="c-42", amount_cents=2600, currency="USD"),
        ChargePayment(order_id=ORDER, customer_id="c-42", amount_cents=2599, currency="EUR"),
        RefundPayment(order_id=ORDER, amount_cents=1, currency="USD", reason="order_cancelled"),
    ],
)
def test_a_command_that_contradicts_the_payment_is_refused(status: str | None, command: Command) -> None:
    if status is None:
        assert decide(None, command) in ("charge", "void")  # nothing to contradict yet
        return
    with pytest.raises(PaymentConflict, match="payment is 2599 USD"):
        decide(row(status), command)


def simulate(commands: list[str], approve: bool) -> tuple[PaymentRow | None, int, list[str]]:
    """The payment service on one order, with a provider that approves or declines: the row, the money taken (net of
    refunds), and each reply's type and detail."""
    current: PaymentRow | None = None
    money = 0
    answers = []
    for name in commands:
        command = CHARGE if name == "charge" else REFUND
        action = decide(current, command)
        if action == "charge":
            current = row("CHARGED" if approve else "DECLINED")
            money += 2599 if approve else 0
        elif action == "void":
            current = row("VOIDED")
        elif action == "refund":
            assert current is not None
            current = replace(current, status="REFUNDED")
            money -= 2599
        assert current is not None
        response = reply(current, command)
        answers.append(f"{response.TYPE}:{getattr(response, 'reason', getattr(response, 'amount_cents', ''))}")
        assert 0 <= money <= 2599, "charged twice, or refunded more than was charged"
    return current, money, answers


@settings(max_examples=500)
@given(st.lists(st.sampled_from(["charge", "refund"]), min_size=1, max_size=10), st.booleans())
def test_charge_and_refund_commute(commands: list[str], approve: bool) -> None:
    """Any order of charges and refunds, any repeats: a refunded order ends with nothing charged, and an order never
    refunded is charged once if the provider approves."""
    final, money, _ = simulate(commands, approve)
    assert final is not None
    if "refund" in commands:
        assert money == 0
        assert final.status in ("REFUNDED", "VOIDED", "DECLINED")
    else:
        assert money == (2599 if approve else 0)
        assert final.status == ("CHARGED" if approve else "DECLINED")


@given(st.lists(st.sampled_from(["charge", "refund"]), min_size=1, max_size=10), st.booleans())
def test_a_repeated_command_gets_the_same_answer(commands: list[str], approve: bool) -> None:
    """A redelivered command (a retry, a replay) is answered as it was the first time when nothing came between."""
    _, _, answers = simulate([*commands, commands[-1]], approve)
    assert answers[-1] == answers[-2]
