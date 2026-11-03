"""The fake provider and mailer, and the e-mails: deterministic, idempotent where a real provider is."""

from __future__ import annotations

import time
import uuid

import pytest

from orderly import emails
from orderly.fakes import (
    Email,
    FakeMailer,
    FakeProvider,
    MailerUnavailable,
    ProviderConflict,
    ProviderUnavailable,
    fraction,
)

ORDERS = [uuid.UUID(int=n) for n in range(2000)]


def test_fraction_is_fixed_by_the_order_and_the_salt_and_spread_over_0_1() -> None:
    values = [fraction(order, "decline") for order in ORDERS]
    assert values == [fraction(order, "decline") for order in ORDERS]
    assert values != [fraction(order, "error:1") for order in ORDERS]
    assert all(0 <= value < 1 for value in values)
    # Spread: each tenth of [0, 1) holds 200 +- 60 of the 2,000.
    tenths = [sum(1 for value in values if k / 10 <= value < (k + 1) / 10) for k in range(10)]
    assert all(140 <= count <= 260 for count in tenths), tenths


async def test_a_charge_is_approved_with_a_reference_and_counted() -> None:
    provider = FakeProvider()
    charge = await provider.charge(ORDERS[1], "c-1", 2599, "USD")
    assert charge.approved and charge.reference == f"ch_{ORDERS[1].hex[:24]}"
    assert provider.charged_cents() == 2599


async def test_charging_an_order_again_returns_the_first_answer_and_charges_nothing_more() -> None:
    provider = FakeProvider()
    first = await provider.charge(ORDERS[1], "c-1", 2599, "USD")
    assert await provider.charge(ORDERS[1], "c-1", 2599, "USD") == first
    assert provider.charged_cents() == 2599
    with pytest.raises(ProviderConflict, match="was charged 2599 USD, now asked for 2600 USD"):
        await provider.charge(ORDERS[1], "c-1", 2600, "USD")


async def test_declined_customers_are_always_declined() -> None:
    provider = FakeProvider(declined_customers=frozenset({"c-broke"}))
    charge = await provider.charge(ORDERS[1], "c-broke", 100, "USD")
    assert not charge.approved and charge.reference is None
    assert provider.charged_cents() == 0


async def test_the_decline_ratio_declines_that_share_of_orders_and_the_same_ones_every_time() -> None:
    first, second = FakeProvider(decline_ratio=0.25), FakeProvider(decline_ratio=0.25)
    outcomes = [(await first.charge(order, "c", 100, "USD")).approved for order in ORDERS]
    assert outcomes == [(await second.charge(order, "c", 100, "USD")).approved for order in ORDERS]
    declined = outcomes.count(False)
    assert 400 <= declined <= 600, declined  # 25% of 2,000 = 500


async def test_errors_are_per_attempt_so_a_retry_can_succeed() -> None:
    provider = FakeProvider(error_ratio=0.5)
    failed_first = []
    for order in ORDERS[:200]:
        try:
            await provider.charge(order, "c", 100, "USD")
        except ProviderUnavailable:
            failed_first.append(order)
    assert 60 <= len(failed_first) <= 140
    # Retrying each until it answers: every order gets charged exactly once, after a few attempts at most.
    for order in failed_first:
        for _ in range(30):
            try:
                await provider.charge(order, "c", 100, "USD")
                break
            except ProviderUnavailable:
                pass
    assert len(provider.charges) == 200
    assert provider.charged_cents() == 200 * 100


async def test_a_failed_call_charges_nothing() -> None:
    provider = FakeProvider(error_ratio=1.0)
    with pytest.raises(ProviderUnavailable, match="attempt 1"):
        await provider.charge(ORDERS[1], "c", 100, "USD")
    assert provider.charges == {}


async def test_latency_is_paid_on_every_call() -> None:
    provider = FakeProvider(latency=0.05)
    started = time.perf_counter()
    await provider.charge(ORDERS[1], "c", 100, "USD")
    await provider.charge(ORDERS[1], "c", 100, "USD")
    assert time.perf_counter() - started >= 0.1


async def test_the_mailer_sends_and_fails_while_down() -> None:
    mailer = FakeMailer()
    email = Email("a@example.com", "Hi", "Body")
    await mailer.send(ORDERS[1], email)
    mailer.down = True
    with pytest.raises(MailerUnavailable, match=r"could not send 'Hi' to a@example\.com"):
        await mailer.send(ORDERS[2], email)
    assert mailer.sent == [email]


async def test_the_mailers_failures_are_per_attempt() -> None:
    mailer = FakeMailer(failure_ratio=0.5)
    email = Email("a@example.com", "Hi", "Body")
    outcomes = []
    for order in ORDERS[:200]:
        try:
            await mailer.send(order, email)
            outcomes.append(True)
        except MailerUnavailable:
            outcomes.append(False)
    assert 60 <= outcomes.count(False) <= 140


@pytest.mark.parametrize(
    ("cents", "text"),
    [(0, "0.00 USD"), (5, "0.05 USD"), (100, "1.00 USD"), (2599, "25.99 USD"), (10**12, "10000000000.00 USD")],
)
def test_money_is_formatted_from_integer_cents(cents: int, text: str) -> None:
    assert emails.money(cents, "USD") == text


def test_money_refuses_a_negative_amount() -> None:
    with pytest.raises(ValueError, match="negative amount -1"):
        emails.money(-1, "USD")


def test_the_two_emails() -> None:
    order = ORDERS[7]
    confirmed = emails.confirmation(order, "a@example.com", 2599, "USD")
    assert confirmed.recipient == "a@example.com"
    assert confirmed.subject == f"Order {order} confirmed"
    assert "charged 25.99 USD" in confirmed.body
    cancelled = emails.cancellation(order, "a@example.com", "payment_timeout")
    assert cancelled.subject == f"Order {order} cancelled"
    assert cancelled.body.endswith("could not confirm your payment in time. Any charge for it will be refunded.")
    declined = emails.cancellation(order, "a@example.com", "payment_declined")
    assert declined.body.endswith("your payment was declined. You have not been charged.")
