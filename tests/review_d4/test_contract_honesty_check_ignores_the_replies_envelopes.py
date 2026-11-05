"""Day-4 review: the contract tests' "ignores every field its contract doesn't list" check can't see a consumer that
reads an unlisted field into the envelope of a message it writes.

tests/test_contracts.py `consume()` reports a consumer's replies as `without_ids(m, order_id)`: the type and the
payload only, every id but the order's blanked. The replies' envelopes (occurred_at, causation_id, correlation_id) are
dropped. So a payment service that stamped its reply with the command's `occurred_at` (or took its causation_id from
the command's `causation_id`), both fields its contract leaves out, does something different when a producer changes
them, and `test_the_consumer_ignores_every_field_its_contract_doesnt_list` still passes: `match` would be dishonest
and the check that is there to catch it can't. Fix idea: include each reply's envelope (minus its own fresh
message_id) in what `consume()` returns, e.g. `(m.type, payload, m.causation_id, m.occurred_at)` with the incoming
message's message_id named rather than blanked.

This test makes that consumer (a wrapper around the real `payments.apply`) and asserts the check tells the two runs
apart; on e2c03bf the effects compare equal, so it fails.
"""

from __future__ import annotations

from typing import Any

import pytest
import test_contracts
from test_contracts import INTERACTIONS, consume, migrated, unlisted, with_value

from orderly import payments
from orderly.messages import Message, new_message

__all__ = ["migrated"]  # the module-scoped fixture: one migrated schema per consumer service

CHARGE = next(i for i in INTERACTIONS if (i.consumer, i.name) == ("payments", "charge an order"))


async def test_a_consumer_reading_an_unlisted_field_into_its_reply_passes_the_honesty_check(
    migrated: dict[str, str], monkeypatch: pytest.MonkeyPatch
) -> None:
    assert "occurred_at" in unlisted(CHARGE), "the charge's contract leaves occurred_at out"
    incoming: list[Message] = []
    stamps: list[Any] = []
    real_apply = payments.apply
    real_new_message = new_message  # what payments.apply calls

    def stamped_like_the_command(payload: Any, **kwargs: Any) -> Message:
        kwargs["occurred_at"] = incoming[-1].occurred_at  # reads the command's occurred_at: not in the contract
        stamps.append(kwargs["occurred_at"])
        return real_new_message(payload, **kwargs)

    async def apply(conn: Any, message: Message, provider: Any) -> None:
        incoming.append(message)
        try:
            await real_apply(conn, message, provider)
        finally:
            incoming.pop()

    monkeypatch.setattr(payments, "new_message", stamped_like_the_command)
    monkeypatch.setattr(payments, "apply", apply)

    url = migrated["payments"]
    baseline = await consume(CHARGE, CHARGE.message, url)
    variant = with_value(CHARGE.message, "occurred_at", test_contracts.ALTERNATES["occurred_at"](CHARGE))
    changed = await consume(CHARGE, variant, url)
    assert len(stamps) == 2 and stamps[0] != stamps[1], "the two replies' envelopes differ"
    # The consumer's reply differs between the two runs (its occurred_at is the command's), so the check must see it.
    assert changed != baseline, "the honesty check compared equal although the consumer read occurred_at"
