"""Defect: a stop whose basic.cancel times out returns while handlers it never waited for are still running.

consumer.py `_consume_until` (a913720) bounds the stop's `queue.cancel(tag)` by `publish_timeout` and suppresses the
TimeoutError. But aiormq forgets a consumer only when Basic.CancelOk arrives (aiormq/channel.py `basic_cancel`, and the
CancelOk handler), so after the timeout the subscription is still live: the broker keeps delivering (up to `prefetch`
unacknowledged, refilled as handlers acknowledge) and each delivery starts a new handler task. `_drain` waits only for
the snapshot `set(self._inflight)` it takes when it starts, so a delivery that arrives during the drain is not waited
for; `channel.close()` then closes the channel under it and `run` returns while it runs. In a service, `_stop` then
closes the pool (service.py lifespan) under that handler, and one that commits acknowledges on a closed channel: the
very "cut off between its commit and its acknowledgement" the stop exists to prevent (DESIGN.md, consumer docstring).

Scenario: cancel never answered (the same model as tests/test_consumer.py's
test_a_broker_that_never_answers_the_cancel_does_not_hold_the_stop), publish_timeout 0.3 s, handlers stall 1 s.
Message A is in flight when the stop is set; message B is published while the stop drains A.
Expected: when `run` returns no handler of this consumer is still running, and B is either not handled or handled and
acknowledged (not left committed with its delivery back in the queue).
Actual: `run` returns with B's handler in flight; B commits after the close and its delivery is redelivered.
"""

from __future__ import annotations

import asyncio

import aio_pika
import pytest
from aio_pika.abc import AbstractChannel
from test_consumer import (  # noqa: F401  (fixtures)
    Start,
    Toy,
    charge,
    effects,
    pool,
    queue_depth,
    short_topology,
    start,
)

from orderly.broker import publish
from orderly.topology import Topology


async def test_a_stop_after_a_timed_out_cancel_waits_for_every_handler_it_started(
    start: Start,  # noqa: F811
    channel: AbstractChannel,
    short_topology: Topology,  # noqa: F811
    fresh_database: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def never(*_: object, **__: object) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(aio_pika.queue.Queue, "cancel", never)
    toy = Toy(stall=1.0)
    # Adapted (day 3): the cancel is bounded by control_timeout since the fix, publish_timeout before it.
    run = await start(toy, control_timeout=0.3)
    first, second = charge(), charge()
    await publish(channel, short_topology, first)
    await run.until(lambda: toy.calls[first.message_id] == 1)
    run.stop.set()
    await asyncio.sleep(0.5)  # past the 0.3 s cancel timeout: the drain is waiting for `first`
    await publish(channel, short_topology, second)
    await asyncio.wait_for(asyncio.shield(run.task), timeout=10)
    still_running = set(run.consumer._inflight)
    # Let anything left behind finish (bounded), then look at what it did.
    if still_running:
        await asyncio.wait(still_running, timeout=5)
    await asyncio.sleep(0.3)
    handled_second = any(row[0] == second.message_id for row in effects(fresh_database))
    back_in_queue = [m.message_id for m in await queue_depth(channel, short_topology.queue("payment.commands"))]
    assert not still_running, f"run returned with {len(still_running)} handler(s) still running"
    assert not (handled_second and str(second.message_id) in back_in_queue), (
        "the second message was committed but its delivery is back in the queue (cut off before its ack)"
    )
