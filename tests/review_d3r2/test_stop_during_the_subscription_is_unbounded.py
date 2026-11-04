"""Defect: `Consumer.stop_bound` is not a bound: a stop set while the consumer subscribes waits on unbounded RPCs.

consumer.py `stop_bound` (a913720) promises "the longest a stop can take once `stop` is set: the cancel, the handlers
in flight, the close", and test_a_stop_fits_inside_the_services_stop_timeout relies on it to keep a consumer's stop
inside service.STOP_TIMEOUT. But `_consume_until` runs `set_qos`, `get_queue` and `queue.consume` with no timeout, and
only checks `stop` after them; the open before them (bounded by publish_timeout) isn't counted either. A broker that
stops answering while a consumer (re)subscribes (the case a913720 bounds for the cancel and the close) holds the stop
until service._stop cancels the worker at STOP_TIMEOUT, cancelling it with whatever handlers the previous channel left
in flight against the pool the lifespan closes next.

Scenario: basic.qos is never answered (Channel.set_qos patched to wait forever, the model the existing cancel test
uses for basic.cancel), publish_timeout 0.3 s, handler_timeout 0.5 s, so stop_bound is 2.4 s. The stop is set while
the consumer waits on it.
Expected: `run` returns within stop_bound (plus a margin). Actual: it is still running 5 s later.
"""

from __future__ import annotations

import asyncio
import time

import aio_pika
import pytest
from test_consumer import Start, Toy, pool, short_topology, start  # noqa: F401  (fixtures)


async def test_a_stop_while_subscribing_ends_within_stop_bound(
    start: Start,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def never(*_: object, **__: object) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr(aio_pika.channel.Channel, "set_qos", never)
    # Adapted (day 3): the channel calls are bounded by control_timeout since the fix; stop_bound counts them all.
    run = await start(Toy(), publish_timeout=0.3, handler_timeout=0.5, control_timeout=0.3)
    bound = run.consumer.stop_bound
    assert bound < 5
    await asyncio.sleep(0.3)  # waiting on basic.qos
    run.stop.set()
    started = time.monotonic()
    done, _ = await asyncio.wait({run.task}, timeout=5)
    elapsed = time.monotonic() - started
    if not done:
        run.task.cancel()
        await asyncio.gather(run.task, return_exceptions=True)
    assert done, f"still running {elapsed:.1f} s after the stop; stop_bound is {bound} s"
