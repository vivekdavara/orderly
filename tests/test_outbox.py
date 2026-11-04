"""The outbox and its relay against a real PostgreSQL and RabbitMQ (DESIGN.md "The transactional outbox")."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from datetime import timedelta
from typing import Any

import psycopg
import psycopg.conninfo
import pytest
from aio_pika.abc import AbstractChannel, AbstractConnection
from psycopg_pool import AsyncConnectionPool

from orderly import broker, outbox
from orderly.messages import ChargePayment, Message, OrderConfirmed, encode, new_message
from orderly.migrate import migrate
from orderly.outbox import Relay, RelayError
from orderly.topology import Topology


def charge(n: int = 0) -> Message:
    order = uuid.uuid4()
    return new_message(ChargePayment(order_id=order, customer_id=f"c-{n}", amount_cents=100 + n, currency="USD"))


def confirmed() -> Message:
    order = uuid.uuid4()
    payload = OrderConfirmed(order_id=order, customer_id="c-1", email="a@example.com", total_cents=1, currency="USD")
    return new_message(payload)


@pytest.fixture
async def pool(fresh_database: str) -> AsyncIterator[AsyncConnectionPool]:
    with psycopg.connect(fresh_database, autocommit=True) as conn:
        migrate(conn, "orders")
    async with AsyncConnectionPool(fresh_database, min_size=1, max_size=4, open=False) as pool:
        yield pool


async def write_all(pool: AsyncConnectionPool, messages: list[Message]) -> None:
    async with pool.connection() as conn, conn.transaction():
        for message in messages:
            await outbox.write(conn, message)


async def unpublished(pool: AsyncConnectionPool) -> int:
    async with pool.connection() as conn:
        row = await (await conn.execute("SELECT count(*) FROM outbox WHERE published_at IS NULL")).fetchone()
    assert row is not None
    return int(row[0])


async def drain(channel: AbstractChannel, queue: str, expected: int, timeout: float = 10.0) -> list[bytes]:  # noqa: ASYNC109
    """Consume exactly `expected` bodies from a queue (acknowledging them), then check nothing more arrives."""
    declared = await channel.get_queue(queue, ensure=False)
    bodies: list[bytes] = []
    deadline = time.monotonic() + timeout
    while len(bodies) < expected:
        incoming = await declared.get(fail=False, no_ack=True)
        if incoming is None:
            assert time.monotonic() < deadline, f"{len(bodies)} of {expected} after {timeout} s"
            await asyncio.sleep(0.01)
            continue
        bodies.append(incoming.body)
    await asyncio.sleep(0.05)
    assert await declared.get(fail=False, no_ack=True) is None, "more messages than expected"
    return bodies


def relay(pool: AsyncConnectionPool, connection: AbstractConnection, topology: Topology, **options: Any) -> Relay:
    return Relay(pool=pool, connection=connection, topology=topology, **options)


async def test_write_adds_the_canonical_bytes_with_the_type(pool: AsyncConnectionPool) -> None:
    message = charge()
    await write_all(pool, [message])
    async with pool.connection() as conn:
        rows = await (await conn.execute("SELECT message_id, message_type, body, published_at FROM outbox")).fetchall()
    assert rows == [(message.message_id, "payment.charge", encode(message), None)]


async def test_the_notify_is_sent_on_commit_and_not_on_rollback(fresh_database: str, pool: AsyncConnectionPool) -> None:
    async with await psycopg.AsyncConnection.connect(fresh_database, autocommit=True) as listener:
        await listener.execute(f"LISTEN {outbox.CHANNEL}")
        row = await (await listener.execute("SELECT current_schema()")).fetchone()
        assert row is not None and row[0].startswith("t_")
        schema = row[0]
        with pytest.raises(ZeroDivisionError):
            async with pool.connection() as conn, conn.transaction():
                await outbox.write(conn, charge())
                raise ZeroDivisionError
        # Only this schema's: other tests (or a review running beside this suite) share the database and the channel.
        assert [n async for n in listener.notifies(timeout=0.3) if n.payload == schema] == []
        await write_all(pool, [charge(), charge()])
        received = [n async for n in listener.notifies(timeout=0.3) if n.payload == schema]
    # Two writes in one transaction: PostgreSQL folds identical notifications into one. The payload names the schema.
    assert [(n.channel, n.payload) for n in received] == [(outbox.CHANNEL, schema)]


async def test_a_batch_is_published_in_id_order_and_marked(
    pool: AsyncConnectionPool, connection: AbstractConnection, channel: AbstractChannel, topology: Topology
) -> None:
    messages = [charge(n) for n in range(50)]
    await write_all(pool, messages)
    assert await relay(pool, connection, topology).publish_batch() == 50
    assert await drain(channel, topology.queue("payment.commands"), 50) == [encode(m) for m in messages]
    assert await unpublished(pool) == 0
    # Nothing is left to publish, and nothing is sent again.
    assert await relay(pool, connection, topology).publish_batch() == 0
    await drain(channel, topology.queue("payment.commands"), 0)


async def test_batches_are_at_most_batch_size(
    pool: AsyncConnectionPool, connection: AbstractConnection, channel: AbstractChannel, topology: Topology
) -> None:
    await write_all(pool, [charge(n) for n in range(25)])
    small = relay(pool, connection, topology, batch_size=10)
    assert [await small.publish_batch() for _ in range(4)] == [10, 10, 5, 0]
    await drain(channel, topology.queue("payment.commands"), 25)


async def test_each_type_goes_where_the_topology_routes_it(
    pool: AsyncConnectionPool, connection: AbstractConnection, channel: AbstractChannel, topology: Topology
) -> None:
    command, event = charge(), confirmed()
    await write_all(pool, [command, event])
    assert await relay(pool, connection, topology).publish_batch() == 2
    assert await drain(channel, topology.queue("payment.commands"), 1) == [encode(command)]
    assert await drain(channel, topology.queue("notification.order-events"), 1) == [encode(event)]


async def test_two_relays_publish_every_row_exactly_once(
    pool: AsyncConnectionPool, connection: AbstractConnection, channel: AbstractChannel, topology: Topology
) -> None:
    messages = [charge(n) for n in range(600)]
    await write_all(pool, messages)
    first, second = (relay(pool, connection, topology, batch_size=50) for _ in range(2))

    async def until_empty(r: Relay) -> int:
        total = 0
        while (n := await r.publish_batch()) or await unpublished(pool):
            total += n
        return total

    counts = await asyncio.gather(until_empty(first), until_empty(second))
    assert sum(counts) == 600 and all(counts), counts  # both did some of the work, together all of it once
    bodies = await drain(channel, topology.queue("payment.commands"), 600)
    assert sorted(bodies) == sorted(encode(m) for m in messages)


async def test_a_crash_before_the_mark_publishes_the_batch_again_with_the_same_ids(
    pool: AsyncConnectionPool,
    connection: AbstractConnection,
    channel: AbstractChannel,
    topology: Topology,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    messages = [charge(n) for n in range(5)]
    await write_all(pool, messages)
    crashing = relay(pool, connection, topology)
    real_execute = psycopg.AsyncConnection.execute

    async def crash_on_mark(self: psycopg.AsyncConnection[Any], query: Any, *args: Any, **kwargs: Any) -> Any:
        if str(query).startswith("UPDATE outbox SET published_at"):
            raise psycopg.OperationalError("server closed the connection unexpectedly")
        return await real_execute(self, query, *args, **kwargs)

    monkeypatch.setattr(psycopg.AsyncConnection, "execute", crash_on_mark)
    with pytest.raises(psycopg.OperationalError):
        await crashing.publish_batch()
    monkeypatch.undo()
    assert await unpublished(pool) == 5  # published, but the mark never committed
    assert await relay(pool, connection, topology).publish_batch() == 5
    bodies = await drain(channel, topology.queue("payment.commands"), 10)
    # At least once: every message twice, byte for byte (so with the same message_id, which consumers dedupe on).
    assert bodies == [encode(m) for m in messages] * 2


async def test_an_unroutable_row_stays_and_the_rest_of_its_batch_is_marked(
    pool: AsyncConnectionPool, connection: AbstractConnection, channel: AbstractChannel, topology: Topology
) -> None:
    commands_queue = await channel.get_queue(topology.queue("payment.commands"), ensure=False)
    await commands_queue.unbind(topology.commands, "payment.charge")  # a broker missing a binding
    stuck, event = charge(), confirmed()
    await write_all(pool, [stuck, event])
    with pytest.raises(RelayError, match=r"1 not published, first outbox row 1: PublishError: .*NO_ROUTE"):
        await relay(pool, connection, topology).publish_batch()
    assert await drain(channel, topology.queue("notification.order-events"), 1) == [encode(event)]
    assert await unpublished(pool) == 1
    async with pool.connection() as conn:
        row = await (
            await conn.execute("SELECT attempts, last_error, next_attempt_at - now() FROM outbox WHERE id = 1")
        ).fetchone()
    assert row is not None
    attempts, last_error, wait = row
    assert attempts == 1 and last_error.startswith("PublishError: ") and "NO_ROUTE" in last_error
    assert timedelta(seconds=0.4) < wait <= timedelta(seconds=0.5)
    # Once the binding is back, the first batch after the row's retry time publishes it; before it, none claims it
    # (not even 0.2 s before: a claim a little early would pass a test that only looked at once).
    await commands_queue.bind(topology.commands, "payment.charge")
    assert await relay(pool, connection, topology).publish_batch() == 0
    await asyncio.sleep(0.3)
    assert await relay(pool, connection, topology).publish_batch() == 0
    await asyncio.sleep(0.3)
    assert await relay(pool, connection, topology).publish_batch() == 1
    assert await drain(channel, topology.queue("payment.commands"), 1) == [encode(stuck)]


async def test_a_closed_channel_is_replaced(
    pool: AsyncConnectionPool, connection: AbstractConnection, channel: AbstractChannel, topology: Topology
) -> None:
    r = relay(pool, connection, topology)
    await write_all(pool, [charge()])
    assert await r.publish_batch() == 1
    assert r._channel is not None
    await r._channel.close()
    await write_all(pool, [charge()])
    assert await r.publish_batch() == 1
    await drain(channel, topology.queue("payment.commands"), 2)


async def wait_until_published(pool: AsyncConnectionPool, timeout: float) -> float:  # noqa: ASYNC109
    started = time.perf_counter()
    while await unpublished(pool):
        assert time.perf_counter() - started < timeout, f"not published within {timeout} s"
        await asyncio.sleep(0.005)
    return time.perf_counter() - started


async def test_run_wakes_on_the_notify_long_before_its_poll(
    fresh_database: str,
    pool: AsyncConnectionPool,
    connection: AbstractConnection,
    channel: AbstractChannel,
    topology: Topology,
) -> None:
    stop = asyncio.Event()
    r = relay(pool, connection, topology, listen_url=fresh_database, poll_interval=30.0)
    task = asyncio.create_task(r.run(stop))
    try:
        await asyncio.sleep(0.3)  # listening, and asleep until a notification or 30 s
        latencies = []
        for _ in range(5):
            await write_all(pool, [charge()])
            latencies.append(await wait_until_published(pool, timeout=5.0))
        assert max(latencies) < 1.0, latencies
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5)
    await drain(channel, topology.queue("payment.commands"), 5)


async def test_run_without_a_listener_polls(
    pool: AsyncConnectionPool, connection: AbstractConnection, channel: AbstractChannel, topology: Topology
) -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(relay(pool, connection, topology, poll_interval=0.2).run(stop))
    try:
        await asyncio.sleep(0.05)
        await write_all(pool, [charge()])
        assert await wait_until_published(pool, timeout=5.0) < 1.0
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5)
    await drain(channel, topology.queue("payment.commands"), 1)


async def test_a_row_written_before_the_relay_starts_is_published_by_its_first_batch(
    fresh_database: str,
    pool: AsyncConnectionPool,
    connection: AbstractConnection,
    channel: AbstractChannel,
    topology: Topology,
) -> None:
    await write_all(pool, [charge()])  # its NOTIFY went to no one
    stop = asyncio.Event()
    task = asyncio.create_task(
        relay(pool, connection, topology, listen_url=fresh_database, poll_interval=30.0).run(stop)
    )
    try:
        assert await wait_until_published(pool, timeout=5.0) < 1.0
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5)
    await drain(channel, topology.queue("payment.commands"), 1)


async def test_a_stop_during_a_batch_lets_it_finish_and_mark(
    pool: AsyncConnectionPool,
    connection: AbstractConnection,
    channel: AbstractChannel,
    topology: Topology,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    started = asyncio.Event()
    real_publish = broker.publish

    async def slow_publish(*args: Any) -> None:
        started.set()
        await asyncio.sleep(0.3)
        await real_publish(*args)

    monkeypatch.setattr(broker, "publish", slow_publish)
    await write_all(pool, [charge(n) for n in range(10)])
    stop = asyncio.Event()
    task = asyncio.create_task(relay(pool, connection, topology, poll_interval=30.0).run(stop))
    await asyncio.wait_for(started.wait(), timeout=5)
    stop.set()  # mid-batch: published, not yet marked
    await asyncio.wait_for(task, timeout=5)
    assert await unpublished(pool) == 0
    await drain(channel, topology.queue("payment.commands"), 10)


async def test_run_backs_off_and_keeps_going_after_a_failed_batch(
    pool: AsyncConnectionPool, connection: AbstractConnection, channel: AbstractChannel, topology: Topology
) -> None:
    commands_queue = await channel.get_queue(topology.queue("payment.commands"), ensure=False)
    await commands_queue.unbind(topology.commands, "payment.charge")
    await write_all(pool, [charge()])
    stop = asyncio.Event()
    task = asyncio.create_task(relay(pool, connection, topology, poll_interval=0.1).run(stop))
    try:
        await asyncio.sleep(0.5)  # failing, backing off: still running
        assert not task.done() and await unpublished(pool) == 1
        await commands_queue.bind(topology.commands, "payment.charge")
        assert await wait_until_published(pool, timeout=10.0) < 10.0
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=10)
    await drain(channel, topology.queue("payment.commands"), 1)


async def test_a_cancelled_error_raised_by_the_broker_library_doesnt_end_the_relay(
    pool: AsyncConnectionPool,
    connection: AbstractConnection,
    channel: AbstractChannel,
    topology: Topology,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """aiormq raises CancelledError into an RPC's caller when the connection closes under it, though the caller's
    task wasn't cancelled. The relay treats that as a broker error and retries (it used to die of it)."""
    real_open = broker.open_channel
    calls = []

    async def open_once_cancelled(conn: AbstractConnection) -> AbstractChannel:
        calls.append(1)
        if len(calls) == 1:
            raise asyncio.CancelledError
        return await real_open(conn)

    monkeypatch.setattr(broker, "open_channel", open_once_cancelled)
    await write_all(pool, [charge()])
    stop = asyncio.Event()
    task = asyncio.create_task(relay(pool, connection, topology, poll_interval=0.1).run(stop))
    try:
        assert await wait_until_published(pool, timeout=5.0) < 5.0
        assert not task.done()
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5)
    assert len(calls) == 2
    await drain(channel, topology.queue("payment.commands"), 1)


async def test_a_real_cancel_still_ends_the_relay(
    pool: AsyncConnectionPool, connection: AbstractConnection, topology: Topology
) -> None:
    task = asyncio.create_task(relay(pool, connection, topology, poll_interval=30.0).run(asyncio.Event()))
    await asyncio.sleep(0.1)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(task, timeout=5)
    assert task.cancelled()


async def test_a_lost_listener_reconnects_and_a_row_written_meanwhile_goes_out_at_the_reconnect(
    fresh_database: str,
    test_database: str,
    pool: AsyncConnectionPool,
    connection: AbstractConnection,
    channel: AbstractChannel,
    topology: Topology,
) -> None:
    """The listener's connection ends (a database restart, an idle timeout): the relay reconnects it after 1 s and
    looks once, so a row written while it was down goes out then, not at the 30 s poll; and NOTIFY works again."""
    stop = asyncio.Event()
    r = relay(pool, connection, topology, listen_url=fresh_database, poll_interval=30.0)
    task = asyncio.create_task(r.run(stop))
    try:
        for _ in range(100):
            if r.listener_pid is not None:
                break
            await asyncio.sleep(0.02)
        first_pid = r.listener_pid
        assert first_pid is not None
        # Exactly the relay's own listener, by its pid (other tests share the server).
        with psycopg.connect(test_database, autocommit=True) as admin:
            assert admin.execute("SELECT pg_terminate_backend(%s)", (first_pid,)).fetchone() == (True,)
        await asyncio.sleep(0.1)
        await write_all(pool, [charge()])  # no one is listening now
        assert 0.5 < await wait_until_published(pool, timeout=5.0) < 3.0  # at the reconnect, about 1 s later
        assert r.listener_pid not in (None, first_pid)
        await write_all(pool, [charge()])
        assert await wait_until_published(pool, timeout=5.0) < 1.0  # by NOTIFY again
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5)
    await drain(channel, topology.queue("payment.commands"), 2)


async def test_a_failing_rows_retry_wait_doubles_from_half_a_second_to_a_minute(
    pool: AsyncConnectionPool, connection: AbstractConnection, channel: AbstractChannel, topology: Topology
) -> None:
    commands_queue = await channel.get_queue(topology.queue("payment.commands"), ensure=False)
    await commands_queue.unbind(topology.commands, "payment.charge")
    await write_all(pool, [charge()])
    r = relay(pool, connection, topology)
    waits = []
    attempts = 0
    for _ in range(10):
        with pytest.raises(RelayError):
            await r.publish_batch()
        async with pool.connection() as conn:
            row = await (
                await conn.execute("SELECT extract(epoch FROM next_attempt_at - now()), attempts FROM outbox")
            ).fetchone()
            assert row is not None
            waits.append(round(float(row[0]), 1))
            attempts = int(row[1])
            await conn.execute("UPDATE outbox SET next_attempt_at = now()")  # skip the wait
    assert waits == [0.5, 1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0, 60.0]
    assert attempts == 10


def test_last_error_holds_no_nul_and_at_most_1000_characters() -> None:
    assert outbox.describe(ValueError("bad\x00byte")) == "ValueError: bad\\0byte"
    long = outbox.describe(ValueError("é" * 2000))
    assert len(long) == 1000 and long.startswith("ValueError: éé")


async def test_another_schemas_writes_dont_wake_the_relay(
    fresh_database: str,
    test_database: str,
    pool: AsyncConnectionPool,
    connection: AbstractConnection,
    topology: Topology,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A relay counts its batches; writes to another schema's outbox in the same database (the channel is per
    database) don't make it claim again before its poll."""
    batches = []
    real_batch = Relay.publish_batch

    async def counting(self: Relay) -> int:
        batches.append(1)
        return await real_batch(self)

    monkeypatch.setattr(Relay, "publish_batch", counting)
    other = f"t_{uuid.uuid4().hex[:12]}"
    with psycopg.connect(test_database, autocommit=True) as admin:
        admin.execute(f"CREATE SCHEMA {other}")
    try:
        other_url = psycopg.conninfo.make_conninfo(test_database, options=f"-c search_path={other}")
        with psycopg.connect(other_url, autocommit=True) as conn:
            migrate(conn, "orders")
        stop = asyncio.Event()
        task = asyncio.create_task(
            relay(pool, connection, topology, listen_url=fresh_database, poll_interval=30.0).run(stop)
        )
        try:
            await asyncio.sleep(0.3)
            before = len(batches)
            async with await psycopg.AsyncConnection.connect(other_url) as other_conn:
                for _ in range(3):
                    async with other_conn.transaction():
                        await outbox.write(other_conn, charge())
            await asyncio.sleep(0.3)
            assert len(batches) == before
            await write_all(pool, [charge()])  # its own schema's write does
            await asyncio.sleep(0.3)
            assert len(batches) > before
        finally:
            stop.set()
            await asyncio.wait_for(task, timeout=5)
    finally:
        with psycopg.connect(test_database, autocommit=True) as admin:
            admin.execute(f"DROP SCHEMA {other} CASCADE")


# The rest kill mutants of outbox.py that the day-2 review found no test noticed.


class HeldRelay(Relay):
    """A relay whose publishes wait for `release` while `hold` is set."""

    hold: asyncio.Event
    release: asyncio.Event
    holding: asyncio.Event

    async def _publish(self, channel: AbstractChannel, body: bytes) -> None:
        if self.hold.is_set():
            self.holding.set()
            await self.release.wait()
        await super()._publish(channel, body)


def held(pool: AsyncConnectionPool, connection: AbstractConnection, topology: Topology, **options: Any) -> HeldRelay:
    r = HeldRelay(pool=pool, connection=connection, topology=topology, **options)
    r.hold, r.release, r.holding = asyncio.Event(), asyncio.Event(), asyncio.Event()
    r.hold.set()
    return r


async def test_a_backlog_drains_batch_after_batch_without_waiting_for_the_poll(
    fresh_database: str,
    pool: AsyncConnectionPool,
    connection: AbstractConnection,
    channel: AbstractChannel,
    topology: Topology,
) -> None:
    stop = asyncio.Event()
    task = asyncio.create_task(
        relay(pool, connection, topology, listen_url=fresh_database, batch_size=50, poll_interval=30.0).run(stop)
    )
    try:
        await asyncio.sleep(0.3)
        await write_all(pool, [charge(n) for n in range(220)])  # one NOTIFY, five batches
        assert await wait_until_published(pool, timeout=5.0) < 3.0
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5)
    await drain(channel, topology.queue("payment.commands"), 220)


async def test_the_backoff_starts_again_from_the_minimum_after_a_success(
    pool: AsyncConnectionPool, connection: AbstractConnection, topology: Topology, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[float] = []

    async def flaky(self: Relay) -> int:
        calls.append(time.perf_counter())
        if len(calls) <= 5 or len(calls) == 7:  # five failures (backoff up to 1.6 s), a success, one more failure
            raise psycopg.OperationalError("the database went away")
        return 0

    monkeypatch.setattr(Relay, "publish_batch", flaky)
    stop = asyncio.Event()
    task = asyncio.create_task(relay(pool, connection, topology, poll_interval=0.05).run(stop))
    try:
        for _ in range(500):
            if len(calls) >= 8:
                break
            await asyncio.sleep(0.02)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=10)
    assert len(calls) >= 8
    assert calls[5] - calls[4] > 1.5  # the fifth failure's backoff: 1.6 s
    assert calls[7] - calls[6] < 0.5  # after the success: 0.1 s again, not 3.2 s


async def test_a_second_relay_takes_the_next_rows_instead_of_waiting_for_the_first(
    pool: AsyncConnectionPool, connection: AbstractConnection, channel: AbstractChannel, topology: Topology
) -> None:
    await write_all(pool, [charge(n) for n in range(20)])
    first = held(pool, connection, topology, batch_size=10)
    first_batch = asyncio.create_task(first.publish_batch())
    try:
        await asyncio.wait_for(first.holding.wait(), timeout=5)  # rows 1-10 claimed and locked
        # SKIP LOCKED: the second relay claims rows 11-20 at once; with plain FOR UPDATE it would wait on row 1.
        assert await asyncio.wait_for(relay(pool, connection, topology, batch_size=10).publish_batch(), timeout=3) == 10
    finally:
        first.release.set()
    assert await asyncio.wait_for(first_batch, timeout=5) == 10
    await drain(channel, topology.queue("payment.commands"), 20)


async def test_a_notify_that_arrives_during_a_batch_is_not_lost(
    fresh_database: str,
    pool: AsyncConnectionPool,
    connection: AbstractConnection,
    channel: AbstractChannel,
    topology: Topology,
) -> None:
    await write_all(pool, [charge()])
    r = held(pool, connection, topology, listen_url=fresh_database, poll_interval=30.0)
    stop = asyncio.Event()
    task = asyncio.create_task(r.run(stop))
    try:
        await asyncio.wait_for(r.holding.wait(), timeout=5)  # the first batch is publishing
        r.hold.clear()
        await write_all(pool, [charge()])  # committed (and notified) while the batch runs
        await asyncio.sleep(0.2)
        r.release.set()
        assert await wait_until_published(pool, timeout=5.0) < 2.0  # not at the 30 s poll
    finally:
        r.release.set()
        stop.set()
        await asyncio.wait_for(task, timeout=5)
    await drain(channel, topology.queue("payment.commands"), 2)


async def test_a_confirm_that_never_comes_gives_up_after_publish_timeout(
    pool: AsyncConnectionPool, connection: AbstractConnection, channel: AbstractChannel, topology: Topology
) -> None:
    class OneHangs(Relay):
        async def _publish(self, channel: AbstractChannel, body: bytes) -> None:
            if body == encode(stuck):
                await asyncio.sleep(3600)
            await super()._publish(channel, body)

    stuck, fine = charge(), charge()
    await write_all(pool, [stuck, fine])
    r = OneHangs(pool=pool, connection=connection, topology=topology, publish_timeout=0.2)
    started = time.perf_counter()
    with pytest.raises(
        RelayError, match=r"1 not published, first outbox row 1: TimeoutError: no confirm within 0\.2 s"
    ):
        await asyncio.wait_for(r.publish_batch(), timeout=5)
    assert time.perf_counter() - started < 1.0
    assert await drain(channel, topology.queue("payment.commands"), 1) == [encode(fine)]
    assert await unpublished(pool) == 1


async def test_a_missing_confirm_is_the_brokers_failure_not_the_rows(
    pool: AsyncConnectionPool, connection: AbstractConnection, topology: Topology
) -> None:
    """No confirm in time (like a nack or a closed channel) says something about the broker, not this row: the row
    isn't charged an attempt or a wait, and the error says the relay should back off (review round 2)."""

    class NeverConfirms(Relay):
        async def _publish(self, channel: AbstractChannel, body: bytes) -> None:
            await asyncio.sleep(3600)

    await write_all(pool, [charge()])
    with pytest.raises(RelayError) as raised:
        await NeverConfirms(pool=pool, connection=connection, topology=topology, publish_timeout=0.2).publish_batch()
    assert raised.value.broker
    async with pool.connection() as conn:
        row = await (await conn.execute("SELECT attempts, last_error, next_attempt_at FROM outbox")).fetchone()
    assert row == (0, None, None)


async def test_an_unroutable_rows_error_is_its_own_and_backs_nothing_off(
    pool: AsyncConnectionPool, connection: AbstractConnection, channel: AbstractChannel, topology: Topology
) -> None:
    commands_queue = await channel.get_queue(topology.queue("payment.commands"), ensure=False)
    await commands_queue.unbind(topology.commands, "payment.charge")
    await write_all(pool, [charge()])
    with pytest.raises(RelayError) as raised:
        await relay(pool, connection, topology).publish_batch()
    assert not raised.value.broker


async def test_after_a_full_batch_of_failing_rows_the_relay_goes_straight_to_the_rows_behind(
    fresh_database: str,
    pool: AsyncConnectionPool,
    connection: AbstractConnection,
    channel: AbstractChannel,
    topology: Topology,
) -> None:
    commands_queue = await channel.get_queue(topology.queue("payment.commands"), ensure=False)
    await commands_queue.unbind(topology.commands, "payment.charge")
    r = relay(pool, connection, topology, listen_url=fresh_database, batch_size=5, poll_interval=30.0)
    stop = asyncio.Event()
    task = asyncio.create_task(r.run(stop))
    try:
        for _ in range(100):
            if r.listener_pid is not None:
                break
            await asyncio.sleep(0.02)
        await asyncio.sleep(0.2)  # its first, empty batch is done: asleep until a NOTIFY
        event = confirmed()
        await write_all(pool, [*(charge(n) for n in range(5)), event])  # one NOTIFY: five failing rows, then one
        started = time.perf_counter()
        while True:
            async with pool.connection() as conn:
                row = await (
                    await conn.execute("SELECT published_at FROM outbox WHERE message_id = %s", (event.message_id,))
                ).fetchone()
            if row is not None and row[0] is not None:
                break
            assert time.perf_counter() - started < 2.0, "the row behind a full batch of failures waited for the poll"
            await asyncio.sleep(0.02)
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5)
    assert await drain(channel, topology.queue("notification.order-events"), 1) == [encode(event)]


def test_last_error_holds_no_lone_surrogate() -> None:
    # psycopg can't encode one, and the failed UPDATE would roll back the batch's marks (review round 2's suspicion).
    assert outbox.describe(ValueError("bad \udc80 text")) == "ValueError: bad \\udc80 text"


async def test_a_batch_waits_at_most_lock_timeout_for_a_table_lock(
    fresh_database: str, pool: AsyncConnectionPool, connection: AbstractConnection, topology: Topology
) -> None:
    await write_all(pool, [charge()])
    with psycopg.connect(fresh_database) as locker:
        locker.execute("LOCK TABLE outbox IN ACCESS EXCLUSIVE MODE")  # what a migration's ALTER TABLE takes
        started = time.perf_counter()
        with pytest.raises(psycopg.errors.LockNotAvailable):  # not TimeoutError: the batch's own bound, not this one
            await asyncio.wait_for(relay(pool, connection, topology, lock_timeout=0.2).publish_batch(), timeout=3)
        assert time.perf_counter() - started < 2.0
        locker.rollback()
    assert await relay(pool, connection, topology).publish_batch() == 1


async def test_a_channel_that_never_opens_times_out(
    pool: AsyncConnectionPool, connection: AbstractConnection, topology: Topology, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def hangs(conn: AbstractConnection) -> AbstractChannel:
        await asyncio.sleep(3600)
        raise AssertionError("unreachable")

    monkeypatch.setattr(broker, "open_channel", hangs)
    await write_all(pool, [charge()])
    started = time.perf_counter()
    task = asyncio.create_task(relay(pool, connection, topology, publish_timeout=0.2).publish_batch())
    done, _ = await asyncio.wait([task], timeout=3)  # a regression fails here instead of hanging the suite
    assert done, "opening the channel wasn't bounded"
    with pytest.raises(TimeoutError):
        await task
    assert time.perf_counter() - started < 2.0
    assert await unpublished(pool) == 1


async def test_a_bare_exception_from_the_broker_client_does_not_kill_the_relay(
    pool: AsyncConnectionPool, topology: Topology, connection: AbstractConnection, monkeypatch: pytest.MonkeyPatch
) -> None:
    """aiormq's bare Exception() for a call pending on a connection closed without a reason (day 3): an outage."""
    real = broker.open_channel
    failures = 0

    async def closed_under_us(conn: AbstractConnection) -> AbstractChannel:
        nonlocal failures
        if failures < 2:
            failures += 1
            raise Exception()
        return await real(conn)

    monkeypatch.setattr(broker, "open_channel", closed_under_us)
    await write_all(pool, [charge()])
    stop = asyncio.Event()
    task = asyncio.create_task(relay(pool, connection, topology, poll_interval=0.05).run(stop))
    deadline = time.monotonic() + 10
    while await unpublished(pool) != 0:
        assert not task.done(), task
        assert time.monotonic() < deadline
        await asyncio.sleep(0.02)
    stop.set()
    await asyncio.wait_for(task, 10)
    assert failures == 2
