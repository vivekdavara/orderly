"""The idempotent consumer against the real servers: handled once, duplicates skipped, retries through the wait queues,
dead letters, and a stop that lets handlers finish."""

from __future__ import annotations

import asyncio
import time
import uuid
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field

import aio_pika
import psycopg
import pytest
from aio_pika.abc import AbstractChannel, AbstractConnection
from psycopg_pool import AsyncConnectionPool

from orderly.broker import publish
from orderly.consumer import Connection, Consumer, Refused, attempt_of
from orderly.messages import ChargePayment, Message, new_message
from orderly.service import _configure
from orderly.topology import ATTEMPT_HEADER, Topology

DELAYS = (0.2, 0.4)


@pytest.fixture
async def short_topology(connection: AbstractConnection) -> AsyncIterator[Topology]:
    """The topology with short retry delays, so a message's retries take well under a second."""
    topology = Topology(prefix=f"test-{uuid.uuid4().hex[:12]}.", retry_delays=DELAYS)
    async with connection.channel() as setup:
        await topology.declare(setup)
    yield topology
    async with connection.channel() as cleanup:
        await topology.delete(cleanup)


@pytest.fixture
async def pool(fresh_database: str) -> AsyncIterator[AsyncConnectionPool]:
    with psycopg.connect(fresh_database) as conn:
        conn.execute(
            "CREATE TABLE processed_messages (consumer text, message_id uuid, processed_at timestamptz DEFAULT now(),"
            " PRIMARY KEY (consumer, message_id))"
        )
        conn.execute("CREATE TABLE effects (message_id uuid, amount bigint)")
    pool = AsyncConnectionPool(fresh_database, min_size=1, max_size=12, open=False, configure=_configure)
    await pool.open(wait=True, timeout=10)
    yield pool
    await pool.close()


@dataclass
class Toy:
    """A handler that records each message's amount in `effects`, and can be told to fail, refuse or stall."""

    fail_first: dict[uuid.UUID, int] = field(default_factory=dict)
    refuse: set[uuid.UUID] = field(default_factory=set)
    stall: float = 0.0
    calls: Counter[uuid.UUID] = field(default_factory=Counter)
    called_at: dict[uuid.UUID, list[float]] = field(default_factory=dict)

    async def __call__(self, conn: Connection, message: Message) -> None:
        self.calls[message.message_id] += 1
        self.called_at.setdefault(message.message_id, []).append(time.monotonic())
        assert isinstance(message.payload, ChargePayment)
        await conn.execute(
            "INSERT INTO effects (message_id, amount) VALUES (%s, %s)",
            (message.message_id, message.payload.amount_cents),
        )
        if self.stall:
            await asyncio.sleep(self.stall)
        if message.message_id in self.refuse:
            raise Refused("no such order")
        if self.calls[message.message_id] <= self.fail_first.get(message.message_id, 0):
            raise RuntimeError(f"transient failure {self.calls[message.message_id]}")


@dataclass
class Running:
    consumer: Consumer
    stop: asyncio.Event
    task: asyncio.Task[None]

    async def until(self, condition: Callable[[], bool], timeout: float = 10.0) -> None:  # noqa: ASYNC109
        deadline = time.monotonic() + timeout
        while not condition():
            assert not self.task.done(), "the consumer stopped"
            assert time.monotonic() < deadline, f"not within {timeout} s; stats {dict(self.consumer.stats)}"
            await asyncio.sleep(0.01)

    async def finish(self) -> None:
        self.stop.set()
        await asyncio.wait_for(self.task, timeout=30)


Start = Callable[..., Awaitable[Running]]


@pytest.fixture
async def start(
    pool: AsyncConnectionPool, connection: AbstractConnection, short_topology: Topology
) -> AsyncIterator[Start]:
    running: list[Running] = []

    async def start(handler: Toy, **options: float) -> Running:
        consumer = Consumer(
            pool=pool,
            connection=connection,
            topology=short_topology,
            queue="payment.commands",
            name="toy",
            handle=handler,
            accepts=frozenset({"payment.charge"}),
            **options,  # type: ignore[arg-type]
        )
        stop = asyncio.Event()
        run = Running(consumer, stop, asyncio.create_task(consumer.run(stop)))
        running.append(run)
        return run

    yield start
    for run in running:
        if not run.task.done():
            await run.finish()


def charge(amount: int = 2599) -> Message:
    return new_message(ChargePayment(order_id=uuid.uuid4(), customer_id="c-42", amount_cents=amount, currency="USD"))


def effects(pool_url: str) -> list[tuple[uuid.UUID, int]]:
    with psycopg.connect(pool_url) as conn:
        return [(row[0], row[1]) for row in conn.execute("SELECT message_id, amount FROM effects").fetchall()]


async def queue_depth(channel: AbstractChannel, name: str) -> list[aio_pika.abc.AbstractIncomingMessage]:
    """Take everything waiting in a queue (acknowledged, so the queue is left empty)."""
    queue = await channel.get_queue(name, ensure=False)
    taken = []
    while (incoming := await queue.get(fail=False, no_ack=True)) is not None:
        taken.append(incoming)
    return taken


async def test_a_message_is_handled_once_and_acknowledged(
    start: Start, channel: AbstractChannel, short_topology: Topology, fresh_database: str
) -> None:
    toy = Toy()
    run = await start(toy)
    message = charge()
    await publish(channel, short_topology, message)
    await run.until(lambda: run.consumer.stats["handled"] == 1)
    await run.finish()
    assert effects(fresh_database) == [(message.message_id, 2599)]
    assert await queue_depth(channel, short_topology.queue("payment.commands")) == []
    assert await queue_depth(channel, short_topology.dead_letter_queue("payment.commands")) == []


async def test_a_duplicate_is_acknowledged_without_running_the_handler(
    start: Start, channel: AbstractChannel, short_topology: Topology, fresh_database: str
) -> None:
    toy = Toy()
    run = await start(toy)
    message = charge()
    await publish(channel, short_topology, message)
    await run.until(lambda: run.consumer.stats["handled"] == 1)
    await publish(channel, short_topology, message)  # the outbox re-publishing it after a crash, say
    await run.until(lambda: run.consumer.stats["duplicate"] == 1)
    assert toy.calls[message.message_id] == 1
    assert effects(fresh_database) == [(message.message_id, 2599)]


async def test_concurrent_copies_of_one_message_are_handled_once(
    start: Start, channel: AbstractChannel, short_topology: Topology, fresh_database: str
) -> None:
    """Eight copies delivered at once to eight handlers: the second insert of the id waits for the first's commit, then
    finds it and skips."""
    toy = Toy(stall=0.2)
    message = charge()
    for _ in range(8):
        await publish(channel, short_topology, message)
    run = await start(toy, prefetch=8)
    await run.until(lambda: run.consumer.stats["handled"] + run.consumer.stats["duplicate"] == 8)
    assert (run.consumer.stats["handled"], run.consumer.stats["duplicate"]) == (1, 7)
    assert toy.calls[message.message_id] == 1
    assert effects(fresh_database) == [(message.message_id, 2599)]


async def test_deliveries_are_handled_concurrently_up_to_the_prefetch(
    start: Start, channel: AbstractChannel, short_topology: Topology
) -> None:
    toy = Toy(stall=0.5)
    for _ in range(8):
        await publish(channel, short_topology, charge())
    started = time.monotonic()
    run = await start(toy, prefetch=8)
    await run.until(lambda: run.consumer.stats["handled"] == 8)
    # One at a time would take 4 s.
    assert time.monotonic() - started < 2.5


async def test_an_undecodable_message_is_dead_lettered_at_once(
    start: Start, channel: AbstractChannel, short_topology: Topology, fresh_database: str
) -> None:
    toy = Toy()
    run = await start(toy)
    exchange = await channel.get_exchange(short_topology.commands, ensure=False)
    await exchange.publish(aio_pika.Message(b'{"not": "a message"}'), routing_key="payment.charge", mandatory=True)
    await run.until(lambda: run.consumer.stats["dead_lettered"] == 1)
    assert toy.calls == Counter()
    dead = await queue_depth(channel, short_topology.dead_letter_queue("payment.commands"))
    assert [incoming.body for incoming in dead] == [b'{"not": "a message"}']


async def test_a_type_the_consumer_does_not_handle_is_dead_lettered(
    start: Start, channel: AbstractChannel, short_topology: Topology
) -> None:
    from orderly.messages import RefundPayment

    toy = Toy()
    run = await start(toy)
    refund = new_message(RefundPayment(order_id=uuid.uuid4(), amount_cents=1, currency="USD", reason="order_cancelled"))
    await publish(channel, short_topology, refund)  # routed to payment.commands, but this consumer takes only charges
    await run.until(lambda: run.consumer.stats["dead_lettered"] == 1)
    assert toy.calls == Counter()
    assert len(await queue_depth(channel, short_topology.dead_letter_queue("payment.commands"))) == 1


async def test_a_failing_handler_is_retried_after_each_delay_and_its_effects_kept_once(
    start: Start, channel: AbstractChannel, short_topology: Topology, fresh_database: str
) -> None:
    message = charge()
    toy = Toy(fail_first={message.message_id: 2})
    run = await start(toy)
    await publish(channel, short_topology, message)
    await run.until(lambda: run.consumer.stats["handled"] == 1)
    assert run.consumer.stats["retried"] == 2 and toy.calls[message.message_id] == 3
    # The failed attempts rolled back: one effect, and the id recorded once.
    assert effects(fresh_database) == [(message.message_id, 2599)]
    first, second, third = toy.called_at[message.message_id]
    assert second - first >= DELAYS[0] * 0.9 and third - second >= DELAYS[1] * 0.9
    assert await queue_depth(channel, short_topology.dead_letter_queue("payment.commands")) == []


async def test_a_handler_that_keeps_failing_is_dead_lettered_after_the_last_delay(
    start: Start, channel: AbstractChannel, short_topology: Topology, fresh_database: str
) -> None:
    message = charge()
    toy = Toy(fail_first={message.message_id: 99})
    run = await start(toy)
    await publish(channel, short_topology, message)
    await run.until(lambda: run.consumer.stats["dead_lettered"] == 1)
    assert toy.calls[message.message_id] == len(DELAYS) + 1
    assert run.consumer.stats["retried"] == len(DELAYS)
    assert effects(fresh_database) == []
    dead = await queue_depth(channel, short_topology.dead_letter_queue("payment.commands"))
    assert [incoming.message_id for incoming in dead] == [str(message.message_id)]
    assert attempt_of(dead[0]) == len(DELAYS) + 1
    # Its id was never recorded, so a replay from the dead-letter queue is handled.
    with psycopg.connect(fresh_database) as conn:
        assert conn.execute("SELECT count(*) FROM processed_messages").fetchone() == (0,)


async def test_a_refused_message_is_dead_lettered_at_once_and_rolled_back(
    start: Start, channel: AbstractChannel, short_topology: Topology, fresh_database: str
) -> None:
    message = charge()
    toy = Toy(refuse={message.message_id})
    run = await start(toy)
    await publish(channel, short_topology, message)
    await run.until(lambda: run.consumer.stats["dead_lettered"] == 1)
    assert toy.calls[message.message_id] == 1 and run.consumer.stats["retried"] == 0
    assert effects(fresh_database) == []


async def test_a_handler_slower_than_its_timeout_is_retried(
    start: Start, channel: AbstractChannel, short_topology: Topology, fresh_database: str
) -> None:
    message = charge()
    toy = Toy(stall=0.6)
    run = await start(toy, handler_timeout=0.3)
    await publish(channel, short_topology, message)
    await run.until(lambda: run.consumer.stats["retried"] >= 1)
    assert effects(fresh_database) == []  # the timed-out transaction rolled back


async def test_a_stop_lets_a_handler_in_flight_commit_and_acknowledge(
    start: Start, channel: AbstractChannel, short_topology: Topology, fresh_database: str
) -> None:
    message = charge()
    toy = Toy(stall=0.5)
    run = await start(toy)
    await publish(channel, short_topology, message)
    await run.until(lambda: toy.calls[message.message_id] == 1)
    await run.finish()  # while the handler sleeps
    assert run.consumer.stats["handled"] == 1
    assert effects(fresh_database) == [(message.message_id, 2599)]
    assert await queue_depth(channel, short_topology.queue("payment.commands")) == []


async def test_the_attempt_header_counts_from_one() -> None:
    @dataclass
    class Fake:
        headers: dict[str, object] | None

    for headers, expected in [
        (None, 1),
        ({}, 1),
        ({ATTEMPT_HEADER: 3}, 3),
        ({ATTEMPT_HEADER: 0}, 1),
        ({ATTEMPT_HEADER: -2}, 1),
        ({ATTEMPT_HEADER: "2"}, 1),
        ({ATTEMPT_HEADER: True}, 1),
    ]:
        assert attempt_of(Fake(headers)) == expected, headers  # type: ignore[arg-type]


async def test_a_bare_exception_from_the_broker_client_is_an_outage_not_a_death(
    start: Start, channel: AbstractChannel, short_topology: Topology, monkeypatch: pytest.MonkeyPatch
) -> None:
    """aiormq fails the calls pending on a connection closed without a reason with a bare Exception(); the consumer
    died of it when a test closed the broker (found on day 3). It now backs off and subscribes again."""
    from orderly import broker

    real = broker.open_channel
    failures = 0

    async def closed_under_us(connection: AbstractConnection) -> AbstractChannel:
        nonlocal failures
        if failures < 2:
            failures += 1
            raise Exception()
        return await real(connection)

    monkeypatch.setattr(broker, "open_channel", closed_under_us)
    run = await start(Toy())
    await publish(channel, short_topology, charge())
    await run.until(lambda: run.consumer.stats["handled"] == 1)
    assert failures == 2 and not run.task.done()


def test_only_the_exact_bare_exception_counts_as_closed() -> None:
    from orderly.broker import closed_without_reason

    assert closed_without_reason(Exception())
    assert not closed_without_reason(Exception("a real error"))
    assert not closed_without_reason(ValueError())
