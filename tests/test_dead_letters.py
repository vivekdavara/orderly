"""The dead-letter tool (day 5) against a real RabbitMQ: listing changes nothing, a replay starts the attempts again and
is handled once, a dead letter that can't be replayed is kept, and the command line."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
import uuid
from collections import Counter
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime

import aio_pika
import psycopg
import pytest
from aio_pika.abc import AbstractChannel, AbstractConnection
from psycopg_pool import AsyncConnectionPool

from orderly import broker, dead_letters, notifications, orders, payments
from orderly.broker import publish, to_amqp
from orderly.consumer import Connection, Consumer
from orderly.dead_letters import DeadLetter, Pass, main_async, run_pass
from orderly.messages import ChargePayment, Message, OrderConfirmed, new_message
from orderly.service import _configure
from orderly.topology import ATTEMPT_HEADER, QUEUES, Topology

QUEUE = "payment.commands"
DELAYS = (0.1, 0.2)


@pytest.fixture
async def short_topology(connection: AbstractConnection) -> AsyncIterator[Topology]:
    topology = Topology(prefix=f"test-{uuid.uuid4().hex[:12]}.", retry_delays=DELAYS)
    async with connection.channel() as setup:
        await topology.declare(setup)
    yield topology
    async with connection.channel() as cleanup:
        await topology.delete(cleanup)


def charge() -> Message:
    return new_message(ChargePayment(order_id=uuid.uuid4(), customer_id="c-42", amount_cents=2599, currency="USD"))


async def depth(channel: AbstractChannel, name: str) -> int:
    queue = await channel.declare_queue(name, passive=True)
    assert queue.declaration_result is not None
    return int(queue.declaration_result.message_count or 0)


async def at_depth(channel: AbstractChannel, name: str, expected: int) -> None:
    """The queue settles at `expected` messages. A quorum queue's count lags the acknowledgements and requeues it was
    sent (they are Raft commands applied after the channel moves on; a count read at once was one short), so wait for
    it, then check that it stays."""

    async def reached() -> bool:
        return await depth(channel, name) == expected

    await until(reached)
    await asyncio.sleep(0.3)
    assert await depth(channel, name) == expected


async def until(condition: Callable[[], Awaitable[bool]], timeout: float = 10.0) -> None:  # noqa: ASYNC109
    deadline = time.monotonic() + timeout
    while not await condition():
        assert time.monotonic() < deadline, f"not within {timeout} s"
        await asyncio.sleep(0.02)


async def kill(channel: AbstractChannel, topology: Topology, message: aio_pika.Message, queue: str = QUEUE) -> None:
    """Dead-letter `message` the way a consumer does: put it in the work queue, take it, reject it."""
    before = await depth(channel, topology.dead_letter_queue(queue))
    await channel.default_exchange.publish(message, routing_key=topology.queue(queue), mandatory=True)
    work = await channel.get_queue(topology.queue(queue), ensure=False)
    deadline = time.monotonic() + 5
    while (incoming := await work.get(no_ack=False, fail=False)) is None:
        assert time.monotonic() < deadline
        await asyncio.sleep(0.01)
    await incoming.reject(requeue=False)

    async def arrived() -> bool:
        return await depth(channel, topology.dead_letter_queue(queue)) == before + 1

    await until(arrived)


def out_of_attempts(message: Message) -> aio_pika.Message:
    """A message as the consumer's last retry sends it: attempt len(DELAYS) + 1."""
    amqp = to_amqp(message)
    amqp.headers = {ATTEMPT_HEADER: len(DELAYS) + 1}
    return amqp


def keep(_: DeadLetter) -> dead_letters.Action:
    return "keep"


def replay_all(_: DeadLetter) -> dead_letters.Action:
    return "replay"


async def test_listing_describes_each_dead_letter_and_changes_nothing(
    connection: AbstractConnection, channel: AbstractChannel, short_topology: Topology
) -> None:
    message = charge()
    await kill(channel, short_topology, out_of_attempts(message))
    first = await run_pass(connection, short_topology, QUEUE, keep)
    second = await run_pass(connection, short_topology, QUEUE, keep)
    assert first.waiting == second.waiting == 1
    [outcome] = first.outcomes
    letter = outcome.letter
    assert outcome.action == "kept" and outcome.detail == ""
    assert (letter.queue, letter.message_id, letter.type, letter.order_id) == (
        QUEUE,
        str(message.message_id),
        "payment.charge",
        str(message.correlation_id),
    )
    assert (letter.attempt, letter.reason, letter.problem) == (3, "rejected", None)
    assert letter.died_at is not None and abs((datetime.now(UTC) - letter.died_at).total_seconds()) < 10
    assert second.outcomes[0].letter == letter
    await at_depth(channel, short_topology.dead_letter_queue(QUEUE), 1)


async def test_a_dead_letter_that_went_through_a_wait_queue_reports_its_last_death(
    connection: AbstractConnection, channel: AbstractChannel, short_topology: Topology
) -> None:
    """A retried message first "died" by expiring in its wait queue: the reason shown is the last death's (probed on the
    broker: `x-first-death-reason` is `expired` there)."""
    message = charge()
    copy = out_of_attempts(message)
    retry = await channel.get_exchange(short_topology.retry, ensure=False)
    await retry.publish(copy, routing_key=short_topology.retry_queue(QUEUE, DELAYS[0]), mandatory=True)
    work = await channel.get_queue(short_topology.queue(QUEUE), ensure=False)
    deadline = time.monotonic() + 5
    while (incoming := await work.get(no_ack=False, fail=False)) is None:
        assert time.monotonic() < deadline
        await asyncio.sleep(0.01)
    assert incoming.headers["x-first-death-reason"] == "expired"
    await incoming.reject(requeue=False)

    async def arrived() -> bool:
        return await depth(channel, short_topology.dead_letter_queue(QUEUE)) == 1

    await until(arrived)
    [outcome] = (await run_pass(connection, short_topology, QUEUE, keep)).outcomes
    assert outcome.letter.reason == "rejected" and outcome.letter.attempt == 3


async def test_a_replayed_copy_is_the_original_with_no_headers(
    connection: AbstractConnection, channel: AbstractChannel, short_topology: Topology
) -> None:
    message = charge()
    await kill(channel, short_topology, out_of_attempts(message))
    done = await run_pass(connection, short_topology, QUEUE, replay_all)
    assert [o.action for o in done.outcomes] == ["replayed"]
    await at_depth(channel, short_topology.dead_letter_queue(QUEUE), 0)
    work = await channel.get_queue(short_topology.queue(QUEUE), ensure=False)
    copy = await work.get(no_ack=True, fail=False)
    assert copy is not None
    original = to_amqp(message)
    assert copy.body == original.body
    assert (copy.message_id, copy.correlation_id, copy.type, copy.content_type) == (
        original.message_id,
        original.correlation_id,
        original.type,
        original.content_type,
    )
    assert copy.delivery_mode == aio_pika.DeliveryMode.PERSISTENT
    # An AMQP timestamp is whole seconds: the copy carries the one the dead letter had.
    assert original.timestamp is not None and copy.timestamp == original.timestamp.replace(microsecond=0)
    # No attempt number (so attempt 1) and no x-death: only what the broker itself adds on a quorum queue's delivery.
    assert set(copy.headers) <= {"x-delivery-count", "x-acquired-count"}
    assert await work.get(no_ack=True, fail=False) is None


async def test_a_dead_letter_that_does_not_decode_is_kept_and_can_be_discarded(
    connection: AbstractConnection, channel: AbstractChannel, short_topology: Topology
) -> None:
    await kill(channel, short_topology, aio_pika.Message(b'{"not": "a message"}', message_id="garbage-1"))
    message = charge()
    await kill(channel, short_topology, to_amqp(message))
    done = await run_pass(connection, short_topology, QUEUE, replay_all)
    by_id = {o.letter.message_id: o for o in done.outcomes}
    assert by_id[str(message.message_id)].action == "replayed"
    garbage = by_id["garbage-1"]
    assert garbage.action == "kept" and garbage.detail.startswith("not replayable: doesn't decode")
    assert garbage.letter.problem is not None and not garbage.letter.replayable
    await at_depth(channel, short_topology.dead_letter_queue(QUEUE), 1)

    def discard_garbage(letter: DeadLetter) -> dead_letters.Action:
        return "discard" if letter.message_id == "garbage-1" else "keep"

    done = await run_pass(connection, short_topology, QUEUE, discard_garbage)
    assert [o.action for o in done.outcomes] == ["discarded"]
    await at_depth(channel, short_topology.dead_letter_queue(QUEUE), 0)


async def test_a_type_routed_to_another_queue_is_not_replayed(
    connection: AbstractConnection, channel: AbstractChannel, short_topology: Topology
) -> None:
    """An event that reached the payment commands' queue by mistake: replaying it there would only dead-letter it
    again."""
    event = new_message(
        OrderConfirmed(order_id=uuid.uuid4(), customer_id="c-1", email="a@b.c", total_cents=1, currency="USD")
    )
    await kill(channel, short_topology, to_amqp(event))
    [outcome] = (await run_pass(connection, short_topology, QUEUE, replay_all)).outcomes
    assert outcome.action == "kept"
    assert outcome.letter.problem == f"order.confirmed isn't routed to {QUEUE}"
    await at_depth(channel, short_topology.dead_letter_queue(QUEUE), 1)


async def test_only_the_chosen_dead_letters_are_replayed_and_the_rest_stay(
    connection: AbstractConnection, channel: AbstractChannel, short_topology: Topology
) -> None:
    chosen, other = charge(), charge()
    for message in (chosen, other, charge()):
        await kill(channel, short_topology, to_amqp(message))

    def decide(letter: DeadLetter) -> dead_letters.Action:
        return "replay" if letter.message_id == str(chosen.message_id) else "keep"

    done = await run_pass(connection, short_topology, QUEUE, decide)
    assert Counter(o.action for o in done.outcomes) == Counter({"replayed": 1, "kept": 2})
    await at_depth(channel, short_topology.dead_letter_queue(QUEUE), 2)
    await at_depth(channel, short_topology.queue(QUEUE), 1)
    left = await run_pass(connection, short_topology, QUEUE, keep)
    assert str(chosen.message_id) not in {o.letter.message_id for o in left.outcomes}
    assert str(other.message_id) in {o.letter.message_id for o in left.outcomes}


async def test_a_dry_run_changes_nothing(
    connection: AbstractConnection, channel: AbstractChannel, short_topology: Topology
) -> None:
    await kill(channel, short_topology, to_amqp(charge()))
    done = await run_pass(connection, short_topology, QUEUE, replay_all, dry_run=True)
    assert [(o.action, o.detail) for o in done.outcomes] == [("kept", "dry run: would be replayed")]
    await at_depth(channel, short_topology.dead_letter_queue(QUEUE), 1)
    await at_depth(channel, short_topology.queue(QUEUE), 0)


async def test_a_replay_the_broker_cannot_route_keeps_the_dead_letter(
    connection: AbstractConnection, channel: AbstractChannel, short_topology: Topology
) -> None:
    await kill(channel, short_topology, to_amqp(charge()))
    await channel.queue_delete(short_topology.queue(QUEUE))
    try:
        [outcome] = (await run_pass(connection, short_topology, QUEUE, replay_all)).outcomes
        assert outcome.action == "kept" and outcome.detail.startswith("the replay wasn't confirmed")
        await at_depth(channel, short_topology.dead_letter_queue(QUEUE), 1)
    finally:
        async with connection.channel() as setup:
            await short_topology.declare(setup)  # the fixture's cleanup deletes the work queue again


async def test_a_pass_takes_at_most_its_limit_and_says_how_many_it_left(
    connection: AbstractConnection, channel: AbstractChannel, short_topology: Topology
) -> None:
    for _ in range(3):
        await kill(channel, short_topology, to_amqp(charge()))
    done = await run_pass(connection, short_topology, QUEUE, replay_all, limit=2)
    assert (done.waiting, len(done.outcomes), done.truncated) == (3, 2, True)
    await at_depth(channel, short_topology.dead_letter_queue(QUEUE), 1)
    done = await run_pass(connection, short_topology, QUEUE, replay_all, limit=1)
    assert (len(done.outcomes), done.truncated) == (1, False)
    with pytest.raises(ValueError, match="limit"):
        await run_pass(connection, short_topology, QUEUE, keep, limit=0)


async def test_an_unknown_queue_is_refused(connection: AbstractConnection, short_topology: Topology) -> None:
    with pytest.raises(ValueError, match="not a work queue"):
        await run_pass(connection, short_topology, "payment.commands.dlq", keep)


def test_the_queues_take_exactly_the_types_routed_to_them() -> None:
    """The tool decides "replayable" by the routes; the consumers dead-letter what isn't in their ACCEPTS. If those
    disagreed, a replay could be dead-lettered again at once."""
    from orderly.messages import PAYLOADS

    accepts = {
        payments.QUEUE: payments.ACCEPTS,
        orders.QUEUE: orders.ACCEPTS,
        notifications.QUEUE: notifications.ACCEPTS,
    }
    topology = Topology()
    assert set(accepts) == {spec.name for spec in QUEUES} == set(dead_letters.WORK_QUEUES)
    for queue, accepted in accepts.items():
        assert {t for t in PAYLOADS if queue in topology.routes(t)} == accepted


# A replay handled by a real consumer.


@dataclass
class Flaky:
    """A handler that records each message in `effects` and fails while `down`, or for its next `fail_next` calls."""

    down: bool = True
    fail_next: int = 0
    calls: Counter[uuid.UUID] = field(default_factory=Counter)

    async def __call__(self, conn: Connection, message: Message) -> None:
        self.calls[message.message_id] += 1
        await conn.execute("INSERT INTO effects (message_id) VALUES (%s)", (message.message_id,))
        if self.down:
            raise RuntimeError("the provider is down")
        if self.fail_next > 0:
            self.fail_next -= 1
            raise RuntimeError("one more failure")


@pytest.fixture
async def pool(fresh_database: str) -> AsyncIterator[AsyncConnectionPool]:
    with psycopg.connect(fresh_database) as conn:
        conn.execute(
            "CREATE TABLE processed_messages (consumer text, message_id uuid, processed_at timestamptz DEFAULT now(),"
            " PRIMARY KEY (consumer, message_id))"
        )
        conn.execute("CREATE TABLE effects (message_id uuid)")
    pool = AsyncConnectionPool(fresh_database, min_size=1, max_size=4, open=False, configure=_configure)
    await pool.open(wait=True, timeout=10)
    yield pool
    await pool.close()


@pytest.fixture
async def consumer(
    pool: AsyncConnectionPool, connection: AbstractConnection, short_topology: Topology
) -> AsyncIterator[tuple[Consumer, Flaky]]:
    handler = Flaky()
    consumer = Consumer(
        pool=pool,
        connection=connection,
        topology=short_topology,
        queue=QUEUE,
        name="flaky",
        handle=handler,
        accepts=frozenset({"payment.charge"}),
    )
    stop = asyncio.Event()
    task = asyncio.create_task(consumer.run(stop))
    yield consumer, handler
    stop.set()
    await asyncio.wait_for(task, timeout=30)


def effects(url: str) -> list[uuid.UUID]:
    with psycopg.connect(url) as conn:
        return [row[0] for row in conn.execute("SELECT message_id FROM effects").fetchall()]


async def stat(consumer: Consumer, name: str, value: int) -> None:
    async def reached() -> bool:
        return consumer.stats[name] >= value

    await until(reached)


async def test_a_replay_after_the_cause_is_fixed_is_handled_once_with_its_attempts_restarted(
    connection: AbstractConnection,
    channel: AbstractChannel,
    short_topology: Topology,
    consumer: tuple[Consumer, Flaky],
    fresh_database: str,
) -> None:
    """Dead-lettered after its three attempts; then the cause is fixed, but the first try after the replay still fails
    once: the copy started again at attempt 1, so it is retried, not dead-lettered at once, and handled."""
    running, handler = consumer
    message = charge()
    await publish(channel, short_topology, message)
    await stat(running, "dead_lettered", 1)
    # Counted at the reject; the broker routes it to the dead-letter queue on its own time.
    await at_depth(channel, short_topology.dead_letter_queue(QUEUE), 1)
    assert handler.calls[message.message_id] == len(DELAYS) + 1
    handler.down, handler.fail_next = False, 1
    done = await run_pass(connection, short_topology, QUEUE, replay_all)
    assert [o.action for o in done.outcomes] == ["replayed"]
    await stat(running, "handled", 1)
    assert running.stats["dead_lettered"] == 1 and running.stats["retried"] == len(DELAYS) + 1
    assert effects(fresh_database) == [message.message_id]
    await at_depth(channel, short_topology.dead_letter_queue(QUEUE), 0)


async def test_a_replay_interrupted_before_its_acknowledgement_is_handled_once_when_replayed_again(
    connection: AbstractConnection,
    channel: AbstractChannel,
    short_topology: Topology,
    consumer: tuple[Consumer, Flaky],
    fresh_database: str,
) -> None:
    """The tool died after the broker confirmed the copy and before it acknowledged the dead letter: the copy is
    handled, the dead letter is still there, and replaying it again gives the consumer a duplicate, which it skips."""
    running, handler = consumer
    message = charge()
    await publish(channel, short_topology, message)
    await stat(running, "dead_lettered", 1)
    # Counted at the reject; the broker routes it to the dead-letter queue on its own time.
    await at_depth(channel, short_topology.dead_letter_queue(QUEUE), 1)
    handler.down = False
    dlq = await channel.get_queue(short_topology.dead_letter_queue(QUEUE), ensure=False)
    dead = await dlq.get(no_ack=False, fail=False)
    assert dead is not None
    await channel.default_exchange.publish(
        dead_letters.replay_copy(dead), routing_key=short_topology.queue(QUEUE), mandatory=True
    )
    await dead.reject(requeue=True)  # the crash: never acknowledged
    await stat(running, "handled", 1)
    done = await run_pass(connection, short_topology, QUEUE, replay_all)
    assert [o.action for o in done.outcomes] == ["replayed"]
    await stat(running, "duplicate", 1)
    assert handler.calls[message.message_id] == len(DELAYS) + 2
    assert effects(fresh_database) == [message.message_id]


# The command line.


async def cli(connection: AbstractConnection, topology: Topology, *argv: str) -> tuple[int, list[str], str]:
    import contextlib
    import io

    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        status = await main_async(list(argv), connection, topology)
    return status, out.getvalue().splitlines(), err.getvalue()


async def test_the_command_line_lists_replays_and_discards(
    connection: AbstractConnection, channel: AbstractChannel, short_topology: Topology
) -> None:
    good, also = charge(), charge()
    await kill(channel, short_topology, out_of_attempts(good))
    await kill(channel, short_topology, to_amqp(also))
    await kill(channel, short_topology, aio_pika.Message(b"\xff", message_id="junk"))

    status, out, _ = await cli(connection, short_topology, "list")
    assert status == 0
    assert out[0] == f"{short_topology.dead_letter_queue(QUEUE)}: 3 waiting"
    assert any(
        str(good.message_id) in line and "payment.charge" in line and "attempt 3 rejected" in line for line in out
    )
    assert f"{short_topology.dead_letter_queue('order.payment-events')}: 0 waiting" in out

    status, out, _ = await cli(connection, short_topology, "list", QUEUE, "--json")
    records = [json.loads(line) for line in out]
    assert status == 0 and len(records) == 3 and {r["action"] for r in records} == {"kept"}
    assert {r["message_id"] for r in records} == {str(good.message_id), str(also.message_id), "junk"}

    status, _, err = await cli(
        connection, short_topology, "replay", QUEUE, "--id", str(good.message_id), "--id", "nope"
    )
    assert status == 1 and "not found" in err and "nope" in err
    await at_depth(channel, short_topology.dead_letter_queue(QUEUE), 2)

    status, out, _ = await cli(connection, short_topology, "replay", QUEUE, "--all", "--dry-run")
    # The junk couldn't be replayed for real either, so the dry run says so too.
    assert status == 1 and sum("dry run: would be replayed" in line for line in out) == 1
    assert sum("not replayable: doesn't decode" in line for line in out) == 1

    status, out, _ = await cli(connection, short_topology, "replay", QUEUE, "--all")
    assert status == 1  # the junk can't be replayed, and is still there
    await at_depth(channel, short_topology.dead_letter_queue(QUEUE), 1)

    status, out, _ = await cli(connection, short_topology, "discard", QUEUE, "--id", "junk")
    assert status == 0 and any(line.strip().startswith("discarded junk") for line in out)
    await at_depth(channel, short_topology.dead_letter_queue(QUEUE), 0)
    await at_depth(channel, short_topology.queue(QUEUE), 2)


async def test_the_command_line_refuses_what_it_cannot_do(
    connection: AbstractConnection, short_topology: Topology
) -> None:
    for argv in (
        ["list", "payment.commands.dlq"],
        ["replay", QUEUE],  # neither --all nor --id
        ["replay", QUEUE, "--all", "--id", "x"],
        ["discard", QUEUE],  # no id: discarding everything isn't offered
        ["discard", QUEUE, "--all"],
        ["replay", "nowhere", "--all"],
        ["list", "--limit", "0"],
    ):
        status, _, _ = await cli(connection, short_topology, *argv)
        assert status == 2, argv


async def test_passes_one_after_another_see_what_the_last_one_kept(
    connection: AbstractConnection, channel: AbstractChannel, short_topology: Topology
) -> None:
    """Back-to-back passes each see all 20: what one gives back, the next takes (the broker's count can lag the
    requeues, which is why a pass reads until the queue is empty instead of trusting it)."""
    for _ in range(20):
        await kill(channel, short_topology, to_amqp(charge()))
    for _ in range(5):
        done = await run_pass(connection, short_topology, QUEUE, keep)
        assert (len(done.outcomes), done.truncated) == (20, False)


def test_a_pass_is_not_truncated_unless_it_says_so() -> None:
    assert not Pass(QUEUE, 5, ()).truncated


async def test_a_pass_over_a_dead_letter_queue_that_does_not_exist_says_so(connection: AbstractConnection) -> None:
    """A wrong ORDERLY_TOPOLOGY_PREFIX: the passive declare closes the channel with NOT_FOUND. The pass reports it
    rather than raising, and the command line exits 1 with the reason (day-5 review: a traceback before)."""
    nowhere = Topology(prefix=f"missing-{uuid.uuid4().hex[:12]}.")
    done = await run_pass(connection, nowhere, QUEUE, keep)
    assert done.outcomes == () and done.failure is not None and "NOT_FOUND" in done.failure
    status, _, err = await cli(connection, nowhere, "list", QUEUE)
    assert status == 1 and "the pass stopped" in err


async def test_a_pass_whose_channel_closes_halfway_reports_what_it_had_done(
    connection: AbstractConnection,
    channel: AbstractChannel,
    short_topology: Topology,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """The pass's channel closes while it replays the second of three: the first is reported replayed, the pass says it
    stopped, and nothing is lost: all three dead letters but the first are back in the dead-letter queue. The second's
    copy may have reached the work queue without its confirm (it did here): a duplicate its consumer skips by id."""
    for _ in range(3):
        await kill(channel, short_topology, to_amqp(charge()))
    opened: list[AbstractChannel] = []
    real_open = broker.open_channel

    async def capture(connection: AbstractConnection) -> AbstractChannel:
        opened.append(await real_open(connection))
        return opened[-1]

    monkeypatch.setattr(broker, "open_channel", capture)
    seen: list[DeadLetter] = []
    closing: list[asyncio.Future[None]] = []

    def decide(letter: DeadLetter) -> dead_letters.Action:
        seen.append(letter)
        if len(seen) == 2:
            closing.append(asyncio.ensure_future(opened[0].close()))
        return "replay"

    done = await run_pass(connection, short_topology, QUEUE, decide)
    await asyncio.gather(*closing, return_exceptions=True)
    assert done.failure is not None and "closed" in done.failure
    first, *rest = done.outcomes
    assert first.action == "replayed" and all(o.action == "kept" for o in rest)
    await at_depth(channel, short_topology.dead_letter_queue(QUEUE), 2)
    work = await channel.get_queue(short_topology.queue(QUEUE), ensure=False)
    copies = []
    while (incoming := await work.get(no_ack=True, fail=False)) is not None:
        copies.append(incoming.message_id)
    assert copies[0] == first.letter.message_id and set(copies[1:]) <= {seen[1].message_id}
    status, out, _ = await cli(connection, short_topology, "replay", QUEUE, "--all")
    assert status == 0 and sum(line.strip().startswith("replayed") for line in out) == 2


async def test_every_dead_letter_has_an_id_the_command_line_accepts(
    connection: AbstractConnection, channel: AbstractChannel, short_topology: Topology
) -> None:
    """Without a message id the id comes from the body: the same on every pass, and `discard --id` takes it."""
    message = aio_pika.Message(b"no id at all")
    message.message_id = None
    await kill(channel, short_topology, message)
    first = await run_pass(connection, short_topology, QUEUE, keep)
    second = await run_pass(connection, short_topology, QUEUE, keep)
    [letter] = [o.letter for o in first.outcomes]
    expected = "sha256:" + hashlib.sha256(b"no id at all").hexdigest()[:16]
    if letter.message_id != expected:  # aiormq filled one in on the way out: then that is the id
        assert letter.message_id and not letter.message_id.startswith("sha256:")
    assert second.outcomes[0].letter.message_id == letter.message_id
    status, _, _ = await cli(connection, short_topology, "discard", QUEUE, "--id", letter.message_id)
    assert status == 0
    await at_depth(channel, short_topology.dead_letter_queue(QUEUE), 0)
