"""Dead letters: list them, replay them into the queue they died in, or discard them (day 5).

A message is dead-lettered when its consumer refuses it (it doesn't decode, isn't the consumer's type, or names an
order the service doesn't have) or fails it on every attempt (the first try and one per retry delay): the work queue's
dead-letter exchange puts it in `<queue>.dlq`, which nothing consumes. The service logs why, under the message id. An
operator then decides, once the cause is fixed (a provider back up, a bug fixed): replay it, or discard it.

**Replaying** publishes a copy to the work queue it died in, through the default exchange, so only that queue's consumer
sees it again (not every subscriber of the event). The copy has the same body and properties and no headers: the attempt
count starts again at 1 (a dead letter carries its last attempt's number, so without the reset its first failure after a
replay would dead-letter it again: the day-3 review's note) and RabbitMQ's `x-death` history is dropped. The broker
confirms the copy before the dead letter is acknowledged, so a crash in between leaves both, and replaying the dead
letter again is harmless: the consumer's `processed_messages` makes the second copy a duplicate. The first copy is
handled, because a dead-lettered message's id was never recorded (its handler's transaction rolled back).

Only a message that decodes and whose type is routed to that queue is replayed; any other would just be dead-lettered
again, so it is kept, and can be discarded by its id.

One pass takes the dead-letter queue's messages one by one (`basic.get`, unacknowledged, until it is empty or `limit`
have been taken), decides for each, and gives back the ones it keeps at the end (rejected with requeue; if the tool
dies, the broker gives them back when its channel closes). Holding them unacknowledged until then means a pass never
sees one twice, and two passes at once take different messages. The pass reads until the queue is empty rather than
taking as many as the broker counts: a quorum queue's count can lag the acknowledgements and requeues just sent (read
at once after a pass that kept two and replayed one, it said one, in this module's first tests).

    python -m orderly.dead_letters list [QUEUE ...]
    python -m orderly.dead_letters replay QUEUE (--all | --id MESSAGE_ID ...) [--dry-run]
    python -m orderly.dead_letters discard QUEUE --id MESSAGE_ID ...

QUEUE is a work queue (`payment.commands`, `order.payment-events`, `notification.order-events`); the broker is
`ORDERLY_AMQP_URL` and the names take `ORDERLY_TOPOLOGY_PREFIX`, as for the services.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import sys
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Literal

import aio_pika
from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractIncomingMessage

from orderly import broker
from orderly.consumer import attempt_of
from orderly.messages import InvalidMessage, decode
from orderly.topology import QUEUES, Topology

Action = Literal["keep", "replay", "discard"]
WORK_QUEUES = tuple(spec.name for spec in QUEUES)
# Each broker call of a pass (the get, the publish and its confirm, the acknowledgement) is bounded by this.
CALL_TIMEOUT = 5.0
DEFAULT_LIMIT = 1000


@dataclass(frozen=True)
class DeadLetter:
    """What a dead letter is, read from its body and headers."""

    queue: str  # the work queue it died in (unprefixed)
    message_id: str | None  # the envelope's id, or the AMQP property's if the body doesn't decode
    type: str | None
    order_id: str | None  # the envelope's correlation id, which is always the order's id
    attempt: int  # the attempt that failed last: 1 if it was never retried, len(retry delays) + 1 if out of attempts
    reason: str | None  # RabbitMQ's for its last death: `rejected` (by its consumer) or `delivery_limit`
    died_at: datetime | None  # when RabbitMQ dead-lettered it (whole seconds)
    problem: str | None  # why it can't be replayed, if it can't

    @property
    def replayable(self) -> bool:
        return self.problem is None


@dataclass(frozen=True)
class Outcome:
    letter: DeadLetter
    action: Literal["replayed", "discarded", "kept"]
    # Why it was kept when it was meant to be replayed or discarded (or that it would have been, on a dry run).
    detail: str = ""


@dataclass(frozen=True)
class Pass:
    queue: str
    # In the dead-letter queue when the pass began, by the broker's count: for the record only, since the count can lag
    # a pass that has just given messages back.
    waiting: int
    outcomes: tuple[Outcome, ...]
    # The pass stopped at its limit with more dead letters still waiting.
    truncated: bool = False


def describe(topology: Topology, queue: str, incoming: AbstractIncomingMessage) -> DeadLetter:
    headers = incoming.headers or {}
    # The most recent death comes first. `x-first-death-*` is no use here: a message that went through a wait queue
    # first "died" there, by expiring (found by probing the broker).
    deaths = headers.get("x-death")
    last = deaths[0] if isinstance(deaths, list) and deaths and isinstance(deaths[0], dict) else {}
    reason = headers.get("x-last-death-reason", last.get("reason"))
    died_at = last.get("time")
    try:
        message = decode(incoming.body)
    except InvalidMessage as error:
        return DeadLetter(
            queue=queue,
            message_id=incoming.message_id,
            type=incoming.type,
            order_id=incoming.correlation_id,
            attempt=attempt_of(incoming),
            reason=reason if isinstance(reason, str) else None,
            died_at=died_at if isinstance(died_at, datetime) else None,
            problem=f"doesn't decode: {error}",
        )
    routed = queue in topology.routes(message.type)
    return DeadLetter(
        queue=queue,
        message_id=str(message.message_id),
        type=message.type,
        order_id=str(message.correlation_id),
        attempt=attempt_of(incoming),
        reason=reason if isinstance(reason, str) else None,
        died_at=died_at if isinstance(died_at, datetime) else None,
        problem=None if routed else f"{message.type} isn't routed to {queue}",
    )


def replay_copy(incoming: AbstractIncomingMessage) -> aio_pika.Message:
    """The message as its producer published it: the same body and properties, no headers (so attempt 1)."""
    return aio_pika.Message(
        body=incoming.body,
        content_type=incoming.content_type,
        delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
        message_id=incoming.message_id,
        correlation_id=incoming.correlation_id,
        type=incoming.type,
        timestamp=incoming.timestamp,
    )


def _check_queue(queue: str) -> None:
    if queue not in WORK_QUEUES:
        raise ValueError(f"not a work queue: {queue!r} (one of {', '.join(WORK_QUEUES)})")


async def run_pass(
    connection: AbstractConnection,
    topology: Topology,
    queue: str,
    decide: Callable[[DeadLetter], Action],
    *,
    limit: int = DEFAULT_LIMIT,
    dry_run: bool = False,
) -> Pass:
    """One pass over `queue`'s dead letters: `decide` says what to do with each. A dead letter to replay that can't be
    (see `DeadLetter.problem`), or whose copy the broker doesn't confirm, is kept. On a dry run every one is kept, with
    what would have happened as its detail."""
    _check_queue(queue)
    if limit < 1:
        raise ValueError(f"limit must be at least 1: {limit}")
    channel = await broker.open_channel(connection)
    try:
        return await _pass(channel, topology, queue, decide, limit=limit, dry_run=dry_run)
    finally:
        with contextlib.suppress(Exception):
            async with asyncio.timeout(CALL_TIMEOUT):
                await channel.close()


async def _pass(
    channel: AbstractChannel,
    topology: Topology,
    queue: str,
    decide: Callable[[DeadLetter], Action],
    *,
    limit: int,
    dry_run: bool,
) -> Pass:
    name = topology.dead_letter_queue(queue)
    waiting = await _count(channel, name)
    async with asyncio.timeout(CALL_TIMEOUT):
        dlq = await channel.get_queue(name, ensure=False)
    outcomes: list[Outcome] = []
    kept: list[AbstractIncomingMessage] = []
    truncated = False
    try:
        while True:
            async with asyncio.timeout(CALL_TIMEOUT):
                incoming = await dlq.get(no_ack=False, fail=False)
            if incoming is None:
                break
            if len(outcomes) == limit:
                # Taken only to know that more are waiting: given back with the kept ones.
                kept.append(incoming)
                truncated = True
                break
            letter = describe(topology, queue, incoming)
            action = decide(letter)
            if action == "keep":
                kept.append(incoming)
                outcomes.append(Outcome(letter, "kept"))
            elif action == "replay" and not letter.replayable:
                kept.append(incoming)
                outcomes.append(Outcome(letter, "kept", f"not replayable: {letter.problem}"))
            elif dry_run:
                kept.append(incoming)
                outcomes.append(Outcome(letter, "kept", f"dry run: would be {action}ed"))
            elif action == "replay":
                try:
                    async with asyncio.timeout(CALL_TIMEOUT):
                        # Mandatory on a channel that raises on returns, and confirmed: a work queue that is gone (or
                        # full: reject-publish) is an error here, and the dead letter stays.
                        await channel.default_exchange.publish(
                            replay_copy(incoming), routing_key=topology.queue(queue), mandatory=True
                        )
                except (aio_pika.exceptions.AMQPError, TimeoutError) as error:
                    kept.append(incoming)
                    outcomes.append(Outcome(letter, "kept", f"the replay wasn't confirmed: {error!r}"))
                    if channel.is_closed:
                        break
                    continue
                async with asyncio.timeout(CALL_TIMEOUT):
                    await incoming.ack()
                outcomes.append(Outcome(letter, "replayed"))
            else:
                async with asyncio.timeout(CALL_TIMEOUT):
                    await incoming.ack()
                outcomes.append(Outcome(letter, "discarded"))
    finally:
        # Give the kept ones back now rather than when the channel closes (either way they stay in the queue).
        for incoming in kept:
            with contextlib.suppress(Exception):
                async with asyncio.timeout(CALL_TIMEOUT):
                    await incoming.reject(requeue=True)
    return Pass(queue, waiting, tuple(outcomes), truncated)


async def _count(channel: AbstractChannel, name: str) -> int:
    async with asyncio.timeout(CALL_TIMEOUT):
        queue = await channel.declare_queue(name, passive=True)
    result = queue.declaration_result
    return int(result.message_count or 0) if result is not None else 0


# The command line.


def _line(outcome: Outcome) -> str:
    letter = outcome.letter
    died = letter.died_at.isoformat() if letter.died_at else "?"
    text = (
        f"{outcome.action:9} {letter.message_id or '?'} {letter.type or '?'} order {letter.order_id or '?'}"
        f" attempt {letter.attempt} {letter.reason or '?'} at {died}"
    )
    notes = [note for note in (letter.problem and f"({letter.problem})", outcome.detail) if note]
    return " ".join([text, *notes]) if outcome.detail or letter.problem else text


def _record(outcome: Outcome) -> dict[str, object]:
    letter = asdict(outcome.letter)
    letter["died_at"] = outcome.letter.died_at.isoformat() if outcome.letter.died_at else None
    return {"action": outcome.action, "detail": outcome.detail, **letter}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m orderly.dead_letters", description=__doc__.split("\n\n")[0])
    commands = parser.add_subparsers(dest="command", required=True)
    listing = commands.add_parser("list", help="show the dead letters, changing nothing")
    listing.add_argument("queues", nargs="*", metavar="QUEUE", help="default: every work queue")
    replay = commands.add_parser("replay", help="publish dead letters again to the queue they died in")
    replay.add_argument("queue", choices=WORK_QUEUES)
    which = replay.add_mutually_exclusive_group(required=True)
    which.add_argument("--all", action="store_true", help="every replayable dead letter")
    which.add_argument("--id", dest="ids", action="append", metavar="MESSAGE_ID")
    replay.add_argument("--dry-run", action="store_true")
    discard = commands.add_parser("discard", help="acknowledge dead letters named by id, dropping them")
    discard.add_argument("queue", choices=WORK_QUEUES)
    discard.add_argument("--id", dest="ids", action="append", metavar="MESSAGE_ID", required=True)
    discard.add_argument("--dry-run", action="store_true")
    for sub in (listing, replay, discard):
        sub.add_argument("--limit", type=int, default=DEFAULT_LIMIT, help="at most this many per queue")
        sub.add_argument("--json", action="store_true", help="one JSON object per dead letter")
    return parser


async def main_async(argv: Sequence[str], connection: AbstractConnection, topology: Topology) -> int:
    """The command line against an open connection: 0 when everything asked for was done, 1 otherwise (a dead letter
    to replay kept, an id not found), 2 for a bad command line."""
    try:
        args = _parser().parse_args(argv)
    except SystemExit as exit_:
        return 2 if exit_.code not in (0, None) else 0
    if args.limit < 1:
        print("--limit must be at least 1", file=sys.stderr)
        return 2
    status = 0
    if args.command == "list" and (unknown := [q for q in args.queues if q not in WORK_QUEUES]):
        print(f"not a work queue: {', '.join(unknown)} (one of {', '.join(WORK_QUEUES)})", file=sys.stderr)
        return 2
    if args.command == "list":
        passes = [
            await run_pass(connection, topology, queue, lambda _: "keep", limit=args.limit)
            for queue in (args.queues or WORK_QUEUES)
        ]
    else:
        ids = set(args.ids or ())
        action: Action = "replay" if args.command == "replay" else "discard"

        def decide(letter: DeadLetter) -> Action:
            if args.command == "replay" and args.all:
                return action
            return action if letter.message_id in ids else "keep"

        passes = [await run_pass(connection, topology, args.queue, decide, limit=args.limit, dry_run=args.dry_run)]
        missing = sorted(ids - {o.letter.message_id for o in passes[0].outcomes if o.letter.message_id})
        if missing:
            print(f"not found in {topology.dead_letter_queue(args.queue)}: {', '.join(missing)}", file=sys.stderr)
            status = 1
        if any(o.action == "kept" and o.detail and not o.detail.startswith("dry run") for o in passes[0].outcomes):
            status = 1  # meant to be replayed or discarded, and still there
    for done in passes:
        if args.json:
            for outcome in done.outcomes:
                print(json.dumps({"queue": done.queue, **_record(outcome)}, sort_keys=True))
        else:
            print(f"{topology.dead_letter_queue(done.queue)}: {done.waiting} waiting")
            for outcome in done.outcomes:
                print(f"  {_line(outcome)}")
        if done.truncated:
            print(f"{done.queue}: stopped at --limit {args.limit}, more are waiting", file=sys.stderr)
            if args.command != "list":
                status = 1
    return status


async def _main(argv: Sequence[str]) -> int:
    from orderly.service import topology_from_environment

    topology = topology_from_environment()
    connection = await aio_pika.connect(broker.amqp_url())
    async with connection:
        return await main_async(argv, connection, topology)


if __name__ == "__main__":
    sys.exit(asyncio.run(_main(sys.argv[1:])))
