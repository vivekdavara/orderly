"""Idempotent consumers with retries and dead letters (DESIGN.md "Idempotent consumers", "RabbitMQ topology").

A `Consumer` takes one work queue's deliveries and, for each one:

1. decodes it; a message that isn't valid, or of a type this consumer doesn't handle, is dead-lettered at once (it will
   never decode, so retrying it is pointless);
2. in one database transaction, inserts `(consumer, message_id)` into `processed_messages` and, if the id was new, runs
   the handler, which applies its change and writes its outgoing messages to the outbox; if the id was there, the
   message is a duplicate and nothing else happens;
3. commits, and only then acknowledges.

A crash before the acknowledgement means a redelivery, which finds the id and is skipped. A handler that fails (the
database, the provider, a timeout: anything but `Refused`) rolls back, and the delivery is copied to the wait queue for
its attempt (`<queue>.retry.<delay>`, which gives it back after the delay) with the next attempt number, and then
acknowledged; after the last delay the next failure dead-letters it. A handler raises `Refused` for a message it can
never handle (an order it doesn't know): dead-lettered at once. Nothing is ever requeued: RabbitMQ 4 doesn't count a
requeue toward the delivery limit, so a requeue on a persistent error would loop forever.

Deliveries are handled concurrently, up to `prefetch` at a time, each bounded by `handler_timeout`. `run` stops between
deliveries: it cancels the subscription, waits for the handlers in flight, then closes its channel, so a stop doesn't
cut a handler off between its commit and its acknowledgement.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

import aio_pika
import psycopg
from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractIncomingMessage
from psycopg_pool import AsyncConnectionPool

from orderly import broker
from orderly.messages import InvalidMessage, Message, decode
from orderly.outbox import wait_any
from orderly.topology import ATTEMPT_HEADER, Topology

Connection = psycopg.AsyncConnection[Any]
# A consumer's logic: apply `message` in the caller's transaction (on `conn`), writing any replies to the outbox.
Handler = Callable[[Connection, Message], Awaitable[None]]

MIN_BACKOFF = 0.1
MAX_BACKOFF = 5.0
# What losing the broker looks like from a subscription (CancelledError: see run).
BROKER_ERRORS = (aio_pika.exceptions.AMQPError, ConnectionError, OSError, TimeoutError, asyncio.CancelledError)

log = logging.getLogger(__name__)


class Refused(Exception):
    """The message can never be handled (it names an order this service doesn't have, or contradicts its records).
    Raised by a handler, it rolls the transaction back and dead-letters the message at once."""


def attempt_of(incoming: AbstractIncomingMessage) -> int:
    """The delivery's attempt number: 1 for the first, carried in a header by the copies in the wait queues. A header
    that isn't a positive integer counts as a first attempt."""
    value = (incoming.headers or {}).get(ATTEMPT_HEADER)
    return value if isinstance(value, int) and not isinstance(value, bool) and value >= 1 else 1


@dataclass
class Consumer:
    pool: AsyncConnectionPool
    connection: AbstractConnection
    topology: Topology
    # The work queue (unprefixed name) and the name its handled messages are recorded under.
    queue: str
    name: str
    handle: Handler
    # The message types this consumer handles; any other is dead-lettered.
    accepts: frozenset[str]
    # Deliveries handled at once. Each holds a pooled connection while it runs, so keep it below the pool's size.
    prefetch: int = 8
    # The most a handler's transaction may take (its waits for a connection, for locks, for the provider); then it
    # fails and is retried. It also bounds how long a stop waits for the handlers in flight.
    handler_timeout: float = 10.0
    # How long publishing a retry may take (part of a handler's time, so of the drain's).
    publish_timeout: float = 5.0
    # How long each of the channel's own calls may take: the open, the subscription (qos, queue, consume), the
    # cancel and the close. All bounded, so a stop has a bound (stop_bound) inside service.STOP_TIMEOUT (day-3
    # review, round 2).
    control_timeout: float = 2.0
    # What happened to each delivery: handled, duplicate, retried, dead_lettered (tests and logs read it).
    stats: Counter[str] = field(default_factory=Counter)
    _inflight: set[asyncio.Task[None]] = field(default_factory=set, init=False, repr=False)
    # The channel it consumes on, which also publishes the retries (confirms and returns on).
    _channel: AbstractChannel | None = field(default=None, init=False, repr=False)
    # Set once a stop has begun (or the channel is lost): deliveries arriving then start no handler. A cancel that timed
    # out leaves the subscription live, and a handler started after the drain began outlived the stop (day-3 review,
    # round 2).
    _stopping: bool = field(default=False, init=False, repr=False)

    async def run(self, stop: asyncio.Event) -> None:
        """Consume until `stop` is set; after a lost channel or connection, back off and subscribe again."""
        with broker.reconnects(self.connection) as reconnected:
            backoff = MIN_BACKOFF
            while not stop.is_set():
                # A reconnect from now on means the broker is back: a backoff for an outage ends then (day 4).
                reconnected.clear()
                try:
                    await self._consume_until(stop)
                    backoff = MIN_BACKOFF
                except BROKER_ERRORS as e:
                    # aiormq raises CancelledError into an RPC's caller when the connection closes under it (the relay
                    # found this on day 2): only a real cancel of this task ends it.
                    current = asyncio.current_task()
                    if isinstance(e, asyncio.CancelledError) and current is not None and current.cancelling():
                        raise
                    log.warning("consumer %s: %r; subscribing again in %.1f s", self.name, e, backoff)
                    await wait_any(stop, reconnected, timeout=backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)
                except Exception as error:
                    if not broker.closed_without_reason(error):
                        raise
                    log.warning("consumer %s: the broker closed a call (%r); subscribing again", self.name, error)
                    await wait_any(stop, reconnected, timeout=backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)

    async def _open_channel(self) -> AbstractChannel:
        if self.connection.is_closed:
            raise ConnectionError("the broker connection is closed")
        try:
            async with asyncio.timeout(self.control_timeout):
                # Confirms and returns on: the channel also publishes the retries.
                return await broker.open_channel(self.connection)
        except RuntimeError as error:  # a robust connection reconnecting (see outbox.Relay._open_channel)
            raise ConnectionError(f"could not open a channel: {error}") from error

    async def _consume_until(self, stop: asyncio.Event) -> None:
        channel = self._channel = await self._open_channel()
        self._stopping = False
        # Set by every close of the channel underneath, a lost connection's included. A robust channel reopens itself
        # after a reconnect and then reports itself open again, but without this subscription (get_queue doesn't
        # register the queue for restoring): watching is_closed alone, the consumer missed a broker drop and never
        # consumed again (found by the drop test). So on any close it closes the channel for good and subscribes anew.
        lost = asyncio.Event()

        def on_close(*_: object) -> None:
            lost.set()

        channel.close_callbacks.add(on_close)
        try:
            try:
                async with asyncio.timeout(self.control_timeout):
                    await channel.set_qos(prefetch_count=self.prefetch)
                    queue = await channel.get_queue(self.topology.queue(self.queue), ensure=False)
                    tag = await queue.consume(self._on_delivery)
            except RuntimeError as error:
                # aiormq's ChannelInvalidStateError ("<Channel> closed") is a RuntimeError, not an AMQPError: a channel
                # the broker closed between the open and the subscription killed the consumer for good (day-3
                # review). It is a broker failure like the others: back off and subscribe on a new channel.
                raise ConnectionError(f"the channel closed before the subscription: {error}") from error
            while not stop.is_set() and not lost.is_set() and not channel.is_closed:
                await wait_any(stop, lost, timeout=0.5)
            self._stopping = True
            if not lost.is_set() and not channel.is_closed:
                # No new deliveries; the ones in flight still finish and acknowledge. Bounded, like the close below:
                # a broker that stops answering mustn't hold a shutdown past service.STOP_TIMEOUT (see stop_bound).
                # RuntimeError: the channel closed after the check (ChannelInvalidStateError); the drain must still run.
                with contextlib.suppress(aio_pika.exceptions.AMQPError, ConnectionError, TimeoutError, RuntimeError):
                    async with asyncio.timeout(self.control_timeout):
                        await queue.cancel(tag)
            await self._drain()
        finally:
            self._stopping = True
            with contextlib.suppress(aio_pika.exceptions.AMQPError, ConnectionError, RuntimeError, TimeoutError):
                async with asyncio.timeout(self.control_timeout):
                    await channel.close()
        if not stop.is_set():
            raise ConnectionError("the consumer's channel closed")

    @property
    def stop_bound(self) -> float:
        """The longest a stop can take once `stop` is set: the open and the subscription it may be in the middle of
        (the stop is seen after them), the cancel, the handlers in flight, the close."""
        return 4 * self.control_timeout + self._drain_timeout

    @property
    def _drain_timeout(self) -> float:
        return self.handler_timeout + self.publish_timeout + 1

    async def _drain(self) -> None:
        """Wait for the handlers in flight; they are each bounded by handler_timeout and a publish. None starts once the
        stop has begun (`_stopping`), but wait for the set as it is, not a snapshot, to be sure."""
        deadline = asyncio.get_running_loop().time() + self._drain_timeout
        while self._inflight and (left := deadline - asyncio.get_running_loop().time()) > 0:
            await asyncio.wait(set(self._inflight), timeout=left)

    async def _on_delivery(self, incoming: AbstractIncomingMessage) -> None:
        if self._stopping:
            # A delivery after the stop began (the cancel timed out, or crossed it): start nothing the stop wouldn't
            # wait for. Left unacknowledged, it goes back to the queue when the channel closes (counted once toward
            # the delivery limit); a requeue here would come straight back to this live subscription, in a loop.
            return
        # In a task of our own rather than aiormq's: closing a channel cancels aiormq's consumer tasks, and a stop must
        # let a handler that has committed acknowledge.
        task = asyncio.create_task(self.process(incoming), name=f"{self.name}:{incoming.delivery_tag}")
        self._inflight.add(task)
        task.add_done_callback(self._done)

    def _done(self, task: asyncio.Task[None]) -> None:
        self._inflight.discard(task)
        if not task.cancelled() and (error := task.exception()) is not None:
            log.error("consumer %s: a delivery failed outside its handler: %r", self.name, error)

    async def process(self, incoming: AbstractIncomingMessage) -> None:
        """Handle one delivery to the end: acknowledged, sent to a wait queue, or dead-lettered."""
        try:
            message = decode(incoming.body)
            if message.type not in self.accepts:
                raise Refused(f"{self.name} doesn't handle {message.type}")
        except (InvalidMessage, Refused) as error:
            await self._dead_letter(incoming, f"undecodable or not ours: {error}")
            return
        attempt = attempt_of(incoming)
        try:
            async with asyncio.timeout(self.handler_timeout):
                duplicate = await self._apply(message)
        except Refused as error:
            await self._dead_letter(incoming, f"refused: {error}", message)
            return
        except Exception as error:
            await self._retry(incoming, message, attempt, error)
            return
        self.stats["duplicate" if duplicate else "handled"] += 1
        await incoming.ack()

    async def _apply(self, message: Message) -> bool:
        """Run the handler in a transaction that first claims the message id; True if it was claimed before."""
        async with self.pool.connection() as conn, conn.transaction():
            await conn.execute("SELECT set_config('lock_timeout', '5s', true)")
            claimed = await conn.execute(
                "INSERT INTO processed_messages (consumer, message_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
                (self.name, message.message_id),
            )
            if claimed.rowcount == 0:
                return True
            await self.handle(conn, message)
            return False

    async def _retry(
        self, incoming: AbstractIncomingMessage, message: Message, attempt: int, error: BaseException
    ) -> None:
        delays = self.topology.retry_delays
        if attempt > len(delays):
            await self._dead_letter(incoming, f"failed {attempt} times, last: {error!r}", message)
            return
        delay = delays[attempt - 1]
        log.warning(
            "consumer %s: %s %s failed (attempt %d): %r; retrying in %s s",
            self.name,
            message.type,
            message.message_id,
            attempt,
            error,
            delay,
        )
        copy = aio_pika.Message(
            body=incoming.body,
            headers={ATTEMPT_HEADER: attempt + 1},
            content_type=incoming.content_type,
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT,
            message_id=incoming.message_id,
            correlation_id=incoming.correlation_id,
            type=incoming.type,
            timestamp=incoming.timestamp,
        )
        channel = self._channel
        try:
            if channel is None or channel.is_closed:
                raise ConnectionError("the consumer's channel is closed")
            async with asyncio.timeout(self.publish_timeout):
                exchange = await channel.get_exchange(self.topology.retry, ensure=False)
                await exchange.publish(copy, routing_key=self.topology.retry_queue(self.queue, delay), mandatory=True)
        except Exception as failure:
            # The broker can't take the copy: leave the delivery unacknowledged and close the channel, so it comes back
            # (counted toward the delivery limit, which bounds this) once the consumer has backed off and resubscribed.
            log.warning("consumer %s: could not schedule a retry (%r); closing the channel", self.name, failure)
            if channel is not None:
                with contextlib.suppress(Exception):
                    async with asyncio.timeout(self.control_timeout):
                        await channel.close()
            return
        self.stats["retried"] += 1
        await incoming.ack()

    async def _dead_letter(
        self, incoming: AbstractIncomingMessage, reason: str, message: Message | None = None
    ) -> None:
        what = "a delivery" if message is None else f"{message.type} {message.message_id}"
        log.error("consumer %s: dead-lettering %s: %s", self.name, what, reason)
        # Rejected without requeue: the queue's dead-letter exchange takes it to <queue>.dlq, at least once. Counted
        # after the reject (the broker then routes it on its own time: a test reading the dead-letter queue waits).
        await incoming.reject(requeue=False)
        self.stats["dead_lettered"] += 1
