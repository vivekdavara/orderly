"""The transactional outbox (DESIGN.md "The transactional outbox").

A service never publishes from inside a request or a handler: it calls `write` in the same transaction as the change
the message describes, so the message exists if and only if the change committed. A `Relay` publishes the rows
afterwards:

1. claim up to `batch_size` unpublished rows in id order, `FOR UPDATE SKIP LOCKED` (two relays take different rows);
2. publish them all on a confirming channel, then wait for every confirm (or `publish_timeout`);
3. mark the confirmed ones published, in the claim's transaction, and commit.

A crash between 2 and 3 publishes those rows again with the same message ids: at least once, which is why consumers
deduplicate. A row whose publish failed stays unpublished and the others in its batch are still marked. If the failure
is the row's own (the broker returned it as unroutable, or it isn't a message), the row gets its attempt count, its
error and a time before which it isn't claimed again (0.5 s after the failure, doubling to a minute), and the claims
after it go on to the rows behind it. If it is the broker's (no confirm in time, a nack, a closed channel), the row is
left as it was and the relay backs off, as for any outage.

`Relay.run` repeats that until its stop event is set, checking the event only between batches, so a stop never
interrupts a batch between publishing it and marking it. It sleeps between batches until a NOTIFY from `write` (on
commit), a poll interval, or the stop, whichever is first; a lost notification only delays a message by the interval.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import aio_pika
import psycopg
from aio_pika.abc import AbstractChannel, AbstractConnection
from psycopg_pool import AsyncConnectionPool, PoolTimeout

from orderly import broker
from orderly.messages import InvalidMessage, Message, decode, encode
from orderly.topology import Topology

# The LISTEN/NOTIFY channel. Channels are per database, so the payload names the outbox's schema and a relay ignores
# other schemas' notifications: each service has its own database, but the tests share one with a schema each, and a
# relay woken by another schema's writes (or a test expecting silence) was flaky under concurrent runs (day-2 review).
CHANNEL = "orderly_outbox"
# The relay's backoff after an error outside a batch's publishes (the database, the broker connection).
MIN_BACKOFF = 0.1
MAX_BACKOFF = 5.0
# A failed row's own wait before its next attempt: 0.5 s after the first failure, doubling, at most a minute.
ROW_RETRY_FIRST = 0.5
ROW_RETRY_MAX = 60.0
# The failures that belong to a row: the broker returned it (no binding takes its type), or it isn't a message. Every
# other failure (no confirm in time, a nack, a closed channel or connection) is the broker's and shared by all the rows:
# charging it to each one left them all waiting out their own delays after the broker recovered (review round 2).
ROW_FAILURES: tuple[type[Exception], ...] = (aio_pika.exceptions.PublishError, InvalidMessage)

log = logging.getLogger(__name__)


async def write(conn: psycopg.AsyncConnection[Any], message: Message) -> None:
    """Add a message to the outbox in the caller's transaction. The NOTIFY is delivered when that commits (PostgreSQL
    holds notifications until then, and drops them on a rollback)."""
    await conn.execute(
        "INSERT INTO outbox (message_id, message_type, body) VALUES (%s, %s, %s)",
        (message.message_id, message.type, encode(message)),
    )
    await conn.execute("SELECT pg_notify(%s, current_schema())", (CHANNEL,))


class RelayError(Exception):
    """Some rows of a batch weren't published (the rest were, and are marked). `broker` says whether any failure was
    the broker's (the relay then backs off) rather than only the rows' own (each waits for its own retry)."""

    def __init__(self, failed: Sequence[tuple[int, BaseException]], published: int) -> None:
        first_id, first = failed[0]
        super().__init__(f"{len(failed)} not published, first outbox row {first_id}: {type(first).__name__}: {first}")
        self.failed = list(failed)
        self.published = published
        self.broker = any(not isinstance(error, ROW_FAILURES) for _, error in failed)


def describe(error: BaseException) -> str:
    """An error as `last_error` holds it: no NUL (PostgreSQL text can't hold one), no lone surrogate (psycopg can't
    encode one, and the failed UPDATE would take the batch's marks with it), at most 1,000 characters (cut on a
    character, never inside one)."""
    text = f"{type(error).__name__}: {error}".replace("\x00", "\\0")
    return text.encode("utf-8", "backslashreplace").decode("utf-8")[:1000]


@dataclass
class Relay:
    """Publishes one database's outbox to RabbitMQ."""

    pool: AsyncConnectionPool
    connection: AbstractConnection
    topology: Topology
    # The database to LISTEN on (its own connection, outside the pool); None polls only.
    listen_url: str | None = None
    batch_size: int = 100
    poll_interval: float = 1.0
    # How long a batch waits for its confirms before giving up on the missing ones (they are published again later),
    # and for a channel to open. Well inside service.STOP_TIMEOUT, so a shutdown waits for the batch instead of
    # cancelling it.
    publish_timeout: float = 5.0
    # How long the batch's statements wait for a table lock (a migration's ALTER TABLE outbox) before the batch fails
    # and the relay backs off; row locks never make it wait (SKIP LOCKED). Also inside STOP_TIMEOUT's margin.
    lock_timeout: float = 5.0
    _channel: AbstractChannel | None = field(default=None, init=False, repr=False)
    # The listener's server process id while it is connected (tests end it to simulate a lost connection).
    listener_pid: int | None = field(default=None, init=False)

    async def _open_channel(self) -> AbstractChannel:
        if self.connection.is_closed:
            raise ConnectionError("the broker connection is closed")
        # A channel the broker closed (an error on it) or lost with the connection is replaced, not reused.
        if self._channel is None or self._channel.is_closed:
            try:
                # Bounded, so a batch's length is too (service.STOP_TIMEOUT counts on it): a TimeoutError backs off.
                async with asyncio.timeout(self.publish_timeout):
                    self._channel = await broker.open_channel(self.connection)
            except RuntimeError as error:
                # aio-pika ("Connection was not opened") and aiormq ("<Connection ...> closed") raise a bare
                # RuntimeError for a channel opened while a robust connection is reconnecting; the relay died of it
                # after a broker drop. It is a broker outage like any other.
                raise ConnectionError(f"could not open a channel: {error}") from error
        return self._channel

    async def publish_batch(self) -> int:
        """Claim, publish and mark one batch; return how many rows were marked published (0: nothing to do).

        Raises RelayError after marking the rest if some rows couldn't be published.
        """
        channel = await self._open_channel()
        async with self.pool.connection() as conn, conn.transaction():
            await conn.execute("SELECT set_config('lock_timeout', %s, true)", (f"{round(self.lock_timeout * 1000)}ms",))
            rows = await (
                await conn.execute(
                    "SELECT id, body FROM outbox"
                    " WHERE published_at IS NULL AND (next_attempt_at IS NULL OR next_attempt_at <= now())"
                    " ORDER BY id LIMIT %s FOR UPDATE SKIP LOCKED",
                    (self.batch_size,),
                )
            ).fetchall()
            if not rows:
                return 0
            # All published before any confirm is awaited: one round trip per batch instead of one per message.
            tasks = [asyncio.create_task(self._publish(channel, bytes(body))) for _, body in rows]
            try:
                _, pending = await asyncio.wait(tasks, timeout=self.publish_timeout)
            finally:
                # The missing confirms, or all of them if this relay is being cancelled: the transaction then rolls
                # back and the rows are published again later.
                for task in tasks:
                    task.cancel()
            failed: list[tuple[int, BaseException]] = []
            published: list[int] = []
            for (row_id, _), task in zip(rows, tasks, strict=True):
                if task in pending:
                    failed.append((row_id, TimeoutError(f"no confirm within {self.publish_timeout} s")))
                elif task.cancelled():  # aiormq cancels a publish whose connection closed under it
                    failed.append((row_id, ConnectionError("publish cancelled: the broker connection closed")))
                elif (error := task.exception()) is not None:
                    failed.append((row_id, error))
                else:
                    published.append(row_id)
            await asyncio.gather(*pending, return_exceptions=True)
            if published:
                await conn.execute("UPDATE outbox SET published_at = now() WHERE id = ANY(%s)", (published,))
            own = [(row_id, error) for row_id, error in failed if isinstance(error, ROW_FAILURES)]
            if own:
                # clock_timestamp(), not now(): now() is the claim's start, and a failure that came late in a slow batch
                # was due again before it was even recorded (review round 2).
                async with conn.cursor() as cursor:
                    await cursor.executemany(
                        "UPDATE outbox SET attempts = attempts + 1, last_error = %s,"
                        " next_attempt_at = clock_timestamp()"
                        " + least(%s * power(2, least(attempts, 10)), %s) * interval '1 second' WHERE id = %s",
                        [(describe(error), ROW_RETRY_FIRST, ROW_RETRY_MAX, row_id) for row_id, error in own],
                    )
        if failed:
            raise RelayError(failed, published=len(published))
        return len(published)

    async def _publish(self, channel: AbstractChannel, body: bytes) -> None:
        # The stored bytes are the message: decoding only finds its type (and refuses a row that isn't a message).
        message = decode(body)
        await broker.publish(channel, self.topology, message)

    async def run(self, stop: asyncio.Event) -> None:
        """Publish batches until `stop` is set; back off on errors (the broker or the database being down)."""
        wake = asyncio.Event()
        listener = None if self.listen_url is None else asyncio.create_task(self._listen(self.listen_url, wake, stop))
        backoff = MIN_BACKOFF
        try:
            while not stop.is_set():
                wake.clear()  # before the batch: a NOTIFY while it runs means another pass
                try:
                    claimed = await self.publish_batch()
                except RelayError as error:
                    if error.broker:  # the broker's failure: back off as for an outage (below)
                        log.warning("outbox relay: %s; retrying in %.1f s", error, backoff)
                        await wait_any(stop, timeout=backoff)
                        backoff = min(backoff * 2, MAX_BACKOFF)
                        continue
                    # Only rows that failed on their own, and wait for their own retries: nothing is wrong with the
                    # relay, so no backoff (it held every other message up for up to 5 s: day-2 review).
                    log.warning("outbox relay: %s", error)
                    claimed = error.published + len(error.failed)
                except (
                    psycopg.Error,
                    PoolTimeout,
                    aio_pika.exceptions.AMQPError,
                    ConnectionError,
                    OSError,
                    TimeoutError,
                    asyncio.CancelledError,
                ) as error:
                    # aiormq raises CancelledError into whoever awaits an RPC when the connection closes under it
                    # (opening a channel, a confirm), though nobody cancelled this task: found when a service's
                    # broker closed while its relay opened a channel, and the relay died. Only a real cancel ends it.
                    current = asyncio.current_task()
                    if isinstance(error, asyncio.CancelledError) and current is not None and current.cancelling():
                        raise
                    log.warning("outbox relay: %r; retrying in %.1f s", error, backoff)
                    await wait_any(stop, timeout=backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)
                    continue
                except Exception as error:
                    if not broker.closed_without_reason(error):
                        raise
                    log.warning("outbox relay: the broker closed a call (%r); retrying in %.1f s", error, backoff)
                    await wait_any(stop, timeout=backoff)
                    backoff = min(backoff * 2, MAX_BACKOFF)
                    continue
                backoff = MIN_BACKOFF
                if claimed < self.batch_size:  # a full batch probably means more are waiting
                    await wait_any(stop, wake, timeout=self.poll_interval)
        finally:
            if listener is not None:
                listener.cancel()
                await asyncio.gather(listener, return_exceptions=True)

    async def _listen(self, url: str, wake: asyncio.Event, stop: asyncio.Event) -> None:
        """Set `wake` on every NOTIFY for this outbox's schema; reconnect after a lost connection (polling covers the
        gap)."""
        while not stop.is_set():
            try:
                async with await psycopg.AsyncConnection.connect(url, autocommit=True) as conn:
                    await conn.execute(f"LISTEN {CHANNEL}")
                    row = await (await conn.execute("SELECT current_schema()")).fetchone()
                    schema = None if row is None else row[0]
                    self.listener_pid = conn.info.backend_pid
                    # A row committed before LISTEN took effect (at startup, or while the listener was down) sent its
                    # NOTIFY to no one: look once now.
                    wake.set()
                    async for notification in conn.notifies():
                        if notification.payload == schema:
                            wake.set()
            except psycopg.Error as error:
                self.listener_pid = None
                log.warning("outbox relay: listener lost (%s); polling until it reconnects", error)
                await wait_any(stop, timeout=1.0)


async def wait_any(*events: asyncio.Event, timeout: float) -> None:  # noqa: ASYNC109 (the timeout is the point)
    """Until any of the events is set, or the timeout."""
    waiters = [asyncio.create_task(event.wait()) for event in events]
    try:
        await asyncio.wait(waiters, timeout=timeout, return_when=asyncio.FIRST_COMPLETED)
    finally:
        for waiter in waiters:
            waiter.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.gather(*waiters, return_exceptions=True)
