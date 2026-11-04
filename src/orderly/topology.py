"""The RabbitMQ topology as code: exchanges, queues, bindings and dead letters (DESIGN.md "RabbitMQ topology").

Every service declares the whole topology when it starts; declaring is idempotent, and a queue that already exists
with other arguments is an error (RabbitMQ refuses the redeclaration), so a drifted broker is noticed at startup.

`Topology(prefix=...)` puts every name under a prefix so tests can declare their own copy on a shared broker.
"""

from __future__ import annotations

from dataclasses import dataclass

import aio_pika
from aio_pika.abc import AbstractChannel, ExchangeType

from orderly.messages import PAYLOADS

# A message whose consumer died this many times more (it crashed, or its connection closed, before acknowledging) is
# dead-lettered instead of cycling forever. A requeue doesn't count (RabbitMQ 4 calls it released, DESIGN.md
# "RabbitMQ topology"), so handlers never requeue: retries go through the wait queues (day 3).
DELIVERY_LIMIT = 10
# How long a message whose handler failed waits before each retry (day 3): it is handled at most len(RETRY_DELAYS) + 1
# times, then dead-lettered. Each delay is a wait queue with that message TTL (`<queue>.retry.<delay>`): RabbitMQ has no
# delayed delivery without a plugin, and a queue's own TTL expires messages in order, unlike per-message TTLs.
RETRY_DELAYS: tuple[float, ...] = (1.0, 5.0, 30.0)
# The delivery's attempt number (1 for the first), carried by the copies sent to the wait queues.
ATTEMPT_HEADER = "x-orderly-attempt"


@dataclass(frozen=True)
class QueueSpec:
    """A work queue: which exchange feeds it, with which binding keys."""

    name: str
    exchange: str  # "commands" or "events"
    bindings: tuple[str, ...]


QUEUES = (
    QueueSpec("payment.commands", "commands", ("payment.charge", "payment.refund")),
    QueueSpec("order.payment-events", "events", ("payment.*",)),
    QueueSpec("notification.order-events", "events", ("order.confirmed", "order.cancelled")),
)


def topic_matches(pattern: str, key: str) -> bool:
    """AMQP topic matching: words split on '.', '*' is exactly one word, '#' is zero or more."""

    def match(p: list[str], k: list[str]) -> bool:
        if not p:
            return not k
        if p[0] == "#":
            return any(match(p[1:], k[i:]) for i in range(len(k) + 1))
        if not k:
            return False
        return (p[0] == "*" or p[0] == k[0]) and match(p[1:], k[1:])

    # The empty string is zero words, not one empty word: RabbitMQ delivers a message with key "" to a binding "" or
    # "#", never to "*" (the day-1 review found topic_matches("*", "") was True).
    return match(pattern.split(".") if pattern else [], key.split(".") if key else [])


def delay_label(delay: float) -> str:
    """A wait queue's name for its delay: `5s`, or `250ms` for a delay that isn't whole seconds."""
    millis = round(delay * 1000)
    return f"{millis // 1000}s" if millis % 1000 == 0 else f"{millis}ms"


@dataclass(frozen=True)
class Topology:
    prefix: str = ""
    # Tests use shorter delays; the services use RETRY_DELAYS.
    retry_delays: tuple[float, ...] = RETRY_DELAYS

    def __post_init__(self) -> None:
        millis = [round(delay * 1000) for delay in self.retry_delays]
        if any(m < 1 for m in millis) or len(set(millis)) != len(millis):
            raise ValueError(f"retry delays must be distinct and at least 1 ms: {self.retry_delays}")

    @property
    def commands(self) -> str:
        return f"{self.prefix}orderly.commands"

    @property
    def events(self) -> str:
        return f"{self.prefix}orderly.events"

    @property
    def dead_letters(self) -> str:
        return f"{self.prefix}orderly.dlx"

    @property
    def retry(self) -> str:
        return f"{self.prefix}orderly.retry"

    def queue(self, name: str) -> str:
        return f"{self.prefix}{name}"

    def dead_letter_queue(self, name: str) -> str:
        return f"{self.prefix}{name}.dlq"

    def retry_queue(self, name: str, delay: float) -> str:
        """The wait queue that holds a message of the work queue `name` for `delay` seconds, then gives it back."""
        return f"{self.prefix}{name}.retry.{delay_label(delay)}"

    def destination(self, message_type: str) -> tuple[str, str]:
        """The exchange and routing key a message of this type is published with."""
        kind = PAYLOADS[message_type].KIND
        return (self.commands if kind == "command" else self.events), message_type

    def routes(self, message_type: str) -> set[str]:
        """The work queues (unprefixed names) a message of this type reaches, by the spec's bindings."""
        kind = "commands" if PAYLOADS[message_type].KIND == "command" else "events"
        return {
            spec.name
            for spec in QUEUES
            if spec.exchange == kind
            and any(
                # The commands exchange is direct: a binding key matches only itself.
                topic_matches(binding, message_type) if kind == "events" else binding == message_type
                for binding in spec.bindings
            )
        }

    def queue_arguments(self, name: str) -> dict[str, aio_pika.abc.FieldValue]:
        return {
            "x-queue-type": "quorum",
            "x-delivery-limit": DELIVERY_LIMIT,
            "x-dead-letter-exchange": self.dead_letters,
            "x-dead-letter-routing-key": self.dead_letter_queue(name),
            # At-least-once dead-lettering: RabbitMQ keeps a dead-lettered message until the dead-letter queue has
            # confirmed it (the default, at-most-once, can lose it). It requires reject-publish overflow.
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
        }

    def dead_letter_queue_arguments(self) -> dict[str, aio_pika.abc.FieldValue]:
        return {
            "x-queue-type": "quorum",
            # The last stop: nothing may remove a message from here but an acknowledgement. Quorum queues have a
            # delivery limit of 20 by default on RabbitMQ 4, and with no dead-letter exchange behind this queue a
            # message whose consumer died 21 times (an inspector crashing on it) was dropped (day-1 review). -1 means
            # no limit.
            "x-delivery-limit": -1,
        }

    def retry_queue_arguments(self, name: str, delay: float) -> dict[str, aio_pika.abc.FieldValue]:
        return {
            "x-queue-type": "quorum",
            "x-message-ttl": round(delay * 1000),
            # Expired: back to the work queue, through the default exchange (every queue is bound to it by name).
            "x-dead-letter-exchange": "",
            "x-dead-letter-routing-key": self.queue(name),
            "x-dead-letter-strategy": "at-least-once",
            "x-overflow": "reject-publish",
        }

    async def declare(self, channel: AbstractChannel) -> None:
        """Declare everything. Safe to run again; fails if something exists with different settings."""
        exchanges = {
            "commands": await channel.declare_exchange(self.commands, ExchangeType.DIRECT, durable=True),
            "events": await channel.declare_exchange(self.events, ExchangeType.TOPIC, durable=True),
        }
        dead_letters = await channel.declare_exchange(self.dead_letters, ExchangeType.DIRECT, durable=True)
        retry = await channel.declare_exchange(self.retry, ExchangeType.DIRECT, durable=True)
        for spec in QUEUES:
            dlq = await channel.declare_queue(
                self.dead_letter_queue(spec.name), durable=True, arguments=self.dead_letter_queue_arguments()
            )
            await dlq.bind(dead_letters, routing_key=self.dead_letter_queue(spec.name))
            queue = await channel.declare_queue(
                self.queue(spec.name), durable=True, arguments=self.queue_arguments(spec.name)
            )
            for binding in spec.bindings:
                await queue.bind(exchanges[spec.exchange], routing_key=binding)
            for delay in self.retry_delays:
                name = self.retry_queue(spec.name, delay)
                wait = await channel.declare_queue(
                    name, durable=True, arguments=self.retry_queue_arguments(spec.name, delay)
                )
                await wait.bind(retry, routing_key=name)

    async def delete(self, channel: AbstractChannel) -> None:
        """Delete everything declare() made (tests use this to clean up their prefixed copy)."""
        for spec in QUEUES:
            await channel.queue_delete(self.queue(spec.name))
            await channel.queue_delete(self.dead_letter_queue(spec.name))
            for delay in self.retry_delays:
                await channel.queue_delete(self.retry_queue(spec.name, delay))
        for name in (self.commands, self.events, self.dead_letters, self.retry):
            await channel.exchange_delete(name)
