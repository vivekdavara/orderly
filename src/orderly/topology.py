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

# A message redelivered this many times (a consumer that keeps crashing on it, or keeps requeueing it) is
# dead-lettered instead of cycling forever. Retries a handler means to make go through the wait queues (day 3).
DELIVERY_LIMIT = 10


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

    return match(pattern.split("."), key.split("."))


@dataclass(frozen=True)
class Topology:
    prefix: str = ""

    @property
    def commands(self) -> str:
        return f"{self.prefix}orderly.commands"

    @property
    def events(self) -> str:
        return f"{self.prefix}orderly.events"

    @property
    def dead_letters(self) -> str:
        return f"{self.prefix}orderly.dlx"

    def queue(self, name: str) -> str:
        return f"{self.prefix}{name}"

    def dead_letter_queue(self, name: str) -> str:
        return f"{self.prefix}{name}.dlq"

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

    async def declare(self, channel: AbstractChannel) -> None:
        """Declare everything. Safe to run again; fails if something exists with different settings."""
        exchanges = {
            "commands": await channel.declare_exchange(self.commands, ExchangeType.DIRECT, durable=True),
            "events": await channel.declare_exchange(self.events, ExchangeType.TOPIC, durable=True),
        }
        dead_letters = await channel.declare_exchange(self.dead_letters, ExchangeType.DIRECT, durable=True)
        for spec in QUEUES:
            dlq = await channel.declare_queue(
                self.dead_letter_queue(spec.name), durable=True, arguments={"x-queue-type": "quorum"}
            )
            await dlq.bind(dead_letters, routing_key=self.dead_letter_queue(spec.name))
            queue = await channel.declare_queue(
                self.queue(spec.name), durable=True, arguments=self.queue_arguments(spec.name)
            )
            for binding in spec.bindings:
                await queue.bind(exchanges[spec.exchange], routing_key=binding)

    async def delete(self, channel: AbstractChannel) -> None:
        """Delete everything declare() made (tests use this to clean up their prefixed copy)."""
        for spec in QUEUES:
            await channel.queue_delete(self.queue(spec.name))
            await channel.queue_delete(self.dead_letter_queue(spec.name))
        for name in (self.commands, self.events, self.dead_letters):
            await channel.exchange_delete(name)
