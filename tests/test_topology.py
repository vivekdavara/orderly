import asyncio
import random
import uuid

import aio_pika
import pytest
from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractIncomingMessage

from orderly.broker import from_amqp, open_channel, publish
from orderly.messages import (
    PAYLOADS,
    ChargePayment,
    Message,
    OrderCancelled,
    OrderConfirmed,
    PaymentFailed,
    PaymentRefunded,
    PaymentSucceeded,
    RefundPayment,
    new_message,
)
from orderly.topology import DELIVERY_LIMIT, QUEUES, Topology, topic_matches

ORDER = uuid.UUID("0b8f6f8e-1d1f-4f4e-8a3c-7f0f2b9d5e11")
PAYMENT = uuid.UUID("9a4a1f0e-55b5-4c1d-8a0b-2f2e1d0c9b8a")

ONE_OF_EACH: list[Message] = [
    new_message(payload, correlation_id=ORDER)
    for payload in (
        ChargePayment(order_id=ORDER, customer_id="c-1", amount_cents=500, currency="USD"),
        RefundPayment(order_id=ORDER, amount_cents=500, currency="USD", reason="payment_timeout"),
        PaymentSucceeded(order_id=ORDER, payment_id=PAYMENT, amount_cents=500, currency="USD"),
        PaymentFailed(order_id=ORDER, reason="declined"),
        PaymentRefunded(order_id=ORDER, payment_id=PAYMENT, amount_cents=0, currency="USD"),
        OrderConfirmed(order_id=ORDER, customer_id="c-1", email="a@b.c", total_cents=500, currency="USD"),
        OrderCancelled(order_id=ORDER, customer_id="c-1", email="a@b.c", reason="payment_timeout"),
    )
]


def test_one_of_each_covers_every_type() -> None:
    assert {message.type for message in ONE_OF_EACH} == set(PAYLOADS)


@pytest.mark.parametrize(
    ("pattern", "key", "expected"),
    [
        ("payment.*", "payment.charge", True),
        ("payment.*", "payment", False),
        ("payment.*", "payment.charge.extra", False),
        ("payment.#", "payment", True),
        ("payment.#", "payment.a.b", True),
        ("#", "anything.at.all", True),
        ("*.charge", "payment.charge", True),
        ("#.charge", "charge", True),
        ("a.#.b", "a.b", True),
        ("a.#.b", "a.x.y.b", True),
        ("a.#.b", "a.x.y", False),
        ("order.confirmed", "order.cancelled", False),
    ],
)
def test_topic_matching(pattern: str, key: str, expected: bool) -> None:
    assert topic_matches(pattern, key) is expected


def test_the_spec_sends_each_type_where_the_design_says() -> None:
    topology = Topology()
    assert {kind: topology.routes(kind) for kind in PAYLOADS} == {
        "payment.charge": {"payment.commands"},
        "payment.refund": {"payment.commands"},
        "payment.succeeded": {"order.payment-events"},
        "payment.failed": {"order.payment-events"},
        "payment.refunded": {"order.payment-events"},
        "order.confirmed": {"notification.order-events"},
        "order.cancelled": {"notification.order-events"},
    }
    assert topology.destination("payment.charge") == ("orderly.commands", "payment.charge")
    assert topology.destination("order.confirmed") == ("orderly.events", "order.confirmed")


async def drain(
    channel: AbstractChannel, queue_name: str, *, no_ack: bool = True, limit: int | None = None
) -> list[AbstractIncomingMessage]:
    """Take what is in the queue now (at most `limit`), without waiting."""
    queue = await channel.get_queue(queue_name, ensure=False)
    got: list[AbstractIncomingMessage] = []
    while limit is None or len(got) < limit:
        incoming = await queue.get(no_ack=no_ack, fail=False)
        if incoming is None:
            break
        got.append(incoming)
    return got


async def test_the_broker_routes_every_type_to_exactly_its_queues(channel: AbstractChannel, topology: Topology) -> None:
    for message in ONE_OF_EACH:
        await publish(channel, topology, message)
    for spec in QUEUES:
        got = await drain(channel, topology.queue(spec.name))
        expected = sorted(message.type for message in ONE_OF_EACH if spec.name in topology.routes(message.type))
        assert sorted(from_amqp(incoming).type for incoming in got) == expected, spec.name


async def test_a_delivery_carries_the_body_and_the_ids_as_properties(
    channel: AbstractChannel, topology: Topology
) -> None:
    message = ONE_OF_EACH[0]
    await publish(channel, topology, message)
    [incoming] = await drain(channel, topology.queue("payment.commands"))
    assert from_amqp(incoming) == message
    assert incoming.message_id == str(message.message_id)
    assert incoming.correlation_id == str(message.correlation_id)
    assert incoming.type == "payment.charge"
    assert incoming.content_type == "application/json"
    assert incoming.delivery_mode == aio_pika.DeliveryMode.PERSISTENT


async def test_declaring_again_is_harmless(channel: AbstractChannel, topology: Topology) -> None:
    await publish(channel, topology, ONE_OF_EACH[0])
    await topology.declare(channel)
    assert len(await drain(channel, topology.queue("payment.commands"))) == 1


async def test_a_queue_that_exists_with_other_settings_is_an_error(
    connection: AbstractConnection, topology: Topology
) -> None:
    async with connection.channel() as channel:
        with pytest.raises(aio_pika.exceptions.ChannelPreconditionFailed):
            await channel.declare_queue(topology.queue("payment.commands"), durable=True)  # a classic queue


async def test_a_message_no_queue_takes_is_an_error_not_a_drop(connection: AbstractConnection) -> None:
    # Only the exchanges exist: nothing is bound, so the mandatory publish comes back.
    bare = Topology(prefix=f"test-{uuid.uuid4().hex[:12]}.")
    async with await open_channel(connection) as channel:
        await channel.declare_exchange(bare.commands, aio_pika.ExchangeType.DIRECT, auto_delete=True)
        with pytest.raises(aio_pika.exceptions.DeliveryError):
            await publish(channel, bare, ONE_OF_EACH[0])
        await channel.exchange_delete(bare.commands)


async def test_a_rejected_message_goes_to_the_dead_letter_queue(channel: AbstractChannel, topology: Topology) -> None:
    await publish(channel, topology, ONE_OF_EACH[0])
    queue = await channel.get_queue(topology.queue("payment.commands"), ensure=False)
    incoming = await queue.get(fail=True)
    assert incoming is not None
    await incoming.reject(requeue=False)
    [dead] = await wait_for_messages(channel, topology.dead_letter_queue("payment.commands"), 1)
    assert from_amqp(dead) == ONE_OF_EACH[0]
    assert dead.headers["x-first-death-reason"] == "rejected"
    assert dead.headers["x-first-death-queue"] == topology.queue("payment.commands")


async def test_a_message_whose_consumer_keeps_dying_is_dead_lettered_at_the_limit(
    connection: AbstractConnection, channel: AbstractChannel, topology: Topology
) -> None:
    """A poison message: every consumer that takes it dies before acknowledging (its channel closes)."""
    await publish(channel, topology, ONE_OF_EACH[0])
    deliveries = 0
    while True:
        async with connection.channel() as doomed:
            queue = await doomed.get_queue(topology.queue("payment.commands"), ensure=False)
            got = await wait_for_messages(doomed, queue.name, 1, no_ack=False, missing_ok=True)
            if not got:
                break
            deliveries += 1
            assert deliveries <= DELIVERY_LIMIT + 1, "still being redelivered past the limit"
        # Leaving the block closes the channel with the delivery unacknowledged: a failed delivery.
    [dead] = await wait_for_messages(channel, topology.dead_letter_queue("payment.commands"), 1)
    assert dead.headers["x-first-death-reason"] == "delivery_limit"
    # Delivered once, then redelivered DELIVERY_LIMIT times; the next failure was over the limit.
    assert deliveries == DELIVERY_LIMIT + 1


async def test_requeueing_does_not_count_toward_the_delivery_limit(
    channel: AbstractChannel, topology: Topology
) -> None:
    """RabbitMQ 4 treats basic.nack/reject with requeue as "released", not failed: the count stays put. So a handler
    that requeues on a transient error would loop forever; retries go through the wait queues instead (day 3)."""
    await publish(channel, topology, ONE_OF_EACH[0])
    queue = await channel.get_queue(topology.queue("payment.commands"), ensure=False)
    for _ in range(2 * DELIVERY_LIMIT):
        [incoming] = await wait_for_messages(channel, queue.name, 1, no_ack=False)
        assert "x-delivery-count" not in (incoming.headers or {})
        await incoming.nack(requeue=True)
    assert await drain(channel, topology.dead_letter_queue("payment.commands")) == []


async def wait_for_messages(
    channel: AbstractChannel, queue_name: str, count: int, *, no_ack: bool = True, missing_ok: bool = False
) -> list[AbstractIncomingMessage]:
    """Dead-lettering and requeueing are asynchronous: poll the queue for up to 2 s."""
    got: list[AbstractIncomingMessage] = []
    for _ in range(40):
        got += await drain(channel, queue_name, no_ack=no_ack, limit=count - len(got))
        if len(got) >= count:
            return got
        await asyncio.sleep(0.05)
    if missing_ok:
        return got
    raise AssertionError(f"{queue_name}: {len(got)} of {count} messages after 2 s")


async def test_topic_matching_agrees_with_the_broker(channel: AbstractChannel) -> None:
    """Random patterns bound to queues on a real topic exchange; random keys published; each queue must get exactly
    the keys topic_matches says it should."""
    rng = random.Random(7)
    words = ["a", "b", "c"]
    candidates = {".".join(rng.choice([*words, "*", "#"]) for _ in range(rng.randint(1, 4))) for _ in range(60)}
    # RabbitMQ 4.3 refuses a binding key with more than two '#' (PRECONDITION_FAILED).
    # The empty pattern and key too: zero words (the day-1 review found topic_matches wrong there).
    patterns = sorted({""} | {pattern for pattern in candidates if pattern.split(".").count("#") <= 2})
    keys = sorted({""} | {".".join(rng.choice(words) for _ in range(rng.randint(1, 4))) for _ in range(60)})
    exchange = await channel.declare_exchange(f"test-{uuid.uuid4().hex[:12]}.topics", "topic", auto_delete=True)
    queues = []
    for pattern in patterns:
        queue = await channel.declare_queue(exclusive=True)
        await queue.bind(exchange, routing_key=pattern)
        queues.append(queue)
    for key in keys:
        await exchange.publish(aio_pika.Message(body=key.encode()), routing_key=key)
    checked = 0
    for pattern, queue in zip(patterns, queues, strict=True):
        received = sorted([incoming.body.decode() for incoming in await drain(channel, queue.name)])
        assert received == [key for key in keys if topic_matches(pattern, key)], pattern
        checked += len(keys)
    assert checked > 1000
