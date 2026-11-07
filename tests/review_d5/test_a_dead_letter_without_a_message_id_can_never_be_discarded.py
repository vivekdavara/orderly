"""Day-5 review: a dead letter that doesn't decode and has no AMQP message_id can never be removed with the tool.

`describe` takes an undecodable dead letter's id from the AMQP `message_id` property, which is None when the producer
didn't set one (any foreign or broken publisher: the day-5 health test itself publishes exactly such a message,
`aio_pika.Message(b"junk")`). `discard` only acts on `--id MESSAGE_ID` (`letter.message_id in ids`), and there is no
`discard --all`, so nothing the command line accepts matches it: `list` prints `?` for its id, `discard --id ?` says
"not found", and the message stays in `<queue>.dlq` for good. Consequences: /health's dead-letter count never returns
to 0 (an alert that can't be cleared), and every later `replay --all` exits 1 because of it ("not replayable"),
so a script can't tell a real failure from this permanent junk. The module docstring promises the opposite: a message
that can't be replayed "is kept, and can be discarded by its id".

Python producers built on aiormq (aio-pika included) never send such a message, because aiormq fills in a random
message_id when the publisher leaves it out; pika, the Java and Go clients, rabbitmqadmin and the management UI's
"Publish message" do not. The test stands in for one of those by stopping aiormq's fill-in for its one publish (the
producer side only; the tool runs unpatched). It kills such a message, reads the id the tool itself shows for it
(text and JSON), and tries to discard it with each; it expects one of them to remove it.
"""

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import AsyncIterator
from types import SimpleNamespace

import aio_pika
import aiormq.channel
import pytest
from aio_pika.abc import AbstractChannel, AbstractConnection

from orderly.dead_letters import main_async
from orderly.topology import Topology

QUEUE = "payment.commands"


@pytest.fixture
async def short_topology(connection: AbstractConnection) -> AsyncIterator[Topology]:
    topology = Topology(prefix=f"test-{uuid.uuid4().hex[:12]}.", retry_delays=(0.1, 0.2))
    async with connection.channel() as setup:
        await topology.declare(setup)
    yield topology
    async with connection.channel() as cleanup:
        await topology.delete(cleanup)


async def depth(channel: AbstractChannel, name: str) -> int:
    queue = await channel.declare_queue(name, passive=True)
    assert queue.declaration_result is not None
    return int(queue.declaration_result.message_count or 0)


async def cli(connection: AbstractConnection, topology: Topology, *argv: str) -> tuple[int, list[str], str]:
    import contextlib
    import io

    out, err = io.StringIO(), io.StringIO()
    with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
        status = await main_async(list(argv), connection, topology)
    return status, out.getvalue().splitlines(), err.getvalue()


async def test_a_dead_letter_without_a_message_id_can_be_discarded(
    connection: AbstractConnection, channel: AbstractChannel, short_topology: Topology, monkeypatch: pytest.MonkeyPatch
) -> None:
    dlq = short_topology.dead_letter_queue(QUEUE)
    with monkeypatch.context() as patch:
        # A producer that isn't aiormq: no message_id property at all.
        patch.setattr(aiormq.channel, "UUID", lambda **_: SimpleNamespace(hex=None))
        await channel.default_exchange.publish(
            aio_pika.Message(b"junk"), routing_key=short_topology.queue(QUEUE), mandatory=True
        )
    work = await channel.get_queue(short_topology.queue(QUEUE), ensure=False)
    deadline = time.monotonic() + 5
    while (incoming := await work.get(no_ack=False, fail=False)) is None:
        assert time.monotonic() < deadline
        await asyncio.sleep(0.01)
    assert incoming.message_id is None
    await incoming.reject(requeue=False)
    while await depth(channel, dlq) != 1:
        assert time.monotonic() < deadline
        await asyncio.sleep(0.02)

    status, out, _ = await cli(connection, short_topology, "list", QUEUE)
    assert status == 0
    [line] = [line for line in out if line.startswith("  ")]
    shown_id = line.split()[1]
    status, out, _ = await cli(connection, short_topology, "list", QUEUE, "--json")
    [record] = [json.loads(line) for line in out]

    tried: dict[str, int] = {}
    for candidate in {shown_id, str(record["message_id"])}:
        status, _, _ = await cli(connection, short_topology, "discard", QUEUE, "--id", candidate)
        tried[candidate] = status
    await asyncio.sleep(0.5)  # the quorum queue's count lags an acknowledgement
    left = await depth(channel, dlq)
    assert left == 0, (
        f"the dead letter is still in {dlq} ({left} waiting): listed with id {shown_id!r}"
        f" (JSON {record['message_id']!r}), and `discard --id` with each exited {tried};"
        " the command line has no other way to remove it"
    )
