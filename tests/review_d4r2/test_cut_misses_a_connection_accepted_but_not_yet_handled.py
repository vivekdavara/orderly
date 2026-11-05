"""Defect: `chaos.Proxy.cut()` still misses a connection the proxy accepted just before the cut, when the cut runs
before that connection's `_accept` task has taken its first step.

ceb2acd closes round 1's window (upstream connect in flight) with a generation: `cut()` bumps `_cuts`, and `_accept`
drops its link if `_cuts` changed during the upstream connect. But `_accept` reads the generation (`cuts =
self._cuts`) in its own first step, and asyncio runs that step later than the accept: `StreamReaderProtocol.
connection_made` only creates the `_accept` task, and other ready callbacks (a test task whose `await proxy.cut()`
is due) run before it. A cut in that window bumps `_cuts` first; `_accept` then reads the bumped value as its own, its
upstream connect succeeds against the still-running broker, and the link is made and pumped: the client goes on
talking to the broker through a proxy whose `cut()` returned with `is_up` False and `dropped == 0`.

Scenario (no broker: the upstream is a local echo server; nothing in the proxy is patched: `asyncio.start_server`
wraps the proxy's callback only to wake the test at that moment): a client connects; the instant the proxy's server has
accepted it (asyncio called the proxy's callback, so the `_accept` task exists but hasn't started) the test
cuts.
Expected: the connection is dropped (reset or EOF). Actual: the echo answers through the "cut" proxy.
Fix idea: take the generation synchronously at accept time, e.g. `start_server(lambda r, w: self._accept(r, w,
self._cuts), ...)`, and/or drop the connection at the top of `_accept` when `self._server is None`.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from chaos import Proxy


async def test_a_cut_right_after_the_accept_drops_the_connection(monkeypatch: pytest.MonkeyPatch) -> None:
    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while data := await reader.read(1024):
            writer.write(data)
            await writer.drain()
        writer.close()

    upstream = await asyncio.start_server(echo, "127.0.0.1", 0)
    proxy = Proxy("127.0.0.1", upstream.sockets[0].getsockname()[1])
    accepted = asyncio.Event()
    real_start_server = asyncio.start_server

    async def start_server(client_connected_cb: Any, *args: Any, **kwargs: Any) -> asyncio.Server:
        def on_accept(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> Any:
            # What asyncio calls in connection_made, which makes a task of the coroutine returned: the proxy's own
            # callback, called unchanged, after noting the moment.
            accepted.set()
            return client_connected_cb(reader, writer)

        return await real_start_server(on_accept, *args, **kwargs)

    monkeypatch.setattr(asyncio, "start_server", start_server)  # what Proxy.start calls
    await proxy.start()
    try:
        client = asyncio.create_task(asyncio.open_connection("127.0.0.1", proxy.port))
        async with asyncio.timeout(5):  # not wait_for: its inner task would resume this one a step too late
            await accepted.wait()  # accepted by the proxy; its _accept task created, not yet run
        await proxy.cut()
        assert not proxy.is_up
        reader, writer = await client

        writer.write(b"still there?")
        try:
            await writer.drain()
            answer = await asyncio.wait_for(reader.read(1024), timeout=5)
        except ConnectionError:
            answer = b""  # reset by the cut: what cut() promises
        writer.transport.abort()
        assert answer == b"", (
            f"a connection accepted before cut() outlived it: the broker answered {answer!r}"
            f" (accepted {proxy.accepted}, dropped {proxy.dropped})"
        )
    finally:
        await proxy.close()
        upstream.close()
        await upstream.wait_closed()
