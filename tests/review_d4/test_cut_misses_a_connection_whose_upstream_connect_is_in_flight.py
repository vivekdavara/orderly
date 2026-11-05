"""Day-4 review: `chaos.Proxy.cut()` doesn't drop a connection it accepted while that connection's upstream connect was
still in flight.

`cut()` aborts the links in `_links`, but `_accept` only adds its link there after `await asyncio.open_connection(...)`
to the broker returns. A client that connected just before the cut (a robust connection's reconnect, say) is accepted,
the cut runs while its upstream connect is pending, and then the link is made and pumped: that client goes on talking
to the "broken" broker through a proxy that claims every connection was dropped and reconnects are refused. Fix idea:
have `cut()` bump a generation (or set a flag) that `_accept` checks after its connect, aborting both sides if a cut
happened meanwhile.

No broker needed: the upstream is a local echo server, whose connect is held at a gate to open the window.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest
from chaos import Proxy


async def test_a_connection_accepted_before_the_cut_but_linked_after_it_survives_the_cut(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def echo(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        while data := await reader.read(1024):
            writer.write(data)
            await writer.drain()
        writer.close()

    upstream = await asyncio.start_server(echo, "127.0.0.1", 0)
    upstream_port = upstream.sockets[0].getsockname()[1]
    real_open = asyncio.open_connection
    connecting = asyncio.Event()
    gate = asyncio.Event()

    async def slow_upstream(host: str, port: int, **kwargs: Any) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        if port == upstream_port:  # the proxy's connect to its target: held until the test lets it through
            connecting.set()
            await gate.wait()
        return await real_open(host, port, **kwargs)

    monkeypatch.setattr(asyncio, "open_connection", slow_upstream)
    proxy = Proxy("127.0.0.1", upstream_port)
    await proxy.start()
    try:
        reader, writer = await real_open("127.0.0.1", proxy.port)
        await asyncio.wait_for(connecting.wait(), timeout=5)  # accepted; its upstream connect is in flight
        await proxy.cut()
        assert not proxy.is_up
        gate.set()  # the upstream connect completes after the cut

        writer.write(b"still there?")
        await writer.drain()
        try:
            answer = await asyncio.wait_for(reader.read(1024), timeout=5)
        except ConnectionError:
            answer = b""  # reset by the cut: what cut() promises
        writer.transport.abort()
        assert answer == b"", f"a connection through the proxy outlived cut(): the broker answered {answer!r}"
    finally:
        gate.set()
        await proxy.close()
        upstream.close()
        await upstream.wait_closed()
