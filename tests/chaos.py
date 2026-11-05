"""Failure injection for the tests: a TCP proxy in front of RabbitMQ that a test can cut and restore.

The services connect to the broker through the proxy. `cut()` drops every connection through it at once (as a broker
crash or a network partition looks to a client: the socket just ends) and stops listening, so reconnects are refused
until `restore()`. The broker itself keeps running, so the tests can stop "it" without touching the shared node, and
the same test runs in CI against a service container.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from urllib.parse import urlsplit, urlunsplit

CHUNK = 65536


@dataclass
class Proxy:
    target_host: str
    target_port: int
    port: int = 0
    # Connections accepted (the clients' reconnects included), and how many a cut dropped.
    accepted: int = 0
    dropped: int = 0
    # Bumped by every cut: a connection accepted before a cut whose upstream connect finishes after it is dropped too
    # (day-4 review: it went on talking to the "unreachable" broker).
    _cuts: int = field(default=0, init=False, repr=False)
    _server: asyncio.Server | None = field(default=None, init=False, repr=False)
    _links: set[tuple[asyncio.StreamWriter, asyncio.StreamWriter]] = field(default_factory=set, init=False, repr=False)
    _tasks: set[asyncio.Task[None]] = field(default_factory=set, init=False, repr=False)

    @property
    def is_up(self) -> bool:
        return self._server is not None

    async def start(self) -> None:
        """Listen on 127.0.0.1 (a free port the first time, the same port after a cut)."""
        self._server = await asyncio.start_server(self._accept, "127.0.0.1", self.port)
        self.port = self._server.sockets[0].getsockname()[1]

    def url(self, amqp_url: str) -> str:
        """`amqp_url` with its host and port replaced by the proxy's."""
        parts = urlsplit(amqp_url)
        credentials = parts.netloc.rpartition("@")[0]
        netloc = f"{credentials}@127.0.0.1:{self.port}" if credentials else f"127.0.0.1:{self.port}"
        return urlunsplit(parts._replace(netloc=netloc))

    async def cut(self) -> None:
        """Stop listening and drop every connection through the proxy, both ways, without a goodbye: those linked and
        those still connecting upstream."""
        self._cuts += 1
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        for client, upstream in list(self._links):
            client.transport.abort()
            upstream.transport.abort()
            self.dropped += 1
        self._links.clear()

    async def restore(self) -> None:
        if self._server is None:
            await self.start()

    async def close(self) -> None:
        await self.cut()
        for task in list(self._tasks):
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def _accept(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self.accepted += 1
        cuts = self._cuts
        try:
            up_reader, up_writer = await asyncio.open_connection(self.target_host, self.target_port)
        except OSError:
            writer.transport.abort()
            return
        if self._cuts != cuts:  # cut while connecting upstream: dropped like the rest
            up_writer.transport.abort()
            writer.transport.abort()
            self.dropped += 1
            return
        link = (writer, up_writer)
        self._links.add(link)
        pumps = [
            asyncio.create_task(self._pump(reader, up_writer)),
            asyncio.create_task(self._pump(up_reader, writer)),
        ]
        self._tasks.update(pumps)
        try:
            await asyncio.wait(pumps, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for pump in pumps:
                pump.cancel()
            await asyncio.gather(*pumps, return_exceptions=True)
            self._tasks.difference_update(pumps)
            self._links.discard(link)
            for side in link:
                side.transport.abort()

    @staticmethod
    async def _pump(source: asyncio.StreamReader, sink: asyncio.StreamWriter) -> None:
        with contextlib.suppress(ConnectionError, OSError):
            while data := await source.read(CHUNK):
                sink.write(data)
                await sink.drain()


@asynccontextmanager
async def proxy_to(amqp_url: str) -> AsyncIterator[Proxy]:
    """A started proxy to the broker at `amqp_url`, closed afterwards."""
    parts = urlsplit(amqp_url)
    proxy = Proxy(parts.hostname or "127.0.0.1", parts.port or 5672)
    await proxy.start()
    try:
        yield proxy
    finally:
        await proxy.close()
