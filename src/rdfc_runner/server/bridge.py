import asyncio
import contextlib
from logging import getLogger

import grpc.aio

HANDSHAKE_TIMEOUT = 5.0
MAX_URI_BYTES = 1024
PUMP_CHUNK_SIZE = 64 * 1024

logger = getLogger("rdfc_runner.server")


class HandshakeError(Exception):
    """The orchestrator connection did not complete the URI-line handshake."""


async def read_uri_line(reader: asyncio.StreamReader, timeout: float = HANDSHAKE_TIMEOUT) -> str:
    """Read the '<runner-uri>\\n' handshake line the orchestrator sends first.

    Bounded by `MAX_URI_BYTES` (as js-runner is) and a timeout, and EOF-safe. Any bytes
    after the newline remain buffered in `reader`, so the subsequent byte pump starts
    exactly where the handshake ended.
    """
    try:
        line = await asyncio.wait_for(reader.readuntil(b"\n"), timeout)
    except asyncio.IncompleteReadError as e:
        raise HandshakeError("connection closed before the URI line was received") from e
    except asyncio.LimitOverrunError as e:
        raise HandshakeError("URI line exceeds the maximum length") from e
    except TimeoutError as e:
        raise HandshakeError("timed out waiting for the URI line") from e

    if len(line) > MAX_URI_BYTES + 1:
        raise HandshakeError("URI line exceeds the maximum length")

    try:
        uri = line.decode("utf-8").strip()
    except UnicodeDecodeError as e:
        raise HandshakeError("URI line is not valid UTF-8") from e
    if not uri:
        raise HandshakeError("received an empty URI line")
    return uri


async def _pump(src: asyncio.StreamReader, dst: asyncio.StreamWriter) -> None:
    """Copy bytes from `src` to `dst` until EOF, then propagate the half-close."""
    try:
        while True:
            chunk = await src.read(PUMP_CHUNK_SIZE)
            if not chunk:
                break
            dst.write(chunk)
            await dst.drain()
    finally:
        if not dst.is_closing():
            with contextlib.suppress(OSError, RuntimeError, NotImplementedError):
                dst.write_eof()


async def pump_pair(
    a: tuple[asyncio.StreamReader, asyncio.StreamWriter],
    b: tuple[asyncio.StreamReader, asyncio.StreamWriter],
) -> None:
    """Transparently pump bytes in both directions until both sides reach EOF.

    A failure in one direction (e.g. a connection reset) tears down the other; connection
    errors are logged rather than raised, as the gRPC traffic flowing through the pump
    surfaces them to the runner as channel failures anyway.
    """
    tasks = [
        asyncio.create_task(_pump(a[0], b[1])),
        asyncio.create_task(_pump(b[0], a[1])),
    ]
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_EXCEPTION)
    finally:
        for task in tasks:
            task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for result in results:
            if isinstance(result, Exception) and not isinstance(result, asyncio.CancelledError):
                logger.debug(f"Byte pump terminated with {result!r}")
        for writer in (a[1], b[1]):
            if not writer.is_closing():
                writer.close()
        for writer in (a[1], b[1]):
            with contextlib.suppress(Exception):
                await writer.wait_closed()


class SocketBridge:
    """Exposes an already-accepted TCP connection as a grpc.aio channel.

    The orchestrator opens the TCP connection and treats its socket end as an incoming
    connection to its own gRPC server, so this side must act as the HTTP/2 client over
    a socket it accepted. grpcio cannot adopt an existing socket, so the bridge starts a
    loopback TCP server on an ephemeral 127.0.0.1 port, points a grpc channel at it, and
    transparently pumps bytes between the orchestrator's TCP connection and the single
    loopback connection that grpc opens. A loopback listener (rather than a unix-domain
    socket) keeps the bridge working cross-platform, including on Windows.

    That portability costs some access control: a unix socket in a 0700 temp directory is
    reachable only by the same user, while this port is reachable by any process on the
    host, which could connect before grpc does and claim the bridge. The window is short
    and the port is never routable off the machine, but a host running untrusted local
    processes is outside what this server already assumes (see the warning in the README).

    Usage:
        async with SocketBridge(reader, writer) as channel:
            await Runner(uri).run_with_channel(channel)
    """

    def __init__(self, tcp_reader: asyncio.StreamReader, tcp_writer: asyncio.StreamWriter):
        self._tcp = (tcp_reader, tcp_writer)
        self._claimed = False
        self._local_server: asyncio.Server | None = None
        self._channel: grpc.aio.Channel | None = None

    async def __aenter__(self) -> grpc.aio.Channel:
        # Bind loopback on an ephemeral port; the OS picks a free one, which grpc then dials.
        self._local_server = await asyncio.start_server(self._on_connect, "127.0.0.1", 0)
        port = self._local_server.sockets[0].getsockname()[1]
        self._channel = grpc.aio.insecure_channel(f"127.0.0.1:{port}")
        return self._channel

    async def _on_connect(self, local_reader: asyncio.StreamReader, local_writer: asyncio.StreamWriter) -> None:
        if self._claimed:
            # The TCP socket is single-use: refuse gRPC reconnect attempts so a dead
            # orchestrator connection surfaces as a channel failure instead of hanging.
            logger.warning("Refusing gRPC reconnect attempt on a single-use socket bridge")
            local_writer.close()
            return
        self._claimed = True
        await pump_pair(self._tcp, (local_reader, local_writer))

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._channel is not None:
            await self._channel.close()
        # Close the TCP side before waiting on the loopback server: the pump (and thereby
        # the loopback connection handler) cannot finish while the TCP peer holds its side
        # open.
        tcp_writer = self._tcp[1]
        if not tcp_writer.is_closing():
            tcp_writer.close()
        if self._local_server is not None:
            self._local_server.close()
            await self._local_server.wait_closed()
        with contextlib.suppress(Exception):
            await tcp_writer.wait_closed()
