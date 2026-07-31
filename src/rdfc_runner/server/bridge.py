import asyncio
import contextlib
import os
import shutil
import tempfile
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
    a socket it accepted. grpcio cannot adopt an existing socket, so the bridge starts
    a unix-domain socket server, points a grpc channel at it (`unix:<path>`), and
    transparently pumps bytes between the TCP connection and the single unix connection
    that grpc opens.

    Usage:
        async with SocketBridge(reader, writer) as channel:
            await Runner(uri).run_with_channel(channel)
    """

    def __init__(self, tcp_reader: asyncio.StreamReader, tcp_writer: asyncio.StreamWriter):
        self._tcp = (tcp_reader, tcp_writer)
        self._tmpdir = tempfile.mkdtemp(prefix="rdfc-")
        self._path = os.path.join(self._tmpdir, "grpc.sock")
        if len(self._path.encode()) >= 100:
            # sun_path is limited to ~104 bytes on macOS (108 on Linux).
            shutil.rmtree(self._tmpdir, ignore_errors=True)
            raise RuntimeError(f"Unix socket path too long for this platform: {self._path}")
        self._claimed = False
        self._unix_server: asyncio.Server | None = None
        self._channel: grpc.aio.Channel | None = None

    async def __aenter__(self) -> grpc.aio.Channel:
        self._unix_server = await asyncio.start_unix_server(self._on_connect, path=self._path)
        # If the injected connection ever rejects the unix-path :authority, add the
        # ("grpc.default_authority", "localhost") channel option here.
        self._channel = grpc.aio.insecure_channel(f"unix:{self._path}")
        return self._channel

    async def _on_connect(self, unix_reader: asyncio.StreamReader, unix_writer: asyncio.StreamWriter) -> None:
        if self._claimed:
            # The TCP socket is single-use: refuse gRPC reconnect attempts so a dead
            # orchestrator connection surfaces as a channel failure instead of hanging.
            logger.warning("Refusing gRPC reconnect attempt on a single-use socket bridge")
            unix_writer.close()
            return
        self._claimed = True
        await pump_pair(self._tcp, (unix_reader, unix_writer))

    async def __aexit__(self, exc_type, exc, tb) -> None:
        if self._channel is not None:
            await self._channel.close()
        # Close the TCP side before waiting on the unix server: the pump (and thereby the
        # unix connection handler) cannot finish while the TCP peer holds its side open.
        tcp_writer = self._tcp[1]
        if not tcp_writer.is_closing():
            tcp_writer.close()
        if self._unix_server is not None:
            self._unix_server.close()
            await self._unix_server.wait_closed()
        with contextlib.suppress(Exception):
            await tcp_writer.wait_closed()
        shutil.rmtree(self._tmpdir, ignore_errors=True)
