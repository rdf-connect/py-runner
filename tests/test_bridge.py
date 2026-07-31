import asyncio
import contextlib
import os
import tempfile

import grpc.aio
import pytest
from google.protobuf import empty_pb2
from rdfc_proto import service_pb2, service_pb2_grpc

from rdfc_runner.runner import Runner
from rdfc_runner.server.bridge import (
    MAX_URI_BYTES,
    HandshakeError,
    SocketBridge,
    pump_pair,
    read_uri_line,
)


async def tcp_pair():
    """An (accepted, client) pair of asyncio TCP stream connections on localhost."""
    accepted = asyncio.Future()

    def on_connect(reader, writer):
        accepted.set_result((reader, writer))

    server = await asyncio.start_server(on_connect, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = await asyncio.open_connection("127.0.0.1", port)
    server_side = await accepted
    # Stop listening; the accepted connection stays usable. (No wait_closed here:
    # since 3.12.1 it would block until the accepted connection is closed too.)
    server.close()
    return server_side, client


### read_uri_line ###

async def test_read_uri_line_plain():
    (reader, writer), (_, client_writer) = await tcp_pair()
    client_writer.write(b"urn:test:runner\n")

    assert await read_uri_line(reader) == "urn:test:runner"
    writer.close()
    client_writer.close()


async def test_read_uri_line_strips_crlf():
    (reader, writer), (_, client_writer) = await tcp_pair()
    client_writer.write(b"urn:test:runner\r\n")

    assert await read_uri_line(reader) == "urn:test:runner"
    writer.close()
    client_writer.close()


async def test_read_uri_line_preserves_leftover_bytes():
    # The HTTP/2 preface may arrive in the same packet as the URI line; it must stay
    # in the reader's buffer for the byte pump.
    (reader, writer), (_, client_writer) = await tcp_pair()
    client_writer.write(b"urn:test:runner\nPRI * HTTP/2.0")

    assert await read_uri_line(reader) == "urn:test:runner"
    assert await reader.read(14) == b"PRI * HTTP/2.0"
    writer.close()
    client_writer.close()


async def test_read_uri_line_eof_before_newline():
    (reader, writer), (_, client_writer) = await tcp_pair()
    client_writer.write(b"urn:test:runner")
    client_writer.close()

    with pytest.raises(HandshakeError, match="closed"):
        await read_uri_line(reader)
    writer.close()


async def test_read_uri_line_oversized():
    # A complete line that fits the stream buffer but exceeds MAX_URI_BYTES.
    (reader, writer), (_, client_writer) = await tcp_pair()
    client_writer.write(b"x" * (MAX_URI_BYTES + 1) + b"\n")

    with pytest.raises(HandshakeError, match="maximum length"):
        await read_uri_line(reader)
    writer.close()
    client_writer.close()


async def test_read_uri_line_at_the_size_limit():
    (reader, writer), (_, client_writer) = await tcp_pair()
    uri = "x" * MAX_URI_BYTES
    client_writer.write(uri.encode() + b"\n")

    assert await read_uri_line(reader) == uri
    writer.close()
    client_writer.close()


async def test_read_uri_line_exceeds_stream_buffer():
    # The backstop for a line that never fits the stream buffer at all.
    (reader, writer), (_, client_writer) = await tcp_pair()
    client_writer.write(b"x" * (64 * 1024 + 1))

    with pytest.raises(HandshakeError, match="maximum length"):
        await read_uri_line(reader)
    writer.close()
    client_writer.close()


async def test_read_uri_line_timeout():
    (reader, writer), (_, client_writer) = await tcp_pair()

    with pytest.raises(HandshakeError, match="timed out"):
        await read_uri_line(reader, timeout=0.05)
    writer.close()
    client_writer.close()


async def test_read_uri_line_empty():
    (reader, writer), (_, client_writer) = await tcp_pair()
    client_writer.write(b"\n")

    with pytest.raises(HandshakeError, match="empty"):
        await read_uri_line(reader)
    writer.close()
    client_writer.close()


### pump_pair ###

async def test_pump_pair_bidirectional_byte_fidelity():
    (a_reader, a_writer), (a_peer_reader, a_peer_writer) = await tcp_pair()
    (b_reader, b_writer), (b_peer_reader, b_peer_writer) = await tcp_pair()

    pump = asyncio.create_task(pump_pair((a_reader, a_writer), (b_reader, b_writer)))

    payload = bytes(range(256)) * 4096  # 1 MiB
    a_peer_writer.write(payload)
    a_peer_writer.write_eof()
    b_peer_writer.write(payload[::-1])
    b_peer_writer.write_eof()

    forward, backward = await asyncio.gather(b_peer_reader.read(-1), a_peer_reader.read(-1))

    assert forward == payload
    assert backward == payload[::-1]
    await asyncio.wait_for(pump, timeout=5)


async def test_pump_pair_survives_abrupt_reset():
    (a_reader, a_writer), (a_peer_reader, a_peer_writer) = await tcp_pair()
    (b_reader, b_writer), (b_peer_reader, b_peer_writer) = await tcp_pair()

    pump = asyncio.create_task(pump_pair((a_reader, a_writer), (b_reader, b_writer)))

    a_peer_writer.write(b"some data")
    # Abort the peer connection without a clean shutdown.
    a_peer_writer.transport.abort()

    # The other peer observes the termination (as data+EOF or a reset) and hangs up,
    # like a real gRPC endpoint would.
    with contextlib.suppress(ConnectionError):
        await asyncio.wait_for(b_peer_reader.read(-1), timeout=5)
    b_peer_writer.close()

    # The pump must terminate without raising.
    await asyncio.wait_for(pump, timeout=5)


### SocketBridge against a fake orchestrator ###

class FakeOrchestrator(service_pb2_grpc.RunnerServicer):
    """Real grpc.aio server acting as the orchestrator side of the protocol."""

    def __init__(self):
        self.identified = asyncio.Future()
        self.stream_ended = asyncio.Future()
        self.log_messages = []

    async def connect(self, request_iterator, context):
        first = await anext(request_iterator)
        assert first.HasField("identify")
        self.identified.set_result(first.identify.uri)

        yield service_pb2.ToRunner(pipeline="<urn:pipeline> a <https://w3id.org/rdf-connect#Pipeline>.")
        yield service_pb2.ToRunner(start=empty_pb2.Empty())

        async for _ in request_iterator:
            pass
        self.stream_ended.set_result(True)

    async def logStream(self, request_iterator, context):
        async for msg in request_iterator:
            self.log_messages.append(msg)
        return empty_pb2.Empty()


async def test_socket_bridge_end_to_end_with_fake_orchestrator():
    with tempfile.TemporaryDirectory(prefix="rdfc-test-") as tmpdir:
        orchestrator_socket = os.path.join(tmpdir, "orch.sock")

        # The fake orchestrator's gRPC server, reachable over a unix socket.
        servicer = FakeOrchestrator()
        grpc_server = grpc.aio.server()
        service_pb2_grpc.add_RunnerServicer_to_server(servicer, grpc_server)
        grpc_server.add_insecure_port(f"unix:{orchestrator_socket}")
        await grpc_server.start()

        # The runner server side: accept a TCP connection, read the URI, run a Runner
        # over the bridged channel. This mirrors what the accept loop will do.
        runner_done = asyncio.Future()

        async def handle_connection(reader, writer):
            try:
                uri = await read_uri_line(reader)
                async with SocketBridge(reader, writer) as channel:
                    await Runner(uri).run_with_channel(channel)
                runner_done.set_result(True)
            except Exception as e:  # pragma: no cover - failure reporting
                runner_done.set_exception(e)

        tcp_server = await asyncio.start_server(handle_connection, "127.0.0.1", 0)
        port = tcp_server.sockets[0].getsockname()[1]

        # The fake orchestrator's instantiator: open TCP, send the URI line, then treat
        # its socket end as an incoming connection to its own gRPC server (byte-pumped
        # into a fresh unix connection, standing in for grpc-js connection injection).
        orch_reader, orch_writer = await asyncio.open_connection("127.0.0.1", port)
        orch_writer.write(b"urn:test:runner\n")
        unix_conn = await asyncio.open_unix_connection(orchestrator_socket)
        injector_pump = asyncio.create_task(pump_pair((orch_reader, orch_writer), unix_conn))

        # identify must arrive with the URI from the handshake line.
        assert await asyncio.wait_for(servicer.identified, timeout=5) == "urn:test:runner"
        # With no processors, start completes immediately and the runner half-closes.
        assert await asyncio.wait_for(servicer.stream_ended, timeout=5)
        assert await asyncio.wait_for(runner_done, timeout=5)
        await asyncio.wait_for(injector_pump, timeout=5)

        # The runner forwarded its logs over the bridged logStream RPC.
        assert any("Runner started" in msg.msg for msg in servicer.log_messages)

        tcp_server.close()
        await tcp_server.wait_closed()
        await grpc_server.stop(grace=None)
