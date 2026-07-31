import asyncio
import json
import os
import tempfile

import grpc.aio
from google.protobuf import empty_pb2
from rdfc_proto import service_pb2, service_pb2_grpc

from rdfc_runner.server.app import RunnerServer
from rdfc_runner.server.bridge import pump_pair
from rdfc_runner.server.config import ServerConfig

import fake_processor


class FakeOrchestrator(service_pb2_grpc.RunnerServicer):
    """Fake orchestrator that initializes one processor and starts the pipeline."""

    def __init__(self):
        self.identified = asyncio.Future()
        self.initialized = asyncio.Future()
        self.stream_ended = asyncio.Future()

    async def connect(self, request_iterator, context):
        first = await anext(request_iterator)
        self.identified.set_result(first.identify.uri)

        yield service_pb2.ToRunner(pipeline="<urn:pipeline> a <https://w3id.org/rdf-connect#Pipeline>.")
        yield service_pb2.ToRunner(proc=service_pb2.Processor(
            uri="urn:test:processor",
            config=json.dumps({"module_path": "fake_processor", "clazz": "FakeProcessor"}),
            arguments=json.dumps({}),
        ))

        async for msg in request_iterator:
            if msg.HasField("initialized"):
                self.initialized.set_result(msg.initialized)
                break

        yield service_pb2.ToRunner(start=empty_pb2.Empty())

        async for _ in request_iterator:
            pass
        self.stream_ended.set_result(True)

    async def logStream(self, request_iterator, context):
        async for _ in request_iterator:
            pass
        return empty_pb2.Empty()


async def run_orchestrator_session(grpc_port: int, uri: str) -> FakeOrchestrator:
    """Connect to the runner server like the orchestrator does and drive one pipeline."""
    with tempfile.TemporaryDirectory(prefix="rdfc-e2e-") as tmpdir:
        socket_path = os.path.join(tmpdir, "orch.sock")
        servicer = FakeOrchestrator()
        grpc_server = grpc.aio.server()
        service_pb2_grpc.add_RunnerServicer_to_server(servicer, grpc_server)
        grpc_server.add_insecure_port(f"unix:{socket_path}")
        await grpc_server.start()

        try:
            tcp = await asyncio.open_connection("127.0.0.1", grpc_port)
            tcp[1].write(uri.encode() + b"\n")
            unix_conn = await asyncio.open_unix_connection(socket_path)
            pump = asyncio.create_task(pump_pair(tcp, unix_conn))

            assert await asyncio.wait_for(servicer.identified, timeout=5) == uri
            initialized = await asyncio.wait_for(servicer.initialized, timeout=5)
            assert initialized.uri == "urn:test:processor"
            assert not initialized.HasField("error")
            assert await asyncio.wait_for(servicer.stream_ended, timeout=5)
            await asyncio.wait_for(pump, timeout=5)
        finally:
            await grpc_server.stop(grace=None)

        return servicer


async def test_server_runs_two_sequential_pipelines():
    config = ServerConfig(http_port=0, grpc_port=0, processor_paths=[], config_path="unused")
    server = RunnerServer(config)

    tcp_server = await asyncio.start_server(server.handle_orchestrator, "127.0.0.1", 0)
    grpc_port = tcp_server.sockets[0].getsockname()[1]

    fake_processor.INSTANCES.clear()
    try:
        await run_orchestrator_session(grpc_port, "urn:runner:one")
        await run_orchestrator_session(grpc_port, "urn:runner:two")

        # Each session got a fresh runner and a fresh processor instance.
        assert len(fake_processor.INSTANCES) == 2
        for instance in fake_processor.INSTANCES:
            assert instance.events == ["init", "transform", "produce"]

        # All connections cleaned up, both runs archived in the history newest first.
        assert len(server._connections) == 0
        snapshot = server.state.snapshot()
        assert [(r["uri"], r["status"]) for r in snapshot] == [
            ("urn:runner:two", "done"), ("urn:runner:one", "done")
        ]
        assert all(r["disconnectedAt"] is not None for r in snapshot)
    finally:
        await server.shutdown(tcp_server)


async def test_server_shutdown_cancels_active_connection():
    config = ServerConfig(http_port=0, grpc_port=0, processor_paths=[], config_path="unused")
    server = RunnerServer(config)

    tcp_server = await asyncio.start_server(server.handle_orchestrator, "127.0.0.1", 0)
    grpc_port = tcp_server.sockets[0].getsockname()[1]

    # Open a connection that sends a URI but no orchestrator follows, so the
    # runner sits waiting on the bridge.
    reader, writer = await asyncio.open_connection("127.0.0.1", grpc_port)
    writer.write(b"urn:runner:stuck\n")
    await writer.drain()
    while not server._connections:
        await asyncio.sleep(0.01)

    await asyncio.wait_for(server.shutdown(tcp_server), timeout=5)

    assert len(server._connections) == 0
    writer.close()
