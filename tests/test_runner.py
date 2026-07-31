import asyncio
import json
import logging

import pytest
from google.protobuf import empty_pb2
from rdfc_proto import service_pb2, service_pb2_grpc

from rdfc_runner.runner import Runner

import fake_processor


def make_runner(captured):
    runner = Runner("urn:test:runner")
    runner.logger = logging.getLogger("test")
    runner._client = None  # Readers and writers only hold on to the stub.

    async def write(msg):
        captured.append(msg)

    runner._write = write
    return runner


def channel_args(reader_uri: str | None = None, writer_uri: str | None = None) -> str:
    arguments = {}
    if reader_uri is not None:
        arguments["incoming"] = {"@type": "https://w3id.org/rdf-connect#Reader", "@id": reader_uri}
    if writer_uri is not None:
        arguments["outgoing"] = {"@type": "https://w3id.org/rdf-connect#Writer", "@id": writer_uri}
    return json.dumps(arguments)


def failing_processor(uri: str = "urn:test:processor", arguments: str = "{}") -> service_pb2.Processor:
    return service_pb2.Processor(
        uri=uri,
        config=json.dumps({"module_path": "nonexistent_module_xyz", "clazz": "Processor"}),
        arguments=arguments,
    )


def working_processor(uri: str = "urn:test:processor", arguments: str = "{}") -> service_pb2.Processor:
    return service_pb2.Processor(
        uri=uri,
        config=json.dumps({"module_path": "fake_processor", "clazz": "FakeProcessor"}),
        arguments=arguments,
    )


async def test_add_processor_reports_import_error_to_orchestrator():
    captured = []
    runner = make_runner(captured)

    processor = service_pb2.Processor(
        uri="urn:test:processor",
        config=json.dumps({"module_path": "nonexistent_module_xyz", "clazz": "Processor"}),
        arguments=json.dumps({}),
    )

    instance = await runner.add_processor(processor)

    assert instance is None
    assert len(captured) == 1
    initialized = captured[0].initialized
    assert initialized.uri == "urn:test:processor"
    assert "ModuleNotFoundError" in initialized.error.cause


async def test_add_processor_reports_init_failure_to_orchestrator():
    captured = []
    runner = make_runner(captured)

    # `json` is importable but has no such class: getattr raises AttributeError.
    processor = service_pb2.Processor(
        uri="urn:test:processor",
        config=json.dumps({"module_path": "json", "clazz": "NoSuchClass"}),
        arguments=json.dumps({}),
    )

    instance = await runner.add_processor(processor)

    assert instance is None
    assert "AttributeError" in captured[0].initialized.error.cause


async def test_failed_processor_leaves_no_orphaned_channels():
    """Channels of a processor that never came up must not stay registered: the orchestrator
    starts the pipeline anyway, and messages routed to them would hang or vanish."""
    captured = []
    runner = make_runner(captured)

    instance = await runner.add_processor(
        failing_processor(arguments=channel_args("urn:channel:in", "urn:channel:out"))
    )

    assert instance is None
    assert runner._readers == {}
    assert runner._writers == {}


async def test_channel_of_a_failed_processor_can_be_recreated():
    captured = []
    runner = make_runner(captured)

    await runner.add_processor(failing_processor("urn:test:broken", channel_args("urn:channel:in")))
    await runner.add_processor(working_processor("urn:test:working", channel_args("urn:channel:in")))

    assert "urn:channel:in" in runner._readers
    assert not captured[-1].initialized.HasField("error")


async def test_failed_processor_keeps_channels_of_earlier_processors():
    captured = []
    runner = make_runner(captured)

    await runner.add_processor(working_processor("urn:test:working", channel_args("urn:channel:shared")))
    established = runner._readers["urn:channel:shared"]

    await runner.add_processor(failing_processor("urn:test:broken", channel_args("urn:channel:shared")))

    # The rollback removes what the failed processor registered, not what was already there.
    assert runner._readers["urn:channel:shared"] is established


class EndingStream:
    """Fake bidi stream that ends immediately without a start message."""

    def __aiter__(self):
        return self

    async def __anext__(self):
        raise StopAsyncIteration

    async def done_writing(self):
        pass


async def test_run_terminates_when_stream_ends_before_start(monkeypatch):
    runner = Runner("urn:test:runner")

    async def fake_connect(stub):
        return EndingStream()

    def fake_initiate_logger(stub):
        runner.logger = logging.getLogger("test")

    monkeypatch.setattr(runner, "connect", fake_connect)
    monkeypatch.setattr(runner, "initiate_logger", fake_initiate_logger)

    with pytest.raises(ConnectionError):
        await runner.run("localhost:1")


class ScriptedStream:
    """Fake bidi stream that yields pushed messages and ends when told to."""

    def __init__(self):
        self._queue = asyncio.Queue()

    def push(self, msg):
        self._queue.put_nowait(msg)

    def end(self):
        self._queue.put_nowait(None)

    def __aiter__(self):
        return self

    async def __anext__(self):
        msg = await self._queue.get()
        if msg is None:
            raise StopAsyncIteration
        return msg

    async def done_writing(self):
        pass


async def test_abnormal_stream_end_cancels_pipeline_tasks(monkeypatch):
    """An orchestrator disappearing mid-pipeline must not leave tasks behind (server mode)."""
    runner = make_runner([])
    stream = ScriptedStream()

    async def fake_connect(stub):
        return stream

    monkeypatch.setattr(runner, "connect", fake_connect)

    fake_processor.BLOCKING_INSTANCES.clear()
    run_task = asyncio.create_task(runner._run_with_stub(None))

    stream.push(service_pb2.ToRunner(proc=service_pb2.Processor(
        uri="urn:test:blocking",
        config=json.dumps({"module_path": "fake_processor", "clazz": "BlockingProcessor"}),
        arguments=json.dumps({}),
    )))
    stream.push(service_pb2.ToRunner(start=empty_pb2.Empty()))

    # Wait until the processor's transform is actually blocked on its never-resolving future.
    while not fake_processor.BLOCKING_INSTANCES:
        await asyncio.sleep(0)
    instance = fake_processor.BLOCKING_INSTANCES[0]
    await asyncio.wait_for(instance.transforming.wait(), timeout=5)
    transforms = list(runner._processor_transforms)
    assert transforms and not transforms[0].done()

    # The orchestrator goes away before the pipeline completed.
    stream.end()

    with pytest.raises(ConnectionError):
        await asyncio.wait_for(run_task, timeout=5)

    results = await asyncio.wait_for(asyncio.gather(*transforms, return_exceptions=True), timeout=5)
    assert all(isinstance(r, asyncio.CancelledError) for r in results)

    # Nothing of this pipeline (transforms, `start()`, the stream listener) is left pending.
    for _ in range(100):
        leftover = [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
        if not leftover:
            break
        await asyncio.sleep(0)
    assert leftover == []


class FlushingLogStub:
    """Stub whose logStream drains the runner's log iterator, yielding between messages."""

    def __init__(self):
        self.messages = []
        self.drained = False

    async def logStream(self, iterator):
        async for msg in iterator:
            # Yielding here models the real RPC's latency: a caller that does not await the
            # log stream task races ahead and closes the channel before these arrive.
            await asyncio.sleep(0)
            self.messages.append(msg)
        self.drained = True
        return empty_pb2.Empty()


async def test_run_with_channel_flushes_trailing_logs(monkeypatch):
    """Logs emitted right before completion reach the orchestrator before the channel closes."""
    stub = FlushingLogStub()
    monkeypatch.setattr(service_pb2_grpc, "RunnerStub", lambda channel: stub)

    runner = Runner("urn:test:runner")

    async def fake_run_with_stub(_stub):
        runner.logger.info("last words")

    monkeypatch.setattr(runner, "_run_with_stub", fake_run_with_stub)

    await runner.run_with_channel(None)

    assert stub.drained
    assert [m.msg for m in stub.messages] == ["Runner started and logger initiated.", "last words"]
