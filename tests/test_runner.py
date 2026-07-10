import json
import logging

import pytest
from rdfc_proto import service_pb2

from rdfc_runner.runner import Runner


def make_runner(captured):
    runner = Runner("urn:test:runner")
    runner.logger = logging.getLogger("test")

    async def write(msg):
        captured.append(msg)

    runner._write = write
    return runner


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
