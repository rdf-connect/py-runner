import asyncio
import logging

from rdfc_runner.logger import Logger as GrpcLogger, _ContextRoutingHandler


class FakeStub:
    async def logStream(self, iterator):
        async for _ in iterator:
            pass


def drain(queue: asyncio.Queue):
    messages = []
    while not queue.empty():
        messages.append(queue.get_nowait())
    return messages


async def test_concurrent_runners_get_isolated_log_queues():
    results = {}

    async def runner_task(uri: str):
        grpc_logger = GrpcLogger(FakeStub(), uri)
        logging.getLogger("rdfc.proc").info(f"hello from {uri}")
        # Log from a child task as well: it must inherit the runner's context.
        await asyncio.create_task(child_log(uri))
        results[uri] = drain(grpc_logger._queue)

    async def child_log(uri: str):
        logging.getLogger("rdfc.proc").info(f"child of {uri}")

    await asyncio.gather(runner_task("urn:a"), runner_task("urn:b"))

    for uri in ("urn:a", "urn:b"):
        assert [m.msg for m in results[uri]] == [f"hello from {uri}", f"child of {uri}"]
        assert all(m.entities[0] == uri for m in results[uri])


async def test_routing_handler_installed_only_once():
    GrpcLogger(FakeStub(), "urn:a")
    GrpcLogger(FakeStub(), "urn:b")

    handlers = [h for h in logging.getLogger("rdfc").handlers if isinstance(h, _ContextRoutingHandler)]
    assert len(handlers) == 1


async def test_close_terminates_log_stream():
    grpc_logger = GrpcLogger(FakeStub(), "urn:a")
    task = asyncio.create_task(grpc_logger.run())
    logging.getLogger("rdfc").info("one message")
    grpc_logger.close()

    await asyncio.wait_for(task, timeout=1)
