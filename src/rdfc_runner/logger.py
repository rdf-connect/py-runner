import asyncio
import logging
from contextvars import ContextVar
from logging import getLogger
from typing import Optional, Tuple

from rdfc_proto import service_pb2_grpc, service_pb2

# Identifies the runner active in the current asyncio context: (runner uri, log message queue).
# Tasks created by a runner inherit its context, so log records emitted anywhere in that task tree
# are routed to the right gRPC log stream, even when multiple runners share one process (server
# mode). Log records emitted from foreign threads (without the context) are dropped.
_log_context: ContextVar[Optional[Tuple[str, asyncio.Queue]]] = ContextVar("rdfc_log_context", default=None)


class _ContextRoutingHandler(logging.Handler):
    """Forwards records on the 'rdfc' logger to the log queue of the context's runner."""

    def emit(self, record: logging.LogRecord) -> None:
        context = _log_context.get()
        if context is None:
            return
        uri, queue = context
        log_message = service_pb2.LogMessage(
            level=record.levelname.lower(),
            msg=record.getMessage(),
            entities=[uri] + record.name.split('.')[1:],
            aliases=[record.name],
        )
        queue.put_nowait(log_message)


def _install_handler_once() -> None:
    logger = getLogger('rdfc')
    if not any(isinstance(handler, _ContextRoutingHandler) for handler in logger.handlers):
        logger.addHandler(_ContextRoutingHandler())
        logger.propagate = False
        logger.setLevel(logging.DEBUG)


class Logger:
    def __init__(self, stub: service_pb2_grpc.RunnerStub, uri: str):
        self._stub = stub
        self._queue = asyncio.Queue()
        self._uri = uri
        self.logger = getLogger('rdfc')
        _install_handler_once()
        # Route all log records emitted in the current asyncio context (and tasks spawned from it)
        # to this runner's queue.
        _log_context.set((uri, self._queue))

    def close(self):
        self._queue.put_nowait(None)

    async def _message_stream(self):
        while True:
            msg = await self._queue.get()
            if msg is None:
                break
            yield msg

    async def run(self):
        """Run the background gRPC log stream."""
        await self._stub.logStream(self._message_stream())
