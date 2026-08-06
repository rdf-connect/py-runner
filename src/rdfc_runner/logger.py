import asyncio
import logging
import sys
from contextvars import ContextVar
from logging import getLogger
from typing import Optional, Tuple

import grpc
import grpc.aio
from rdfc_proto import service_pb2_grpc, service_pb2

# Identifies the runner active in the current asyncio context: (runner uri, log message queue).
# Tasks created by a runner inherit its context, so log records emitted anywhere in that task tree
# are routed to the right gRPC log stream, even when multiple runners share one process (server
# mode). Log records emitted from foreign threads (without the context) fall back to stderr.
_log_context: ContextVar[Optional[Tuple[str, asyncio.Queue]]] = ContextVar("rdfc_log_context", default=None)


class _StderrFallbackHandler(logging.StreamHandler):
    """Handles the records no runner context can claim, writing them to stderr.

    The stream is late-bound: redirections of sys.stderr (tests, service managers) keep
    working even though this handler outlives them.
    """

    def __init__(self):
        logging.Handler.__init__(self)
        self.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))

    @property
    def stream(self):
        return sys.stderr


_fallback_handler = _StderrFallbackHandler()


class _ContextRoutingHandler(logging.Handler):
    """Forwards records on the 'rdfc' logger to the log queue of the context's runner.

    Records emitted outside any runner context (a raw thread, an executor that does not
    propagate contextvars) have no orchestrator log stream to go to; they fall back to
    stderr instead of vanishing.
    """

    def emit(self, record: logging.LogRecord) -> None:
        context = _log_context.get()
        if context is None:
            _fallback_handler.handle(record)
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
        """Run the background gRPC log stream until the orchestrator or we close it.

        At the end of a pipeline the orchestrator tears down the connection (in remote
        server mode it simply closes the bridged TCP socket). The still-open log stream RPC
        then terminates with a transport-level status such as UNAVAILABLE ("Socket closed")
        or CANCELLED. That is expected shutdown, not a failure, so it must not bubble up to
        the background-task handler and be logged as a spurious ERROR.
        """
        try:
            await self._stub.logStream(self._message_stream())
        except grpc.aio.AioRpcError as exc:
            if exc.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.CANCELLED):
                getLogger(__name__).debug("Log stream closed during shutdown: %s", exc.code())
                return
            raise
