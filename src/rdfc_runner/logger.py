import asyncio
import logging
from logging import getLogger
from typing import Optional

from rdfc_proto import log_pb2
from rdfc_proto import service_pb2_grpc


class Logger:
    def __init__(self, stub: service_pb2_grpc.RunnerStub, uri: str):
        self._stub = stub
        self._queue = asyncio.Queue()
        self._closed = False
        self._worker_task: Optional[asyncio.Task] = None
        self._uri = uri
        self.logger = getLogger('rdfc')
        self._add_logger_handler()

    def _add_logger_handler(self):
        logger_ref = self

        class GrpcHandler(logging.Handler):
            def __init__(self):
                logging.Handler.__init__(self)

            def emit(self, record) -> None:
                log_message = log_pb2.LogMessage(
                    level=record.levelname.lower(),
                    msg=record.getMessage(),
                    entities=[logger_ref._uri] + record.name.split('.')[1:],
                    aliases=[record.name],
                )
                logger_ref._queue.put_nowait(log_message)

        self.logger.addHandler(GrpcHandler())
        self.logger.propagate = False
        self.logger.setLevel(logging.DEBUG)

    def close(self):
        self._closed = True
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
