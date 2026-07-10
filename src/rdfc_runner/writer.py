import asyncio
import time
from abc import abstractmethod, ABC
from collections.abc import Callable
from logging import Logger
from typing import AsyncIterable, Optional

from rdfc_proto import service_pb2_grpc, common_pb2, service_pb2

from .convertor import AnyType
from .types import Writable


### Interface ###
class Writer(ABC):
    @property
    @abstractmethod
    def uri(self) -> str:
        """Return the URI of the reader."""
        raise NotImplementedError()

    @abstractmethod
    async def string(self, buffer: str):
        """Write a string to the writer."""
        raise NotImplementedError()

    @abstractmethod
    async def stream(self, buffer: AsyncIterable, transform: Optional[Callable[[object], bytes]] = None):
        """Write a stream of bytes to the writer."""
        raise NotImplementedError()

    @abstractmethod
    async def buffer(self, buffer: bytes) -> None:
        """Write a buffer of bytes to the writer."""
        raise NotImplementedError()

    @abstractmethod
    async def any(self, any_obj: AnyType) -> None:
        """Write an AnyType object to the writer."""
        raise NotImplementedError()

    @abstractmethod
    async def close(self, issued: bool = False) -> None:
        """Close the writer and release resources."""
        raise NotImplementedError()


### Implementations ###
class WriterInstance(Writer):
    local_sequence_number: int
    awaiting_processed: list[tuple[asyncio.Future, float, int]]
    open_streams: int
    should_close: list[asyncio.Future]

    def __init__(self, uri: str, client: service_pb2_grpc.RunnerStub, notify_orchestrator: Writable, runner_id: str,
                 logger: Logger, tracker=None):
        self._uri = uri
        self.client = client
        self.notify_orchestrator = notify_orchestrator
        self.runner_id = runner_id
        self.logger = logger
        self.tracker = tracker
        self.local_sequence_number = 1
        self.awaiting_processed = []
        self.open_streams = 0
        self.should_close = []

    @property
    def uri(self) -> str:
        """Return the URI of the writer."""
        return self._uri

    async def string(self, msg: str):
        """Write a string to the writer."""
        self.logger.debug(f"{self.uri} sends string of {len(msg)} characters")
        encoded = msg.encode("utf-8")
        await self.buffer(encoded)

    async def stream(self, buffer: AsyncIterable, transform: Optional[Callable[[object], bytes]] = None):
        """Write a stream of bytes to the writer."""
        self.open_streams += 1
        transform = transform or (lambda x: x if isinstance(x, bytes) else str(x).encode('utf-8'))

        # Initiate a sending stream with an RPC.sendStreamMessage. (6.3.4.3)
        sending_stream = self.client.sendStreamMessage()
        handled_stream_msg = self.await_processed()
        local_sequence_number = self.local_sequence_number
        self.local_sequence_number += 1

        # Send the stream message notification
        await sending_stream.write(
            common_pb2.StreamChunk(
                id=common_pb2.StreamIdentify(
                    localSequenceNumber=local_sequence_number,
                    channel=self.uri,
                    runner=self.runner_id,
                )
            )
        )

        # Wait for the first message which contains the ID
        msg_id = await self.sending_stream_ready(sending_stream=sending_stream)

        self.logger.debug(f"{self.uri} streams message with id {msg_id}")

        async for msg in buffer:
            # Start a future to start listening for the processed acknowledgment of this chunk we are sending
            chunk_processed_future = asyncio.create_task(self.sending_stream_ready(sending_stream=sending_stream))

            # Send the chunk over the stream
            await sending_stream.write(
                common_pb2.StreamChunk(
                    data=common_pb2.DataChunk(data=transform(msg))
                )
            )

            # Await a message on the stream, indicating that the chunk has been processed
            await chunk_processed_future

        await sending_stream.done_writing()

        await handled_stream_msg
        self.open_streams -= 1

        if len(self.should_close) > 0:
            await self.close()

    async def buffer(self, buffer: bytes) -> None:
        """Write a buffer of bytes to the writer."""
        self.logger.debug(f"{self.uri} sends buffer of {len(buffer)} bytes")
        # Send the message as an RPC.msg over the normal stream. (6.3.4.3)
        local_sequence_number = self.local_sequence_number
        self.local_sequence_number += 1
        processed_msg_future = self.await_processed(len(buffer))

        msg = common_pb2.SendingMessage(
            localSequenceNumber=local_sequence_number,
            channel=self.uri,
            data=buffer,
        )
        await self.notify_orchestrator(
            service_pb2.FromRunner(
                msg=msg
            )
        )
        await processed_msg_future

    async def any(self, any_obj: AnyType) -> None:
        """Write an AnyType object to the writer."""
        if "stream" in any_obj:
            await self.stream(any_obj["stream"])
        elif "buffer" in any_obj:
            await self.buffer(any_obj["buffer"])
        elif "string" in any_obj:
            await self.string(any_obj["string"])
        else:
            raise ValueError("Unsupported AnyType object")

    def await_processed(self, num_bytes: int = 0) -> asyncio.Future:
        """Wait until all messages sent to the writer are processed."""
        event = asyncio.Future()
        self.awaiting_processed.append((event, time.monotonic(), num_bytes))
        return event

    async def sending_stream_ready(self, sending_stream: AsyncIterable) -> int:
        """Wait until the sending stream is ready, and return its stream sequence number."""
        async for chunk in sending_stream:
            return chunk.streamSequenceNumber
        raise Exception("Sending stream finish before finding a streamSequenceNumber")

    async def close(self, issued: bool = False) -> None:
        """
        Gracefully closes this channel.

        Behavior:
        - If there are still active streams, closing is deferred until all streams are closed.
        - If multiple callers invoke `close()` while waiting, their Futures are queued and resolved once the channel actually closes.
        - If this side initiated the close (`issued=True`), a close message is sent to the orchestrator.

        @param issued: Whether this side initiated the close.
        """
        # Case 1: Active streams are still running, so wait until they finish.
        if self.open_streams > 0:
            close_future = asyncio.Future()
            self.should_close.append(close_future)
            await close_future
            return

        # Case 2: No active streams, proceed to close immediately.
        self.logger.debug(f"Closing writer channel {self.uri}")
        if not issued:
            # Inform the orchestrator using an RPC.close message that the processor closed the channel. (6.3.4.4)
            await self.notify_orchestrator(
                service_pb2.FromRunner(
                    close=common_pb2.Close(channel=self.uri)
                )
            )

        # Resolve all pending close requests.
        for future in self.should_close:
            if not future.done():
                future.set_result(None)
        self.should_close.clear()

    def handled(self):
        """Notify that a message has been processed."""
        if len(self.awaiting_processed) > 0:
            event, started_at, num_bytes = self.awaiting_processed.pop(0)
            if self.tracker:
                self.tracker.record_message(num_bytes, (time.monotonic() - started_at) * 1000)
            if not event.done():
                event.set_result(None)
        else:
            self.logger.debug(f"{self.uri} expected to be waiting for a message to be processed, but none found.")
