import asyncio
from abc import abstractmethod, ABC
from collections.abc import AsyncIterable
from logging import Logger
from typing import AsyncGenerator, List, Awaitable

from rdfc_proto import common_pb2, service_pb2_grpc, service_pb2

from .convertor import AnyType
from .convertor import StringConvertor, StreamConvertor, NoConvertor, AnyConvertor
from .iterable import MyIter
from .types import Writable
from .utils import fanout_stream, spawn_logged


### Interface ###
class Reader(ABC):
    @property
    @abstractmethod
    def uri(self) -> str:
        """Return the URI of the reader."""
        raise NotImplementedError()

    @abstractmethod
    def strings(self) -> AsyncIterable[str]:
        """Return an async iterator of strings."""
        raise NotImplementedError()

    @abstractmethod
    def streams(self) -> AsyncIterable[AsyncGenerator[bytes, None]]:
        """Return an async iterator of byte streams."""
        raise NotImplementedError()

    @abstractmethod
    def buffers(self) -> AsyncIterable[bytes]:
        """Return an async iterator of byte buffers."""
        raise NotImplementedError()

    @abstractmethod
    def anys(self) -> AsyncIterable[AnyType]:
        """Return an async iterator of AnyType."""
        raise NotImplementedError()

    @abstractmethod
    def handle_msg(self, msg: common_pb2.ReceivingMessage):
        """Handle a message from the orchestrator."""
        raise NotImplementedError()

    @abstractmethod
    def handle_streaming_msg(self, msg: common_pb2.ReceivingStreamMessage):
        """Handle a streaming message from the orchestrator."""
        raise NotImplementedError()

    @abstractmethod
    def close(self):
        """Close the reader and release resources."""
        raise NotImplementedError()


### Implementations ###
class ReaderInstance(Reader):
    def __init__(self, uri: str, client: service_pb2_grpc.RunnerStub, notify_orchestrator: Writable, logger: Logger,
                 tracker=None):
        self._uri = uri
        self.client = client
        self.notify_orchestrator = notify_orchestrator
        self.logger = logger
        self.tracker = tracker
        self.consumers: List[MyIter] = []
        self.closed = False
        # Released on close: consumers that are dropped while suspended inside a stream
        # message never run their `finally`, so the fan-out barrier needs a way out.
        self._abort = asyncio.Event()

    @property
    def uri(self) -> str:
        """Return the URI of the reader."""
        return self._uri

    def strings(self) -> AsyncIterable[str]:
        """Return an async iterator of strings."""
        my_iter = MyIter(StringConvertor())
        self.consumers.append(my_iter)
        return my_iter

    def streams(self) -> AsyncIterable[AsyncGenerator[bytes, None]]:
        """Return an async iterator of byte streams."""
        my_iter = MyIter(StreamConvertor())
        self.consumers.append(my_iter)
        return my_iter

    def buffers(self) -> AsyncIterable[bytes]:
        """Return an async iterator of byte buffers."""
        my_iter = MyIter(NoConvertor())
        self.consumers.append(my_iter)
        return my_iter

    def anys(self) -> AsyncIterable[AnyType]:
        """Return an async iterator of AnyType."""
        my_iter = MyIter(AnyConvertor())
        self.consumers.append(my_iter)
        return my_iter

    def handle_msg(self, msg: common_pb2.ReceivingMessage):
        """Handle a message from the orchestrator."""
        self.logger.debug(f"{self.uri} handling incoming message of {len(msg.data)} bytes")
        if self.tracker:
            self.tracker.record_message(len(msg.data))

        if self.closed:
            # The consumers are gone; ack anyway so the orchestrator is not left waiting.
            self.logger.debug(f"{self.uri} is closed, message {msg.globalSequenceNumber} is not processed")
            spawn_logged(self._notify_processed(msg.globalSequenceNumber, msg.channel), self.logger,
                         f"ack of message on closed reader {self.uri}")
            return

        async def push_to_consumer(consumer: MyIter, data: bytes):
            future = asyncio.Future()
            consumer.push(data, lambda: future.set_result(None))
            await future

        async def push_to_consumers():
            # Wait for all consumers to process the message
            await asyncio.gather(
                *[
                    push_to_consumer(consumer, msg.data)
                    for consumer in self.consumers
                ]
            )

            # Notify the orchestrator after all consumers have processed the message
            await self._notify_processed(msg.globalSequenceNumber, msg.channel)

        spawn_logged(push_to_consumers(), self.logger, f"push message to consumers of {self.uri}")

    async def _notify_processed(self, global_sequence_number: int, channel: str) -> None:
        await self.notify_orchestrator(
            service_pb2.FromRunner(
                processed=common_pb2.GlobalAck(
                    globalSequenceNumber=global_sequence_number,
                    channel=channel,
                )
            )
        )

    async def handle_streaming_msg(self, msg: common_pb2.ReceivingStreamMessage):
        """Handle a streaming message from the orchestrator."""
        self.logger.debug(
            f"{self.uri} handling incoming streaming message with global sequence number {msg.globalSequenceNumber}")

        if self.closed:
            # No consumer can read this stream any more; ack it without opening one.
            self.logger.debug(f"{self.uri} is closed, stream {msg.globalSequenceNumber} is not processed")
            await self._notify_processed(msg.globalSequenceNumber, msg.channel)
            return

        # Start a receiving stream to receive the streaming messages over the stream message channel.
        receiving_stream = self.client.receiveStreamMessage()

        idx = 0

        async def write_sending_stream_control_message(message: common_pb2.SendingStreamControl = None) -> None:
            if message is not None:
                await receiving_stream.write(message)
            else:
                nonlocal idx
                await receiving_stream.write(common_pb2.SendingStreamControl(streamSequenceNumber=idx))
                idx += 1

        # Set once the last chunk was taken from the orchestrator's stream (also when the
        # fan-out only drained it because nobody was left to consume it).
        stream_exhausted = asyncio.Event()

        async def tracked_chunks():
            try:
                async for chunk in receiving_stream:
                    if self.tracker:
                        self.tracker.record_message(len(chunk.data))
                    yield chunk
            finally:
                stream_exhausted.set()

        # fan out the stream to all iterators
        consumers_done: List[Awaitable[None]] = []

        stream_iters = fanout_stream(
            tracked_chunks(),
            len(self.consumers),
            write_sending_stream_control_message,
            abort=self._abort,
        )

        for consumer in self.consumers:
            consumed_future = asyncio.Future()

            substream = stream_iters.pop()
            assert substream is not None

            # Bind the future to this iteration: a shared closure over the loop variable
            # would resolve the last consumer's future for every consumer.
            def consumed(future=consumed_future):
                if not future.done():
                    future.set_result(None)

            spawn_logged(consumer.push_stream(substream, consumed),
                         self.logger, f"push stream to consumer of {self.uri}")
            consumers_done.append(consumed_future)

        await write_sending_stream_control_message(
            common_pb2.SendingStreamControl(globalSequenceNumber=msg.globalSequenceNumber)
        )

        async def notify_after_all():
            if not consumers_done:
                # Nobody consumes this channel: the fan-out drains and acks the chunks, and
                # only once it is through is the message handled as far as this runner goes.
                await stream_exhausted.wait()
                await self._notify_processed(msg.globalSequenceNumber, msg.channel)
                return

            consumed = asyncio.gather(*consumers_done)
            aborted = asyncio.ensure_future(self._abort.wait())
            try:
                done, _ = await asyncio.wait([consumed, aborted], return_when=asyncio.FIRST_COMPLETED)
            finally:
                aborted.cancel()
            if consumed in done:
                self.logger.debug("Processed streaming message for all consumers")
            else:
                # Closed mid-message: a consumer that was already closed never signals the
                # stream it was handed. Wait for the fan-out to finish draining the stream
                # so the global ack still comes after the last chunk was handled.
                consumed.cancel()
                await stream_exhausted.wait()
                self.logger.debug(f"{self.uri} closed while handling a streaming message; acking it")
            await self._notify_processed(msg.globalSequenceNumber, msg.channel)

        spawn_logged(notify_after_all(), self.logger, f"streaming message ack for {self.uri}")

    def close(self) -> None:
        """Close all iterators and release the fan-out of any in-flight stream message."""
        if self.closed:
            return
        self.closed = True
        # A consumer that is still suspended inside a stream message will never resume, so
        # the fan-out barrier must stop waiting for it: it drains and acks the rest instead.
        self._abort.set()
        for consumer in self.consumers:
            consumer.close(lambda: None)
