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
from .utils import fanout_stream


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
    def __init__(self, uri: str, client: service_pb2_grpc.RunnerStub, notify_orchestrator: Writable, logger: Logger):
        self._uri = uri
        self.client = client
        self.notify_orchestrator = notify_orchestrator
        self.logger = logger
        self.consumers: List[MyIter] = []

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
            await self.notify_orchestrator(
                service_pb2.FromRunner(
                    processed=common_pb2.GlobalAck(
                        globalSequenceNumber=msg.globalSequenceNumber,
                        channel=msg.channel,
                    )
                )
            )

        asyncio.create_task(push_to_consumers())

    async def handle_streaming_msg(self, msg: common_pb2.ReceivingStreamMessage):
        """Handle a streaming message from the orchestrator."""
        self.logger.debug(
            f"{self.uri} handling incoming streaming message with global sequence number {msg.globalSequenceNumber}")
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

        # fan out the stream to all iterators
        consumers_done: List[Awaitable[None]] = []

        stream_iters = fanout_stream(
            receiving_stream,
            len(self.consumers),
            write_sending_stream_control_message,
        )

        for consumer in self.consumers:
            consumed_future = asyncio.Future()

            substream = stream_iters.pop()
            assert substream is not None

            asyncio.create_task(consumer.push_stream(substream, lambda: consumed_future.set_result(None)))
            consumers_done.append(consumed_future)

        await write_sending_stream_control_message(
            common_pb2.SendingStreamControl(globalSequenceNumber=msg.globalSequenceNumber)
        )

        async def notify_after_all():
            await asyncio.gather(*consumers_done)
            self.logger.debug("Processed streaming message for all consumers")
            await self.notify_orchestrator(
                service_pb2.FromRunner(
                    processed=common_pb2.GlobalAck(
                        globalSequenceNumber=msg.globalSequenceNumber,
                        channel=msg.channel
                    )
                )
            )

        asyncio.create_task(notify_after_all())

    def close(self) -> None:
        """Close all iterators."""
        for consumer in self.consumers:
            consumer.close(lambda: None)
