import asyncio
from abc import abstractmethod, ABC
from collections.abc import AsyncIterable
from typing import AsyncGenerator, List

from rdfc_proto import common_pb2
from rdfc_proto import service_pb2_grpc

from .convertor import AnyType
from .convertor import StringConvertor, StreamConvertor, NoConvertor, AnyConvertor
from .iterable import MyIter


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
    def handle_msg(self, msg: common_pb2.Message):
        """Handle a message from the orchestrator."""
        raise NotImplementedError()

    @abstractmethod
    def handle_streaming_msg(self, msg: common_pb2.StreamMessage):
        """Handle a streaming message from the orchestrator."""
        raise NotImplementedError()

    @abstractmethod
    def close(self):
        """Close the reader and release resources."""
        raise NotImplementedError()


### Implementations ###
class ReaderInstance(Reader):
    def __init__(self, uri: str, client: service_pb2_grpc.RunnerStub):
        self._uri = uri
        self.client = client
        self.iterators: List[MyIter] = []

    @property
    def uri(self) -> str:
        """Return the URI of the reader."""
        return self._uri

    def strings(self) -> AsyncIterable[str]:
        """Return an async iterator of strings."""
        my_iter = MyIter(StringConvertor())
        self.iterators.append(my_iter)
        return my_iter

    def streams(self) -> AsyncIterable[AsyncGenerator[bytes, None]]:
        """Return an async iterator of byte streams."""
        my_iter = MyIter(StreamConvertor())
        self.iterators.append(my_iter)
        return my_iter

    def buffers(self) -> AsyncIterable[bytes]:
        """Return an async iterator of byte buffers."""
        my_iter = MyIter(NoConvertor())
        self.iterators.append(my_iter)
        return my_iter

    def anys(self) -> AsyncIterable[AnyType]:
        """Return an async iterator of AnyType."""
        my_iter = MyIter(AnyConvertor())
        self.iterators.append(my_iter)
        return my_iter

    def handle_msg(self, msg: common_pb2.Message):
        """Handle a message from the orchestrator."""
        for iterator in self.iterators:
            iterator.push(msg.data)

    def handle_streaming_msg(self, msg: common_pb2.StreamMessage):
        """Handle a streaming message from the orchestrator."""
        # Start a receiving stream to receive the streaming messages over the stream message channel.
        chunks = self.client.receiveStreamMessage(msg.id)
        # Then, send the message chunk to the processor's reader instance.
        for iterator in self.iterators:
            asyncio.create_task(iterator.push_stream(chunks))

    def close(self):
        """Close all iterators."""
        for iterator in self.iterators:
            iterator.close()
