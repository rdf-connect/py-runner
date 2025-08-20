from abc import ABC, abstractmethod
from typing import TypeVar, Generic, AsyncGenerator, Union, AsyncIterable, TypedDict

T = TypeVar("T")


class StringAny(TypedDict):
    string: str


class BufferAny(TypedDict):
    buffer: bytes


class StreamAny(TypedDict):
    stream: AsyncIterable[bytes]


AnyType = Union[StringAny, BufferAny, StreamAny]


#### Interface ###
class Convertor(ABC, Generic[T]):
    @abstractmethod
    def from_bytes(self, buffer: bytes) -> T:
        """Convert bytes to an object of type T."""
        raise NotImplementedError()

    async def from_stream(self, chunks: AsyncGenerator[bytes, None]) -> T:
        """Convert an asynchronous stream of bytes to an object of type T."""
        raise NotImplementedError()


### Implementations ###
class StringConvertor(Convertor[str]):
    def from_bytes(self, buffer: bytes) -> str:
        """Convert bytes to a string."""
        return buffer.decode()


class NoConvertor(Convertor[bytes]):
    def from_bytes(self, buffer: bytes) -> bytes:
        """Return the bytes as is."""
        return buffer


class StreamConvertor(Convertor[AsyncGenerator[bytes, None]]):
    def from_bytes(self, buffer: bytes) -> AsyncGenerator[bytes, None]:
        """Convert bytes to an asynchronous generator yielding the bytes."""

        async def generator() -> AsyncGenerator[bytes, None]:
            yield buffer

        return generator()

    async def from_stream(self, chunks: AsyncGenerator[bytes, None]) -> AsyncGenerator[bytes, None]:
        """Return the asynchronous stream as is."""
        return chunks


class AnyConvertor(Convertor[AnyType]):
    def from_bytes(self, buffer: bytes) -> BufferAny:
        """Convert bytes to an AnyType."""
        return {"buffer": buffer}

    async def from_stream(self, chunks: AsyncGenerator[bytes, None]) -> StreamAny:
        """Convert an asynchronous stream of bytes to an AnyType."""
        return {"stream": chunks}
