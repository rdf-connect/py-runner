import asyncio
from collections.abc import Callable
from typing import AsyncIterable, Generic, Optional, AsyncGenerator

from rdfc_proto import common_pb2

from .convertor import T, Convertor


class MyIter(AsyncIterable[T], Generic[T]):
    def __init__(self, convertor: Convertor[T]):
        self.convertor = convertor
        self.queue: asyncio.Queue[tuple[Optional[T], Callable[[], None]]] = asyncio.Queue()
        self._closed = False

    def push(self, buffer: bytes, on_complete: Callable[[], None]):
        if self._closed:
            raise RuntimeError("Cannot push to a closed iterator.")
        item = self.convertor.from_bytes(buffer)
        self.queue.put_nowait((item, on_complete))

    def close(self, on_complete: Callable[[], None]):
        self._closed = True
        self.queue.put_nowait((None, on_complete))

    async def push_stream(self, chunks: AsyncGenerator[common_pb2.DataChunk, None], on_complete: Callable[[], None]):
        async def extract_chunks() -> AsyncGenerator[bytes, None]:
            async for chunk in chunks:
                yield chunk.data

        stream = extract_chunks()
        item = await self.convertor.from_stream(stream)
        self.queue.put_nowait((item, on_complete))

    def __aiter__(self) -> AsyncGenerator[T, None]:
        return self._iterate()

    async def _iterate(self) -> AsyncGenerator[T, None]:
        while True:
            item, on_complete = await self.queue.get()
            if item is None:
                # Signal to close the iterator
                on_complete()
                break
            yield item
            # Notify that the item has been processed
            on_complete()
