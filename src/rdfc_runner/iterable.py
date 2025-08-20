import asyncio
from typing import AsyncIterable, Generic, Optional, AsyncGenerator

from rdfc_proto import common_pb2

from .convertor import T, Convertor


class MyIter(AsyncIterable[T], Generic[T]):
    def __init__(self, convertor: Convertor[T]):
        self.convertor = convertor
        self.queue: asyncio.Queue[Optional[T]] = asyncio.Queue()
        self._closed = False

    def push(self, buffer: bytes):
        if self._closed:
            raise RuntimeError("Cannot push to a closed iterator.")
        item = self.convertor.from_bytes(buffer)
        self.queue.put_nowait(item)

    def close(self):
        self._closed = True
        self.queue.put_nowait(None)

    async def push_stream(self, chunks: AsyncGenerator[common_pb2.DataChunk, None]):
        async def extract_chunks():
            async for chunk in chunks:
                yield chunk.data

        stream = extract_chunks()
        item = await self.convertor.from_stream(stream)
        self.queue.put_nowait(item)

    def __aiter__(self) -> AsyncGenerator[T, None]:
        return self._iterate()

    async def _iterate(self) -> AsyncGenerator[T, None]:
        while True:
            item = await self.queue.get()
            if item is None:
                break
            yield item
