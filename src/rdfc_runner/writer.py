import asyncio
from abc import abstractmethod, ABC
from collections.abc import Callable
from typing import AsyncIterable, Optional

from rdfc_proto import service_pb2_grpc, common_pb2, orchestrator_pb2

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
    def __init__(self, uri: str, client: service_pb2_grpc.RunnerStub, write: Writable):
        self._uri = uri
        self.client = client
        self.write = write

    @property
    def uri(self) -> str:
        """Return the URI of the writer."""
        return self._uri

    async def string(self, msg: str):
        """Write a string to the writer."""
        encoded = msg.encode("utf-8")
        await self.buffer(encoded)

    async def stream(self, buffer: AsyncIterable, transform: Optional[Callable[[object], bytes]] = None):
        """Write a stream of bytes to the writer."""
        transform = transform or (lambda x: x if isinstance(x, bytes) else bytes(x))

        # Initiate a sending stream with a RPC.sendStreamMessage. (6.3.4.3)
        stream = self.client.sendStreamMessage()
        id_future = asyncio.Future()

        async def read_id():
            try:
                async for chunk in stream:
                    id_future.set_result(chunk.id)
                    break
            except Exception as e:
                id_future.set_exception(e)

        asyncio.create_task(read_id())

        # Wait for the first message which contains the ID
        msg_id = await id_future

        # Send the stream message notification
        await self.write(
            orchestrator_pb2.OrchestratorMessage(
                streamMsg=common_pb2.StreamMessage(
                    id=msg_id,
                    channel=self.uri
                )
            )
        )

        async for msg in buffer:
            await stream.write(common_pb2.DataChunk(data=transform(msg)))

        await stream.done_writing()

    async def buffer(self, buffer: bytes) -> None:
        """Write a buffer of bytes to the writer."""
        # Send the message as a RPC.msg over the normal stream. (6.3.4.3)
        msg = orchestrator_pb2.OrchestratorMessage(
            msg=common_pb2.Message(
                channel=self.uri,
                data=buffer
            )
        )
        await self.write(msg)

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

    async def close(self, issued: bool = False) -> None:
        """Close the writer and release resources."""
        # Inform the orchestrator using a RPC.close message that the processor closed the channel. (6.3.4.4)
        if not issued:
            await self.write(
                orchestrator_pb2.OrchestratorMessage(
                    close=common_pb2.Close(channel=self.uri)
                )
            )
