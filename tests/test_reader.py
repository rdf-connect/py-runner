import asyncio
import logging

from rdfc_proto import common_pb2

from rdfc_runner.reader import ReaderInstance

logger = logging.getLogger("test")

CHANNEL = "urn:test:channel"


class FakeReceiveStream:
    """The orchestrator's side of `receiveStreamMessage`, with its flow control.

    Like the real writer it sends the next chunk only once the runner acked the previous
    one, so a missing ack shows up as a stream that never advances.
    """

    def __init__(self, chunks, events):
        self._chunks = list(chunks)
        self._events = events
        self._queue = asyncio.Queue()
        self._sent = 0
        self.controls = []

    async def write(self, message: common_pb2.SendingStreamControl) -> None:
        self.controls.append(message)
        if message.globalSequenceNumber:
            self._events.append(("open", message.globalSequenceNumber))
        else:
            self._events.append(("ack", message.streamSequenceNumber))
        self._send_next()

    def _send_next(self) -> None:
        if self._sent < len(self._chunks):
            self._queue.put_nowait(common_pb2.DataChunk(data=self._chunks[self._sent]))
            self._sent += 1
        else:
            self._queue.put_nowait(None)

    def __aiter__(self):
        return self

    async def __anext__(self) -> common_pb2.DataChunk:
        chunk = await self._queue.get()
        if chunk is None:
            raise StopAsyncIteration
        return chunk


class FakeClient:
    def __init__(self, stream: FakeReceiveStream):
        self._stream = stream

    def receiveStreamMessage(self) -> FakeReceiveStream:
        return self._stream


def make_reader(chunks):
    """A reader wired to a fake orchestrator, plus the ordered event log of both sides."""
    events = []
    stream = FakeReceiveStream(chunks, events)

    async def notify(msg):
        events.append(("processed", msg.processed.globalSequenceNumber))

    return ReaderInstance(CHANNEL, FakeClient(stream), notify, logger), events


def stream_msg(global_sequence_number: int = 7) -> common_pb2.ReceivingStreamMessage:
    return common_pb2.ReceivingStreamMessage(channel=CHANNEL, globalSequenceNumber=global_sequence_number)


async def wait_for(predicate, timeout: float = 2.0):
    async def poll():
        while not predicate():
            await asyncio.sleep(0)

    await asyncio.wait_for(poll(), timeout=timeout)


async def settle(turns: int = 50):
    for _ in range(turns):
        await asyncio.sleep(0)


async def test_streaming_message_without_consumers_is_drained_and_acked():
    """A declared but unconsumed reader must not leave the orchestrator's writer blocked."""
    reader, events = make_reader([b"one", b"two"])

    await reader.handle_streaming_msg(stream_msg())
    await wait_for(lambda: ("processed", 7) in events)

    assert events == [("open", 7), ("ack", 0), ("ack", 1), ("processed", 7)]


async def test_streaming_message_reaches_every_consumer_once():
    reader, events = make_reader([b"he", b"llo"])
    first, second = reader.strings(), reader.strings()

    async def collect(consumer, out):
        async for message in consumer:
            out.append(message)

    outputs = ([], [])
    tasks = [asyncio.create_task(collect(c, out)) for c, out in zip((first, second), outputs)]

    await reader.handle_streaming_msg(stream_msg())
    await wait_for(lambda: ("processed", 7) in events)

    assert outputs == (["hello"], ["hello"])
    assert events == [("open", 7), ("ack", 0), ("ack", 1), ("processed", 7)]

    reader.close()
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=2)


async def test_close_releases_a_stream_whose_consumer_walked_away():
    """A processor that reads part of a substream and drops it stalls the fan-out barrier:
    the generator stays suspended at its `yield`, so nothing ever marks the chunk handled.
    Closing the reader must drain and ack the rest so the orchestrator is not left waiting."""
    reader, events = make_reader([b"one", b"two", b"three"])
    consumer = reader.streams()
    held = []

    async def read_one_chunk():
        async for substream in consumer:
            held.append(substream)  # Keep a reference: no garbage collection to the rescue.
            await anext(substream)
            break

    task = asyncio.create_task(read_one_chunk())
    await reader.handle_streaming_msg(stream_msg())
    await asyncio.wait_for(task, timeout=2)
    await settle()

    # The abandoned consumer holds the barrier: no chunk was acked, the stream is stuck.
    assert events == [("open", 7)]

    reader.close()
    await wait_for(lambda: ("processed", 7) in events)

    assert events == [("open", 7), ("ack", 0), ("ack", 1), ("ack", 2), ("processed", 7)]


async def test_message_on_a_closed_reader_is_still_acked():
    reader, events = make_reader([])
    reader.close()

    reader.handle_msg(common_pb2.ReceivingMessage(channel=CHANNEL, globalSequenceNumber=3, data=b"x"))
    await wait_for(lambda: ("processed", 3) in events)

    await reader.handle_streaming_msg(stream_msg(4))
    assert events == [("processed", 3), ("processed", 4)]
