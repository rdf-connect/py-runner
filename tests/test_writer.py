import asyncio
import logging
from types import SimpleNamespace

from rdfc_proto import common_pb2

from rdfc_runner.writer import WriterInstance

logger = logging.getLogger("test")


class FakeSendingStream:
    """The orchestrator's side of sendStreamMessage: acks the identify and every chunk."""

    def __init__(self):
        self.chunks = []
        self._acks = asyncio.Queue()
        self.done = False

    async def write(self, chunk: common_pb2.StreamChunk) -> None:
        self.chunks.append(chunk)
        self._acks.put_nowait(SimpleNamespace(streamSequenceNumber=len(self.chunks)))

    async def done_writing(self):
        self.done = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        return await self._acks.get()


class RecordingTracker:
    def __init__(self):
        self.records = []

    def record_message(self, num_bytes, latency_ms=None):
        self.records.append((num_bytes, latency_ms))


def make_writer(tracker=None):
    stream = FakeSendingStream()
    client = SimpleNamespace(sendStreamMessage=lambda: stream)
    sent = []

    async def notify(msg):
        sent.append(msg)

    writer = WriterInstance("urn:test:channel", client, notify, "urn:test:runner", logger, tracker=tracker)
    return writer, stream, sent


async def test_stream_records_chunk_stats_not_its_own_lifetime():
    """The global ack of a stream message arrives when the stream ends, however long it
    ran: recording it like a buffered message would put one 0-byte, stream-lifetime
    'latency' sample in the channel stats, dwarfing every real message."""
    tracker = RecordingTracker()
    writer, stream, _ = make_writer(tracker)

    async def chunks():
        yield b"abc"
        yield b"defgh"

    task = asyncio.create_task(writer.stream(chunks()))
    while not stream.done:
        await asyncio.sleep(0)
    writer.handled()  # the orchestrator's 'processed' ack for the whole stream message
    await asyncio.wait_for(task, timeout=2)

    assert [num_bytes for num_bytes, _ in tracker.records] == [3, 5]
    assert all(latency is not None for _, latency in tracker.records)


async def test_orchestrator_close_deferred_by_an_open_stream_is_not_echoed_back():
    """A close that waits for an open stream must remember who issued it: echoing the
    orchestrator's own close back would announce a close it never has to be told about."""
    writer, stream, sent = make_writer()

    async def chunks():
        yield b"abc"

    stream_task = asyncio.create_task(writer.stream(chunks()))
    while not stream.done:
        await asyncio.sleep(0)

    close_task = asyncio.create_task(writer.close(True))  # the orchestrator's close arrives mid-stream
    while not writer.should_close:
        await asyncio.sleep(0)

    writer.handled()  # the global 'processed' ack lets the stream (and deferred close) finish
    await asyncio.wait_for(stream_task, timeout=2)
    await asyncio.wait_for(close_task, timeout=2)

    assert not any(msg.HasField("close") for msg in sent)
