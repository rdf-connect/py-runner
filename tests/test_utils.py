import asyncio
import contextlib
import json

from rdfc_runner.utils import fanout_stream, parse_args


class FakeRunner:
    """Minimal stand-in for Runner exposing the surface parse_args uses."""

    def __init__(self):
        self.readers = []
        self.writers = []

    def create_reader(self, uri):
        self.readers.append(uri)
        return f"reader:{uri}"

    def create_writer(self, uri):
        self.writers.append(uri)
        return f"writer:{uri}"


def test_parse_args_plain_values():
    runner = FakeRunner()
    args = json.dumps({"@id": "urn:proc", "name": "example", "count": 42})

    parsed = parse_args(args, runner)

    assert parsed == {"name": "example", "count": 42}


def test_parse_args_reader_and_writer():
    runner = FakeRunner()
    args = json.dumps({
        "incoming": {"@type": "https://w3id.org/rdf-connect#Reader", "@id": "urn:channel:in"},
        "outgoing": {"@type": "https://w3id.org/rdf-connect#Writer", "@id": "urn:channel:out"},
    })

    parsed = parse_args(args, runner)

    assert parsed["incoming"] == "reader:urn:channel:in"
    assert parsed["outgoing"] == "writer:urn:channel:out"
    assert runner.readers == ["urn:channel:in"]
    assert runner.writers == ["urn:channel:out"]


async def _source(chunks):
    for chunk in chunks:
        yield chunk


async def test_fanout_stream_delivers_all_chunks_to_all_consumers():
    # Regression test: when the producer runs ahead of the consumers, every chunk
    # must still be delivered exactly once to each consumer.
    handled = 0

    def on_all_handled():
        nonlocal handled
        handled += 1

    consumers = fanout_stream(_source([b"a", b"b", b"c"]), 2, on_all_handled)

    async def collect(gen):
        return [chunk async for chunk in gen]

    results = await asyncio.gather(*(collect(c) for c in consumers))

    assert results == [[b"a", b"b", b"c"], [b"a", b"b", b"c"]]
    assert handled == 3


async def test_fanout_stream_async_ack_and_slow_consumer():
    acks = []

    async def on_all_handled():
        acks.append(True)

    consumers = fanout_stream(_source([1, 2, 3, 4]), 2, on_all_handled)

    async def fast(gen):
        return [chunk async for chunk in gen]

    async def slow(gen):
        chunks = []
        async for chunk in gen:
            await asyncio.sleep(0.001)
            chunks.append(chunk)
        return chunks

    results = await asyncio.gather(fast(consumers[0]), slow(consumers[1]))

    assert results == [[1, 2, 3, 4], [1, 2, 3, 4]]
    assert len(acks) == 4


async def test_fanout_stream_consumer_exiting_early_does_not_stall_others():
    consumers = fanout_stream(_source([1, 2, 3]), 2, lambda: None)

    async def take_one(gen):
        async for chunk in gen:
            await gen.aclose()
            return [chunk]

    async def take_all(gen):
        return [chunk async for chunk in gen]

    first, rest = await asyncio.gather(take_one(consumers[0]), take_all(consumers[1]))

    assert first == [1]
    assert rest == [1, 2, 3]


async def test_fanout_stream_single_consumer_empty_stream():
    consumers = fanout_stream(_source([]), 1, lambda: None)

    chunks = [chunk async for chunk in consumers[0]]

    assert chunks == []


async def _settle(turns: int = 20):
    """Give the pump and the consumer generators room to run."""
    for _ in range(turns):
        await asyncio.sleep(0)


async def test_fanout_stream_without_consumers_still_acks_every_chunk():
    # The ack is the sender's flow control: abandoning the stream after the first chunk
    # leaves the sender blocked forever, waiting for an ack that never comes.
    acks = []

    consumers = fanout_stream(_source([1, 2, 3]), 0, lambda: acks.append(1))

    assert consumers == []
    await _settle()
    assert len(acks) == 3


async def test_fanout_stream_acks_the_rest_after_the_last_consumer_leaves():
    acks = []

    consumers = fanout_stream(_source([1, 2, 3]), 1, lambda: acks.append(1))

    async def take_one(gen):
        async for chunk in gen:
            await gen.aclose()
            return chunk

    assert await take_one(consumers[0]) == 1
    await _settle()
    # One ack for the chunk that was handled, plus one for each drained chunk.
    assert len(acks) == 3


async def test_fanout_stream_abort_releases_the_barrier_and_drains():
    # A consumer that is dropped while suspended inside the stream never runs its
    # `finally`, so nothing removes it from the barrier: the abort is the way out.
    acks = []
    abort = asyncio.Event()

    consumers = fanout_stream(_source([1, 2, 3]), 1, lambda: acks.append(1), abort=abort)

    abandoned = consumers[0]
    assert await anext(abandoned) == 1
    await _settle()
    assert acks == []  # The pump waits for the consumer that will never come back.

    abort.set()
    await _settle()

    assert len(acks) == 3


async def test_fanout_stream_acks_a_chunk_once_while_the_ack_is_in_flight():
    acks = []
    release = asyncio.Event()

    async def on_all_handled():
        acks.append(1)
        await release.wait()

    first, second = fanout_stream(_source([1, 2]), 2, on_all_handled)

    collected = []

    async def drive(gen):
        async for chunk in gen:
            collected.append(chunk)

    driver = asyncio.create_task(drive(first))
    assert await anext(second) == 1
    await _settle()
    assert collected == [1]  # The first consumer handled the chunk and marked it.

    # The second consumer leaves: the barrier is complete, the ack write is in flight.
    closing = asyncio.create_task(second.aclose())
    await _settle()
    assert len(acks) == 1

    # The first consumer's generator is finalized while that ack is still in flight; its
    # `finally` must not fire a second ack for the same chunk.
    driver.cancel()
    await _settle()
    assert len(acks) == 1

    release.set()
    await asyncio.wait_for(closing, timeout=1)
    with contextlib.suppress(asyncio.CancelledError):
        await driver
    await _settle()
