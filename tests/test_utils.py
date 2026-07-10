import asyncio
import json

import pytest

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
