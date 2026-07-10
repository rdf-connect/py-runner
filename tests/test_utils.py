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


async def test_fanout_stream_single_consumer_empty_stream():
    consumers = fanout_stream(_source([]), 1, lambda: None)

    chunks = [chunk async for chunk in consumers[0]]

    assert chunks == []
