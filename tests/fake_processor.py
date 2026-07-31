"""A minimal processor used by the server end-to-end tests (imported via module_path)."""

import asyncio

from rdfc_runner import Processor

INSTANCES = []
BLOCKING_INSTANCES = []


class FakeProcessor(Processor):
    async def init(self):
        INSTANCES.append(self)
        self.events = ["init"]

    async def transform(self):
        self.events.append("transform")

    async def produce(self):
        self.events.append("produce")


class BlockingProcessor(Processor):
    """A processor whose transform never completes, like one awaiting a reader that stays silent."""

    async def init(self):
        BLOCKING_INSTANCES.append(self)
        self.transforming = asyncio.Event()

    async def transform(self):
        self.transforming.set()
        await asyncio.Future()  # Never resolves.

    async def produce(self):
        pass
