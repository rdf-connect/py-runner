"""A minimal processor used by the server end-to-end tests (imported via module_path)."""

from rdfc_runner import Processor

INSTANCES = []


class FakeProcessor(Processor):
    async def init(self):
        INSTANCES.append(self)
        self.events = ["init"]

    async def transform(self):
        self.events.append("transform")

    async def produce(self):
        self.events.append("produce")
