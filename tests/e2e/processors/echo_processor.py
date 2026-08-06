"""Example processors for the py-runner remote server demo."""

import logging

from rdfc_runner import Processor


class SendProcessor(Processor):
    """Sends the configured string messages to a writer, then closes it."""

    async def init(self):
        self.logger = logging.getLogger("rdfc.SendProcessor")

    async def transform(self):
        pass

    async def produce(self):
        for msg in self.msgs:
            self.logger.info(f"Sending message: {msg}")
            await self.writer.string(msg)
        await self.writer.close()


class EchoProcessor(Processor):
    """Echoes every incoming message to a writer."""

    async def init(self):
        self.logger = logging.getLogger("rdfc.EchoProcessor")

    async def transform(self):
        async for msg in self.reader.strings():
            self.logger.info(f"Echoing message: {msg}")
            await self.writer.string(msg)
        await self.writer.close()

    async def produce(self):
        pass


class LogProcessor(Processor):
    """Logs every incoming message."""

    async def init(self):
        self.logger = logging.getLogger("rdfc.LogProcessor")

    async def transform(self):
        async for msg in self.reader.strings():
            self.logger.info(f"Received message: {msg}")

    async def produce(self):
        pass
