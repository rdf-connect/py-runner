import asyncio
import importlib
import json
from logging import getLogger, Logger
from typing import List, Awaitable, Any, Dict

import grpc.aio
from rdfc_proto import service_pb2_grpc, orchestrator_pb2, runner_pb2, common_pb2

from .logger import Logger as GrpcLogger
from .processor import Processor
from .reader import Reader, ReaderInstance
from .types import Writable, AttrDict
from .utils import parse_args
from .writer import Writer, WriterInstance


class Runner:
    _readers: dict[str, List[Reader]]
    _writers: dict[str, List[Writer]]
    _client: service_pb2_grpc.RunnerStub
    _write: Writable

    logger: Logger
    uri: str

    _processors: List[Processor]
    _processor_transforms: List[Awaitable[Any]]

    def __init__(self, runner_iri: str):
        self.uri = runner_iri
        self.pipeline = None
        self._readers = dict()
        self._writers = dict()
        self._processors = []
        self._processor_transforms = []

    async def connect(self, stub: service_pb2_grpc.RunnerStub):
        self._client = stub

        ### 1.1. Let the runner connect back with the orchestrator, setting up a bidirectional stream—the "normal stream".
        normal_stream = stub.connect()

        # Define async writable function to send messages to the orchestrator.
        async def writable(msg: orchestrator_pb2.OrchestratorMessage):
            await normal_stream.write(msg)

        self._write = writable

        ### 1.2. Send the initial 'identify' message to the orchestrator.
        await self._write(orchestrator_pb2.OrchestratorMessage(
            identify=orchestrator_pb2.Identify(uri=self.uri)
        ))

        return normal_stream

    def initiate_logger(self, stub: service_pb2_grpc.RunnerStub):
        # Initiate the RPC.logStream log stream to the orchestrator by creating a logger iterator.
        logger = GrpcLogger(stub, self.uri)
        asyncio.create_task(logger.run())
        self.logger = getLogger('rdfc')

    def create_reader(self, uri: str) -> Reader:
        self._readers.setdefault(uri, [])
        reader = ReaderInstance(uri, self._client)
        self._readers[uri].append(reader)
        return reader

    def create_writer(self, uri: str) -> Writer:
        self._writers.setdefault(uri, [])
        writer = WriterInstance(uri, self._client, self._write)
        self._writers[uri].append(writer)
        return writer

    async def handle_orchestrator_message(self, message: runner_pb2.RunnerMessage):
        ### 4.1. Handle a RPC.msg normal message received from the orchestrator. (6.2.2.1 / 6.3.4.1)
        if message.HasField('msg'):
            # Process the message from the orchestrator.
            self.logger.debug("Received message from orchestrator")
            # Send the message to each reader consuming this channel.
            for reader in self._readers.get(message.msg.channel, []):
                reader.handle_msg(message.msg)

        ### 4.2. Handle a RPC.streamMsg streaming message received from the orchestrator. (6.2.2.2 / 6.4.4.2)
        elif message.HasField('streamMsg'):
            # Process the stream message from the orchestrator.
            self.logger.debug("Received stream message from orchestrator")
            # For each reader consuming the channel: set up a receiving stream to receive the stream message
            # from the orchestrator and send it to the processor's reader instance.
            for reader in self._readers.get(message.streamMsg.channel, []):
                reader.handle_streaming_msg(message.streamMsg)

        elif message.HasField('close'):
            # Handle the close message from the orchestrator.
            self.logger.info("Received close message from orchestrator, shutting down.")
            for reader in self._readers.get(message.close.channel, []):
                reader.close()
            for writer in self._writers.get(message.close.channel, []):
                await writer.close(True)
        else:
            self.logger.warning("Received unknown message type from orchestrator.")

    async def add_processor(self, processor: runner_pb2.Processor):
        # Start the processor with the given configuration.
        self.logger.debug(f"Adding processor {processor.uri}")
        args = AttrDict(parse_args(processor.arguments, self))

        config: Dict[str, Any] = json.loads(processor.config)
        module_path = config.get("module_path")
        class_name = config.get("clazz")

        module = importlib.import_module(module_path)
        processor_class = getattr(module, class_name)

        instance: Processor[Any] = processor_class(args)
        await instance.init()
        self.logger.info(f"Processor {processor.uri} initialized")

        self._processors.append(instance)
        self._processor_transforms.append(instance.transform())

        ### 2.1. Notify the orchestrator that the processor is successfully initiated using a RPC.init message.
        await self._write(orchestrator_pb2.OrchestratorMessage(init=orchestrator_pb2.ProcessorInit(uri=processor.uri)))

        return instance

    async def handle_message(self, message: common_pb2.Message):
        # Get the readers attached to the message channel.
        pass

    async def listen_to_normal_stream(self, normal_stream):
        async for message in normal_stream:
            await self.handle_orchestrator_message(message)

    async def start(self):
        await asyncio.gather(*(p.produce() for p in self._processors))
        await asyncio.gather(*self._processor_transforms)

    async def run(self, grpc_url: str):
        # Connect with the Orchestrator's gRPC server.
        async with grpc.aio.insecure_channel(grpc_url) as channel:
            # Create a stub (client)
            stub = service_pb2_grpc.RunnerStub(channel)

            # Initiate the logger
            self.initiate_logger(stub)
            self.logger.info("Runner started and logger initiated.")

            ### 1. Connect to the orchestrator and identify the runner to the orchestrator. (6.2.1.2 / 6.3.2)
            normal_stream = await self.connect(stub)

            # Await all processors to finish
            processors_ended = asyncio.Future()

            async def listen_to_normal_stream():
                try:
                    async for msg in normal_stream:
                        ### 1.3. The orchestrator responds to the RPC.identify message with a RPC.pipeline message,
                        # containing the full expanded pipeline in Turtle format.
                        if msg.HasField('pipeline'):
                            self.pipeline = msg.pipeline
                            self.logger.debug("Pipeline received")

                        ### 2. The orchestrator sends a RPC.proc message for each processor the runner should initiate. (6.2.1.3 / 6.3.3)
                        elif msg.HasField('proc'):
                            await self.add_processor(msg.proc)

                        ### 3. The orchestrator starts the pipeline by sending a RPC.start message to each runner. (6.2.1.4)
                        elif msg.HasField('start'):
                            # Execute the start function of each processor instantiation.
                            asyncio.create_task(self.start()).add_done_callback(
                                # Wait (in the background) until all processors are done executing, and then resolve the task.
                                lambda _: processors_ended.set_result(True)
                            )

                        ### 4. Handle incoming messages by the orchestrator. (6.2.2 / 6.3.4)
                        else:
                            await self.handle_orchestrator_message(msg)
                    self.logger.debug("Stream ended")
                except Exception as e:
                    self.logger.error(f"Error in normal stream listener: {e}")
                    processors_ended.set_result(False)

            # Run listener concurrently in the background
            asyncio.create_task(listen_to_normal_stream())

            ### 5. Wait till all processors complete their execution.
            await processors_ended
            self.logger.debug("Processors ended")

            ### 6. Close the normal stream to signal completion.
            await normal_stream.done_writing()
            await channel.close()
