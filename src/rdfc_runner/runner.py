import asyncio
import importlib
import json
import traceback
from logging import getLogger, Logger
from typing import List, Any, Dict

import grpc.aio
from rdfc_proto import service_pb2_grpc, service_pb2, common_pb2

from .logger import Logger as GrpcLogger
from .processor import Processor
from .reader import Reader, ReaderInstance
from .types import Writable, AttrDict
from .utils import parse_args, spawn_logged
from .writer import Writer, WriterInstance

# Grace period for the log stream to deliver its trailing messages before the caller
# closes the channel underneath the RPC.
LOG_FLUSH_TIMEOUT = 2.0


class Runner:
    _readers: dict[str, Reader]
    _writers: dict[str, Writer]
    _client: service_pb2_grpc.RunnerStub
    _write: Writable

    logger: Logger
    uri: str

    _processors: List[Processor]
    _processor_transforms: List[asyncio.Task]

    def __init__(self, runner_iri: str, state=None, runner_id: str | None = None):
        self.uri = runner_iri
        self.pipeline = None
        self._readers = dict()
        self._writers = dict()
        self._processors = []
        self._processor_transforms = []
        self._grpc_logger = None
        self._log_stream_task = None
        # Optional server-mode statistics (rdfc_runner.server.state.State).
        self._state = state
        self._runner_id = runner_id

    async def connect(self, stub: service_pb2_grpc.RunnerStub):
        self._client = stub

        ### 1.1. Let the runner connect back with the orchestrator, setting up a bidirectional stream—the "normal stream".
        normal_stream = stub.connect()

        # Define async writable function to send messages to the orchestrator.
        async def writable(msg: service_pb2.FromRunner):
            await normal_stream.write(msg)

        self._write = writable

        ### 1.2. Send the initial 'identify' message to the orchestrator.
        await self._write(service_pb2.FromRunner(
            identify=service_pb2.RunnerIdentify(uri=self.uri)
        ))

        return normal_stream

    def initiate_logger(self, stub: service_pb2_grpc.RunnerStub):
        # Initiate the RPC.logStream log stream to the orchestrator by creating a logger iterator.
        self._grpc_logger = GrpcLogger(stub, self.uri)
        self.logger = getLogger('rdfc')
        self._log_stream_task = spawn_logged(self._grpc_logger.run(), getLogger(__name__), "gRPC log stream")

    def _track_channel(self, uri: str, role: str):
        if self._state is not None and self._runner_id is not None:
            return self._state.track_channel(self._runner_id, uri, role)
        return None

    def _untrack_channel(self, uri: str, role: str) -> None:
        if self._state is not None and self._runner_id is not None:
            self._state.untrack_channel(self._runner_id, uri, role)

    def _rollback_registrations(self, current: dict, before: dict, role: str) -> None:
        """Undo the channel registrations made since `before` was snapshotted.

        Only what this call added is dropped: a channel that already existed keeps the
        instance it had, so a processor that failed to initialize cannot take a working
        channel with it. The side effects of a registration are rolled back along with it:
        the stats entry a fresh registration created, and the discarded reader is closed
        so a message racing in on it is acked instead of pushed to a processor that never
        came up.
        """
        for uri, instance in list(current.items()):
            if uri in before and before[uri] is instance:
                continue
            if uri not in before:
                del current[uri]
                # A re-registration reused the stats entry of the instance it displaced,
                # which the restored instance still feeds; only a fresh one is dropped.
                self._untrack_channel(uri, role)
            else:
                current[uri] = before[uri]
            if role == "reader":
                instance.close()

    def create_reader(self, uri: str) -> Reader:
        reader = ReaderInstance(uri, self._client, self._write, self.logger,
                                tracker=self._track_channel(uri, "reader"))
        self._readers[uri] = reader
        return reader

    def create_writer(self, uri: str) -> Writer:
        writer = WriterInstance(uri, self._client, self._write, self.uri, self.logger,
                                tracker=self._track_channel(uri, "writer"))
        self._writers[uri] = writer
        return writer

    async def handle_orchestrator_message(self, message: service_pb2.ToRunner):
        ### 4.1. Handle an RPC.msg normal message received from the orchestrator. (6.2.2.1 / 6.3.4.1)
        if message.HasField('msg'):
            # Process the message from the orchestrator.
            self.logger.debug("Received message from orchestrator")
            # Send the message to the reader consuming this channel.
            reader = self._readers.get(message.msg.channel)
            if reader:
                reader.handle_msg(message.msg)
            else:
                self.logger.error(f"No reader found for channel {message.msg.channel} to handle msg.")

        ### 4.2. Handle a RPC.streamMsg streaming message received from the orchestrator. (6.2.2.2 / 6.4.4.2)
        elif message.HasField('streamMsg'):
            # Process the stream message from the orchestrator.
            self.logger.debug("Received stream message from orchestrator")
            # For the reader consuming the channel: set up a receiving stream to receive the stream message
            # from the orchestrator and send it to the processor's reader instance.
            reader = self._readers.get(message.streamMsg.channel)
            if reader:
                await reader.handle_streaming_msg(message.streamMsg)
            else:
                self.logger.error(f"No reader found for channel {message.streamMsg.channel} to handle streaming msg.")

        elif message.HasField('close'):
            # Handle the close message from the orchestrator.
            self.logger.info("Received close message from orchestrator, shutting down.")
            reader = self._readers.get(message.close.channel)
            if reader:
                reader.close()
            else:
                self.logger.error(f"No reader found for channel {message.close.channel} to handle close.")
            writer = self._writers.get(message.close.channel)
            if writer:
                # Not awaited inline: with an open stream the close resolves only after a
                # 'processed' ack that this same, strictly sequential listener dispatches.
                spawn_logged(writer.close(True), self.logger, f"close of writer {message.close.channel}")
            else:
                self.logger.error(f"No writer found for channel {message.close.channel} to handle close.")
        elif message.HasField('processed'):
            # Handle the processed acknowledgment from the orchestrator.
            self.logger.debug(
                "Received message processed acknowledgment from orchestrator for channel " + message.processed.channel)
            writer = self._writers.get(message.processed.channel)
            if writer:
                writer.handled()
            else:
                self.logger.error(f"No writer found for channel {message.processed.channel} to handle processed ack.")
        else:
            self.logger.error("Received unknown message type from orchestrator.")

    async def add_processor(self, processor: service_pb2.Processor):
        # Start the processor with the given configuration.
        self.logger.debug(f"Adding processor {processor.uri}")
        # parse_args registers a reader/writer per channel argument before the processor
        # itself can be imported or initialized; a failure below must not leave those
        # registrations behind, or messages on them would be routed to a processor that
        # does not exist.
        readers_before = dict(self._readers)
        writers_before = dict(self._writers)
        try:
            args = AttrDict(parse_args(processor.arguments, self))

            config: Dict[str, Any] = json.loads(processor.config)
            module_path = config.get("module_path")
            class_name = config.get("clazz")

            module = importlib.import_module(module_path)
            processor_class = getattr(module, class_name)

            instance: Processor[Any] = processor_class(args)
            await instance.init()
        except Exception as e:
            self.logger.error(f"Failed to initialize processor {processor.uri}:\n{traceback.format_exc()}")
            self._rollback_registrations(self._readers, readers_before, "reader")
            self._rollback_registrations(self._writers, writers_before, "writer")
            ### 2.1. Notify the orchestrator that the processor failed to initiate using an RPC.init message.
            await self._write(service_pb2.FromRunner(initialized=service_pb2.ProcessorInitialized(
                uri=processor.uri,
                error=common_pb2.Error(cause=f"{type(e).__name__}: {e}"),
            )))
            return None
        self.logger.info(f"Processor {processor.uri} initialized")

        self._processors.append(instance)
        self._processor_transforms.append(
            spawn_logged(instance.transform(), self.logger, f"transform of processor {processor.uri}")
        )

        ### 2.1. Notify the orchestrator that the processor is successfully initiated using an RPC.init message.
        await self._write(service_pb2.FromRunner(initialized=service_pb2.ProcessorInitialized(uri=processor.uri)))

        return instance

    async def listen_to_normal_stream(self, normal_stream):
        async for message in normal_stream:
            await self.handle_orchestrator_message(message)

    async def start(self):
        await asyncio.gather(*(p.produce() for p in self._processors))
        await asyncio.gather(*self._processor_transforms)

    async def run(self, grpc_url: str):
        # Connect with the Orchestrator's gRPC server.
        async with grpc.aio.insecure_channel(grpc_url) as channel:
            await self.run_with_channel(channel)

    async def run_with_channel(self, channel: grpc.aio.Channel):
        """Run the runner over an already-established gRPC channel.

        The caller owns the channel and is responsible for closing it. This allows serving
        runners over transports other than a freshly dialed connection (e.g. the remote
        runner server, which bridges an orchestrator-initiated TCP socket into a channel).
        """
        # Create a stub (client)
        stub = service_pb2_grpc.RunnerStub(channel)

        # Initiate the logger
        self.initiate_logger(stub)
        self.logger.info("Runner started and logger initiated.")

        try:
            await self._run_with_stub(stub)
        finally:
            # Terminate the log stream so its background task completes, even when the
            # runner is cancelled before or during the connect handshake.
            if self._grpc_logger is not None:
                self._grpc_logger.close()
            # Wait for the log stream RPC to finish before returning: the caller closes the
            # channel right after us, which would cancel the in-flight RPC and drop the
            # trailing log messages. `wait` never re-raises the task's own outcome.
            if self._log_stream_task is not None:
                done, _ = await asyncio.wait([self._log_stream_task], timeout=LOG_FLUSH_TIMEOUT)
                if not done:
                    getLogger(__name__).debug("Timed out flushing the log stream to the orchestrator")

    async def _run_with_stub(self, stub: service_pb2_grpc.RunnerStub):
        ### 1. Connect to the orchestrator and identify the runner to the orchestrator. (6.2.1.2 / 6.3.2)
        normal_stream = await self.connect(stub)

        # Await all processors to finish
        processors_ended = asyncio.Future()
        start_task: asyncio.Task | None = None

        def on_start_done(task: asyncio.Task) -> None:
            # Resolve `processors_ended` with the outcome of `start()`, propagating failures.
            if processors_ended.done():
                return
            if task.cancelled():
                processors_ended.cancel()
            elif task.exception() is not None:
                processors_ended.set_exception(task.exception())
            else:
                processors_ended.set_result(True)

        async def listen_to_normal_stream():
            nonlocal start_task
            try:
                async for msg in normal_stream:
                    ### 1.3. The orchestrator responds to the RPC.identify message with a RPC.pipeline message,
                    # containing the full expanded pipeline in Turtle format.
                    if msg.HasField('pipeline'):
                        self.pipeline = msg.pipeline
                        self.logger.debug("Pipeline received")

                    ### 2. The orchestrator sends an RPC.proc message for each processor the runner should initiate. (6.2.1.3 / 6.3.3)
                    elif msg.HasField('proc'):
                        await self.add_processor(msg.proc)

                    ### 3. The orchestrator starts the pipeline by sending a RPC.start message to each runner. (6.2.1.4)
                    elif msg.HasField('start'):
                        # Execute the start function of each processor instantiation.
                        # Wait (in the background) until all processors are done executing, and then resolve the task.
                        start_task = asyncio.create_task(self.start())
                        start_task.add_done_callback(on_start_done)

                    ### 4. Handle incoming messages by the orchestrator. (6.2.2 / 6.3.4)
                    else:
                        await self.handle_orchestrator_message(msg)
                self.logger.debug("Stream ended")
                if not processors_ended.done():
                    # The orchestrator went away before the pipeline completed; unblock `run`.
                    processors_ended.set_exception(
                        ConnectionError("Orchestrator stream ended before the pipeline completed")
                    )
            except Exception as e:
                if not processors_ended.done():
                    processors_ended.set_exception(e)
                else:
                    raise

        # Run listener concurrently in the background
        listener_task = spawn_logged(listen_to_normal_stream(), self.logger, "orchestrator stream listener")

        completed = False
        try:
            ### 5. Wait till all processors complete their execution.
            await processors_ended
            self.logger.debug("Processors ended")
            completed = True

            ### 6. Close the normal stream to signal completion.
            await normal_stream.done_writing()
        finally:
            if not listener_task.done():
                listener_task.cancel()
            if not completed:
                # The pipeline is being torn down abnormally (the orchestrator went away or
                # something failed). Whatever is still running can never make progress on a
                # dead channel, so cancel it instead of leaking it for the process' lifetime.
                # On a clean completion these tasks are already done.
                self._cancel_pipeline_tasks(start_task)

    def _cancel_pipeline_tasks(self, start_task: asyncio.Task | None) -> None:
        """Cancel the tasks driving the pipeline: `start()` and the processor transforms."""
        for task in [start_task, *self._processor_transforms]:
            if task is not None and not task.done():
                task.cancel()
        # Close the readers as well: a stream message that is still being fanned out waits
        # for its consumers, which are the very tasks cancelled above. Closing releases that
        # barrier so no background pump is left waiting on a dead channel.
        for reader in self._readers.values():
            reader.close()
