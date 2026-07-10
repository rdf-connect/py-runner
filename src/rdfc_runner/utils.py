import asyncio
import json
from logging import Logger, getLogger
from typing import AsyncGenerator, TypeVar, Callable, List, Awaitable, Coroutine, Any

T = TypeVar("T")


def spawn_logged(coro: Coroutine[Any, Any, Any], logger: Logger, what: str) -> asyncio.Task:
    """Schedule a fire-and-forget coroutine as a task, logging any exception it raises."""
    task = asyncio.create_task(coro)

    def _report(t: asyncio.Task) -> None:
        if t.cancelled():
            return
        exc = t.exception()
        if exc is not None:
            logger.error(f"Background task '{what}' failed: {exc!r}")

    task.add_done_callback(_report)
    return task


def parse_args(args, runner: "Runner"):
    """
    Parse the as-string passed arguments into a dictionary.
    """
    parsed_args = {}
    json_args = json.loads(args)

    # Ignore arguments starting with '@', and deserialize the rest.
    for key, value in json_args.items():
        if not key.startswith('@'):
            parsed_args[key] = deserialize_arg(value, runner)
    return parsed_args


def deserialize_arg(arg, runner: "Runner"):
    """
    Deserialize a single argument.
    This function can be extended to handle specific deserialization logic.
    """
    if isinstance(arg, dict):
        type = arg["@type"]
        id = arg["@id"]
        if type == "https://w3id.org/rdf-connect#Reader":
            reader = runner.create_reader(id)
            return reader
        elif type == "https://w3id.org/rdf-connect#Writer":
            writer = runner.create_writer(id)
            return writer
        else:
            runner.logger.error(f"Unknown type {type} for argument {id}")
            return None
    else:
        return arg

def fanout_stream(
    stream: AsyncGenerator[T, None],
    num_consumers: int,
    on_all_handled: Callable[[], Awaitable[None]] | Callable[[], None],
) -> List[AsyncGenerator[T, None]]:
    """Duplicate an async generator stream for multiple consumers, waiting for all to handle each chunk.

    Chunks are delivered one at a time: once every (still active) consumer has finished handling
    the current chunk, `on_all_handled` is invoked and only then is the next chunk read from the
    source. Consumers that stop iterating early are excluded from the barrier.
    """

    _end = object()  # Sentinel signalling the end of the stream.
    queues: List[asyncio.Queue] = [asyncio.Queue() for _ in range(num_consumers)]
    active: set[int] = set(range(num_consumers))
    pending: set[int] = set()
    chunk_handled: asyncio.Event | None = None

    async def mark_handled(consumer_id: int) -> None:
        pending.discard(consumer_id)
        if not pending and chunk_handled is not None and not chunk_handled.is_set():
            result = on_all_handled()
            if asyncio.iscoroutine(result):
                await result
            chunk_handled.set()

    async def pump_source() -> None:
        nonlocal pending, chunk_handled
        try:
            async for chunk in stream:
                if not active:
                    break
                pending = set(active)
                chunk_handled = asyncio.Event()
                for consumer_id in pending:
                    queues[consumer_id].put_nowait(chunk)
                await chunk_handled.wait()
        finally:
            for consumer_id in set(active):
                queues[consumer_id].put_nowait(_end)

    spawn_logged(pump_source(), getLogger("rdfc"), "fanout stream pump")

    def make_iterable(consumer_id: int) -> AsyncGenerator[T, None]:
        async def generator() -> AsyncGenerator[T, None]:
            try:
                while True:
                    chunk = await queues[consumer_id].get()
                    if chunk is _end:
                        break
                    yield chunk
                    await mark_handled(consumer_id)
            finally:
                # Drop this consumer from the barrier so an early exit does not stall the others.
                active.discard(consumer_id)
                await mark_handled(consumer_id)

        return generator()

    return [make_iterable(consumer_id) for consumer_id in range(num_consumers)]
