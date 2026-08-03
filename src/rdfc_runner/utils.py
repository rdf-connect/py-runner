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
    abort: asyncio.Event | None = None,
) -> List[AsyncGenerator[T, None]]:
    """Duplicate an async generator stream for multiple consumers, waiting for all to handle each chunk.

    Chunks are delivered one at a time: once every (still active) consumer has finished handling
    the current chunk, `on_all_handled` is invoked and only then is the next chunk read from the
    source. Consumers that stop iterating early are excluded from the barrier.

    `on_all_handled` doubles as the sender's flow control (it acks the chunk), so it is called
    exactly once for every chunk taken from the source — including the chunks nobody consumes.
    When no consumer is left (none were registered, all of them stopped iterating, or `abort`
    was set) the remaining source chunks are drained and acked, so a sender that waits for an
    ack per chunk is never left blocked.

    `abort` releases the barrier from the outside: a consumer generator that is dropped while
    suspended inside the stream never runs its `finally`, so its slot in the barrier would
    otherwise stall the source forever. Setting the event makes the pump stop waiting for the
    consumers: chunks are still delivered to whoever is left — a consumer that is actively
    iterating receives the rest of the stream, not a silently truncated end — but each chunk
    is acked immediately so a consumer that is gone cannot stall the source.
    """

    _end = object()  # Sentinel signalling the end of the stream.
    queues: List[asyncio.Queue] = [asyncio.Queue() for _ in range(num_consumers)]
    active: set[int] = set(range(num_consumers))
    pending: set[int] = set()
    chunk_handled: asyncio.Event | None = None
    # Whether `on_all_handled` already ran for the chunk in flight. Guards against a second
    # call slipping in while the first one is still awaiting (a duplicate ack would confuse
    # the sender and put two concurrent writes on the same stream).
    chunk_acked: bool = True

    # `on_all_handled` typically writes on a stream that tolerates one writer at a time; an
    # abort can make the pump ack a chunk whose barrier ack is still in flight, so serialize.
    ack_lock = asyncio.Lock()

    async def ack_chunk() -> None:
        async with ack_lock:
            result = on_all_handled()
            if asyncio.iscoroutine(result):
                await result

    async def mark_handled(consumer_id: int) -> None:
        nonlocal chunk_acked
        pending.discard(consumer_id)
        if pending or chunk_handled is None or chunk_acked:
            return
        # Claim the ack before awaiting it, so the decision to fire is atomic within
        # this event loop turn.
        chunk_acked = True
        handled = chunk_handled
        try:
            await ack_chunk()
        finally:
            handled.set()

    async def wait_for_barrier(handled: asyncio.Event) -> None:
        if abort is None:
            await handled.wait()
            return
        waiters = [asyncio.ensure_future(handled.wait()), asyncio.ensure_future(abort.wait())]
        try:
            await asyncio.wait(waiters, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for waiter in waiters:
                waiter.cancel()

    async def pump_source() -> None:
        nonlocal pending, chunk_handled, chunk_acked
        try:
            async for chunk in stream:
                if not active:
                    # Nobody is going to handle this chunk; ack it anyway and keep draining
                    # so the sender's flow control completes.
                    await ack_chunk()
                    continue
                if abort is not None and abort.is_set():
                    # Aborted: stop gating on the barrier (a consumer that is gone would
                    # stall it forever) but keep delivering, so a consumer that is still
                    # iterating receives the rest of the stream instead of a truncated end.
                    for consumer_id in set(active):
                        queues[consumer_id].put_nowait(chunk)
                    await ack_chunk()
                    continue
                pending = set(active)
                chunk_handled = asyncio.Event()
                chunk_acked = False
                for consumer_id in pending:
                    queues[consumer_id].put_nowait(chunk)
                await wait_for_barrier(chunk_handled)
                if not chunk_acked:
                    # Aborted while consumers were still handling this chunk.
                    chunk_acked = True
                    await ack_chunk()
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
