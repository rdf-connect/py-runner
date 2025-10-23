import asyncio
import json
from typing import AsyncGenerator, TypeVar, Callable, List, Awaitable

T = TypeVar("T")


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
    """Duplicate an async generator stream for multiple consumers, waiting for all to handle each chunk."""

    buffer: List[T] = []
    pending: List[asyncio.Future[T | None]] = []
    ended = False
    awaiting_ack = 0
    active_consumers = num_consumers

    def flush() -> None:
        nonlocal awaiting_ack
        while buffer and pending:
            chunk = buffer[0]
            waiter = pending.pop(0)
            waiter.set_result(chunk)
            awaiting_ack += 1

    def end() -> None:
        nonlocal ended
        ended = True
        while pending:
            waiter = pending.pop(0)
            waiter.set_result(None)

    async def ack() -> None:
        nonlocal awaiting_ack
        awaiting_ack -= 1
        if awaiting_ack == 0 and buffer:
            buffer.pop(0)
            result = on_all_handled()
            if asyncio.iscoroutine(result):
                await result
            flush()

    async def pump_source() -> None:
        try:
            async for chunk in stream:
                buffer.append(chunk)
                flush()
        finally:
            end()

    asyncio.create_task(pump_source())

    def make_iterable() -> AsyncGenerator[T, None]:
        async def generator() -> AsyncGenerator[T, None]:
            nonlocal active_consumers
            try:
                while True:
                    if buffer:
                        chunk = buffer[0]
                    elif ended:
                        break
                    else:
                        waiter: asyncio.Future[T | None] = asyncio.get_event_loop().create_future()
                        pending.append(waiter)
                        chunk = await waiter
                        if chunk is None:
                            break
                    yield chunk
                    await ack()
            finally:
                active_consumers -= 1
                if active_consumers == 0:
                    # must be awaited because it's async
                    end()

        return generator()

    return [make_iterable() for _ in range(num_consumers)]
