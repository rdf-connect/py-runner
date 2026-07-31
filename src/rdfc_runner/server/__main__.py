import asyncio
import logging
import os
import sys

from .app import serve

LOG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warn": logging.WARNING,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}


def resolve_log_level(value: str) -> int:
    """Translate a LOG_LEVEL name, as js-runner spells them, to a stdlib level."""
    return LOG_LEVELS.get(value.strip().lower(), logging.INFO)


def main() -> None:
    if len(sys.argv) != 2:
        print("usage: rdfc-runner-server <server-config.ttl>", file=sys.stderr)
        raise SystemExit(2)

    level = resolve_log_level(os.environ.get("LOG_LEVEL", "info"))
    logging.basicConfig(level=level, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    asyncio.run(serve(sys.argv[1]))


if __name__ == "__main__":
    main()
