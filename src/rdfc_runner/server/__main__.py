import asyncio
import logging
import sys

from .app import serve


def main() -> None:
    if len(sys.argv) != 2:
        print("usage: rdfc-runner-server <server-config.ttl>", file=sys.stderr)
        raise SystemExit(2)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")
    asyncio.run(serve(sys.argv[1]))


if __name__ == "__main__":
    main()
