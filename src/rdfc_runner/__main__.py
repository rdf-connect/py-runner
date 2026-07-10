import asyncio
import sys

from .runner import Runner


async def async_main():
    if len(sys.argv) != 3:
        print("usage: rdfc-runner <orchestrator-address> <runner-uri>", file=sys.stderr)
        raise SystemExit(2)

    # first argument passed is the URL of the orchestrator's Protobuf server
    # second argument is the IRI that uniquely identifies the runner
    grpc_url = sys.argv[1]
    runner_iri = sys.argv[2]

    ### 0. The Python runner is instantiated via the commandline, typically by the Orchestrator.
    # Create a Runner instance
    runner = Runner(runner_iri)
    # Run the runner
    await runner.run(grpc_url)


def main():
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
