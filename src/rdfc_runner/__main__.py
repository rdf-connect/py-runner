import asyncio
import sys

from .runner import Runner


async def main():
    # first argument passed is the URL of the orchestrator's Protobuf server
    # second argument is the IRI that uniquely identifies the runner
    grpc_url = sys.argv[1]
    runner_iri = sys.argv[2]

    ### 0. The Python runner is instantiated via the commandline, typically by the Orchestrator.
    # Create a Runner instance
    runner = Runner(runner_iri)
    # Run the runner
    await runner.run(grpc_url)


if __name__ == "__main__":
    asyncio.run(main())
