# py-runner for RDF-Connect

## Usage

To use the Python runner for RDF-Connect, you need to have a pipeline configuration that includes Python processors.
The Python runner can be added to your RDF-Connect pipeline as follows:

```turtle
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.

### Import the runner
<> owl:imports <./.venv/lib/python3.13/site-packages/rdfc_runner/index.ttl>.

### Define the pipeline and add the Python runner
<> a rdfc:Pipeline;
   rdfc:consistsOf [
       rdfc:instantiates rdfc:PyRunner;
       rdfc:processor <log>, <send>;  # List of Python processors to be used in the pipeline. You should define and configure these processors separately.
   ].
```

This example configuration assumes that you use Python 3.13 and that the Python runner is installed in a virtual environment called `.venv` in the current directory.

You can install the Python runner package using the following command:

```shell
uv add rdfc_runner
```

## Remote runner server

Instead of letting the orchestrator spawn the runner as a local subprocess (`rdfc:PyRunner` with `rdfc:command`),
the runner can be hosted as a long-running server — on another machine, or in Docker.
The server serves the runner definition and its processor configurations over HTTP,
and instantiates a runner for every incoming orchestrator connection.

> [!WARNING]
> **The gRPC port is unauthenticated and executes what a connecting orchestrator asks for.**
> Anything that can connect to `rdfc:grpcPort` can potentially make the server import and run
> Python modules from its environment, and the HTTP port publicly serves the configured
> processor files. The server has the same posture as js-runner's: it is meant for networks
> where every host is trusted. Bind or firewall both ports accordingly — a private Docker
> network, a VPN, or a loopback-only deployment — and never expose them to the public internet.

Create a Turtle configuration for the server:

```turtle
@prefix rdfc: <https://w3id.org/rdf-connect#>.

<> a rdfc:PyRunnerServer;
  rdfc:httpPort 3000;                       # HTTP: index, processor configs, /health, /api/state, /dashboard
  rdfc:grpcPort 50051;                      # TCP: orchestrator-initiated runner connections
  rdfc:hostname "localhost";                # the host the orchestrator must dial to reach rdfc:grpcPort
  rdfc:historySize 5;                       # finished runs kept on the dashboard (-1 keeps all, 0 keeps none)
  rdfc:processorConfig <./processors.ttl>.  # one or more processor configuration files to serve
```

`rdfc:hostname` defaults to `localhost` and is advertised verbatim; an IPv6 address must
therefore be written with brackets (`rdfc:hostname "[::1]"`), as the orchestrator's address
parser expects them.

And start the server (the processor modules referenced by `rdfc:modulePath` must be importable,
e.g. installed in the environment or on `PYTHONPATH`):

```shell
PYTHONPATH=processors rdfc-runner-server server.ttl
```

The server's own log verbosity is set with the `LOG_LEVEL` environment variable
(`debug`, `info`, `warn` or `error`; default `info`). At `debug` every served HTTP request is
logged as well. This is independent of the pipeline logs, which are forwarded to the orchestrator.

A pipeline uses the remote runner by importing the runner definition and processor
configurations from the server, and instantiating the served `rdfc:TcpRunner`:

```turtle
@prefix owl: <http://www.w3.org/2002/07/owl#>.
@prefix rdfc: <https://w3id.org/rdf-connect#>.

<> owl:imports <http://localhost:3000/>, <http://localhost:3000/processors.ttl>.

<> a rdfc:Pipeline;
  rdfc:consistsOf [
    rdfc:processor <logProc>, <sendProc>;
    rdfc:instantiates <http://localhost:3000/pyRunner>;
  ].
```

The index the server generates describes that runner as `a rdfc:TcpRunner` with
`rdfc:grpc "<hostname>:<grpcPort>"` — the address built from the `rdfc:hostname` and
`rdfc:grpcPort` of the server configuration. The orchestrator dials exactly that address,
writes the runner IRI followed by a newline, and then reverse-upgrades the socket: it treats
its own end as an incoming gRPC connection. The server instantiates a runner that speaks the
regular gRPC protocol over that same connection — the runner never dials the orchestrator, so
only the runner's ports need to be reachable. **The hostname in the runner IRI (i.e. in the
`runner:` prefix of the pipeline) plays no role in connectivity; it only says where the
configuration was imported from. It is `rdfc:hostname` that must be reachable from the
orchestrator.**

The server exposes some introspection endpoints next to the served configuration files:
`/health` (status + active connection count), `/api/state` (per-runner status and channel
statistics as JSON), and `/dashboard` (a live HTML view of the same). Next to the runners that
are currently connected, these keep the last `rdfc:historySize` finished runs (default 5;
`-1` keeps all of them, `0` none), so a pipeline run remains visible after it completed.

See [`tests/e2e`](tests/e2e) for a complete, runnable pipeline. It doubles as the
project's end-to-end test.

## Docker

The repository ships a `Dockerfile` that packages the runner server. The image expects a
config directory mounted at `/config` containing `server.ttl`, the processor configuration
files, and the processor modules (added to `PYTHONPATH` via `/config/processors`):

```shell
docker build -t rdfc/py-runner .
docker run -p 3000:3000 -p 4001:4001 -v ./tests/e2e:/config:ro rdfc/py-runner
```

Or with the compose example:

```shell
docker compose -f tests/e2e/docker-compose.yml up --build
```

For real deployments with published processor packages, extend the image instead of mounting code:

```dockerfile
FROM rdfc/py-runner
RUN pip install my-processor-package
COPY server.ttl processors.ttl /config/
```

Both published ports are unauthenticated (see the warning above): prefer letting the orchestrator
reach the server over a private compose network instead of publishing the ports on the host.

Remember to set `rdfc:hostname` in `server.ttl` to a name the orchestrator can resolve: the
compose service name (e.g. `"py-runner"`) when the orchestrator runs in the same compose
network, or the published host (e.g. `"localhost"`) when it runs on the Docker host with
published ports.

## Testing

The test suite uses [pytest](https://docs.pytest.org):

```shell
uv run pytest
```

An additional end-to-end test lives in [`tests/e2e`](tests/e2e): a runnable echo pipeline
driven by the real RDF-Connect orchestrator. It is excluded from the default run (it needs
Node and the orchestrator) and is opt-in via its marker:

```shell
uv run pytest -m e2e tests/e2e
```

See [`tests/e2e/README.md`](tests/e2e/README.md) for the one-time `npm install` and for
running the same pipeline manually during development.

## Logging

The Python runner and processors uses the [standard Python logging module](https://docs.python.org/3/library/logging.html) to log messages.
The Python runner initiates a root logger called `rdfc` that is configured to forward log messages to the RDF-Connect logging system.
This means you can view and manage these logs in the RDF-Connect logging interface, allowing for consistent log management across different components of your RDF-Connect pipeline.

Using the standard Python logging module, you can initialize child loggers in your Python processors by calling `logging.getLogger("rdfc.<your_processor_name>")`.
By NOT setting any handlers and NOT setting `propagate` to `False`, the log messages will be automatically forwarded to the root logger `rdfc`, which is configured to forward messages to the RDF-Connect logging system.
This allows you to use the standard Python logging module in your processors without having to worry about how the messages are handled or where they are sent.
You can use the standard logging levels (DEBUG, INFO, WARNING, ERROR, CRITICAL) to log messages in your processors. For example:

```python
import logging
logger = logging.getLogger("rdfc.MyProcessor")

def my_function():
    logger.info("This is an info message")
    logger.debug("This is a debug message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")
```


## Develop a processor for this runner

The simplest way to start developing a processor for the Python runner, is to start from the [template-processor-py](https://github.com/rdf-connect/template-processor-py) template repository.
It has everything set up to get you started quickly and let you focus on the actual processor logic.

At the very least, a Python processor should consist of a class that inherits from the `rdfc_runner.Processor` abstract base class.
This class should implement the `init` method, which is called when the processor is initialized. This method is where you can set up any necessary configuration or state for your processor like opening a database connection or loading a model.
Additionally, you should implement the `transform` method, which is called before the `produce` method. In this `transform` method, you should put any logic that handles incoming data by consuming readers, possibly transforming it, and passing it to the next processor in the pipeline.
This method should only write to writers as reply to the data it receives from the readers, not produce new data, as it is important that it does not write data to channels before all readers have been initialized and are ready to consume data.
Finally, you should implement the `produce` method, which is called after the `transform` method. This method is where you can produce (new) output data by writing to writers to send the data to the next step in the pipeline.

Next to the class, you should define a configuration for the processor in the `processor.ttl` file of your package.
Python processor configurations must include the Python specific configuration parameters `rdfc:modulePath` and `rdfc:class`, which specify the module and class name of the processor.


## Development of the Python Runner

The [Packaging Python Projects](https://packaging.python.org/en/latest/tutorials/packaging-projects/) guide was used to set up this project.
As build backend, the default [Hatchling](https://hatch.pypa.io/latest/) is used, for which the `pyproject.toml` file is configured.
That file tells build frontend tools like [pip](https://pip.pypa.io/en/stable/) which backend to use.
This project uses [uv](https://docs.astral.sh/uv/) as package manager.

First, make sure you have [Hatch installed](https://hatch.pypa.io/latest/install/):

```shell
pip install hatch
# OR
brew install hatch
# OR another method of your choice
```

Then, create a virtual environment and spawn a shell. This will automatically install the project dependencies defined in `pyproject.toml`:

```shell
hatch env create
hatch shell
```

You can build the project with:

```shell
hatch build
```

Lastly, you can publish the package to PyPI with:

```shell
hatch publish
```


### Project Structure

```
py-runner/                # Root directory of the project
├── src/                  # Source code directory
│   └── rdfc_runner/      # Package directory
│       ├── __init__.py   # Package initialization, allows importing as a regular package
│       ├── __init__.pyi  # Type stub for the package, useful for type checking and IDE support while importing this package
│       ├── __main__.py   # Main entry point for the package, allows running as a script
│       ├── convertor.py  # Contains the different convertors used by the readers and writers
│       ├── index.ttl     # RDF schema for the package, used for metadata and configuration
│       ├── iterable.py   # Contains the iterable class used by the reader to process data to the processors
│       ├── logger.py     # Logger configuration and setup of the standard Python logging module for the package, forwarding log messages to the RDF-Connect logging system
│       ├── processor.py  # Abstract base class for Python processors, defining the interface for all Python processors
│       ├── reader.py     # Contains the main logic for the Python reader
│       ├── runner.py     # Contains the main logic for the Python runner
│       ├── server/       # The remote runner server (rdfc-runner-server)
│       ├── types.py      # Contains type definitions and classes used throughout the package
│       ├── utils.py      # Utility functions used by the runner
│       └── writer.py     # Contains the main logic for the Python writer
├── tests/                # Unit tests, plus tests/e2e (a runnable end-to-end pipeline)
├── Dockerfile            # Docker image for the remote runner server
└── pyproject.toml        # Project metadata and build configuration
```
