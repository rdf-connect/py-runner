# End-to-end test: echo pipeline

This directory is **not** an example for pipeline architects. It is a small end-to-end
test bed for **py-runner developers**.

It hosts a tiny pipeline whose custom processors (`processors/echo_processor.py`) are
implemented *locally, in-tree*. That lets a runner developer change the runner/server
implementation, tweak the test processors alongside it if needed, and verify the whole
`send -> echo -> log` chain still works — all **without** having to publish a processor
package first.

> [!NOTE]
> Implementing processors directly next to a pipeline like this is exactly what real
> pipeline architects should **not** do (published processor packages belong on
> `PYTHONPATH`, not inlined in the pipeline folder). It is done here only because this is
> a runner development test, not an example to copy.

The same pipeline can be run two ways: **manually** during development, and
**automatically** as an opt-in part of the test suite. Both use the real RDF-Connect
orchestrator (`@rdfc/orchestrator-js`), so they exercise the actual TTL parsing, HTTP
import of the runner/processor definitions, and the remote (TCP) runner protocol.

## Files

| File                           | Purpose                                                              |
|--------------------------------|----------------------------------------------------------------------|
| `processors/echo_processor.py` | The developer-editable `Send`, `Echo` and `Log` test processors.     |
| `processors/*.ttl`             | One processor configuration file per processor, served by the runner. |
| `server.ttl`                   | Runner server config (`httpPort 3000`, `grpcPort 4001`).             |
| `remote_pipeline.ttl`          | The pipeline the orchestrator runs.                                  |
| `docker-compose.yml`           | Optional: run the server as a container instead of a local process.  |
| `package.json`                 | Pins the `@rdfc/orchestrator-js` version used to drive the pipeline. |
| `test_echo_e2e.py`             | The automated, `e2e`-marked pytest test.                             |

> [!NOTE]
> The runner server uses `rdfc:grpcPort 4001` because the orchestrator binds its **own**
> gRPC server on `50051` by default; using `4001` for the runner avoids the clash.

## One-time setup

Install the orchestrator (creates `node_modules/`, which is git-ignored):

```shell
cd tests/e2e
npm install
```

## Run it manually (developer loop)

In one terminal, start the py-runner server (serving the local test processors).
`uv run` executes it inside the project's virtual environment (where the
`rdfc-runner-server` command lives), so you don't have to activate `.venv` yourself:

```shell
cd tests/e2e
PYTHONPATH=processors uv run rdfc-runner-server server.ttl
```

> [!TIP]
> `uv run` re-resolves the project environment on every call. Since the dev loop
> restarts the server often, you can activate the environment once
> (`source ../../.venv/bin/activate`) and then run
> `PYTHONPATH=processors rdfc-runner-server server.ttl` directly to skip that overhead.

In another terminal, run the pipeline with the orchestrator:

```shell
cd tests/e2e
npx rdfc remote_pipeline.ttl
```

You should see the messages flow through the chain, e.g.:

```
[http://localhost:3000/pyRunner, SendProcessor] info: Sending message: Hello
[http://localhost:3000/pyRunner, EchoProcessor] info: Echoing message: Hello
[http://localhost:3000/pyRunner, LogProcessor] info: Received message: Hello
```

Edit `processors/echo_processor.py` (or the runner source) and re-run to check your
changes on the go.

Alternatively, run the server in a container (see `docker-compose.yml`) and point the
orchestrator at it the same way:

```shell
cd tests/e2e
docker compose up --build      # start the server
npx rdfc remote_pipeline.ttl   # in another terminal
```

## Run it automatically (test suite)

The automated test is **excluded from the default `uv run pytest`** (it is marked `e2e`).
Run it explicitly:

```shell
uv run pytest -m e2e tests/e2e
```

It starts the server as a subprocess, runs the orchestrator against `remote_pipeline.ttl`,
and asserts that the echoed messages appear in the orchestrator output. If Node or the
orchestrator (`npm install` above) is missing, the test **skips** rather than fails.
