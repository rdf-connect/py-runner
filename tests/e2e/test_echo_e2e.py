"""End-to-end tests that drive the echo pipeline with the real RDF-Connect orchestrator.

These are the automated counterpart of the manual developer loop described in the
``tests/e2e/README.md``. They start the py-runner server as a local subprocess and run the
pinned ``@rdfc/orchestrator-js`` CLI against ``remote_pipeline.ttl``.

They are marked ``e2e`` and therefore excluded from the default ``uv run pytest`` (see the
``addopts`` in ``pyproject.toml``). Run them explicitly with::

    uv run pytest -m e2e tests/e2e

The tests skip themselves when Node or the orchestrator (``npm install`` in ``tests/e2e``)
is not available, so they never fail a Node-free environment.
"""

import contextlib
import os
import shutil
import signal
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

import pytest

E2E_DIR = Path(__file__).parent
PROCESSORS_DIR = E2E_DIR / "processors"
RDFC_BIN = E2E_DIR / "node_modules" / ".bin" / "rdfc"

# Ports must match server.ttl (grpcPort 4001, httpPort 3000). The orchestrator binds its
# own gRPC server on 50051 by default, which is why the runner server uses 4001.
HTTP_PORT = 3000
HEALTH_URL = f"http://localhost:{HTTP_PORT}/health"

# The pipeline should complete near-instantly; these bound a hung run instead.
SERVER_READY_TIMEOUT = 20.0
ORCHESTRATOR_TIMEOUT = 90.0
SHUTDOWN_TIMEOUT = 15.0

pytestmark = [
    pytest.mark.e2e,
    pytest.mark.skipif(shutil.which("node") is None, reason="Node.js is not installed"),
    pytest.mark.skipif(
        not RDFC_BIN.exists(),
        reason="RDF-Connect orchestrator not installed; run `npm install` in tests/e2e",
    ),
]


def _server_env() -> dict:
    env = os.environ.copy()
    # Make the locally-implemented echo_processor module importable via rdfc:modulePath.
    env["PYTHONPATH"] = os.pathsep.join(
        [str(PROCESSORS_DIR), env.get("PYTHONPATH", "")]
    ).rstrip(os.pathsep)
    return env


def _wait_for_server(process: subprocess.Popen, log, deadline: float) -> None:
    """Poll /health until it responds ok, failing with the server log if it never does."""
    last_error: Exception | None = None
    while time.monotonic() < deadline:
        if process.poll() is not None:
            break
        try:
            with urllib.request.urlopen(HEALTH_URL, timeout=1) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, ConnectionError, OSError) as exc:
            last_error = exc
        time.sleep(0.1)
    log.seek(0)
    pytest.fail(
        f"py-runner server did not become healthy in time (exit code {process.poll()}, "
        f"last error {last_error}).\nserver output:\n{log.read()}"
    )


@contextlib.contextmanager
def _running_server():
    """Start the runner server as a subprocess, yielding (process, log_file) once healthy."""
    log = tempfile.TemporaryFile(mode="w+")
    process = subprocess.Popen(
        [sys.executable, "-m", "rdfc_runner.server", "server.ttl"],
        cwd=E2E_DIR,
        env=_server_env(),
        stdout=log,
        stderr=subprocess.STDOUT,
    )
    try:
        _wait_for_server(process, log, time.monotonic() + SERVER_READY_TIMEOUT)
        yield process, log
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=10)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait(timeout=10)
        log.close()


def _run_pipeline() -> tuple[str, int]:
    """Run the orchestrator against the pipeline once; return its combined output and code."""
    try:
        result = subprocess.run(
            [str(RDFC_BIN), "remote_pipeline.ttl"],
            cwd=E2E_DIR,
            capture_output=True,
            text=True,
            timeout=ORCHESTRATOR_TIMEOUT,
        )
    except subprocess.TimeoutExpired as exc:
        output = (exc.stdout or "") + (exc.stderr or "")
        pytest.fail(f"orchestrator timed out.\norchestrator output:\n{output}")
    return result.stdout + result.stderr, result.returncode


def test_echo_pipeline_runs_end_to_end():
    with _running_server():
        output, returncode = _run_pipeline()

    assert returncode == 0, f"orchestrator exited with {returncode}.\noutput:\n{output}"
    # The log processor logs every message it receives; assert the full chain ran.
    assert "Received message: Hello" in output, output
    assert "Received message: World" in output, output


@pytest.mark.skipif(sys.platform == "win32", reason="POSIX SIGINT shutdown semantics")
def test_server_shuts_down_cleanly_after_pipeline():
    """After a pipeline has run, a SIGINT (Ctrl+C) must shut the server down without dumping
    a traceback. grpc's aio channels sit in reference cycles; if they are only reclaimed at
    interpreter finalization, joining grpc's poller thread then raises PythonFinalizationError
    on Python 3.13+. `serve()` forces a GC pass on shutdown to avoid exactly that."""
    with _running_server() as (process, log):
        _, returncode = _run_pipeline()
        assert returncode == 0, "pipeline must run before testing shutdown"
        time.sleep(0.5)

        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=SHUTDOWN_TIMEOUT)
        except subprocess.TimeoutExpired:
            pytest.fail("server did not exit after SIGINT")

        log.seek(0)
        output = log.read()

    assert process.returncode == 0, f"server exited with {process.returncode}.\noutput:\n{output}"
    assert "Traceback (most recent call last)" not in output, output
    assert "PythonFinalizationError" not in output, output
    assert "__dealloc__" not in output, output
