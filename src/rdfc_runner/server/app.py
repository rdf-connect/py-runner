import asyncio
import contextlib
import errno
import gc
import os
import signal
from functools import lru_cache
from importlib.resources import files
from logging import getLogger

from aiohttp import web

from ..runner import Runner
from .bridge import HandshakeError, SocketBridge, read_uri_line
from .config import ServerConfig, parse_server_config
from .index import extract_processor_descriptions, generate_index_graph
from .state import State
from .whitelist import build_whitelist

logger = getLogger("rdfc_runner.server")


class ServerStartupError(Exception):
    """A listener could not be opened, e.g. because its port is already in use.

    Raised instead of letting a raw ``OSError`` traceback escape, so the CLI can print a
    single actionable line and exit cleanly.
    """


def _bind_error(exc: OSError, purpose: str, port: int) -> ServerStartupError:
    """Turn a bind ``OSError`` into an actionable ServerStartupError message."""
    if exc.errno == errno.EADDRINUSE:
        detail = (
            f"port {port} is already in use — another py-runner-server (or a different process) "
            f"is likely still listening on it. Stop it, or set a different "
            f"rdfc:{'grpcPort' if purpose == 'gRPC' else 'httpPort'} in the server config."
        )
    else:
        detail = f"could not bind port {port}: {exc.strerror or exc}"
    return ServerStartupError(f"Cannot start the {purpose} listener: {detail}")

MAX_GRPC_CONNECTIONS = 32
MAX_REQUEST_SIZE = 64 * 1024
SHUTDOWN_GRACE = 10.0


class RunnerServer:
    """Serves runner + processor configs over HTTP and runners over orchestrator-initiated TCP.

    Server-side operational logs go to the 'rdfc_runner.server' stdlib logger; the 'rdfc'
    logger is reserved for per-runner logs forwarded to the orchestrator.
    """

    def __init__(self, config: ServerConfig, cwd: str | None = None):
        self.config = config
        self.whitelist = build_whitelist(config.processor_paths)
        # The HTTP root maps onto the common ancestor of the server config's directory and
        # every whitelisted file — never onto the process' working directory: the
        # orchestrator resolves the served file IRIs against the index document, and a root
        # that does not contain a served file would advertise '..'-containing IRIs that RFC
        # 3986 clients normalize into paths this server then refuses to serve. `cwd` stays
        # overridable for tests.
        config_dir = os.path.dirname(os.path.realpath(config.config_path))
        if cwd is not None:
            self.cwd = cwd
        elif self.whitelist:
            self.cwd = os.path.commonpath([config_dir, *self.whitelist])
        else:
            self.cwd = config_dir
        self.state = State(history_size=config.history_size)
        self._connections: set[asyncio.Task] = set()
        self._stopping = False
        # Parsing the processor catalog is base-independent; it happens once here, not in
        # the request handler — the cache below is keyed on the client-controlled Host
        # header, so a miss must stay cheap (graph assembly, no file I/O or parsing).
        self._descriptions = extract_processor_descriptions(config.processor_paths)
        self._dashboard_html = files("rdfc_runner.server").joinpath("dashboard.html").read_text()
        # The index depends only on the requested base URL; cache a bounded number of variants.
        self._index_for = lru_cache(maxsize=32)(self._generate_index)

    def _generate_index(self, base: str) -> str:
        return generate_index_graph(self._descriptions, self.cwd, self.config.hostname,
                                    self.config.grpc_port, base).serialize(format="turtle")

    ### HTTP ###

    def make_app(self) -> web.Application:
        app = web.Application(client_max_size=MAX_REQUEST_SIZE, middlewares=[self._log_request])
        app.add_routes([
            web.get("/health", self._handle_health),
            web.get("/api/state", self._handle_state),
            web.get("/dashboard", self._handle_dashboard),
            web.get("/", self._handle_index),
            web.get("/{tail:.+}", self._handle_file),
        ])
        return app

    @web.middleware
    async def _log_request(self, request: web.Request, handler) -> web.StreamResponse:
        """Logs one line per served request; visible with LOG_LEVEL=debug."""
        try:
            response = await handler(request)
        except web.HTTPException as e:
            logger.debug(f"{request.method} {request.path} -> {e.status}")
            raise
        logger.debug(f"{request.method} {request.path} -> {response.status}")
        return response

    async def _handle_health(self, _request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "activeConnections": len(self._connections)})

    async def _handle_state(self, _request: web.Request) -> web.Response:
        return web.json_response(self.state.snapshot())

    async def _handle_dashboard(self, _request: web.Request) -> web.Response:
        return web.Response(text=self._dashboard_html, content_type="text/html")

    async def _handle_index(self, request: web.Request) -> web.Response:
        base = f"{request.scheme}://{request.host}/"
        return web.Response(text=self._index_for(base), content_type="text/turtle")

    async def _handle_file(self, request: web.Request) -> web.Response:
        tail = request.match_info["tail"]
        # realpath + whitelist membership makes `..` segments and symlink tricks moot:
        # whatever the path resolves to must literally be a whitelisted file.
        absolute = os.path.realpath(os.path.join(self.cwd, tail))
        if absolute not in self.whitelist:
            raise web.HTTPForbidden(text="Forbidden")
        try:
            content = await asyncio.to_thread(_read_text, absolute)
        except OSError:
            raise web.HTTPNotFound(text="Not found")
        return web.Response(text=content, content_type="text/turtle")

    ### TCP (orchestrator connections) ###

    async def handle_orchestrator(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        peer = writer.get_extra_info("peername")
        host = peer[0] if peer else "unknown"
        if self._stopping or len(self._connections) >= MAX_GRPC_CONNECTIONS:
            reason = "shutting down" if self._stopping else "connection limit reached"
            logger.warning(f"Refusing runner connection from {host}: {reason}")
            writer.close()
            return

        task = asyncio.current_task()
        self._connections.add(task)
        runner_id = None
        try:
            uri = await read_uri_line(reader)
            logger.info(f"Orchestrator connection from {host} for runner {uri}")
            runner_id = self.state.register_runner(host, uri)
            async with SocketBridge(reader, writer) as channel:
                watcher = asyncio.create_task(self._watch_grpc_state(channel, runner_id))
                try:
                    self.state.set_status(runner_id, "running")
                    # The runner stays decoupled from State: it gets a channel-tracker
                    # factory, so this handler remains the only place that mutates State.
                    runner = Runner(uri, track_channel=lambda u, r: self.state.track_channel(runner_id, u, r))
                    await runner.run_with_channel(channel)
                    self.state.set_status(runner_id, "done")
                finally:
                    watcher.cancel()
            logger.info(f"Runner {uri} completed")
        except HandshakeError as e:
            logger.warning(f"Handshake failed from {host}: {e}")
        except asyncio.CancelledError:
            if runner_id is not None:
                self.state.mark_error(runner_id)
            raise
        except Exception:
            if runner_id is not None:
                self.state.mark_error(runner_id)
            logger.exception(f"Runner connection from {host} failed")
        finally:
            if runner_id is not None:
                self.state.deregister_runner(runner_id)
            self._connections.discard(task)
            if not writer.is_closing():
                writer.close()
            # The finished runner's grpc.aio channel sits in a reference cycle, so refcounting
            # alone never frees it (see the shutdown note below for why that matters). Force a
            # cyclic GC pass now, while the loop is healthy, to reclaim it promptly instead of
            # letting closed channels accumulate for the server's lifetime.
            gc.collect()

    async def _watch_grpc_state(self, channel, runner_id: str) -> None:
        with contextlib.suppress(asyncio.CancelledError):
            state = channel.get_state(try_to_connect=False)
            while True:
                self.state.set_grpc_state(runner_id, state.name)
                if state.name == "SHUTDOWN":
                    return
                await channel.wait_for_state_change(state)
                state = channel.get_state(try_to_connect=False)

    ### Lifecycle ###

    async def shutdown(self, tcp_server: asyncio.Server) -> None:
        self._stopping = True
        tcp_server.close()
        # No tcp_server.wait_closed() here: since 3.12 it waits for the in-flight runner
        # connections too; cancel those with a grace period instead.
        connections = list(self._connections)
        for task in connections:
            task.cancel()
        if connections:
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(
                    asyncio.gather(*connections, return_exceptions=True), SHUTDOWN_GRACE
                )


def _read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as file:
        return file.read()


async def serve(config_path: str) -> None:
    config = parse_server_config(config_path)
    server = RunnerServer(config)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    try:
        tcp_server = await asyncio.start_server(
            server.handle_orchestrator, "0.0.0.0", config.grpc_port
        )
    except OSError as e:
        raise _bind_error(e, "gRPC", config.grpc_port) from None

    app_runner = web.AppRunner(server.make_app())
    await app_runner.setup()
    site = web.TCPSite(app_runner, "0.0.0.0", config.http_port)
    try:
        await site.start()
    except OSError as e:
        # The gRPC listener already opened; roll it back so a failed start leaks nothing.
        tcp_server.close()
        await tcp_server.wait_closed()
        await app_runner.cleanup()
        raise _bind_error(e, "HTTP", config.http_port) from None

    logger.info(f"py-runner server listening: http://0.0.0.0:{config.http_port} "
                f"(/, /health, /api/state, /dashboard), gRPC TCP on port {config.grpc_port}")
    logger.info(f"Serving {len(server.whitelist)} whitelisted file(s) relative to {server.cwd}")

    try:
        await stop_event.wait()
        logger.info("Shutting down...")
    finally:
        await server.shutdown(tcp_server)
        await app_runner.cleanup()
        # grpc's aio channels sit in reference cycles, so refcounting alone never frees the
        # channel a finished runner used; only a cyclic GC pass reclaims it. Force one now,
        # while the event loop and interpreter are still healthy: reclaiming the last channel
        # here joins grpc's completion-queue poller thread in-loop. Left to interpreter
        # finalization, that same thread join raises PythonFinalizationError on Python 3.13+,
        # printing a spurious traceback on every Ctrl+C after a pipeline has run.
        gc.collect()
