import asyncio
import contextlib
import os
import signal
from functools import lru_cache
from importlib.resources import files
from logging import getLogger

from aiohttp import web

from ..runner import Runner
from .bridge import HandshakeError, SocketBridge, read_uri_line
from .config import ServerConfig, parse_server_config
from .index import generate_index_ttl
from .state import State
from .whitelist import build_whitelist

logger = getLogger("rdfc_runner.server")

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
        self.cwd = cwd or os.getcwd()
        self.whitelist = build_whitelist(config.processor_paths)
        self.state = State(history_size=config.history_size)
        self._connections: set[asyncio.Task] = set()
        self._stopping = False
        # The index depends only on the requested base URL; cache a bounded number of variants.
        self._index_for = lru_cache(maxsize=32)(self._generate_index)

    def _generate_index(self, base: str) -> str:
        return generate_index_ttl(self.config.processor_paths, self.cwd, self.config.hostname,
                                  self.config.grpc_port, base)

    ### HTTP ###

    def make_app(self) -> web.Application:
        app = web.Application(client_max_size=MAX_REQUEST_SIZE)
        app.add_routes([
            web.get("/health", self._handle_health),
            web.get("/api/state", self._handle_state),
            web.get("/dashboard", self._handle_dashboard),
            web.get("/", self._handle_index),
            web.get("/{tail:.+}", self._handle_file),
        ])
        return app

    async def _handle_health(self, _request: web.Request) -> web.Response:
        return web.json_response({"status": "ok", "activeConnections": len(self._connections)})

    async def _handle_state(self, _request: web.Request) -> web.Response:
        return web.json_response(self.state.snapshot())

    async def _handle_dashboard(self, _request: web.Request) -> web.Response:
        html = files("rdfc_runner.server").joinpath("dashboard.html").read_text()
        return web.Response(text=html, content_type="text/html")

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
                    await Runner(uri, state=self.state, runner_id=runner_id).run_with_channel(channel)
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

    tcp_server = await asyncio.start_server(server.handle_orchestrator, "0.0.0.0", config.grpc_port)

    app_runner = web.AppRunner(server.make_app())
    await app_runner.setup()
    site = web.TCPSite(app_runner, "0.0.0.0", config.http_port)
    await site.start()

    logger.info(f"py-runner server listening: http://0.0.0.0:{config.http_port} "
                f"(/, /health, /api/state, /dashboard), gRPC TCP on port {config.grpc_port}")
    logger.info(f"Serving {len(server.whitelist)} whitelisted file(s) relative to {server.cwd}")

    try:
        await stop_event.wait()
        logger.info("Shutting down...")
    finally:
        await server.shutdown(tcp_server)
        await app_runner.cleanup()
