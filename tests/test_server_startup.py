import asyncio
import socket

import pytest

from rdfc_runner.server.app import ServerStartupError, serve


def _write_config(tmp_path, http_port, grpc_port):
    path = tmp_path / "server.ttl"
    path.write_text(
        "@prefix rdfc: <https://w3id.org/rdf-connect#>.\n"
        "<> a rdfc:PyRunnerServer;\n"
        f"  rdfc:httpPort {http_port};\n"
        f"  rdfc:grpcPort {grpc_port}.\n"
    )
    return str(path)


def _occupy_port() -> tuple[socket.socket, int]:
    """Bind and listen on an ephemeral port, returning the still-open socket and its port."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("0.0.0.0", 0))
    sock.listen(1)
    return sock, sock.getsockname()[1]


async def test_serve_reports_grpc_port_in_use(tmp_path):
    busy, port = _occupy_port()
    try:
        config_path = _write_config(tmp_path, http_port=0, grpc_port=port)
        with pytest.raises(ServerStartupError) as excinfo:
            await serve(config_path)
    finally:
        busy.close()

    message = str(excinfo.value)
    assert "gRPC" in message
    assert str(port) in message
    assert "already in use" in message


async def test_serve_reports_http_port_in_use_and_frees_grpc(tmp_path):
    busy, http_port = _occupy_port()
    # Any free port for gRPC; serve() must open it, then roll it back when HTTP binding fails.
    grpc_probe, grpc_port = _occupy_port()
    grpc_probe.close()
    try:
        config_path = _write_config(tmp_path, http_port=http_port, grpc_port=grpc_port)
        with pytest.raises(ServerStartupError) as excinfo:
            await serve(config_path)
    finally:
        busy.close()

    message = str(excinfo.value)
    assert "HTTP" in message
    assert str(http_port) in message

    # The gRPC listener must have been released, so it can be bound again.
    reclaim = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    reclaim.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    try:
        reclaim.bind(("0.0.0.0", grpc_port))
    finally:
        reclaim.close()
