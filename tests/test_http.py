import logging

import pytest
from aiohttp.test_utils import TestClient, TestServer
from rdflib import Graph, Literal, Namespace, RDF, URIRef

from rdfc_runner.server.app import RunnerServer
from rdfc_runner.server.config import ServerConfig

RDFC = Namespace("https://w3id.org/rdf-connect#")

PROCESSORS_TTL = """
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.

<> owl:imports <./helper.ttl>.

rdfc:TestProcessor rdfc:pyImplementationOf rdfc:Processor;
  rdfc:modulePath "test_module";
  rdfc:class "TestProcessor".
"""


@pytest.fixture
async def server(tmp_path):
    (tmp_path / "processors.ttl").write_text(PROCESSORS_TTL)
    (tmp_path / "helper.ttl").write_text("")
    (tmp_path / "secret.ttl").write_text("not served")
    config = ServerConfig(
        http_port=0,
        grpc_port=50999,
        processor_paths=[str(tmp_path / "processors.ttl")],
        config_path=str(tmp_path / "server.ttl"),
        hostname="runner.example",
    )
    return RunnerServer(config, cwd=str(tmp_path))


@pytest.fixture
async def client(server):
    client = TestClient(TestServer(server.make_app()))
    await client.start_server()
    yield client
    await client.close()


async def test_serving_root_is_the_config_directory(tmp_path, monkeypatch):
    """`rdfc-runner-server /elsewhere/server.ttl` must serve relative to that config file,
    not to the shell's working directory: the orchestrator resolves the advertised file IRIs
    against the served document, and '..'-containing IRIs are refused by this very server."""
    config_dir = tmp_path / "config"
    config_dir.mkdir()
    (config_dir / "processors.ttl").write_text(PROCESSORS_TTL)
    (config_dir / "helper.ttl").write_text("")
    elsewhere = tmp_path / "elsewhere"
    elsewhere.mkdir()
    monkeypatch.chdir(elsewhere)

    config = ServerConfig(
        http_port=0,
        grpc_port=50999,
        processor_paths=[str(config_dir / "processors.ttl")],
        config_path=str(config_dir / "server.ttl"),
        hostname="runner.example",
    )
    # Constructed exactly as `serve()` does it, without a cwd to mask the default.
    server = RunnerServer(config)

    assert server.cwd == str(config_dir)

    client = TestClient(TestServer(server.make_app()))
    await client.start_server()
    try:
        index = await (await client.get("/")).text()
        assert ".." not in index
        assert (await client.get("/processors.ttl")).status == 200
    finally:
        await client.close()


async def test_health(client):
    response = await client.get("/health")

    assert response.status == 200
    body = await response.json()
    assert body["status"] == "ok"
    assert body["activeConnections"] == 0


async def test_api_state(server, client):
    server.state.register_runner("127.0.0.1", "urn:runner")

    response = await client.get("/api/state")

    assert response.status == 200
    body = await response.json()
    assert len(body) == 1
    assert body[0]["uri"] == "urn:runner"
    assert body[0]["status"] == "connecting"


async def test_dashboard(client):
    response = await client.get("/dashboard")

    assert response.status == 200
    assert "py-runner dashboard" in await response.text()


async def test_index_uses_request_host(client):
    response = await client.get("/")

    assert response.status == 200
    assert response.content_type == "text/turtle"
    ttl = await response.text()

    graph = Graph()
    graph.parse(data=ttl, format="turtle")
    base = f"http://{client.host}:{client.port}/"
    runner = URIRef(base + "pyRunner")
    assert (runner, RDF.type, RDFC.TcpRunner) in graph
    assert graph.value(runner, RDFC.grpc) == Literal("runner.example:50999")
    # The processor's definition file is referenced relative to the served base.
    processor = RDFC.TestProcessor
    assert graph.value(processor, RDF.type) == RDFC.Processor


async def test_whitelisted_file_served(client):
    response = await client.get("/processors.ttl")

    assert response.status == 200
    assert response.content_type == "text/turtle"
    assert "TestProcessor" in await response.text()


async def test_imported_file_served(client):
    response = await client.get("/helper.ttl")

    assert response.status == 200


async def test_non_whitelisted_file_forbidden(client):
    response = await client.get("/secret.ttl")

    assert response.status == 403


async def test_path_traversal_forbidden(client):
    response = await client.get("/../../../etc/hosts")

    assert response.status in (400, 403, 404)
    if response.status == 200:  # pragma: no cover
        pytest.fail("path traversal must not be served")


async def test_missing_whitelisted_file_not_found(server, tmp_path):
    (tmp_path / "helper.ttl").unlink()
    client = TestClient(TestServer(server.make_app()))
    await client.start_server()
    try:
        response = await client.get("/helper.ttl")
        assert response.status == 404
    finally:
        await client.close()


async def test_post_not_allowed(client):
    response = await client.post("/health")

    assert response.status == 405


async def test_requests_logged_at_debug_level(client, caplog):
    with caplog.at_level(logging.DEBUG, logger="rdfc_runner.server"):
        await client.get("/health")
        await client.get("/secret.ttl")

    assert "GET /health -> 200" in caplog.text
    assert "GET /secret.ttl -> 403" in caplog.text
