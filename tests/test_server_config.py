import os

import pytest

from rdfc_runner.server.config import ConfigError, parse_server_config


def write(tmp_path, name, content):
    path = tmp_path / name
    path.write_text(content)
    return str(path)


def test_parse_server_config_defaults(tmp_path):
    config_path = write(tmp_path, "server.ttl", """
        @prefix rdfc: <https://w3id.org/rdf-connect#>.
        <> a rdfc:PyRunnerServer.
    """)

    config = parse_server_config(config_path)

    assert config.http_port == 3000
    assert config.grpc_port == 50051
    assert config.hostname == "localhost"
    assert config.history_size == 5
    assert config.processor_paths == []


def test_parse_server_config_explicit(tmp_path):
    write(tmp_path, "processors.ttl", "")
    config_path = write(tmp_path, "server.ttl", """
        @prefix rdfc: <https://w3id.org/rdf-connect#>.
        <> a rdfc:PyRunnerServer;
          rdfc:httpPort 8080;
          rdfc:grpcPort 60051;
          rdfc:hostname "example.com";
          rdfc:historySize 10;
          rdfc:processorConfig <./processors.ttl>.
    """)

    config = parse_server_config(config_path)

    assert config.http_port == 8080
    assert config.grpc_port == 60051
    assert config.hostname == "example.com"
    assert config.history_size == 10
    # The relative IRI resolves against the config document.
    assert config.processor_paths == [os.path.realpath(str(tmp_path / "processors.ttl"))]


def test_parse_server_config_unlimited_history(tmp_path):
    config_path = write(tmp_path, "server.ttl", """
        @prefix rdfc: <https://w3id.org/rdf-connect#>.
        <> a rdfc:PyRunnerServer;
          rdfc:historySize -1.
    """)

    config = parse_server_config(config_path)

    assert config.history_size == -1


def test_parse_server_config_multiple_processor_configs(tmp_path):
    config_path = write(tmp_path, "server.ttl", """
        @prefix rdfc: <https://w3id.org/rdf-connect#>.
        <> a rdfc:PyRunnerServer;
          rdfc:processorConfig <./a.ttl>, <./b.ttl>.
    """)

    config = parse_server_config(config_path)

    assert [os.path.basename(p) for p in config.processor_paths] == ["a.ttl", "b.ttl"]


def test_parse_server_config_missing_type(tmp_path):
    config_path = write(tmp_path, "server.ttl", """
        @prefix rdfc: <https://w3id.org/rdf-connect#>.
        <> rdfc:httpPort 8080.
    """)

    with pytest.raises(ConfigError, match="No rdfc:PyRunnerServer"):
        parse_server_config(config_path)


def test_parse_server_config_missing_file(tmp_path):
    with pytest.raises(ConfigError, match="not found|Failed to parse"):
        parse_server_config(str(tmp_path / "nope.ttl"))
