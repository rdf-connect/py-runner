import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

from rdflib import Graph, Namespace, RDF

RDFC = Namespace("https://w3id.org/rdf-connect#")

DEFAULT_HTTP_PORT = 3000
DEFAULT_GRPC_PORT = 50051
DEFAULT_HOSTNAME = "localhost"
DEFAULT_HISTORY_SIZE = 5


class ConfigError(Exception):
    """The server configuration file is missing or invalid."""


@dataclass
class ServerConfig:
    http_port: int
    grpc_port: int
    processor_paths: list[str]
    config_path: str
    hostname: str = DEFAULT_HOSTNAME
    history_size: int = DEFAULT_HISTORY_SIZE


def iri_to_path(value) -> str:
    """Resolve a file IRI (or plain path literal) to a canonical filesystem path."""
    text = str(value)
    if text.startswith("file://"):
        text = url2pathname(urlparse(text).path)
    return os.path.realpath(text)


def _as_int(value, prop: str, default: int, config_path: str) -> int:
    """Convert a configured literal to an int, reporting bad values as a ConfigError."""
    if value is None:
        return default
    try:
        return int(str(value))
    except (TypeError, ValueError):
        raise ConfigError(
            f"Invalid rdfc:{prop} in {config_path}: expected an integer, got '{value}'"
        ) from None


def parse_server_config(path: str) -> ServerConfig:
    """Parse an rdfc:PyRunnerServer Turtle configuration file.

    Relative IRIs (e.g. `rdfc:processorConfig <./processors.ttl>`) are resolved
    against the configuration document itself.
    """
    config_path = os.path.realpath(path)
    graph = Graph()
    try:
        graph.parse(config_path, format="turtle", publicID=Path(config_path).as_uri())
    except FileNotFoundError as e:
        raise ConfigError(f"Server config not found: {config_path}") from e
    except Exception as e:
        raise ConfigError(f"Failed to parse server config {config_path}: {e}") from e

    subjects = list(graph.subjects(RDF.type, RDFC.PyRunnerServer))
    if not subjects:
        raise ConfigError(f"No rdfc:PyRunnerServer found in {config_path}")
    subject = subjects[0]

    http_port = graph.value(subject, RDFC.httpPort)
    grpc_port = graph.value(subject, RDFC.grpcPort)
    hostname = graph.value(subject, RDFC.hostname)
    history_size = graph.value(subject, RDFC.historySize)
    processor_paths = sorted(iri_to_path(o) for o in graph.objects(subject, RDFC.processorConfig))

    return ServerConfig(
        http_port=_as_int(http_port, "httpPort", DEFAULT_HTTP_PORT, config_path),
        grpc_port=_as_int(grpc_port, "grpcPort", DEFAULT_GRPC_PORT, config_path),
        processor_paths=processor_paths,
        config_path=config_path,
        hostname=str(hostname) if hostname is not None else DEFAULT_HOSTNAME,
        history_size=_as_int(history_size, "historySize", DEFAULT_HISTORY_SIZE, config_path),
    )
