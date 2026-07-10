import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse
from urllib.request import url2pathname

from rdflib import Graph, Namespace, RDF

RDFC = Namespace("https://w3id.org/rdf-connect#")

DEFAULT_HTTP_PORT = 3000
DEFAULT_GRPC_PORT = 50051


class ConfigError(Exception):
    """The server configuration file is missing or invalid."""


@dataclass
class ServerConfig:
    http_port: int
    grpc_port: int
    processor_paths: list[str]
    config_path: str


def iri_to_path(value) -> str:
    """Resolve a file IRI (or plain path literal) to a canonical filesystem path."""
    text = str(value)
    if text.startswith("file://"):
        text = url2pathname(urlparse(text).path)
    return os.path.realpath(text)


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
    processor_paths = sorted(iri_to_path(o) for o in graph.objects(subject, RDFC.processorConfig))

    return ServerConfig(
        http_port=int(http_port) if http_port is not None else DEFAULT_HTTP_PORT,
        grpc_port=int(grpc_port) if grpc_port is not None else DEFAULT_GRPC_PORT,
        processor_paths=processor_paths,
        config_path=config_path,
    )
