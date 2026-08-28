import os
from dataclasses import dataclass
from functools import lru_cache
from importlib.resources import files
from logging import getLogger
from pathlib import Path
from typing import Iterable

from rdflib import Graph, Literal, Namespace, RDF, RDFS, URIRef

RDFC = Namespace("https://w3id.org/rdf-connect#")

logger = getLogger("rdfc_runner.server")


@dataclass
class ProcessorDescription:
    uri: str
    label: str | None
    comment: str | None
    source_file: str


def extract_processor_descriptions(processor_paths: Iterable[str]) -> list[ProcessorDescription]:
    """Collect the processors (subjects of rdfc:pyImplementationOf) declared in the config files."""
    descriptions: list[ProcessorDescription] = []
    seen: set[str] = set()

    for file_path in processor_paths:
        document = Path(file_path).as_uri()
        graph = Graph()
        try:
            graph.parse(file_path, format="turtle", publicID=document)
        except Exception as e:
            logger.warning(f"Skipping {file_path} while extracting processor descriptions: {e}")
            continue

        for subject in sorted(graph.subjects(RDFC.pyImplementationOf, None)):
            uri = str(subject)
            if uri in seen:
                continue
            seen.add(uri)
            label = graph.value(subject, RDFS.label)
            comment = graph.value(subject, RDFS.comment)
            descriptions.append(ProcessorDescription(
                uri=uri,
                label=str(label) if label is not None else None,
                comment=str(comment) if comment is not None else None,
                source_file=file_path,
            ))

    return descriptions


@lru_cache(maxsize=1)
def _prelude_text() -> str:
    return files("rdfc_runner.server").joinpath("index_prelude.ttl").read_text()


def generate_index_graph(descriptions: Iterable[ProcessorDescription], cwd: str, hostname: str,
                         grpc_port: int, base: str) -> Graph:
    """Build the index document served at the HTTP root.

    It declares the rdfc:TcpRunner (with the `host:port` address the orchestrator must
    connect to), the SHACL shape for Python processor declarations, and a description of
    every processor the server hosts. All IRIs are absolute against `base` (the URL the
    server is reached on, with trailing slash), so the document is correct however the
    server is addressed. The descriptions come precomputed (extract_processor_descriptions):
    parsing the catalog is base-independent and belongs at startup, not in a request handler.
    """
    graph = Graph()
    graph.parse(data=_prelude_text(), format="turtle", publicID=base)

    runner = URIRef(base + "pyRunner")
    graph.add((runner, RDF.type, RDFC.TcpRunner))
    graph.add((runner, RDFC.handlesSubjectsOf, RDFC.pyImplementationOf))
    graph.add((runner, RDFC.grpc, Literal(f"{hostname}:{grpc_port}")))

    for description in descriptions:
        subject = URIRef(description.uri)
        graph.add((subject, RDF.type, RDFC.Processor))
        if description.label is not None:
            graph.add((subject, RDFS.label, Literal(description.label)))
        if description.comment is not None:
            graph.add((subject, RDFS.comment, Literal(description.comment)))
        relative_path = os.path.relpath(description.source_file, cwd)
        if relative_path == os.pardir or relative_path.startswith(os.pardir + os.sep):
            # Clients normalize the '..' away and end up requesting a different, refused
            # path: this IRI is advertised but can never be fetched from this server.
            logger.warning(f"Processor config {description.source_file} lies outside the serving "
                           f"root {cwd}; its advertised IRI will not be servable")
        graph.add((subject, RDFS.isDefinedBy, URIRef(base + relative_path)))

    return graph


def generate_index_ttl(processor_paths: Iterable[str], cwd: str, hostname: str, grpc_port: int,
                       base: str) -> str:
    descriptions = extract_processor_descriptions(processor_paths)
    return generate_index_graph(descriptions, cwd, hostname, grpc_port, base).serialize(format="turtle")
