import os
from dataclasses import dataclass
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


def generate_index_graph(processor_paths: Iterable[str], cwd: str, grpc_port: int, base: str) -> Graph:
    """Build the index document served at the HTTP root.

    It declares the rdfc:HttpRunner (with the gRPC port the orchestrator must connect to),
    the SHACL shape for Python processor declarations, and a description of every processor
    the server hosts. All IRIs are absolute against `base` (the URL the server is reached
    on, with trailing slash), so the document is correct however the server is addressed.
    """
    graph = Graph()
    prelude = files("rdfc_runner.server").joinpath("index_prelude.ttl").read_text()
    graph.parse(data=prelude, format="turtle", publicID=base)

    runner = URIRef(base + "pyRunner")
    graph.add((runner, RDF.type, RDFC.HttpRunner))
    graph.add((runner, RDFC.handlesSubjectsOf, RDFC.pyImplementationOf))
    graph.add((runner, RDFC.grpcPort, Literal(grpc_port)))

    for description in extract_processor_descriptions(processor_paths):
        subject = URIRef(description.uri)
        graph.add((subject, RDF.type, RDFC.Processor))
        if description.label is not None:
            graph.add((subject, RDFS.label, Literal(description.label)))
        if description.comment is not None:
            graph.add((subject, RDFS.comment, Literal(description.comment)))
        relative_path = os.path.relpath(description.source_file, cwd)
        graph.add((subject, RDFS.isDefinedBy, URIRef(base + relative_path)))

    return graph


def generate_index_ttl(processor_paths: Iterable[str], cwd: str, grpc_port: int, base: str) -> str:
    return generate_index_graph(processor_paths, cwd, grpc_port, base).serialize(format="turtle")
