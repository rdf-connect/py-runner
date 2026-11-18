import os
from logging import getLogger
from pathlib import Path
from typing import Iterable

from rdflib import Graph, OWL, URIRef

from .config import iri_to_path

logger = getLogger("rdfc_runner.server")


def build_whitelist(processor_paths: Iterable[str]) -> set[str]:
    """Collect the set of files the HTTP server may serve.

    Starting from the processor config files, follow `<doc> owl:imports <file://...>`
    triples whose subject is the document itself, transitively (cycle-safe). Non-file
    imports (e.g. http://) are ignored; unreadable or unparsable files stay whitelisted
    but are not followed. All paths are canonicalized with realpath — lookups must do
    the same, so symlinks or `..` segments cannot escape the whitelist.
    """
    whitelist: set[str] = set()
    todo = [os.path.realpath(path) for path in processor_paths]

    while todo:
        file_path = todo.pop()
        if file_path in whitelist:
            continue
        whitelist.add(file_path)

        document = URIRef(Path(file_path).as_uri())
        graph = Graph()
        try:
            graph.parse(file_path, format="turtle", publicID=str(document))
        except Exception as e:
            logger.warning(f"Not following {file_path} while building the whitelist: {e}")
            continue

        for imported in graph.objects(document, OWL.imports):
            if str(imported).startswith("file://"):
                todo.append(iri_to_path(imported))

    return whitelist
