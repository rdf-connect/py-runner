from importlib.resources import files

from rdflib import Graph, Literal, Namespace, RDF, RDFS, URIRef
from rdflib.namespace import SH

from rdfc_runner.server.index import extract_processor_descriptions, generate_index_ttl

RDFC = Namespace("https://w3id.org/rdf-connect#")

PROCESSORS_TTL = """
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix rdfs: <http://www.w3.org/2000/01/rdf-schema#>.

rdfc:TestProcessor rdfc:pyImplementationOf rdfc:Processor;
  rdfs:label "Test processor";
  rdfs:comment "A processor used in tests";
  rdfc:modulePath "test_module";
  rdfc:class "TestProcessor".
"""


def test_extract_processor_descriptions(tmp_path):
    path = tmp_path / "processors.ttl"
    path.write_text(PROCESSORS_TTL)

    descriptions = extract_processor_descriptions([str(path)])

    assert len(descriptions) == 1
    assert descriptions[0].uri == str(RDFC.TestProcessor)
    assert descriptions[0].label == "Test processor"
    assert descriptions[0].comment == "A processor used in tests"
    assert descriptions[0].source_file == str(path)


def test_generate_index_ttl(tmp_path):
    path = tmp_path / "processors.ttl"
    path.write_text(PROCESSORS_TTL)
    base = "http://runner.example:1234/"

    ttl = generate_index_ttl([str(path)], str(tmp_path), "runner.example", 50055, base)

    graph = Graph()
    graph.parse(data=ttl, format="turtle")

    runner = URIRef(base + "pyRunner")
    assert (runner, RDF.type, RDFC.TcpRunner) in graph
    assert (runner, RDFC.handlesSubjectsOf, RDFC.pyImplementationOf) in graph
    assert graph.value(runner, RDFC.grpc) == Literal("runner.example:50055")
    assert graph.value(runner, RDFC.grpc).datatype is None

    processor = RDFC.TestProcessor
    assert (processor, RDF.type, RDFC.Processor) in graph
    assert graph.value(processor, RDFS.label) == Literal("Test processor")
    assert graph.value(processor, RDFS.isDefinedBy) == URIRef(base + "processors.ttl")

    # The prelude subclass triples and processor shape are included.
    assert (RDFC.pyImplementationOf, RDFS.subPropertyOf, None) in graph
    shapes = list(graph.subjects(SH.targetSubjectsOf, RDFC.pyImplementationOf))
    assert len(shapes) == 1


def test_prelude_shape_matches_packaged_index_shape():
    """The processor shape served remotely must not drift from the packaged index.ttl."""
    packaged = Graph()
    packaged.parse(data=files("rdfc_runner").joinpath("index.ttl").read_text(), format="turtle",
                   publicID="http://example.org/packaged/")
    prelude = Graph()
    prelude.parse(data=files("rdfc_runner.server").joinpath("index_prelude.ttl").read_text(),
                  format="turtle", publicID="http://example.org/prelude/")

    def shape_properties(graph):
        properties = set()
        for shape in graph.subjects(SH.targetSubjectsOf, RDFC.pyImplementationOf):
            for prop in graph.objects(shape, SH.property):
                path = graph.value(prop, SH.path)
                name = graph.value(prop, SH.name)
                datatype = graph.value(prop, SH.datatype)
                min_count = graph.value(prop, SH.minCount)
                max_count = graph.value(prop, SH.maxCount)
                properties.add((path, name, datatype, min_count, max_count))
        return properties

    packaged_properties = shape_properties(packaged)
    prelude_properties = shape_properties(prelude)

    assert packaged_properties, "packaged index.ttl must declare the processor shape"
    assert packaged_properties == prelude_properties
