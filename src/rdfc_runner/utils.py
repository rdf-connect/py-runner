import json

def parse_args(args, runner: "Runner"):
    """
    Parse the as-string passed arguments into a dictionary.
    """
    parsed_args = {}
    json_args = json.loads(args)

    # Ignore arguments starting with '@', and deserialize the rest.
    for key, value in json_args.items():
        if not key.startswith('@'):
            parsed_args[key] = deserialize_arg(value, runner)
    return parsed_args


def deserialize_arg(arg, runner: "Runner"):
    """
    Deserialize a single argument.
    This function can be extended to handle specific deserialization logic.
    """
    if isinstance(arg, dict):
        type = arg["@type"]
        id = arg["@id"]
        if type == "https://w3id.org/rdf-connect#Reader":
            reader = runner.create_reader(id)
            return reader
        elif type == "https://w3id.org/rdf-connect#Writer":
            writer = runner.create_writer(id)
            return writer
        else:
            runner.logger.error(f"Unknown type {type} for argument {id}")
            return None
    else:
        return arg
