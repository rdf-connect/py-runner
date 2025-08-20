# py-runner for RDF-Connect

## Usage

To use the Python runner for RDF-Connect, you need to have a pipeline configuration that includes Python processors.
The Python runner can be added to your RDF-Connect pipeline as follows:

```turtle
@prefix rdfc: <https://w3id.org/rdf-connect#>.
@prefix owl: <http://www.w3.org/2002/07/owl#>.

### Import the runner
<> owl:imports <./.venv/lib/python3.13/site-packages/rdfc_runner/index.ttl>.

### Define the pipeline and add the Python runner
<> a rdfc:Pipeline;
   rdfc:consistsOf [
       rdfc:instantiates rdfc:PyRunner;
       rdfc:processor <log>, <send>;  # List of Python processors to be used in the pipeline. You should define and configure these processors separately.
   ].
```

This example configuration assumes that you use Python 3.13 and that the Python runner is installed in a virtual environment called `.venv` in the current directory.

You can install the Python runner package using the following command:

```shell
uv add rdfc_runner
```

## Logging

The Python runner and processors uses the [standard Python logging module](https://docs.python.org/3/library/logging.html) to log messages.
The Python runner initiates a root logger called `rdfc` that is configured to forward log messages to the RDF-Connect logging system.
This means you can view and manage these logs in the RDF-Connect logging interface, allowing for consistent log management across different components of your RDF-Connect pipeline.

Using the standard Python logging module, you can initialize child loggers in your Python processors by calling `logging.getLogger("rdfc.<your_processor_name>")`.
By NOT setting any handlers and NOT setting `propagate` to `False`, the log messages will be automatically forwarded to the root logger `rdfc`, which is configured to forward messages to the RDF-Connect logging system.
This allows you to use the standard Python logging module in your processors without having to worry about how the messages are handled or where they are sent.
You can use the standard logging levels (DEBUG, INFO, WARNING, ERROR, CRITICAL) to log messages in your processors. For example:

```python
import logging
logger = logging.getLogger("rdfc.MyProcessor")

def my_function():
    logger.info("This is an info message")
    logger.debug("This is a debug message")
    logger.warning("This is a warning message")
    logger.error("This is an error message")
    logger.critical("This is a critical message")
```


## Develop a processor for this runner

The simplest way to start developing a processor for the Python runner, is to start from the [template-processor-py](https://github.com/rdf-connect/template-processor-py) template repository.
It has everything set up to get you started quickly and let you focus on the actual processor logic.

At the very least, a Python processor should consist of a class that inherits from the `rdfc_runner.Processor` abstract base class.
This class should implement the `init` method, which is called when the processor is initialized. This method is where you can set up any necessary configuration or state for your processor like opening a database connection or loading a model.
Additionally, you should implement the `transform` method, which is called before the `produce` method. In this `transform` method, you should put any logic that handles incoming data by consuming readers, possibly transforming it, and passing it to the next processor in the pipeline.
This method should only write to writers as reply to the data it receives from the readers, not produce new data, as it is important that it does not write data to channels before all readers have been initialized and are ready to consume data.
Finally, you should implement the `produce` method, which is called after the `transform` method. This method is where you can produce (new) output data by writing to writers to send the data to the next step in the pipeline.

Nest to the class, you should define a configuration for the processor in the `processor.ttl` file of your package.
Python processor configurations must include the Python specific configuration parameters `rdfc:module_path` and `rdfc:class`, which specify the module and class name of the processor.


## Development of the Python Runner

The [Packaging Python Projects](https://packaging.python.org/en/latest/tutorials/packaging-projects/) guide was used to set up this project.
As build backend, the default [Hatchling](https://hatch.pypa.io/latest/) is used, for which the `pyproject.toml` file is configured.
That file tells build frontend tools like [pip](https://pip.pypa.io/en/stable/) which backend to use.
This project uses [uv](https://docs.astral.sh/uv/) as package manager.

First, make sure you have [Hatch installed](https://hatch.pypa.io/latest/install/):

```shell
pip install hatch
# OR
brew install hatch
# OR another method of your choice
```

Then, create a virtual environment and spawn a shell. This will automatically install the project dependencies defined in `pyproject.toml`:

```shell
hatch env create
hatch shell
```

You can build the project with:

```shell
hatch build
```

Lastly, you can publish the package to PyPI with:

```shell
hatch publish
```


### Project Structure

```
py-runner/                # Root directory of the project
├── src/                  # Source code directory
│   └── rdfc_py_runner/   # Package directory
│       ├── __init__.py   # Package initialization, allows importing as a regular package
│       ├── __init__.pyi  # Type stub for the package, useful for type checking and IDE support while importing this package
│       ├── __main__.py   # Main entry point for the package, allows running as a script
│       ├── convertor.py  # Contains the different convertors used by the readers and writers
│       ├── index.ttl     # RDF schema for the package, used for metadata and configuration
│       ├── iterable.py   # Contains the iterable class used by the reader to process data to the processors
│       ├── logger.py     # Logger configuration and setup of the standard Python logging module for the package, forwarding log messages to the RDF-Connect logging system
│       ├── processor.py  # Abstract base class for Python processors, defining the interface for all Python processors
│       ├── reader.py     # Contains the main logic for the Python reader
│       ├── runner.py     # Contains the main logic for the Python runner
│       ├── types.py      # Contains type definitions and classes used throughout the package
│       ├── utils.py      # Utility functions used by the runner
│       └── writer.py     # Contains the main logic for the Python writer
├── tests/                # Directory for unit tests
└── pyproject.toml        # Project metadata and build configuration
```
