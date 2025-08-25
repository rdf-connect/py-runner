from abc import ABC, abstractmethod
from dataclasses import asdict, is_dataclass
from typing import Union, Dict, List, TypeVar, Generic, Any

from .reader import Reader
from .writer import Writer

Primitive = Union[
    str,
    int,
    Reader,
    Writer,
    "ProcessorArgs",
]

ProcessorArgs = Dict[str, Union[Primitive, List[Primitive]]]

T = TypeVar("T")


class Processor(Generic[T], ABC):
    args: T

    def __init__(self, args: T):
        self.args = args

        # Prepare dict for attribute injection
        if is_dataclass(args):
            args_dict = asdict(args)
        elif isinstance(args, dict):
            args_dict = args
        else:
            raise TypeError("Processor args must be a dict or dataclass")

        # Expose args also as attributes
        for key, value in args_dict.items():
            setattr(self, key, value)

    def get(self, key: str) -> Any:
        """Get the argument by key."""
        if is_dataclass(self.args):
            return getattr(self.args, key)
        return self.args[key]

    @abstractmethod
    async def init(self) -> None:
        """This is the first function that is called (and awaited) when creating a processor.
        This is the perfect location to start things like database connections."""
        raise NotImplementedError("The init method must be implemented by the subclass.")

    @abstractmethod
    async def transform(self) -> None:
        """Function to start reading channels.
        This function is called for each processor before `produce` is called."""
        raise NotImplementedError("The transform method must be implemented by the subclass.")

    @abstractmethod
    async def produce(self) -> None:
        """Function to start the production of data, starting the pipeline.
        This function is called after all processors are completely set up."""
        raise NotImplementedError("The produce method must be implemented by the subclass.")
