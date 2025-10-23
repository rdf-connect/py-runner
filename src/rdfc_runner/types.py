from typing import Callable, Awaitable, Any

from rdfc_proto import service_pb2

Writable = Callable[[service_pb2.FromRunner], Awaitable[Any]]


class AttrDict(dict):
    """A dict where keys can be accessed as attributes."""

    def __getattr__(self, item):
        try:
            return self[item]
        except KeyError:
            return None

    def __setattr__(self, key, value):
        self[key] = value
