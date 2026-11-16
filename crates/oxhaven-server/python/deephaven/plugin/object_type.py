"""Object types: how a plugin lets clients open objects of its own.

An `ObjectType` names a kind of object and says which objects are of it. A variable of the
console that holds such an object is listed to clients with the type's name, and a client
then opens it: a `FetchOnlyObjectType` answers with bytes once, a `BidirectionalObjectType`
with a stream of messages in both directions. Either may refer the client to other objects
of the server, such as tables.

The names and signatures are those of `deephaven.plugin.object_type` in the
`deephaven-plugin` package (0.8.0).
"""

import abc
from typing import Any, List, Literal, Optional

from . import Plugin, Registration, register_all_into  # noqa: F401


class Reference:
    """An object of the server that a message refers its client to."""

    def __init__(self, index: int, type: Optional[str]):
        self._index = index
        self._type = type

    @property
    def index(self) -> int:
        """The place of the object among the references of its message, from 0. The bytes
        of the message may name the object by it."""
        return self._index


class Exporter(abc.ABC):
    """What `FetchOnlyObjectType.to_bytes` makes its references with."""

    @abc.abstractmethod
    def reference(self, obj: object) -> Reference:
        """Refers the client to `obj`. Every call makes a new reference, also for an object
        that was referred to before."""


class MessageStream(abc.ABC):
    """One direction of the messages between a client and an object. An object type is
    given one to send to the client with, and returns one that takes what the client
    sends."""

    def __init__(self):
        pass

    @abc.abstractmethod
    def on_close(self) -> None:
        """Ends the stream, in both directions."""

    @abc.abstractmethod
    def on_data(self, payload: bytes, references: List[Any]) -> None:
        """A message: bytes, and the objects of the server they refer to."""


class ObjectType(Plugin):
    """A kind of object that clients can open."""

    @property
    @abc.abstractmethod
    def name(self) -> str:
        """What clients are told the type of such an object is."""

    @abc.abstractmethod
    def is_type(self, obj: Any) -> bool:
        """Whether `obj` is of this type."""

    def authorization_export_behavior(self) -> Literal["transform", "manual", "unset"]:
        """Who authorizes the objects this type refers a client to: the server
        ("transform"), the type itself ("manual"), or whatever the server's policy says
        ("unset"). This server authorizes nothing per object, as Deephaven Community does
        not, so the three behave alike."""
        return "unset"


class BidirectionalObjectType(ObjectType):
    """An object type whose objects exchange messages with their clients for as long as
    the client keeps the stream open."""

    @abc.abstractmethod
    def create_client_connection(self, obj: object, connection: MessageStream) -> MessageStream:
        """Called when a client opens `obj`. `connection` sends to that client; the stream
        returned is given each message the client sends. Before returning, send the client
        what it needs to show the object with `connection.on_data`."""


class FetchOnlyObjectType(ObjectType):
    """An object type whose objects are sent to a client once."""

    @abc.abstractmethod
    def to_bytes(self, exporter: Exporter, obj: Any) -> bytes:
        """The bytes a client is sent for `obj`, which is of this type."""
