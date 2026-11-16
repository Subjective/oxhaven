"""What stands between the server and an object type that a plugin wrote.

Behavior: `deephaven_internal.plugin.object` of the reference, and
`io.deephaven.server.plugin.python.ObjectTypeAdapter`, `ExporterAdapter`
"""

from typing import Any, List, Optional, Tuple

from deephaven.plugin.object_type import (
    Exporter,
    FetchOnlyObjectType,
    MessageStream,
    ObjectType,
    Reference,
)

_AUTHORIZATION_EXPORT_BEHAVIORS = ("transform", "manual", "unset")


def _payload(payload: Any) -> bytes:
    """The bytes of a message as a plugin gave them: `bytes`, anything else with a buffer,
    or a sequence of small integers. Java's bridge takes these and refuses a `str` with
    this text."""
    if isinstance(payload, bytes):
        return payload
    if isinstance(payload, str):
        raise TypeError("'str' object cannot be interpreted as an integer")
    try:
        return bytes(memoryview(payload))
    except TypeError:
        if isinstance(payload, (list, tuple)):
            return bytes(payload)
        raise


class ExporterAdapter(Exporter):
    """Collects the objects that `to_bytes` refers its client to.

    Behavior: `io.deephaven.plugin.type.Exporter#reference`
    """

    def __init__(self):
        self.references: List[Any] = []

    def reference(  # type: ignore
        self, obj: Any, allow_unknown_type: bool = True, force_new: bool = True
    ) -> Optional[Reference]:
        if not allow_unknown_type:
            raise RuntimeError("java.lang.IllegalArgumentException: allowUnknownType must be true")
        if not force_new:
            raise RuntimeError("java.lang.IllegalArgumentException: forceNew must be true")
        self.references.append(obj)
        return Reference(len(self.references) - 1, None)

    def __str__(self) -> str:
        return f"an exporter with {len(self.references)} references"


class ClientResponseStreamAdapter(MessageStream):
    """The stream a plugin sends to its client with."""

    def __init__(self, wrapped):
        self._wrapped = wrapped

    def on_data(self, payload: bytes, references: List[Any]) -> None:
        self._wrapped.on_data(_payload(payload), list(references))

    def on_close(self) -> None:
        self._wrapped.on_close()


class ServerRequestStreamAdapter(MessageStream):
    """The stream of a plugin that takes the messages of a client."""

    def __init__(self, wrapped: MessageStream):
        self._wrapped = wrapped

    def on_data(self, payload: bytes, references: List[Any]) -> None:
        self._wrapped.on_data(payload, references)

    def on_close(self) -> None:
        self._wrapped.on_close()


class ObjectTypeAdapter:
    """What the server calls for an object type of a plugin."""

    def __init__(self, user_object_type: ObjectType):
        self._user_object_type = user_object_type

    def is_type(self, obj: Any) -> Any:
        return self._user_object_type.is_type(obj)

    def is_fetch_only(self) -> bool:
        return isinstance(self._user_object_type, FetchOnlyObjectType)

    def authorization_export_behavior(self) -> str:
        """The declaration of the plugin, checked. This server authorizes nothing per
        object, so the answer changes nothing; one that is none of the three fails the
        client's call, as in Java."""
        # a plugin written before the method existed has none
        behavior = getattr(self._user_object_type, "authorization_export_behavior", "unset")
        if callable(behavior):
            behavior = behavior()
        result = str(behavior).lower() if behavior is not None else "unset"
        if result not in _AUTHORIZATION_EXPORT_BEHAVIORS:
            raise ValueError(
                f"ObjectType '{self._user_object_type}' returned invalid authorization_export_behavior "
                f"'{result}'; must be in {_AUTHORIZATION_EXPORT_BEHAVIORS}"
            )
        return result

    def to_bytes(self, obj: Any) -> Tuple[bytes, List[Any]]:
        """The one message of an object that is only fetched: its bytes and the objects
        it refers to."""
        exporter = ExporterAdapter()
        payload = self._user_object_type.to_bytes(exporter, obj)  # type: ignore
        return _payload(payload), exporter.references

    def create_client_connection(self, obj: Any, connection) -> MessageStream:
        return ServerRequestStreamAdapter(
            self._user_object_type.create_client_connection(  # type: ignore
                obj, ClientResponseStreamAdapter(connection)
            )
        )

    def __str__(self) -> str:
        return str(self._user_object_type)
