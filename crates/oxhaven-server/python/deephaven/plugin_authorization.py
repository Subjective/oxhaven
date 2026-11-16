"""The authorization transform, for a plugin that authorizes the objects it refers its
clients to by itself (`ObjectType.authorization_export_behavior` answering "manual").

Behavior: `deephaven.plugin_authorization` of the reference, with the transform of
Deephaven Community, which is the identity
"""

from typing import Any

__all__ = ["transform"]


def transform(obj: Any) -> Any:
    """What the current user may see of `obj`: all of it. This server, like Deephaven
    Community, authorizes a session and nothing per object."""
    return obj
