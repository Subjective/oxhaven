"""Tools for resolving Uniform Resource Identifiers (URIs) into objects.

Behavior: `deephaven.uri` (the name, the signature and the error text). The resolvers are
those of Deephaven 42.9: a query scope variable (`dh:///scope/<name>`), an application
field (`dh:///app/<id>/field/<name>`), a table of another server (`dh+plain://host:port/...`,
or `dh://host:port/...` over TLS with the system's root certificates), a CSV file (`csv:`,
`csv+file:`) and a Parquet file (`parquet:`). `docs/CONSOLE.md`, "Remote tables and URIs", has the details.
"""

from typing import Any

from . import _engine
from .appmode import _pythonify
from .dherror import DHError


def resolve(uri: str) -> Any:
    """Resolves a Uniform Resource Identifier (URI) string into an object. Objects with custom
    Python wrappers, like Table, return an instance of the wrapper class; otherwise, the
    object is returned as it is. A variable or an application field that does not exist
    resolves to None.

    Args:
        uri (str): a URI string

    Returns:
        an object

    Raises:
        DHError
    """
    try:
        if not isinstance(uri, str):
            # what jpy raises when no overload of `ResolveTools#resolve` takes the argument
            raise RuntimeError("no matching Java method overloads found")
        return _pythonify(_engine.native.resolve_uri(uri))
    except Exception as e:
        raise DHError(e, "failed to resolve the URI.") from e
