"""The one place the package calls into the server: `_oxhaven`, the module the server
registers in its embedded interpreter.

A table operation is sent as the `BatchTableRequest.Operation` message of the table service
in protobuf JSON form, so it runs through the same handler, with the same checks, as the
operation a gRPC client sends. Tables are named by their position in `sources`.
"""

import json

import _oxhaven


def source(index: int = 0) -> dict:
    """The table reference of the `index`-th source of an operation."""
    return {"batch_offset": index}


def table_op(name: str, request: dict, sources=()):
    """Runs the table service operation `name` and returns the handle of its result."""
    return _oxhaven.table_op(json.dumps({name: request}), list(sources))


def new_table(columns):
    """A static table from `(name, type, values)` triples."""
    return _oxhaven.new_table(list(columns))


def to_sequence(value) -> list:
    """`None` is no item, a string (or anything that is not a sequence) is one item."""
    if value is None:
        return []
    if isinstance(value, (str, bytes)) or not hasattr(value, "__iter__"):
        return [value]
    return list(value)
