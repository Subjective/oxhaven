"""The one place the package calls into the server: `_oxhaven`, the module the server
registers in its embedded interpreter.

A table operation is sent as the `BatchTableRequest.Operation` message of the table service
in protobuf JSON form, so it runs through the same handler, with the same checks, as the
operation a gRPC client sends. Tables are named by their position in `sources`.

A table method that the table service has no operation for is a local operation
(`console/python/ops.rs`): a name and its arguments as a JSON object.
"""

import json
from collections.abc import Sequence

import _oxhaven

# The native module, for the functions that read a table: `is_flat`, `is_blink`,
# `attributes`, `read_columns`, `to_string`.
native = _oxhaven


def source(index: int = 0) -> dict:
    """The table reference of the `index`-th source of an operation."""
    return {"batch_offset": index}


def sourced(request: dict) -> dict:
    """`request` with the first source as its `source_id`."""
    return {"source_id": source(0), **request}


def table_op(name: str, request: dict, sources=()):
    """Runs the table service operation `name` and returns the handle of its result."""
    return _oxhaven.table_op(json.dumps({name: request}), list(sources))


def local_op(name: str, args: dict, sources=()):
    """Runs the local operation `name` and returns the handle of its result."""
    return _oxhaven.local_op(name, json.dumps(args), list(sources))


def new_table(columns):
    """A static table from `(name, type, values)` triples."""
    return _oxhaven.new_table(list(columns))


def to_sequence(value=None) -> tuple:
    """`None` or an empty sequence is no item, a string or anything that is not a sequence
    is one item, and a sequence is its items.

    Behavior: `deephaven.jcompat.to_sequence`
    """
    if value is None or (isinstance(value, Sequence) and not value):
        return ()
    if isinstance(value, str) or not isinstance(value, Sequence):
        return (value,)
    return tuple(value)
