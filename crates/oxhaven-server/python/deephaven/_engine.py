"""The one place the package calls into the server: `_oxhaven`, the module the server
registers in its embedded interpreter.

A table operation is sent as the `BatchTableRequest.Operation` message of the table service
in protobuf JSON form, so it runs through the same handler, with the same checks, as the
operation a gRPC client sends. Tables are named by their position in `sources`.

A table method that the table service has no operation for is a local operation
(`console/python/ops.rs`): a name and its arguments as a JSON object.
"""

import json
import threading
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


_recording = threading.local()


class Recording:
    """The operations that table methods send to the engine while it is entered, as
    steps that the engine can run again on other tables (`console/python/proxy.rs`):
    what a `PartitionedTableProxy` applies to every constituent.

    `inputs` are the handles of the tables that stand for the tables of a later run.
    Every other table an operation reads that no step made is an external one, the same
    table in every run. A source is `(kind, index)`: an input, an external table, or
    what a step made.
    """

    def __init__(self, inputs):
        self.inputs = list(inputs)
        self.externals = []
        self.steps = []
        self._made = []

    def source(self, handle) -> tuple:
        """The source that `handle` is; one that is not known yet is an external table."""
        for kind, known in (("step", self._made), ("input", self.inputs)):
            for index in reversed(range(len(known))):
                if known[index].is_same(handle):
                    return (kind, index)
        for index, external in enumerate(self.externals):
            if external.is_same(handle):
                return ("external", index)
        self.externals.append(handle)
        return ("external", len(self.externals) - 1)

    def add(self, kind: str, name: str, body: str, sources, made) -> None:
        self.steps.append((kind, name, body, [self.source(handle) for handle in sources]))
        self._made.append(made)

    def __enter__(self):
        self._before = getattr(_recording, "current", None)
        _recording.current = self
        return self

    def __exit__(self, *error):
        _recording.current = self._before
        return False


def _record(kind: str, name: str, body: str, sources, made):
    recording = getattr(_recording, "current", None)
    if recording is not None:
        recording.add(kind, name, body, sources, made)
    return made


_recording = threading.local()


class Recording:
    """The operations that table methods send to the engine while it is entered, as
    steps that the engine can run again on other tables (`console/python/proxy.rs`):
    what a `PartitionedTableProxy` applies to every constituent.

    `inputs` are the handles of the tables that stand for the tables of a later run.
    Every other table an operation reads that no step made is an external one, the same
    table in every run. A source is `(kind, index)`: an input, an external table, or
    what a step made.
    """

    def __init__(self, inputs):
        self.inputs = list(inputs)
        self.externals = []
        self.steps = []
        self._made = []

    def source(self, handle) -> tuple:
        """The source that `handle` is; one that is not known yet is an external table."""
        for kind, known in (("step", self._made), ("input", self.inputs)):
            for index in reversed(range(len(known))):
                if known[index].is_same(handle):
                    return (kind, index)
        for index, external in enumerate(self.externals):
            if external.is_same(handle):
                return ("external", index)
        self.externals.append(handle)
        return ("external", len(self.externals) - 1)

    def add(self, kind: str, name: str, body: str, sources, made) -> None:
        self.steps.append((kind, name, body, [self.source(handle) for handle in sources]))
        self._made.append(made)

    def __enter__(self):
        self._before = getattr(_recording, "current", None)
        _recording.current = self
        return self

    def __exit__(self, *error):
        _recording.current = self._before
        return False


def _record(kind: str, name: str, body: str, sources, made):
    recording = getattr(_recording, "current", None)
    if recording is not None:
        recording.add(kind, name, body, sources, made)
    return made


def table_op(name: str, request: dict, sources=()):
    """Runs the table service operation `name` and returns the handle of its result."""
    body, sources = json.dumps({name: request}), list(sources)
    return _record("table", name, body, sources, _oxhaven.table_op(body, sources))


def local_op(name: str, args: dict, sources=()):
    """Runs the local operation `name` and returns the handle of its result."""
    body, sources = json.dumps(args), list(sources)
    return _record("local", name, body, sources, _oxhaven.local_op(name, body, sources))


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
