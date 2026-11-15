"""What the embedded interpreter runs before any user code.

No Java counterpart for the importer: the Java server finds the `deephaven` package in the
Python environment, and this server carries its own inside the binary. The session helpers
are the counterpart of `py/server/deephaven_internal/script_session` and of the output
redirection in `io.deephaven.engine.util.PythonEvaluatorJpy`.
"""

import importlib.abc
import importlib.util
import sys
import traceback

import _oxhaven


class _EmbeddedImporter(importlib.abc.MetaPathFinder, importlib.abc.InspectLoader):
    """Imports the modules whose source the server binary holds.

    `sources` maps a module name to `(source text, is package)`. The importer comes first on
    `sys.meta_path`, so a `deephaven` package installed in the environment is not used.
    """

    def __init__(self, sources):
        self._sources = sources

    @staticmethod
    def _filename(name, is_package):
        path = name.replace(".", "/")
        return f"<oxhaven>/{path}/__init__.py" if is_package else f"<oxhaven>/{path}.py"

    def find_spec(self, name, path=None, target=None):
        if name not in self._sources:
            return None
        is_package = self._sources[name][1]
        return importlib.util.spec_from_loader(
            name, self, origin=self._filename(name, is_package), is_package=is_package
        )

    def is_package(self, name):
        return self._sources[name][1]

    def get_source(self, name):
        return self._sources[name][0]

    def get_code(self, name):
        source, is_package = self._sources[name]
        return compile(source, self._filename(name, is_package), "exec")


def install(sources):
    """Makes the embedded modules importable. Called once per process."""
    sys.meta_path.insert(0, _EmbeddedImporter(sources))


class _LogStream:
    """A text stream that hands each complete line to the server's log under one level."""

    encoding = "utf-8"
    errors = "replace"

    def __init__(self, level):
        self._level = level
        self._pending = ""

    def write(self, text):
        if not isinstance(text, str):
            raise TypeError(f"write() argument must be str, not {type(text).__name__}")
        self._pending += text
        while "\n" in self._pending:
            line, _, self._pending = self._pending.partition("\n")
            _oxhaven.log(self._level, line + "\n")
        return len(text)

    def flush(self):
        if self._pending:
            _oxhaven.log(self._level, self._pending)
            self._pending = ""

    def writable(self):
        return True

    def readable(self):
        return False

    def isatty(self):
        return False


def redirect_output():
    """Sends what Python code prints to the server's log, where `SubscribeToLogs` reads it."""
    sys.stdout = _LogStream("STDOUT")
    sys.stderr = _LogStream("STDERR")


def new_globals():
    """The globals of a script session: those of a fresh `__main__`."""
    return {"__name__": "__main__", "__builtins__": __builtins__, "__doc__": None}


def _handle(value):
    """The table handle of a value clients can open, or None."""
    if isinstance(value, _oxhaven.TableHandle):
        return value
    handle = getattr(value, "j_object", None)
    return handle if isinstance(handle, _oxhaven.TableHandle) else None


def changes(before, after):
    """What differs between two snapshots of the globals, for the values clients can open.

    Returns `(created, updated, removed)`, each a list of `(name, handle)`; a variable whose
    kind of value changed would be in `created` and in `removed`.

    Behavior: `deephaven_internal.script_session.create_change_list` and
    `io.deephaven.engine.util.AbstractScriptSession#applyVariableChangeToDiff`
    """
    created, updated, removed = [], [], []
    for name, new_value in after.items():
        if not isinstance(name, str):
            continue
        old_value = before.get(name)
        if name in before and new_value is old_value:
            continue
        old, new = _handle(old_value), _handle(new_value)
        if old is None and new is None:
            continue
        if old is not None and new is not None and old.is_same(new):
            continue
        if old is None:
            created.append((name, new))
        elif new is None:
            removed.append((name, old))
        else:
            updated.append((name, new))
    for name, old_value in before.items():
        if not isinstance(name, str) or name in after:
            continue
        old = _handle(old_value)
        if old is not None:
            removed.append((name, old))
    return created, updated, removed


def run(code, namespace):
    """Runs `code` as a module body in `namespace`. Returns None, or the text of the error
    as the interpreter would print it.

    Behavior: `io.deephaven.engine.util.PythonEvaluatorJpy#evalScript`
    """
    try:
        exec(compile(code, "<string>", "exec"), namespace)
    except BaseException as error:  # a script may raise SystemExit or KeyboardInterrupt
        # the frame of this function is not part of what the user ran
        trace = error.__traceback__.tb_next if error.__traceback__ else None
        return "".join(traceback.format_exception(type(error), error, trace))
    finally:
        sys.stdout.flush()
        sys.stderr.flush()
    return None
