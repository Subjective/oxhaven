"""What the embedded interpreter runs before any user code.

No Java counterpart for the importer: the Java server finds the `deephaven` package in the
Python environment, and this server carries its own inside the binary. The session helpers
are the counterpart of `py/server/deephaven_internal/script_session` and of the output
redirection in `io.deephaven.engine.util.PythonEvaluatorJpy`.
"""

import importlib.abc
import importlib.machinery
import importlib.util
import os
import sys
import traceback

import _oxhaven


class _EmbeddedImporter(importlib.abc.MetaPathFinder, importlib.abc.InspectLoader):
    """Imports the modules whose source the server binary holds.

    `sources` maps a module name to `(source text, is package)`. The importer comes first on
    `sys.meta_path`, so a `deephaven` package installed in the environment is not used,
    but for the modules it has and the binary has not (`_find_added`).
    """

    def __init__(self, sources):
        self._sources = sources

    @staticmethod
    def _filename(name, is_package):
        path = name.replace(".", "/")
        return f"<oxhaven>/{path}/__init__.py" if is_package else f"<oxhaven>/{path}.py"

    def find_spec(self, name, path=None, target=None):
        if name not in self._sources:
            return self._find_added(name)
        is_package = self._sources[name][1]
        return importlib.util.spec_from_loader(
            name, self, origin=self._filename(name, is_package), is_package=is_package
        )

    def _find_added(self, name):
        """A module that a distribution of the environment adds to a package of the binary,
        as the plugins `deephaven.plot.express` and `deephaven.ui` add theirs to the package
        of the reference: it is looked for in the directories of the package's name on
        `sys.path`. A module the binary holds is never looked for there.
        """
        parent = name.rpartition(".")[0]
        if not self._sources.get(parent, ("", False))[1]:
            return None
        relative = parent.replace(".", os.sep)
        directories = [os.path.join(entry, relative) for entry in sys.path if entry]
        directories = [directory for directory in directories if os.path.isdir(directory)]
        if not directories:
            return None
        return importlib.machinery.PathFinder.find_spec(name, directories)

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


_session_globals = None


def new_globals():
    """The globals of a script session: those of a fresh `__main__`. They are the query
    scope of the server from now on."""
    global _session_globals
    _session_globals = {"__name__": "__main__", "__builtins__": __builtins__, "__doc__": None}
    return _session_globals


def unwrap(value):
    """The engine's handle of a value that wraps one, such as the package's `Table`;
    otherwise the value itself.

    Behavior: `deephaven._wrapper.javaify`, without the wrapping of other Python objects
    """
    handles = (
        _oxhaven.TableHandle,
        _oxhaven.PartitionedHandle,
        _oxhaven.HierarchicalHandle,
        _oxhaven.FigureHandle,
    )
    if isinstance(value, handles):
        return value
    try:
        handle = getattr(value, "j_object", None)
    except Exception:  # a property of somebody else's class
        return value
    return handle if isinstance(handle, handles) else value


def changes(before, after):
    """What differs between two snapshots of the globals: a list of
    `(name, old value, new value)`, with None for a variable that does not exist. Which of
    the values clients can open is for the caller to say.

    Behavior: `deephaven_internal.script_session.create_change_list`
    """
    changed = []
    for name, new_value in after.items():
        if not isinstance(name, str):
            continue
        if name not in before:
            changed.append((name, None, unwrap(new_value)))
        elif new_value is not before[name]:
            changed.append((name, unwrap(before[name]), unwrap(new_value)))
    for name, old_value in before.items():
        if isinstance(name, str) and name not in after:
            changed.append((name, unwrap(old_value), None))
    return changed


def caller_scope():
    """The variables that a formula of the table operation being called may name: None for
    the globals of the session, or a dict of the globals and the locals of the code that
    called the `deephaven` package, the locals first.

    The caller is the nearest frame outside this package. Code at the top level of a
    command sees the session's globals as they are. Code in a function, a class body, a
    lambda or another module sees a copy of its own globals and locals, as they are now;
    the variables of an enclosing function are not among them unless the code uses them.

    Behavior: `deephaven._query_scope.query_scope_ctx`
    """
    frame = sys._getframe(1)
    while frame is not None and frame.f_code.co_filename.startswith("<oxhaven>/"):
        frame = frame.f_back
    if frame is None:
        return None
    if frame.f_globals is _session_globals and frame.f_locals is frame.f_globals:
        return None
    scope = dict(frame.f_globals)
    scope.update(frame.f_locals)
    return scope


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
