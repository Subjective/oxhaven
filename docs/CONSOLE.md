# The console and the Python script session

How Oxhaven runs the code a user types into the web IDE's console or sends with
`pydeephaven`'s `run_script`. Written by task 6.1 (`tasks/6.1-console-design.md`), which also
built the first slice. The follow-on tasks are 6.2 to 6.18.

## Decision

1. **CPython is embedded in the server process**, through PyO3, as the Java server embeds it
   through jpy. User code runs in that interpreter. `crates/oxhaven-server/src/console/python`.
2. **The `deephaven` package is Oxhaven's own**, written anew with the names and signatures of
   the reference package (`py/server/deephaven`), and compiled into the server binary
   (`crates/oxhaven-server/python/deephaven`). Its `Table` wraps a handle to an engine table.
3. **Every table operation goes through the table service's handler for that operation.** The
   Python method builds the `BatchTableRequest.Operation` message of the operation as protobuf
   JSON, and one native function (`_oxhaven.table_op`) runs it through the same code as a gRPC
   client's request (`table::run_local`), with the tables passed by position. Query strings go
   to the Rust expression engine.
4. **The scope is the interpreter's global variables**, and nothing else holds it: a scope
   ticket `s/<name>` reads the global, and a client that publishes to one sets it. The
   globals are compared with a snapshot before and after each command, after each variable
   a client sets, and every 100 ms; variables that now hold, or no longer hold, a table are
   sent to `ListFields` listeners under the application id `scope`, and those a command
   changed are reported in its answer. Formulas name the variables; see "The query scope".
5. **It is a cargo feature**, `python`, off by default. A default build needs no Python, has no
   console and behaves as before. `cargo build -p oxhaven-server --features python` gives a
   server whose console type is `python`.

The language-independent half (the gRPC service, scope publication, the log buffer) is in
`console/` behind the trait `ScriptSession` and is compiled and tested in every build.

## Why

- **It is how users write Deephaven Python.** Tables are plain objects in top-level
  variables, a method call returns immediately with a new table, and anything in the process
  can be handed to anything else. Three things users rely on need the interpreter and the
  engine in one address space: Python functions called from query strings, table listeners
  written in Python, and conversion to and from numpy, pandas and Arrow without copying
  through a socket.
- **The handler seam gives the whole operation surface at once.** The server already has a
  handler per table operation with Java's argument checks. Going through them means a
  Python method is a few lines of Python and no Rust, an operation cannot behave
  differently for Python than for a gRPC client, and the Python API tasks can be done in
  parallel without touching shared Rust.
- **The engine does not need the interpreter lock.** Engine work runs with the lock
  released, and update cycles never take it. That stays true until Python functions run
  inside formulas (task 6.11).

## Alternatives rejected

- **A sidecar Python process that talks to the server over gRPC.** It isolates crashes, a
  runaway command can be killed, and the server binary needs no libpython. But every table
  is then a remote export with a lifetime to manage, a Python function in a formula costs a
  serialized round trip per chunk, listeners and data conversion cross a socket, the scope
  has to be synchronised between two processes, and the `deephaven` API is not
  `pydeephaven`'s, so little is reused beyond the transport. It gives up the three things
  above to buy isolation that the Java server does not have either. The `ScriptSession` trait
  and the JSON operation seam do not rule it out later as a second kind of session.
- **A console in another language first.** There is no user base for one: the IDE, the
  documentation and the clients' `run_script` are Python (or Groovy, which needs a JVM). It
  would postpone the question, not answer it.
- **One native function per operation over the engine's Rust API.** More Rust per operation,
  and it bypasses the handlers' request checks, which would then exist twice and drift.
- **Using the reference `deephaven` package as it is.** It is written against jpy: nearly every
  method calls a Java object (`self.j_table.update(...)`), so it cannot run without a JVM or a
  jpy imitation as large as the package. It is also not open source: `py/server` is under
  the Deephaven Community License Agreement 1.0 (`LICENSE.md` of the reference; `setup.py`
  declares "Other/Proprietary License"), which allows internal use and derivative works for
  internal use, and redistribution only under the same agreement with the restriction of its
  section 4. Nothing from it is copied into this repository. The names, signatures and error
  message texts that callers depend on are reproduced; docstrings and code are not. Whether
  anything may ever be copied is the owner's decision, with the licence read by someone
  qualified.

## How it fits together

```
ConsoleService (console/mod.rs)          any build
  Console (console/session.rs)           holds the Scope, publishes Changes to Applications
    trait ScriptSession
      PythonSession (console/python)     feature `python`
        _oxhaven            native module: TableHandle, table_op, new_table, log
        _oxhaven_bootstrap  importer for the embedded modules, output streams, globals diff
        deephaven           the package users import
```

- **One session per server**, as in Java: every console a client starts is the same
  interpreter and sees the same variables. One interpreter per process also means the
  package reaches the engine through process-wide state (`python::RUNTIME`).
- **Locks**: update graph first, then the interpreter lock. A command runs under the graph's
  shared lock, so no cycle runs during it; Java holds the exclusive lock, which also keeps
  other clients' operations out. Commands of one client session run one after the other on
  its serial queue.
- **Output**: `sys.stdout` and `sys.stderr` are replaced by streams that hand each line to the
  server's log buffer under the levels `STDOUT` and `STDERR`. Clients read it with
  `SubscribeToLogs`; `ExecuteCommand` does not carry output, in Java or here.
- **Errors**: an exception in user code is an `OK` answer with `error_message`, and the
  variables assigned before it are still reported. An engine error reaches Python as
  `DHError` with the reference package's message for the operation and the engine's text.

## Build and CI

- PyO3 0.27 chooses the Python to link when the server is built: the one `PYO3_PYTHON`
  names, else the `python` of the virtual environment `VIRTUAL_ENV` names, else `python` or
  `python3` on `PATH`. It links that Python's shared libpython, and the server then needs
  that libpython at run time (`LD_LIBRARY_PATH`, if it is not in a directory the loader
  searches). No Python is needed for the default build.
- This machine: Python 3.13.18 in `/usr/local`, `libpython3.13.so` present, links and runs.
- **Supported Python versions: 3.11 to 3.14.** 3.11 is the oldest that pandas 3 and
  pyarrow 26 install on, the versions the reference's recordings were made with; 3.14 is
  the newest that PyO3 0.27 builds against. The reference package declares 3.9 to 3.13.
  The oracle's environment, and so the script scenarios, are Python 3.13 only.
- GitHub Actions (`.github/workflows/ci.yml`):
  - `python-console` runs clippy and the whole test suite of `oxhaven-server` with the
    feature, once on the oldest and once on the newest supported Python
    (`actions/setup-python`). It installs no packages and names no environment, so the
    tests of the server may import only the standard library: they have to pass on every
    supported Python, with whatever is or is not installed for it.
  - `oracle-replay` builds the server with the feature (`oracle/bin/ci-replay.sh` does) and
    runs the script scenarios against it. What needs numpy, pandas or pyarrow is gated
    there, in the pinned environment described below.
  - `rust` builds without the feature.

## The Python environment

The interpreter in the server is the libpython the server was linked against. Which
standard library and which installed packages it sees is decided when the server starts
(`console/python/env.rs`), in this order:

1. `--python-home <directory>`: a virtual environment (a directory with `pyvenv.cfg`) or the
   prefix of a Python installation (as `PYTHONHOME` names one).
2. The directory `VIRTUAL_ENV` names, which is what activating a virtual environment sets.
3. Neither: the installation the linked Python finds by itself, with that installation's
   own site-packages. This is the Python the server was built with when it has not moved.

`--python-path <directories>`, separated as in `PATH`, adds directories that are searched
before the environment's own, as `PYTHONPATH` does (which is honoured too). They are plain
module directories: `.pth` files in them are not read.

In a virtual environment the interpreter starts as that environment's `python` would: it
reads `pyvenv.cfg`, takes the standard library from the base installation and the packages
from the environment, and with `include-system-site-packages = false`, the default, does not
see the base installation's packages. The `deephaven` package is always the server's own
(the embedded importer comes first), whatever the environment has installed.

**The environment must be one of the linked Python's minor version.** Packages with native
code are built for one minor version, and an environment of another keeps them in a
directory this interpreter does not look in. The server checks before it starts the
interpreter (the version in `pyvenv.cfg`, and the directory `lib/pythonX.Y`) and refuses
to start:

```
the Python environment /home/me/venv is for Python 3.12, and this server is linked against
Python 3.13: name an environment made with Python 3.13, or build the server against Python
3.12 (PYO3_PYTHON, docs/CONSOLE.md)
```

A server with a console in a new environment:

```bash
python3 -m venv /tmp/console-env                 # the python3 the server was built with
/tmp/console-env/bin/pip install numpy
VIRTUAL_ENV=/tmp/console-env oxhaven-server      # or: . /tmp/console-env/bin/activate
# or: oxhaven-server --python-home /tmp/console-env
```

The server logs `Python console: Python 3.13.18, packages from /tmp/console-env`, and
`import numpy` works in the console.

`GetConfigurationConstants` reports the interpreter's version as `python.version`, as Java
does (`oracle/ide/traces`: `python.version` is `3.13.18` there). A server without a Python
console leaves the key out. One difference: Java reports it also when its console is
disabled (the `noconsole` traces), because a Java server with a disabled console still has
its Python session, and this server started with `--console none` has no interpreter.

### The same packages in CI and on a developer's machine

A script scenario's outcome can depend on what is installed: `import pandas` fails on a
runner whose Python has no pandas and works on a machine that has it. So the replay gate
does not use whatever the linked Python happens to have. It runs the server in the
harness's environment, `oracle/.venv`, which is made from `oracle/requirements-replay.txt`:
numpy, pandas and pyarrow in the versions the Java reference had when the recordings were
made. The `oracle-replay` job sets `VIRTUAL_ENV` to it; locally, do the same:

```bash
VIRTUAL_ENV=$PWD/oracle/.venv oracle/bin/ci-replay.sh
```

`oracle/.venv` has to be made by the Python that the server is built with, which is
Python 3.13 in both places, the version the oracle is pinned to; the server refuses an
environment of another version. Without `VIRTUAL_ENV` the gate's server uses the
linked Python's own packages, and its outcome is that machine's.

### Distribution: a binary per Python minor version

The server stays a binary that embeds the interpreter, linked against one libpython minor
version per build, and finds its packages as above. A release ships one binary per supported
Python minor version, each needing that version's libpython on the machine.

The alternative was the server as an extension module in a wheel: `pip install`, then
`python -m oxhaven`, as `deephaven-server` starts Java. Python then starts the server, not
the other way round, and `pip` owns the environment. It was weighed and not built:

- What it adds for a user is small once a virtual environment is honoured: `pip install`
  what the scripts need into an environment and start the server in it.
- It removes the coupling to a Python minor version only as a stable-ABI (`abi3`) wheel,
  which confines the console to the stable ABI from then on. Any other wheel is one build
  per minor version, as the binary is.
- It is a second way to build the crate that the tests do not cover: an extension module
  must not link libpython, and the test binaries, which embed the interpreter, must. The
  checks would run on one form and users would get the other.
- The process would be Python's: its main thread, its signal handling, its interpreter
  lock held around the call that runs the server. With the binary the server owns the
  process and Python is a library in it.

Nothing here rules a wheel out later as packaging around the same crate. The embedded
importer for the `deephaven` package serves both forms.

## Risks

1. **Python functions in query strings** (task 6.11) are the hard part and are not designed
   in detail. Update cycles will then need the interpreter lock, which serialises them with
   console commands and with each other, and makes the lock order above a rule every
   caller has to keep. This is the main risk of the design; a sidecar would not remove it.
2. **The package is large and reimplemented**: the reference is about 15,000 lines, 5,000 of
   them `table.py`. Compatibility is only as good as its tests. Task 6.3 adds recorded
   script scenarios so that each API task is gated against Java, as the operators are.
3. **No isolation**: a crash in a native extension, or a Python thread that never ends, is
   the server's. The same holds for Java.
4. **A command cannot be interrupted** (task 6.15). Java does not implement `CancelCommand`
   either.
5. **Deployment is coupled to a libpython minor version**: one binary per supported Python
   ("Distribution" above).
6. **Error text** inside `error_message` is this engine's, not a Java stack trace. Clients
   show it and do not parse it; `PARITY.md` A5 does not compare error text.

## What exists after task 6.1

With `--features python`:

- `GetConsoleTypes`, `StartConsole`, `ExecuteCommand`, `BindTableToVariable`,
  `SubscribeToLogs`, `GetHeapInfo`, `AutoCompleteStream`; `CancelCommand` and
  `CancelAutoComplete` answer `UNIMPLEMENTED` with Java's text, as Java does.
- `from deephaven import empty_table, time_table, new_table`, `deephaven.column`
  (`bool_col`, `int_col`, `long_col`, `double_col`, `string_col`), `DHError`, and `Table` with
  `update`, `lazy_update`, `view`, `update_view`, `select`, `where`, `head`, `tail`, `size`,
  `is_refreshing`, `column_names`, `j_table`, `j_object`.
- The web IDE's console route passes unchanged (`oracle/ide/ide-test.js --routes console`),
  and `pydeephaven`'s `run_script`, `open_table` and `bind_table` work.

Stubbed or missing, each with its task:

| What | Now | Task |
| --- | --- | --- |
| Values that are not tables (figures, partitioned tables, plugin objects) | resolve as objects; none has a type name yet, so none is listed (`console::session::type_name_if_displayable`) | 7.6 to 7.10 |
| Scope variables in formulas | numbers, strings and booleans; the rest is refused, see "The query scope" | no task yet |
| The rest of the Python API | absent: `ImportError` or `AttributeError` | 6.4 to 6.12 |
| Autocomplete | answers "nothing found" (Java without `jedi`); no `Open`/`NextAutoCompleteStream` | 6.13 |
| The log stream | holds only what scripts print, not the server's log; never refuses a slow client; ends with the connection, not the session | 6.14 |
| Stopping a running command | not possible | 6.15 |
| `GetHeapInfo` | the process's resident memory of the machine's physical memory; `free_memory` is 0 | 6.14 |

## The query scope

Added by task 6.2. What Java does was observed on Java 42.9 with the formulas in
`crates/oxhaven-server/src/app/scope.rs` (tests) and `oracle/console/probe.py`.

- **One holder.** `app::Scope` asks the script session (`ScopeHolder`, which `Console`
  implements) when there is one, and keeps its own map only in a server without a console.
  `s/<name>` resolves any variable: a table as a table, anything else as an object export.
  `None` and a missing name are `NOT_FOUND`. A Flight descriptor `scope/<name>` resolves a
  table to the ticket `s/<name>`.
- **Publishing** a table to `s/<name>` sets the global to the package's `Table`; publishing
  an object that came from Python sets the same object. Any other server object would need
  Java's wrapper for it (`PythonObjectWrapper#wrap`) and is refused.
- **Observation.** Java compares the scope with its last snapshot before and after each
  command, after `setVariable`, and every 100 ms (`DeephavenApiServer`); so does
  `Console::observe`, with `Console::watch` for the interval. A command's answer holds what
  changed during the command, not what a thread changed before it.
- **Formulas.** A name in a formula that is no column, no column array and none of `i`, `ii`,
  `k` is looked up in the scope, before the language's constants. The value is the one at
  the moment the formula is compiled. Called from a function, class body, lambda or
  comprehension, the scope is the globals and locals of that caller only, as in Java. The
  formulas of gRPC clients see the session's globals. There is no contract change: the
  server wraps the `FormulaCompiler` it hands to operations (`app::ScopedCompiler`), which
  replaces each variable by a literal of the type Java gives it.

| Python value | Type in a formula |
| --- | --- |
| `int` | the smallest of `byte`, `short`, `int`, `long` that holds it |
| `float` | `double` |
| `str` | `String` |
| `bool` | `Boolean` |

Java holds these as boxed objects, and its parser treats a boxed variable unlike a
primitive in places. Those that were observed are kept: the negation of a numeric variable
(`-x`) fails; a cast of a variable is the identity or a widening one; a conditional with a
variable as a branch needs the other branch to be `null`, a variable of the same type, or
a value of exactly the variable's primitive type. A Python `int` outside 64 bits anywhere in
the scope fails every formula that is compiled (`OverflowError: int too big to convert`),
as in Java.

Refused here though Java computes a result (`FormulaError::Unsupported` unless noted):

- A variable that holds `None`, a list, a dict, a callable or any other object.
- A variable whose value is the null value of its type (`-128`, `-32768`, `-2**31`,
  `-2**63`, the most negative `double`): Java treats it as null in some places and as a
  number in others.
- A method called on a numeric or boolean variable (`x.intValue()`).
- A function called with a `byte` variable where the result type depends on the overload
  Java picks for a `Byte` (`abs(x)` is a `short` in Java, `max(x, y)` of two bytes a `byte`).
- A comparison of a column with a variable, as the whole formula, where converting the
  variable to the column's type would change it (`X >= 1.5` for an `int` column, an `int`
  column against a `long`): Java makes these match and range filters over the converted
  value. In `where` the simple forms of these fail in the engine's own filter parsing
  ("Failed to convert value"), and so do `in`, `not in` and `icase in` with a variable.
- `where("X > 1")`, a filter that never reaches the compiler, succeeds while an `int`
  outside 64 bits is in the scope; Java fails it.

## Differences from Java, per call

Observed with `oracle/console/probe.py` on Java 42.9 and on this server; the two transcripts
are `oracle/console/java-42.9.txt` and `oracle/console/oxhaven.txt`, and their `diff` is this
list.

| Call | Difference |
| --- | --- |
| `GetConsoleTypes`, `StartConsole` | none |
| `ExecuteCommand` | `changes` and the status are the same in every case tried. `error_message` starts `Error in Python interpreter:` and holds the same Python traceback; Java's starts `java.lang.RuntimeException: Error in Python interpreter:` and continues with the JVM's stack. For an engine error the traceback names this package's files and this engine's message. |
| `ExecuteCommand` on a console id that is never exported | none: both wait for the export |
| `BindTableToVariable` | none |
| `SubscribeToLogs` | script output is the same, record for record. Java also sends its server log (each line under its level and again as a `STDERR` line), including "Evaluating command" and the stack of every script error. |
| `GetHeapInfo` | different quantities; see above |
| `CancelCommand`, `CancelAutoComplete` | none: `UNIMPLEMENTED`, same text |
| `AutoCompleteStream` | Java with `jedi` installed returns completions and does not answer `open_document`; this server answers every request with success and no items, as Java does without `jedi` |
| `ListFields` | none for tables |
| `FetchTable` of `s/<name>` | none: a variable that is not a table resolves and is refused with `INVALID_ARGUMENT` |
| `ExportFromTicket` and `PublishFromTicket` with `s/<name>` | none in the cases tried: a number is exported and published as the same Python object, a table is published as the package's `Table` |
| `FetchTable` of a table with columns made from scope variables | the same columns and types; the encoded schema of the probe's table `r` is 16 bytes longer here and decodes to the same schema. Not looked into: it is the schema encoding, not the scope. |
| A formula that names a list | Java makes a column of `org.jpy.PyListWrapper`; refused here |
| The IDE | the same 20 calls in the same order with the same statuses on both servers for the console route |
