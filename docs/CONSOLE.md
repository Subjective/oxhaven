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
4. **The scope is the interpreter's global variables.** After each command the globals are
   compared with a snapshot from before it. Variables that now hold, or no longer hold, a
   table are reported in the answer, become resolvable as `s/<name>`, and are sent to
   `ListFields` listeners under the application id `scope`.
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
  Console (console/session.rs)           publishes Changes to Scope and Applications
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

- PyO3 0.27 finds `python3` on `PATH` at build time, or the Python `PYO3_PYTHON` names, and
  links its shared libpython; the server then needs that libpython at run time
  (`LD_LIBRARY_PATH`, if it is not in a directory the loader searches). No Python is needed
  for the default build.
- This machine: Python 3.13.18 in `/usr/local`, `libpython3.13.so` present, links and runs.
- **Supported Python versions: 3.11 to 3.13.** 3.11 is the oldest that pandas 3 and
  pyarrow 26 install on, the versions the reference's recordings were made with; 3.13 is
  the newest the reference package declares and the one the oracle is pinned to.
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
`import numpy` works in the console. `GetConfigurationConstants` reports the interpreter's
version as `python.version`, as Java does when its console is Python; a server without a
Python console leaves the key out.

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

`oracle/.venv` has to be made by the `python3` that the server is built with, which is
Python 3.13 in both places: the pins install on no other supported version, and the server
refuses an environment of another one. Without `VIRTUAL_ENV` the gate's server uses the
linked Python's own packages, and its outcome is that machine's.

### Distribution: a binary per Python minor version

The server stays a binary that embeds the interpreter, linked against one libpython minor
version per build, and finds its packages as above. The alternative, the server as an
extension module in a wheel (`pip install`, then `python -m oxhaven`, as `deephaven-server`
starts Java), was weighed and not built:

- It does not remove the coupling to a Python version. PyO3's stable-ABI build (`abi3`)
  would, but the console needs what the stable ABI does not have, among it the
  initialization configuration used here and, with task 6.11, buffer access for numpy; a
  wheel without `abi3` is one build per minor version, as the binary is.
- It needs a second build of the crate that the tests cannot share: an extension module
  must not link libpython, and the test binaries embed it. Every check would run twice or
  the wheel would go untested.
- The process then belongs to Python: the main thread, signal handling and the interpreter
  lock are Python's, and the server's runtime has to live inside them. With the binary the
  server owns the process and Python is a library in it, which is also what the lock order
  above assumes.
- What it would buy, `pip` owning the environment, a virtual environment already gives:
  `pip install` what the scripts need into it and start the server there.

A wheel remains possible later as packaging around the same crate; nothing here rules it
out. What a release has to ship is one binary per supported Python minor version, each
needing that version's libpython on the machine.

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
| Values that are not tables (figures, partitioned tables, plugin objects) | not scope variables; `s/<name>` is `NOT_FOUND` where Java resolves any variable | 6.2 |
| A client publishing to `s/<name>` | the value is a scope variable but not a Python global | 6.2 |
| Variables changed outside a command (a thread, a listener) | seen at the next command | 6.2 |
| The rest of the Python API | absent: `ImportError` or `AttributeError` | 6.4 to 6.12 |
| Autocomplete | answers "nothing found" (Java without `jedi`); no `Open`/`NextAutoCompleteStream` | 6.13 |
| The log stream | holds only what scripts print, not the server's log; never refuses a slow client; ends with the connection, not the session | 6.14 |
| Stopping a running command | not possible | 6.15 |
| `GetHeapInfo` | the process's resident memory of the machine's physical memory; `free_memory` is 0 | 6.14 |

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
| `FetchTable` of `s/<name>` | none for a table. For a variable that is not a table Java answers `INVALID_ARGUMENT` (it resolves, and is not a table); this server answers `NOT_FOUND`. |
| The IDE | the same 20 calls in the same order with the same statuses on both servers for the console route |
