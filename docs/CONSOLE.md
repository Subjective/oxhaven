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
  its serial queue. A command gives the lock up only while it waits for its own write to an
  input table ("Input tables written from a command").
- **Threads**: the interpreter is started on a thread of its own, Python's main thread, and
  every command runs there, one at a time; that is what lets a command be stopped
  ("Stopping a running command"). Everything else that needs the interpreter (reading a
  scope variable, a formula's variables, the scope watcher) takes its lock on the thread
  it is called on.
- **Output**: `sys.stdout` and `sys.stderr` are replaced by streams that hand each line to the
  server's log buffer under the levels `STDOUT` and `STDERR`. Clients read it with
  `SubscribeToLogs`, with the server's own log ("The log stream"); `ExecuteCommand` does
  not carry output, in Java or here.
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
    supported Python, with whatever is or is not installed for it. The tests of
    completion that need `jedi` say that they did not run, and pass.
  - `oracle-replay` builds the server with the feature (`oracle/bin/ci-replay.sh` does) and
    runs the script scenarios against it. What needs numpy, pandas or pyarrow is gated
    there, in the pinned environment described below. That environment has `jedi` and
    `docstring_parser`, and the job then runs the tests of completion in it with
    `OXHAVEN_REQUIRE_JEDI=1`, which makes a test that cannot find them fail.
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
(the embedded importer comes first), whatever the environment has installed. A module of
`deephaven` that the server does not have and a distribution of the environment does, as the
plugins `deephaven.plot.express` and `deephaven.ui` install theirs, is taken from the
environment.

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
made, and `jedi` and `docstring_parser` in the versions the Java reference completes with. `oracle/bin/ci-replay.sh` sets `VIRTUAL_ENV` to it unless `VIRTUAL_ENV` already names an environment, in CI and locally alike:

```bash
oracle/bin/ci-replay.sh
```

`oracle/.venv` has to be made by the Python that the server is built with, which is
Python 3.13 in both places, the version the oracle is pinned to; the server refuses an
environment of another version. A server started without `VIRTUAL_ENV`, outside the gate,
uses the linked Python's own packages, and its outcome is that machine's.

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
4. **A command that never ends** holds the graph's lock, so no table ticks. Java has no
   way out but a restart; here the client cancels the call ("Stopping a running
   command"). Native code that does not return still cannot be stopped.
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
| Values that are not tables | a partitioned table and an object of a plugin's type are listed with the type (`docs/OBJECTS.md`); a figure and a hierarchical table have no type yet, so they are not listed | 7.8, 7.11 |
| Scope variables in formulas | numbers, strings and booleans; the rest is refused, see "The query scope" | no task yet |
| The rest of the Python API | absent: `ImportError` or `AttributeError` | 6.4 to 6.12 |

Done since: the log stream and `GetHeapInfo` (task 6.14, "The log stream" and "The memory
figures"), stopping a running command (task 6.15), completion (task 6.13, "Completion").

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
| `java.time.Instant` of the package (`to_j_instant`) | `Instant`, as an instant literal is (task 5.8) |
| A Java array of `jpy.array` or `dtypes.array` | its class (`long[]`), the same array in every row (task 11.2, "Java arrays") |
| `list`, `dict` | none; an item under a cast is a value, see below (task 5.8) |

**Items of a list and a dict** (task 5.8). Java holds a list as `org.jpy.PyListWrapper` and
a dict as `org.jpy.PyDictWrapper`; `name[index]` and `name.get(index)` are an
`org.jpy.PyObject`, and a cast of that is a value. `(int)`, `(long)`, `(double)` and
`(String)` of an item are computed here, by the Python side of the functions
(`_oxhaven_udf.item_reader`), from the object itself at the time a row is computed, as in
Java: a list by Python's rules for an index (`-1` is the last item, an index outside the
list or a null one fails with `IndexError`), with an index of `byte`, `short`, `char` or
`int`; a dict with `get`, so that a key it has not is null, with a key of any primitive
type, `String` or `Boolean`, where a null `String` or `Boolean` is the key `None` and a
null number the number Java reserves for null. `None` is null. Any other item is converted
as jpy converts it, with CPython's errors: `(int)` and `(long)` take what has `__index__`
(a `bool` is 0 or 1, a `float` fails) up to 64 bits, and `(int)` keeps the low 32 bits;
`(double)` takes a real number; `(String)` takes a `str` only. Refused though Java computes
them: a cast to another type (`(float)`, `(short)`, `(byte)`, `(char)`, `(Boolean)`), an
item without a cast (a column of `org.jpy.PyObject`), `isNull` of an item, the other
methods of the wrappers (`size`, `containsKey`), and a tuple (Java fails for one too, when
a row is computed). A client's formula is refused the variable, as Java's validator refuses
the method (`get(int) on class org.jpy.PyListWrapper`).

An `Instant` outside the years 1677 to 2262 is refused: see `tasks/5.8-plot-formulas.md`,
"For the owner's ruling".

Java holds these as boxed objects, and its parser treats a boxed variable unlike a
primitive in places. Those that were observed are kept: the negation of a numeric variable
(`-x`) fails; a cast of a variable is the identity or a widening one; a conditional with a
variable as a branch needs the other branch to be `null`, a variable of the same type, or
a value of exactly the variable's primitive type. A Python `int` outside 64 bits anywhere in
the scope fails every formula that is compiled (`OverflowError: int too big to convert`),
as in Java.

Refused here though Java computes a result (`FormulaError::Unsupported` unless noted):

- A variable that holds `None`, a callable or any other object, and a list or a dict
  where it is not an item under a cast.
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

## The log stream

Task 6.14; `crates/oxhaven-server/src/console/logs.rs`. What Java does was read in
`ConsoleServiceGrpcImpl.LogsClient`, `LogBuffer` and the server's `logback.xml`, and
observed on Java 42.9.

- **What is in it.** What scripts print, under `STDOUT` and `STDERR`, and the server's own
  log: every `tracing` event from `INFO` up (the root level of Java's configuration), under
  its level, whose names are Java's (`ERROR`, `WARN`, `INFO`). The text of a log record is
  laid out as Java's `LOGBUFFER` appender lays it out, `thread | logger | message`, the
  thread in 20 characters and the logger in 25; the thread is the name of the Rust thread
  and the logger the module path of the event, cut as logback cuts package names. Both
  servers send `Evaluating command: ...` under `INFO` and `Error running script: ...`
  under `ERROR`, which is what the IDE's Log panel lists. The records before a client
  subscribes are kept (1024, as Java) and sent first, the server's start among them.
- **The second copy is not reproduced.** The Java server of the oracle sends every log
  record again, cut into lines under `STDERR`. That is the oracle's logging configuration
  (`oracle/bin/logback-oracle.xml` adds a console appender on the captured `System.err`,
  so that errors can be found in the server's log file), not Java's: the configuration the
  server ships with writes its console output around the captured stream, with the comment
  "so we don't double up messages to the LogBuffer". A Java server started the usual way
  sends each record once, as this server does.
- **Descriptors 1 and 2 are not captured**, in either server. Java captures `System.out`
  and `System.err`, the objects; this server captures Python's `sys.stdout` and
  `sys.stderr`, the objects. What native code or a subprocess writes to the descriptors
  (`os.write(1, ...)`, `os.system("echo ...")`) goes to the server's own output: observed
  on Java 42.9, and tested here. Capturing the descriptors would mean redirecting them to
  a pipe for the whole process, the server's own log included.
- **A client that does not keep up is ended.** At most 32768 records wait for one client
  (Java's `deephaven.console.subscribeToLogs.bufferSize`, which this server has no flag
  for). The record that does not fit ends the stream with `RESOURCE_EXHAUSTED` and Java's
  text; the records that were waiting are not sent, as in Java.
- **The stream ends with the session**: `CANCELLED`, `Session closed`, as in Java.
- **Not reproduced: the 100 ms send interval.** Java gathers records for up to 100 ms
  before it sends them; this server sends each when it is written.

## The memory figures

Task 6.14; `crates/oxhaven-server/src/console/memory.rs`. Java answers `GetHeapInfo` with
the heap of its JVM, and the IDE's memory widget shows `total_memory - free_memory` as in
use, of `max_memory`. A process without a managed heap has no such three numbers, so each
is the nearest it has:

| Field | Java | Here |
| --- | --- | --- |
| `max_memory` | the largest the heap may become (`-Xmx`) | the memory the process may use: the smallest `memory.max` (or `memory.limit_in_bytes`) of its control group and the groups above it, and the machine's physical memory when none limits it |
| `total_memory` | what the JVM has taken for the heap | the resident set of the process (`VmRSS`): everything it holds in physical memory, the engine's, the interpreter's and native libraries' |
| `free_memory` | the part of the heap that holds no objects | what the C allocator holds and has not handed out (`mallinfo2`: `fordblks`); 0 where the C library is not glibc |

The widget's bar is then the resident memory, less what the allocator can hand out again
without asking the system, of the amount at which the kernel would end the process. The
figures need `/proc`; without it the call is `UNIMPLEMENTED`. The interpreter's own
allocator keeps free memory that `free_memory` does not count, so "in use" errs high.
Since task 9.13 the binary's Rust allocations go to mimalloc (the default `mimalloc`
feature), so `fordblks` covers only what C code (the interpreter, librdkafka) freed into
glibc's allocator; mimalloc soon returns freed pages to the system (under 90 MB resident
30 seconds after a benchmark run), so little free memory is left uncounted (`docs/BENCHMARK.md`, "After task 9.13").

## Stopping a running command

Task 6.15; `crates/oxhaven-server/src/console/python/interrupt.rs`, whose module
documentation is the full account.

**What Java does: nothing.** Observed on Java 42.9 with `time.sleep(25)` and with a loop
of 25 seconds: after the client cancelled the `ExecuteCommand` call, and also after it
closed its session, the command ran to its end, and the next command (of the same
session, and of a new one) waited the remaining 23 seconds for it. `SessionState` cancels
the export and never interrupts the thread that runs it; `CancelCommand` is not
implemented; the web client's cancel throws `UnsupportedOperationException` before it
sends anything. A command that never ends (`while True: pass`) therefore holds the update
graph's lock until the Java server is restarted: no table ticks, and no other command
runs.

**What this server does: it stops the command.** This is a deliberate difference from
Java, the one place where the console does more than the reference; the task's gate asks
for it, and it is allowed difference A7 of `PARITY.md` (approved by the owner on
2026-11-15). It is observable only by a client that cancels a call or closes its session while
its command runs: here the rest of the command does not happen.

- A command is stopped when the client cancels its `ExecuteCommand` call (or the
  connection is lost), and when its session ends. `CancelCommand` stays `UNIMPLEMENTED`.
- A command that is still waiting on its session's queue then does not run at all. (Java
  runs it when its call was cancelled, and does not when its session ended.)
- A running Python command gets `KeyboardInterrupt` where it is. What it assigned before
  is kept and published; its error, the traceback, is logged.

**The mechanism.** Python interrupts code, and wakes it from a blocking call, only through
a signal, whose handler runs on the interpreter's main thread. So the interpreter is
started on a thread of its own, every command runs on that thread, a handler that raises
`KeyboardInterrupt` is installed for `SIGUSR2`, and a stop sends that signal to that
thread (`pthread_kill`, the one `unsafe` call). The thread that asks touches neither the
interpreter nor its lock. When the command has ended no more signals are sent, and one
that arrived too late is consumed before the next command, so a stop never reaches
another command.

**Limits.**

- Stopped: Python code, at the next instruction (`while True: pass`); and the blocking
  calls that Python makes interruptible, at once: `time.sleep`, waiting for a lock, an
  event, a queue, a subprocess, a socket.
- Not stopped: native code that does not return to the interpreter, such as a C extension
  that loops or blocks in a call the signal does not wake. The interrupt is raised when it
  returns. The server is then where the Java server is.
- An engine operation in progress (`t.update(...)`, a sort) is **finished, not
  abandoned**: it runs to its end with the interpreter lock released, its result is
  dropped, and the interrupt is raised when it returns. No engine state is left half built.
- Code that catches `KeyboardInterrupt` and goes on is not stopped by one request. The
  session's end after the call's cancel is a second one.
- Threads a script started are not interrupted, nor is anything that is not a command.
- The process uses `SIGUSR2`. A script that installs its own handler for it disables
  stopping. Scripts run on Python's main thread, so `signal.signal` works in them, which
  it does not in the Java server; a script that takes `SIGINT` takes `Ctrl-C` from the
  server.
- Unix only.

## Input tables written from a command

Task 6.10; `crates/oxhaven-server/src/console/python/input.rs`, and
`oxhaven_engine::graph::yielding_to_cycles`.

`InputTable.add` and `delete` return when the change is in the table, and the script reads
it on the next line. The change is applied by an update cycle, and a command keeps cycles
out. In Java the command holds the exclusive lock and waits on a condition of that lock,
which gives the lock up for the wait. Here the command holds the shared lock, and:

- A command runs inside `yielding_to_cycles`. There, and only there, `run_cycle` called by
  a thread that holds the shared lock releases the thread's holds, waits for the cycle and
  takes them again. Anywhere else the call panics as before, so a handler that reads a
  snapshot under the lock cannot lose it by accident.
- The command releases the interpreter lock before it waits and takes it again after it
  has the graph lock back: graph lock first, interpreter lock second
  (`docs/PYTHON_UDF.md`).
- What the script read from a ticking table before the write may have changed after it, as
  in Java.

`add_async`, `delete_async` and the rows of a `DynamicTableWriter` do not wait. They are
checked, and their rows are read, at the call; one thread, the writer thread, then makes
the changes in the order of the calls and calls the callbacks. A command keeps cycles out,
so these changes show after the command that asked for them, as in Java, unless the
command waits for a synchronous change, which is then made after them. Changes that are
waiting together and are additions to one table, or deletions from one, are made in one
cycle.

| | Java | Here |
| --- | --- | --- |
| `input_table` with `init_table` | the factory fills the table | made empty by the table service's `CreateInputTable`, then the rows are the first change, before the table is handed out |
| A value that is not a string for a string column of a `DynamicTableWriter` | stored unchecked, reads as its `toString` | an `int` or `bool` is written as that text; anything else is refused by the name of the Java method |
| `TableReplayer` | exists | refused by the name of the Java method: the engine has none |
| The text of a `ring_table` error after `failed to create a ring table.` | the Java exception and its stack | the Java exception's class and message |
| A function generated table whose function returns a table without one of the columns | the column keeps the values of the generation before, and null in new rows | the table fails, by the name `FunctionGeneratedTableFactory#copyTable` |
| A function generated table whose function returns another reference type for a column (`String`, then `BigDecimal`) | the values are stored in the column, which keeps its declared type | the table fails, by the name `FunctionGeneratedTableFactory#copyTable` |
| A function generated table that has failed | cannot be fetched; operations on it fail | serves the rows it had; operations on it fail |
| `function_generated_table` with source tables, called on a thread the command then joins | the command never returns: the thread waits for the lock the command holds | the table is made: the thread takes the shared lock beside the command's |
| Callbacks of the asynchronous forms | called on a thread of Java's common pool, in no defined order | called on the writer thread, in the order of the calls |

## Listeners, locks, liveness scopes and execution contexts

Task 6.12; `crates/oxhaven-server/src/console/python/listener.rs` and the modules
`table_listener`, `update_graph`, `liveness_scope` and `execution_context` of the package.

A listener is a node of the update graph (`oxhaven_engine::graph::MergedListener`, for one
table as for several). Its Python function is called by the update cycle, once per cycle
in which a table it listens to changed, after its dependencies are done, and before the
command that made the change has its `add` back. The lock rule is the one of
`docs/PYTHON_UDF.md`: graph lock first, interpreter lock second. The cycle holds the graph
lock and takes the interpreter lock for the call; `shared_lock`, `exclusive_lock`,
`listen` with a replay and `await_update` release the interpreter lock before they wait
for the graph lock. The module documentation of `listener.rs` lists the places and names
the test that stops when one of them is taken out.

A blocking input table edit on a thread inside its own `shared_lock` is refused with
Java's text ("Attempted to make a blocking input table edit while holding the update
graph's shared lock"): the thread keeps out the cycle it would wait for.

In a listener's function, as in Java: neither lock is held, taking one is refused
("Non-terminal notifications must not lock the update graph"), a blocking input table
edit is refused, `await_update` fails, and table operations and `listen` work.

The engine's graph lock got explicit holds for this (`SteppedUpdateGraph::acquire_shared`
and the three beside it): a `with` block cannot be a closure. A thread that holds the
exclusive side reads without taking the shared side, gives the hold up for the cycle of
its own blocking write and for `await_update`, and has it again after.

Observed on Java 42.9 with scripts through `pydeephaven`, and on this server with the
same scripts:

| | Java | Here |
| --- | --- | --- |
| The lock a command holds | exclusive | the engine's shared lock, shown to scripts as the exclusive one: `has_exclusive_lock` is true and `has_shared_lock` false in a command, both locks nest in it |
| `shared_lock`, or an operation, on a thread while a command runs | waits for the command | does not wait: it shares the lock with the command. `exclusive_lock` waits, as in Java |
| `auto_locking = False` | operations on a thread take no lock | no effect: the engine takes the lock for every operation |
| Formula operations and `time_table` on a thread, or in a listener, with no `ExecutionContext` | refused: "No ExecutionContext registered" | run. `with ctx:` and `@ctx` are accepted and change nothing |
| `make_user_exec_ctx(freeze_vars=[...])` | a context that sees only those variables, at their values of that moment | refused by the name of `ExecutionContext.Builder#captureQueryScopeVars` |
| A table released with its liveness scope | operations fail with `LivenessStateException`; `size` and reads give the rows it had | every use raises `DHError` with a `LivenessStateException` text. A variable that holds it stays listed for clients until it is assigned again |
| A table of a scope that something else holds | a client's export keeps it live; other holders not probed | an export, a table made from it or a listener keeps it live, and it is then the session's: no later scope releases it |
| The update of a replay, read after the function returned | still reads the rows | reads `{}`, as every other update does on both |
| `modified_prev()` in a cycle that also shifts rows | reads previous values at the keys after the shift, which are the previous values of other rows, or nulls where no row was: after `sort("K").flatten()` of rows 10, 20, 30, adding key 5 and changing key 20 gives `modified_prev` the row of key 30 | reads them at the keys before the shift: the row of key 20 as it was |
| A handle that nothing refers to | the listener ends when Java collects it | ends when Python frees the handle, which is at once |
| The thread of a listener call | one Python thread name for the update graph's thread | a new `Dummy-N` name per cycle |
| Several listeners of one table | order not probed | called in no defined order |
| The exception given to `on_error`, and the logged text of a failing `on_error` | text starts `java.lang.RuntimeException: Error in Python interpreter:` and ends with the JVM's stack | starts `Error in Python interpreter:` and holds the same traceback |

Not observable here, so not built: the reference counts themselves (`LivenessScope`'s
`j_scope`, `tryManage` on a dead referent), and anything of an `ExecutionContext` but its
update graph.

A partitioned table is taken by a scope and never released (task 7.10.1). On Java 42.9 one
of a static or a ticking table stayed usable (`table`, `constituent_tables`, `merge`, `keys`)
after `manage` and `release`, twice over, after `unmanage`, after `preserve`, and when it
was made inside a `liveness_scope` block that ended; so it does here. Not the same: a table
that `merge()` or `keys()` returned inside a block is released with the block here, as any
table is, and stayed usable in Java.

### Executors and calendars

`deephaven.server.executors` (task 7.10.1) has Java's two names, `serial` and `concurrent`,
and one thread behind both, as Java 42.9 was seen to have
(`PythonDeephavenSession#registerJavaExecutor` is given the same single-threaded executor
under both names): tasks run one at a time in the order they were submitted, whichever
name took them. What a task raises is seen by no one, and what is no function is taken and
fails unseen, as in Java. An unknown name is a `KeyError`. The thread is a Python thread
made by this package; in Java it is a Java thread, which Python shows as a `_DummyThread`.
A task has no `ExecutionContext` in Java, so a formula in it is refused there and runs here
(the row of the table above). `_register_named_java_executor` of a new name is refused by
the name of `PythonDeephavenSession#registerJavaExecutor`.

`deephaven.calendar` is described in "Business calendars" below.

### Business calendars

Task 11.1. The reference keeps its business calendars in Java
(`io.deephaven.time.calendar.Calendars`) and hands a script the Java `BusinessCalendar`.
Here the calendars are the server's (`oxhaven_expr::calendar`: the calendar file format, the
business days and hours, the arithmetic over business dates and times, the collection of
named calendars), and a script gets a `deephaven._calendar.BusinessCalendar` that has every
public method of Java's `BusinessCalendar` and `Calendar`, with Java's overloads.

- **The functions of `deephaven.calendar`** (`calendar`, `calendar_name`,
  `calendar_names`, `set_calendar`, `add_calendar`, `remove_calendar`) work as in Java 42.9,
  with the reference's `DHError` texts. A file is read as
  `BusinessCalendarXMLParser` reads it: the same elements, the same defaults (valid dates
  taken from the holidays), the same refusals (no `name`, `timeZone` or `default`, a close of
  `24:00`, both `businessTime` and `businessPeriod`, overlapping ranges, a name that is no
  valid query name, a name already taken), and the legacy `businessPeriod` form.
- **The server starts with the three calendars Deephaven starts with** (task 12.5): `UTC`,
  the default, `USNYSE_EXAMPLE` and `USBANK_EXAMPLE`, with Deephaven's zones, hours,
  weekends and valid dates. Deephaven's files are under the Deephaven Community License, so
  these are our own, written by `crates/oxhaven-expr/src/calendar/shipped/generate.py` from
  public sources it cites: the NYSE's holiday rule (Rule 7.2), its announced early closes
  and its unscheduled closings for the exchange; the legal public holidays of 5 U.S.C.
  6103(a), observed as the Federal Reserve Banks observe them, for the banks. Like
  Deephaven's, the two are examples, not for trading. `UTC` is the same as Deephaven's on
  every date; where the other two differ is in the table below. Adding a calendar of one of
  those names is refused as Java refuses a second calendar of a name, and `remove_calendar`
  removes one as in Java.
- **The methods of a calendar** are chosen among Java's overloads by the Python types of the
  arguments, as jpy chooses: `str` for `String`, the values of `deephaven.time` for
  `LocalDate`, `Instant`, `ZonedDateTime`, `LocalTime` and `ZoneId`, `None` for any of them,
  `bool` and `int` for `int` (truncated to 32 bits, as jpy does), and jpy's two texts when no
  overload or more than one fits (`no matching Java method overloads found`, `ambiguous Java
  method call, too many matching method overloads found`). The overloads that are not public
  in Java (`isLastBusinessDayOfMonth(LocalDate)`, `isLastBusinessDayOfYear(LocalDate)` and
  `(String)`) are not reachable here either.
- **What a calendar hands out** is a value of `deephaven._java_time` (`LocalDate`,
  `Instant`, `ZonedDateTime`, `LocalTime`, `Duration`, `ZoneId`, `DayOfWeek`), a
  `CalendarDay` or `TimeRange` of `deephaven._calendar`, a Python list for a Java array, and
  small stand-ins of `java.util.List`, `Set` and `Map` with the methods FigureCalendar and
  `deephaven.numpy` call (`toArray`, `get`, `size`, `isEmpty`, `contains`, `containsKey`,
  `keySet`, `values`, `entrySet`); any other method of those is refused by its name.
- `deephaven.numpy.to_np_busdaycalendar` works, as in Java.

Not the same as Java 42.9:

| What | Java | Here |
| --- | --- | --- |
| `holidays()`, `weekendDays()` | a `Map` and a `Set` from `Map.copyOf` and `Set.copyOf`, whose order changes from run to run | in order of date and of day |
| The type of what `start()`, `end()`, `businessStart()` hand out | `java.lang.Comparable`, as jpy sees the generic type | the concrete `LocalTime` or `Instant`; its text is the same |
| An array (`businessDates` and the like) | a jpy array | a Python list of the same items |
| `hashCode()`, `getClass()` | the Java values | refused by name |
| The texts of errors | Java's; for a calendar file, the message of the root cause | the same class and message for every failure probed, except: an invalid date in a file names `DateTimeParseException: Cannot parse local date` where Java's root cause is `DateTimeException: Invalid value for MonthOfYear`, an unknown weekend day is `Invalid day of week` where Java's root cause is `No enum constant java.time.DayOfWeek.X`, and a malformed file names `roxmltree`'s error |
| A calendar file that is not UTF-8, or that names an external entity | read | refused |
| `USNYSE_EXAMPLE`: 1999-11-26, 2000-07-03, 2000-11-24, 2001-07-03, 2001-11-23, 2001-12-24 | full days | early closes at 13:00, as the NYSE held them |
| `USNYSE_EXAMPLE`: 2022-06-20 (Juneteenth observed), 2025-01-09 (national day of mourning) | business days | closed, as the NYSE was |
| `USNYSE_EXAMPLE`: 2024-12-24, 2025-12-24 | 10:30 to 14:00 | 09:30 to 13:00, the NYSE's hours |
| `USBANK_EXAMPLE`: 2019-11-26 and 2019-11-28 | the 26th closed, Thanksgiving (the 28th) a business day | the 26th a business day, the 28th closed |
| `USBANK_EXAMPLE`: 2009-01-19, before the valid range (only `holidays()` and a figure's calendar show it) | not listed | listed: Martin Luther King, Jr. Day |
| The description of `USNYSE_EXAMPLE` and `USBANK_EXAMPLE` (`description()`, `str()`) | Deephaven's text | our own |

Reproduced from Java, though they look like defects: `remove_calendar` is case-sensitive
while names are kept in upper case, so a lower-case name removes nothing; `set_calendar`
takes any name and `calendar()` fails later; a holiday `date` of eight characters is read
as `yyyyMMdd` whatever it holds (`2024-2-3` fails); a file with neither valid dates nor
holidays fails with `NoSuchElementException`; the root element may have any name and the
valid range may be reversed; a range that a change of the zone's offset makes run backwards
fails the day it falls on (`Start is after end`); `includeClose` is true only for `true`;
`diffBusinessYears` does not put a later start after the end and fails;
`isLastBusinessDayOfMonth` compares the month only.

The sources of each of the calendar rows are in `tasks/12.5-shipped-calendars.md`; the
owner chose to keep them (item 7.22), and `PARITY.md` A25 records them.

Not ported, each refused by its name: the business calendar of a `deephaven.plot` figure (`Figure#businessTime`, "Business time and transforms
of an axis" in `docs/FIGURES.md`); the calendar functions of the formula language
(`io.deephaven.time.calendar.StaticCalendarMethods`), which are not in the formula library.

### The jpy module

Task 7.10.2. The reference's Python reaches Java through the module `jpy`, and packages
written for it import the module: `deephaven.plot.express` has `import jpy` and a
`jpy.get_type(...)` in a module body, so without a module of that name the plugin does not
load and the server does not start. The binary holds a module `jpy` (`python/jpy.py`) that
has the names of jpy 2.2.0 and no Java behind them. The owner decided this on 2026-11-16,
over Python versions of the Java calls and over no module: **every use of a Java class
fails by that class's name.** The owner then approved two sets of exceptions, which the
module carries out without a Java VM: two methods of `DateTimeUtils` (task 11.1, below)
and Java arrays with the casts and conversions of them (task 11.2, "Java arrays").

| Call | jpy 2.2.0 in the reference | Here |
| --- | --- | --- |
| `import jpy` | the module | the module, from the binary; a `jpy` installed in the environment is not used |
| `jpy.get_type(name, resolve=True)` | the Java class as a Python class, `<class 'a.b.C'>`; `ValueError: Java class 'a.b.C' not found` for a name without a class | a class of that text, `__name__`, `__module__` and `jclassname`, the same one for the same name, **for any name**: no class is looked up. `/` in a name is `.`, an empty name and a name that is no `str` fail with jpy's texts |
| Calling a type | a Java object | `NotImplementedError: not ported: a.b.C#<init>` |
| An attribute of a type: a static method or field, `jclass` | the method, the value | `not ported: a.b.C#<name>`, when the attribute is read, called or not. `hasattr` and `getattr` with a default raise it too |
| An attribute named `__x__` that a class does not have | `AttributeError` | the same, with jpy's text: Python asks every class for such names |
| Setting or deleting an attribute | `TypeError: cannot set 'x' attribute of immutable type 'a.b.C'` | the same |
| `isinstance(x, T)` | whether `x` is a Java object of the class | the same for a Java array (task 11.2), whose class extends `java.lang.Object` only, as in jpy; `False` for anything else |
| `issubclass(T, U)` of two Java types | by the superclasses of the Java class | the same where `T` is an array class; else `not ported: a.b.T#getSuperclass: whether it extends a.b.U`; `True` for a type and itself, and for `jpy.JType` |
| `jpy.array(type, init)` | a Java array | a Java array of a primitive type or of a class listed under "Java arrays" below; `not ported: a.b.C[]: jpy.array` for any other class |
| `jpy.cast(obj, type)` | the object as that type; `None` for `None`, whatever the type | see "Java arrays" below; `None` for `None` |
| `jpy.convert(obj, type)` | a Java object | to an array class or a class of `java.time` the package has, see "Java arrays"; else `not ported: a.b.C: jpy.convert` |
| `jpy.byte_buffer(obj)` | a `java.nio.ByteBuffer` | `not ported: java.nio.ByteBuffer: jpy.byte_buffer` |
| `jpy.has_jvm()` | `True` | `False` |
| `jpy.create_jvm(options)` | nothing, with a Java VM running | `not ported: jpy.create_jvm: this server has no Java VM` |
| `jpy.destroy_jvm()` | **destroys the server's Java VM** (the reference stopped when the probe called it) | nothing, as jpy does in a process without a Java VM |
| `jpy.types` | the 686 classes loaded so far | the types handed out so far |
| `jpy.diag`, `jpy.VerboseExceptions`, `jpy.type_callbacks`, `jpy.type_translations` | settings of the bridge | objects with the same attributes and jpy's values in a process without a Java VM (`flags` 0, `enabled` false); nothing reads them |
| `jpy.JType`, `JField`, `JMethod`, `JOverloadedMethod`, `JException` | the classes of jpy's wrappers | classes of those names; `JType` is a base of every type, `JException` is an `Exception` that nothing raises |

One exception, task 11.1: two static methods of `io.deephaven.time.DateTimeUtils` are
carried out, because `deephaven.plot.express` calls them for a figure with `calendar=`
(`deephaven_figure/FigureCalendar.py`, `unpack_time_range`), and their Java overloads are
chosen as jpy chooses:

| Call | Java 42.9, as observed | Here |
| --- | --- | --- |
| `timeZone()` | the server's zone, `Etc/UTC` in the reference's container | `Etc/UTC` |
| `timeZone(str)` | the zone of the text, by Deephaven's aliases (`ET` is `America/New_York`); `ZoneRulesException` for an unknown one; `None` for `None` | the same, through `deephaven.time`'s aliases |
| `toLocalTime(Instant, ZoneId)`, `toLocalTime(ZonedDateTime)` | the time of day; `None` when an argument is `None` | the same |
| `toLocalTime(None)`, `timeZone(ZoneId)`, `timeZone(5)` | jpy's `ambiguous ...` and `no matching ...` | the same |

Every other member of `DateTimeUtils`, and of any class, is refused as above. This widens
what the owner's ruling of 2026-11-16 allowed ("every use of a Java class fails by that
class's name"); the owner approved it, and the Java arrays below, and `PARITY.md` (A10)
now says the stand-in carries out the Java classes this file lists and refuses the rest.

A second exception, task 11.3, approved with the task:
`io.deephaven.engine.table.impl.util.ColumnsToRowsTransform.columnsToRows`, which
`dx.data.marketing` of `deephaven.plot.express` calls on a table's `j_table`. The engine
carries it out (`oxhaven_engine::ops::columns_to_rows`, static and refreshing tables), and
`deephaven/_columns_to_rows.py` chooses among its three overloads as jpy 2.2.0 does:

| Call | Java 42.9, as observed | Here |
| --- | --- | --- |
| `columnsToRows(t, label, value, "A", "B", "C")`, three or more names after the value, or one list or tuple of them | the overload `(Table, String, String, String...)`: the names are the labels | the same |
| `columnsToRows(t, label, value, ["x", "y"], ["A", "B"])` | `(Table, String, String, String[], String[])` | the same |
| `columnsToRows(t, label, ["V", "W"], labels, [["A", "B"], ["C", "D"]])` | `(Table, String, String[], String[], String[][])` | the same |
| five arguments whose last two are strings, `columnsToRows(t, "L", "V", "AB", "CD")` | jpy prefers the second overload and makes each string an array of its characters: labels `A`, `B` of the columns `C`, `D` | the same, and so for a string where any array is expected |
| `None` in place of an argument, a `None` name | the `NullPointerException` and `NoSuchColumnException` (`Unknown column names [null]`) Java throws, in its order | the same texts |
| a Python `Table` in place of its `j_table`, a number, `None` inside a list of labels; three `None` | jpy's `no matching Java method overloads found`; `ambiguous Java method call, ...` | the same |
| bad arguments | `IllegalArgumentException`s (no value column, counts that differ, two types in a group: `Incompatible transpose types A is int, D is long`), `AssertionFailure` for no labels | the same texts, as `RuntimeError` |

Refused as `not ported: ...ColumnsToRowsTransform#columnsToRows`, where Java's result is
undefined or inconsistent (owner question 7.12): a column in two groups whose value columns
have different types, and on a refreshing table a column named twice or a group whose
columns are not in the table's order (Java's update then names the wrong rows as
modified). Every other member of the class is refused as above.

The refusal is `deephaven._java_time.NotPorted`, the error of this package for what Java
does and it does not: a `NotImplementedError`, and so a `RuntimeError`, which is what jpy
raises for a Java exception and what code written for jpy catches.

A type that is only held is no use: `_JFoo = jpy.get_type("a.b.Foo")` in a module body, a
type as an annotation or a default, in a collection, compared or printed. A package that
reads a member of a type in a module body (`LIMIT = _JDouble.MAX_VALUE`) fails there, by
the name, and does not import; when the import is part of a plugin's registration the
server does not start, as for any plugin that fails to register. Of
`deephaven-plugin-plotly-express` 0.22.0, `deephaven-plugin-utilities` 0.1.0 and
`deephaven-plugin-ui` 0.43.2, none does: plotly-express holds one type at import
(`deephaven_figure/FigureCalendar.py:21`) and uses it in a function, the other two do not
import `jpy`.

Not here: the module `jpyutil` of the same distribution (it finds and starts a Java VM),
and jpy's own texts for a wrong number of arguments (Python's are given).

### Java arrays

Task 11.2. `jpy.array`, `jpy.cast` and `jpy.convert` make and take Java arrays, and
`deephaven.dtypes.array` returns them, as in the reference. An array is an object of the
class `jpy.get_type` hands out for its name (`[J`, `[Ljava.lang.String;`), whose `__name__`,
`__module__` and bases (`java.lang.Object`, `jpy.JType`) are jpy's. Arrays of these
elements are made:

- the eight primitive types; the elements are kept in an `array.array` of jpy's C type, so
  `memoryview(a)` and `numpy.frombuffer(a, ...)` read them as jpy's (`q` for `long`, `B`
  for `boolean`, `H` for `char`);
- `java.lang.String`, `Boolean`, `Byte`, `Short`, `Integer`, `Long`, `Character`,
  `Float`, `Double`, `Object`, and `org.jpy.PyObject`;
- the classes of `java.time` the package has values of (`Instant`, `LocalDate`,
  `LocalTime`, `ZonedDateTime`, `Duration`, `Period`, `ZoneId`, `DayOfWeek`) and of the
  calendars (`BusinessCalendar`, `CalendarDay`, `TimeRange`);
- arrays of any of these, to any depth (`[[J`).

What a script sees, as observed on jpy 2.2.0 in Deephaven 42.9 (`jpy_array_tests.rs` has
each case):

- **Making one.** `init` is a length (an `int` from 0 to `2**31 - 1`) or a sequence read by
  position (a list, tuple, range, `str`, `bytes`, a numpy array); anything else is
  `ValueError: array: argument 2 (init) must be either an integer array length or any
  sequence`. A length gives zeros, `False` or `None`.
- **Conversions.** An integer type takes what has `__index__` and keeps the low bits
  (`300` is `44` in a `byte[]`), and `None` is 0; a `float` fails with Python's text, an
  `int` beyond 64 bits with `OverflowError` (`int too big to convert` for `long`, `Python
  int too large to convert to C long` for the others). `float` and `double` take any real
  number (`float` rounds, to an infinity beyond its range), `boolean` the truth of any
  value. A `String` takes a `str`; the boxed integers an `int` or `bool`, kept to their
  bits (`Integer` of `2**40` is 0); `Float` and `Double` an `int` or `float`; `Boolean`
  the truth of any value; `Object` a `bool`, `int`, `float`, `str` or Java object, given
  back as the same Python value; `PyObject` anything, given back as itself; a class of
  `java.time` a value of it. Anything else is `ValueError: cannot convert a Python 'int'
  to a Java 'java.lang.String'`, and `None` is a null everywhere. An array of arrays keeps
  an array of its element class (so a change to it shows in both) and makes a new one of
  a sequence (`cannot convert a Python 'int' to a Java array of type 'long'` otherwise).
- **Positions.** `len`, `a[i]` with Python's negative positions, iteration, `in`,
  `reversed`; `a[i] = v`. Reading past either end is `IndexError: Java array index out of
  bounds`; writing is Java's `ArrayIndexOutOfBoundsException` (`Array region 3..4 out of
  bounds for length 3` for a primitive array, `Index 3 out of bounds for length 3` for
  another); a slice or a non-integer is `TypeError: sequence index must be integer, not
  'slice'`; `del a[0]` is `RuntimeError: cannot delete items of Java arrays`.
- **A Java object, not a Python sequence.** No `append`, `+` or `*`, no `length`
  attribute; it cannot be pickled or copied (`cannot pickle '[J' object`). `==` is
  identity, as Java's `equals`. `equals`, `hashCode` and `toString` are carried out;
  `getClass`, `wait`, `notify`, `notifyAll` and `jclass` are refused by name.
- **`jpy.cast(obj, type)`.** An array, or a value of `java.time` or a calendar, cast to
  its own class is itself (jpy gives a new object of the same array). Cast to a class it is
  no instance of, it is `None`: by the Java language's rules for arrays (an array is an
  `Object`, `Cloneable` and `Serializable`, and an array of `S` is an array of each
  supertype of `S`) and for the final classes above. A cast to a superclass (`Object`,
  `Object[]`, `Comparable`), which in jpy gives an object with only that class's methods,
  and a cast to a class whose relation needs Java's classes (`java.util.List`), are
  refused: `not ported: java.lang.Object: jpy.cast of a [J`. What is no Java object (a
  `str`, an `int`) is `ValueError: cast: argument 1 (obj) must be a Java object`; a table
  or another handle of the server is refused by the class's name.
- **`jpy.convert(obj, type)`** to an array class converts as an element of an array of
  arrays does; to a class of `java.time` the package has, `None` and a value of the class
  are themselves. Every other conversion gives a Java object the package has no value
  for, and is refused by the class's name.
- **In a formula.** A query scope variable that holds an array is that array in every row
  (`X = arr`, a column of `long[]`), and the language's operations on an array apply:
  `arr[i]`, `arr.length`, `sum(arr)`, `len(arr)`, a cast to its class, `ungroup` of the
  column. The array is read when the formula is compiled. An array of `boolean`, `Object`
  or another class the engine has no column type for is refused (`the engine has no
  column type for it`). A formula a gRPC client sends is refused such a variable, as a
list or a dict (what Java does with it was not observed).

Reproduced from Java, though they look like defects: setting an element of a primitive
array to a value it cannot take stores -1 (`65535` for `char`, `-1.0` for `float` and
`double`) and raises nothing, where making an array of the same value fails; an `int`
beyond 64 bits in an array of `Object`, `Long` or `Integer` is a `SystemError` (`returned
a result with an exception set`); `a != 1` is `False`, as is every comparison of an array
with a Python value.

Not the same as Java 42.9:

| What | Java | Here |
| --- | --- | --- |
| A change to an array after a formula read it | shows in the column: the column holds the array | does not: the column holds the elements the array had |
| The order of two different arrays (`a < b`) | by their references in the JVM | by their identities in Python; neither is stable from run to run |
| `str(a)`, `repr(a)`, `hash(a)` | the identity hash code and the JNI reference | the same shape, `[J@1c998901` and `[J(objectRef=0x...)`, with numbers from the Python object |
| `jpy.cast(a, type(a))`, `a[i]` of an array of arrays or of `java.time` values | a new Python object of the same Java object | the same Python object |
| `memoryview(a)` of a primitive array | read-only | writable, and a write is seen by the array |
| `isinstance(a, collections.abc.Sequence)` | `False` | `True` for a primitive array (Python registers `array.array`), `False` for another |
| An array of any other class (`java.util.List[]`, `BigDecimal[]`), even of nulls | made | `not ported: java.util.List[]: jpy.array` |
| The text of `memoryview` of an array of a class | names `[Ljava.lang.String;` | names `String;`, the type's `__name__` |

## Completion

Task 6.13. Java completes Python with the library `jedi`, which runs in the session's
interpreter and looks at the session's variables, so that `t.` offers the methods of the
object `t` holds. This server does the same. Its package
`deephaven_internal.auto_completer` has the names, signatures and behaviour of the
reference's and none of its code. `jedi` and `docstring_parser` (which renders docstrings
for signature help) are packages of the server's Python environment, both under the MIT
licence and neither part of this server:

```bash
/tmp/console-env/bin/pip install jedi docstring_parser
```

Without them a session answers every message with "success, nothing found", as Java does
without `jedi`, and says why in the log once.

- **The stream.** `AutoCompleteStream` takes `open_document`, `change_document` and
  `close_document` in order and does not answer them, and answers
  `get_completion_items`, `get_signature_help`, `get_hover` and `get_diagnostic` (always
  empty, as in Java) once the console they name is exported, in no fixed order. The
  documents live in the session's completer, the variable `jedi_settings`, by URI: streams
  share them, in Java too. A script may switch completion off (`jedi_settings.mode =
  'OFF'`); requests are then answered without success.
- **Edits.** Positions are in UTF-16 code units, and an edit replaces `range_length` units
  from the start of its range, as in `CompletionParser#updateDocumentChanges`: an edit
  that starts beyond the document's lines is ignored together with the rest of its
  message, and one whose start is beyond the end of the document replaces all of it.
- **What ends a stream.** A message without a request, an edit or a close of a document
  that is not open, and a console ticket that does not resolve end it with the statuses
  Java gives (`console/completer.rs`). A request that fails is answered with `success:
  false` and the stream goes on.
- **The browser pair.** `OpenAutoCompleteStream` and `NextAutoCompleteStream` carry the
  stream as one call per message, with the headers `x-deephaven-stream-ticket`,
  `-sequence` and `-halfclose`; messages are delivered in the order of their sequence
  numbers whatever order they arrive in (`console/browser.rs`,
  `BrowserStream`/`BrowserStreamInterceptor` in Java). The headers are checked on every
  call of the console service, as Java's interceptor does.
- **A running command.** A completion takes the interpreter lock for as long as `jedi`
  runs and no other lock: not the update graph's, and not a turn behind the command. It
  is answered while a command sleeps or computes (the interpreter switches between the two
  threads every 5 ms), and a cycle never waits for it. `console/completer.rs` has the
  reasoning.

Differences from Java:

| Case | Java | This server |
| --- | --- | --- |
| Opening a stream while a command runs | waits for the command: the completer is set up by a command of the session | does not wait: it is set up under the interpreter lock alone |
| Several requests at once | `jedi` is called from several threads | one answer at a time |
| Docstrings and signatures of the `deephaven` package | the reference package's; its annotations are strings (`head(num_rows: 'int')`) | this package's (`head(num_rows: int)`), and its docstrings are its own text |
| `NextAutoCompleteStream` | returns when the message was handled | returns when the message is queued in order |
| A session whose variable `jedi_settings` is not the completer and whose package cannot be imported | the exception of the call reaches the client | `UNIMPLEMENTED`, naming `ConsoleServiceGrpcImpl#autoCompleteStream` |
| The log line for a missing package | names the reference's `pip` extra and a JVM flag | names `jedi` and `docstring_parser` |
| A session without the packages | the process warns once, and no session of it completes again | each session warns once |
| The property `deephaven.console.autocomplete.disable` | switches completion off | no counterpart: this server has no such property |

## Applications defined by scripts

Task 6.18; `crates/oxhaven-server/src/app/script.rs`, whose module documentation has the
file format, and `deephaven.appmode`. `oxhaven-server --app-dir <directory>` is Java's
`-Ddeephaven.application.dir=<directory>`: each `.app` file of the directory names an
application and the Python scripts that start it, the scripts run before the server
accepts clients, and what they set with `get_app_state()` are the application's fields,
listed by `ListFields` and named by tickets `a/<id>/f/<field>`.

What Java does was read in `ApplicationInjector`, `ApplicationFactory`,
`io.deephaven.appmode` and the reference's `appmode.py`, and observed on Java 42.9 with
application directories made for the purpose. The same directories are the
tests of `app::script_tests` (with a session that needs no interpreter) and
`console::python::appmode_tests`.

- **Order.** The `.app` files in the order of their names, the scripts of each in the order
  of the numbers of their `file_<number>` keys (`file_10` after `file_2`). A file whose
  name ends in `.APP`, a directory and a link to a file are not application files. An
  application with `enabled` other than `true` (in any case) is skipped, and so is every
  application of type `qst`.
- **Paths.** An absolute path is used as it is and a relative one is appended to the
  application directory, without normalizing: `../outside/x.py` and `/somewhere/x.py` both
  ran on Java, and run here. Nothing confines a script to the directory.
- **What a script sees.** The text of the file without the blanks at its start and its
  end, run as a console command is: in the session's globals (`__name__` is `__main__`),
  under the update graph's lock, logged as `Evaluating command: ...`. The globals of one
  script are there for the next, of the same or another application, and for every
  console afterwards. A table left in a global is a query scope variable and is listed as
  one. `get_app_state()` is the application of the running script, on the thread that runs
  it and until it ends; anywhere else it raises `DHError`. The object it returned keeps
  working: a later command sets and removes fields with it, and listening clients are
  told.
- **Startup scripts yield to cycles** as console commands do (`yielding_to_cycles`): the
  update graph runs before applications start in Java too, and a script that writes to
  an input table and reads it back got its row there (observed), so it does here.
- **Failures.** An `.app` file that cannot be read (no `type`, an unknown type, no `id`,
  `name` or `scriptType`, no script, bytes that are not UTF-8, a bad `\u` escape), a
  script file that does not exist, a script of a language the console is not, two
  applications with one id, and a script that raises (`SyntaxError` and `SystemExit`
  included) each keep the server from starting: Java's too, in every one of these cases.
  The applications and scripts before it have run. A directory that does not exist or
  holds no `.app` file is a warning.
- **Fields.** A table is listed with the type `Table`, a partitioned table with
  `PartitionedTable`, any other Python object with no type; `None` is refused. A field
  name is `str()` of the key and must be printable ASCII. `ListFields` lists the fields in
  the order they came to be, the scope's variables among those of the applications, each
  variable after the script that left it. A field that a script replaces while no client
  listens moves behind the others; with a client listening it is one `updated` and keeps
  its place. The module has no way to give a description; the Java method does
  (`app.j_app_state.setField(name, value, description)`), and that call works here.
- **`del app[name]` removes the field and then raises `KeyError`**, whether there was a
  field or not: the reference tests the result of Java's `removeField`, which returns
  nothing. Reproduced, and reported for a ruling.

`--demo-tables` stays. It is the same application (`ide_app`, `ide_static`, `ide_ticking`)
built in Rust, for a server without the Python console: the default build has no
interpreter to run an `.app` file with, and the replay of the recorded IDE calls
(`crate::ide`) runs in that build. `oracle/ide/app.d` is that application as scripts; the
Java reference (`start-java-ide.sh <port> app`) and this server load the same files:

```bash
OXHAVEN_IDE_APP_DIR=$PWD/oracle/ide/app.d VIRTUAL_ENV=$PWD/oracle/.venv \
  target/debug/oxhaven-server --psk oxhaven --web-root ~/oxhaven-web --app-dir oracle/ide/app.d
node oracle/ide/ide-test.js --url dh+psk://oxhaven@127.0.0.1:10000
```

(`OXHAVEN_IDE_APP_DIR` is how that application's script finds its module, on both
servers.) The two flags together are refused: both make the application `ide_app`.

Differences from Java:

| Case | Java | This server |
| --- | --- | --- |
| Types `static` and `dynamic` | a Java class makes the application | refused by the name of the Java method; the server does not start (Java's does not either when the class is missing, enabled or not) |
| `scriptType=groovy` | refused on a Python server: "Cannot instantiate Groovy application on a Python app session" | the same refusal |
| Two scripts with the same number (`file_1`, `file_01`) | run in the order of a hash table (observed: `file_001`, `file_1`, `file_01`) | refused, naming `ApplicationUtil#findFilesFrom` |
| A `\u` escape in an `.app` file that is half of a surrogate pair | kept | refused, naming `Properties#load` |
| An `.app` file the server's user may not read | not an application file | an error |
| `app.fields`, `j_app_state.listFields()` | in the order of a hash table | in the order of the names |
| `app[name]` of a partitioned table | the jpy object of `PartitionedTableImpl`: the reference has no wrapper for that class | the package's `PartitionedTable` |
| `j_app_state` | a jpy object with every method of `ApplicationState` | an object with `id`, `name`, `setField`, `getField`, `removeField`, `listFields`, `numFieldsExported` |
| Two applications with one id | the scripts of the second run, then the server stops | the second is refused before its scripts run |
| An application with the id `scope` | starts, and fails when it sets its first field | refused before its scripts run |
| A server without a console | has a session of the type `NoLanguage`, and a Python application is refused on it | the same refusal, with that name |
| The default directory `app.d` in the configuration directory | used when the property is not set | no counterpart: this server has no configuration directory |
| `ApplicationState.Factory` applications (Java's `GcApplication`, which has no fields) | started first | none |
| Error text | Java's exception with its stack | the exception's first line, after the path of the `.app` file |

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
| `ExecuteCommand` whose call is cancelled, or whose session ends, while it runs | Java runs the command to its end; this server stops it, and the command after it finds that it did not finish ("Stopping a running command") |
| `SubscribeToLogs` | script output is the same, record for record, and the server's log arrives under the same levels in the same layout, with this server's thread and module names and its own wording. The oracle's Java also sends each log record again as `STDERR` lines, which is its logging configuration; and Java gathers records for 100 ms ("The log stream"). A slow client and the end of the session end the stream as in Java. |
| `GetHeapInfo` | different quantities ("The memory figures") |
| `CancelCommand`, `CancelAutoComplete` | none: `UNIMPLEMENTED`, same text |
| `AutoCompleteStream`, `OpenAutoCompleteStream`, `NextAutoCompleteStream` | the same messages and statuses in every case of the probe, with `jedi` on both sides; what differs is text that comes from the `deephaven` package: its docstrings, and the annotations in the signatures of its methods ("Completion") |
| `ListFields` | none for tables |
| `FetchTable` of `s/<name>` | none: a variable that is not a table resolves and is refused with `INVALID_ARGUMENT` |
| `ExportFromTicket` and `PublishFromTicket` with `s/<name>` | none in the cases tried: a number is exported and published as the same Python object, a table is published as the package's `Table` |
| `FetchTable` of a table with columns made from scope variables | the same columns and types; the encoded schema of the probe's table `r` is 16 bytes longer here and decodes to the same schema. Not looked into: it is the schema encoding, not the scope. |
| A formula that names a list | Java makes a column of `org.jpy.PyListWrapper`; refused here, except an item under a cast (task 5.8) |
| The IDE | the same 20 calls in the same order with the same statuses on both servers for the console route |
