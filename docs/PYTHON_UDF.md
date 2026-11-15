# Python functions in query strings

How `t.update("Y = f(X)")` runs when `f` is a Python function of the console's scope. Written
by task 6.11 (`tasks/6.11-python-udf.md`), which also built the first form. `docs/CONSOLE.md`
names this as risk 1 of the console; read its sections "How it fits together" and "The query
scope" first.

What Java does was observed on Java 42.9 with scripts sent through `run_script`; the cases that
are kept are recorded as the script scenarios `oracle/scenarios/python/udf_*.py`. Where this
document says "Java", it means that observation, and the source it names says why.

## Decision

1. **Resolution is the expression crate's, the function is the server's.** `oxhaven-expr` gets
   one extension point: a compiler made with `Compiler::with_functions` asks an
   `ExternalFunctions` resolver about a call whose name the function library cannot take, and
   the resolver answers with an `ExternalFunction`: a result type and a call over Arrow arrays.
   The call is a node of the typed tree like a library call, so it nests in any expression
   (`f(X) + 1`, `g(f(X))`, a branch of `?:`). The expression crate knows nothing of Python,
   and `oxhaven-contracts` does not change.
2. **Only the Python package's operations can call Python.** The resolver is given to the
   compiler of the Python runtime's operations (`_oxhaven.table_op`) and to no other. A formula
   that a gRPC client sends is compiled without it, so a call to a Python function there is an
   unknown function and is answered `INVALID_ARGUMENT`, as Java answers it: Java's expression
   validator refuses the call (`IllegalStateException: Cannot validate user expression`).
3. **The signature is read once, when the formula is compiled.** The server's Python module
   `_oxhaven_udf` (`crates/oxhaven-server/python/_oxhaven_udf.py`, loaded by
   `console/python/udf.rs`) reads the function's signature with `inspect.signature`, maps the
   return annotation to a column type, checks the argument types against the parameter
   annotations, and returns one Python callable that runs a whole chunk. A formula holds that
   callable, and so the function object of that moment: assigning the name again later does
   not change a table that exists, as in Java.
4. **Evaluation is by chunk.** The evaluator computes the argument arrays of a call for the
   rows it was asked for, then, for at most 4096 rows at a time, takes the interpreter lock,
   turns the arguments into Python lists, runs the chunk callable (a Python loop that converts
   the arguments of a row, calls the function, and converts what it returns), turns the list
   of results into an Arrow array, and releases the lock. Rows are called in row order, once
   each per evaluation.
5. **A return annotation is required.** Without one, or with one that names no column type,
   Java makes a column of `org.jpy.PyObject`, which the contracts cannot hold. Such a call is
   refused when the formula is compiled. That is a refusal, not an allowed difference; see
   "Refused".

## What a call may be

- The name is no column of the table, the function library has no function of that name that
  takes the arguments, and the scope of the code that called the table operation (the globals,
  or the globals and locals of the calling function) holds a callable under that name.
  The library comes first when its function fits, a Python function of the same name is called
  when it does not (`sqrt(T)` for a `String` column), and a column of the same name hides the
  function ("Cannot find method").
  Behavior: `QueryLanguageParser#getMethod`.
- A name of Java's function library that this server has not ported (`cbrt`, `var`) stays
  the refusal it was ("unsupported in formulas"), whatever the scope holds: Java would call
  its own function when the arguments fit, and that is not known here.
- Any callable whose signature `inspect.signature` can read: a function, a `functools.partial`,
  an object with `__call__`. A lambda and a class have no return annotation and are refused
  for that reason.
- `obj.method(X)` and `f.call(X)` are not calls of this kind: they name a scope variable that
  holds an object, which was refused before this task and still is.

### The result type

The return annotation, after `Optional[...]` (or `T | None`) is taken off:

| Annotation | Column type |
| --- | --- |
| `int`, `numpy.int64`, `dtypes.int64` | `long` |
| `numpy.int32`, `dtypes.int32` | `int` |
| `numpy.int16`, `dtypes.short` | `short` |
| `numpy.int8`, `dtypes.byte` | `byte` |
| `float`, `numpy.float64`, `dtypes.double` | `double` |
| `numpy.float32`, `dtypes.float32` | `float` |
| `bool`, `numpy.bool_`, `dtypes.bool_` | `Boolean` |
| `str`, `numpy.str_`, `dtypes.string` | `String` |

An annotation written as a string (`-> "int"`, `from __future__ import annotations`) is
evaluated. The numpy types are recognised when numpy is installed; nothing here imports it.

### What the function is given

| Column type of the argument | Python value | A null cell |
| --- | --- | --- |
| `byte`, `short`, `int`, `long` | `int` | the value Java reserves for null: `-128`, `-32768`, `-2**31`, `-2**63` |
| `char` | `int`, the code unit | `65535` |
| `float`, `double` | `float` | `-3.4028234663852886e+38`, `-1.7976931348623157e+308` |
| `Boolean` | `bool` | `None` |
| `String` | `str` | `None` |
| the `null` literal | `None` | |

A function sees the reserved value, not `None`, for a null number: that is Java's behaviour
and scripts test for it with `deephaven.constants`. The exception is a parameter annotated
`Optional[...]`, for which a null number is `None`. A parameter annotated with a numpy type is
given that type's value (`numpy.int32(x)`).

The argument types are checked when the formula is compiled, with Java's rule
(`PyCallableWrapperJpyImpl#verifyArguments`): each argument must have a type the parameter's
annotation names, or one that widens to it without loss (`byte` to `short`, `int`, `long`;
`short` and `char` to `int`, `long`; `int` to `long`; `float` to `double`). So `x: int` takes
every integer column and no `double`, and `x: float` takes `float` and `double` and no `int`.
A parameter without an annotation, or annotated `Any` or `object`, takes everything. Arguments
beyond the last parameter are checked against the last one. The number of arguments is not
checked: Python raises `TypeError` when the function is called.

### What the function may return

Java converts a returned value in one of two ways, depending on whether the arguments of the
call are all columns and constants (`PyCallableWrapperJpyImpl#vectorizedCallable`, into a Java
array) or not (`PyCallableWrapperJpyImpl#call`, through `PythonScopeJpyImpl#convert` and a
cast). For a value that fits the annotation the two agree, and that is what is built. Where
they disagree the row fails; see "Refused".

| Annotation | Returned value | Cell |
| --- | --- | --- |
| `int` | an `int` (not a `bool`) within 64 bits | the value; null when it is `-2**63` |
| `float` | a `float`, or an `int` within 64 bits | the value as a `double` |
| `bool`, `str` | a `bool`, a `str`; `None` | the value; null |
| `Optional[int]`, a numpy integer type | anything `int()` takes, within 64 bits | `int(x)`, wrapped to the width of the type; null when it is the reserved value |
| `Optional[float]`, a numpy float type | anything `float()` takes | `float(x)`, narrowed for `float` |
| `Optional[bool]`, `numpy.bool_` | anything | `bool(x)` |
| any `Optional[...]` | `None` | null |

An exception the function raises fails the evaluation with the exception's type and text
(`ValueError: ...`): the operation, for a static table, and the result table, in a cycle.

## Locks

The rule, which `console/python/mod.rs` states for the session and this task makes general:

> **Update graph lock first, interpreter lock second. A thread that holds the interpreter lock
> never waits for the update graph lock, or for anything a cycle holds.**

How each party keeps it:

- **A console command** takes the graph's shared lock, then the interpreter lock, and runs
  Python. No cycle runs during a command.
- **A table operation called from Python** (`_oxhaven.table_op` and every other native
  function that reaches the engine) releases the interpreter lock for as long as the engine
  runs. A formula of that operation that calls Python takes the lock again, on the same
  thread, for a chunk at a time. So the order is graph, interpreter, even when Python called
  the operation.
- **An update cycle** holds the graph's exclusive lock and takes the interpreter lock per
  chunk. It can wait only for a Python thread that is running, never for a command, which
  holds the shared lock and so excludes the cycle.
- **A reader** of a column that is computed on read (`view`, `update_view`, a snapshot for
  Flight or Barrage) takes the interpreter lock per chunk, whatever else it holds.
- **A Python thread** the user started holds the interpreter lock while it runs Python and
  gives it up at every native call into the engine, like a command. It must not hold a lock
  of its own that a function called from a formula also takes; that is the user's deadlock,
  in Java as here.

The test `console::python::udf::tests::a_cycle_calls_python_while_commands_run` runs commands
on one thread, cycles of a ticking table whose formula calls Python on another, and a Python
thread that calls table operations and the function on a third, and fails if any of them
stops making progress. With the `py.detach` of `_oxhaven.table_op` taken out it fails: the
Python thread waits for the graph lock with the interpreter lock held, and the cycle waits
for the interpreter lock.

A new native function keeps the rule by doing what the existing ones do: `py.detach` around
every call into the engine.

## Determinism

`PORTING.md` rule 7 is kept for the engine: the order of calls is fixed (row order within a
chunk, chunks in row order, the arguments of a call left to right), nothing is evaluated in
parallel, and the result of a pure function is the same on every run. A Python function may
be impure, and Java does not prevent it; then the result depends on how often and in what
order it is called, in Java and here:

- `update` and `select` call the function once per added or modified row; `view`,
  `update_view` and `lazy_update` call it on every read; `where` calls it for added and
  modified rows.
- Between the formulas of one operation the order of evaluation is this engine's (column by
  column, in the order given). Java's order is not fixed: with a counter function,
  `update(["A = nxt()", "B = nxt()", "E = nxt() + 0"])` gave `B` the first four numbers, `E`
  the next and `A` the last in one run, and `A`, `B`, `E` in that order in another. A script
  that depends on it gets different numbers from Java itself. Within one formula the numbers
  are the same on every run (`udf_calls`).

## Cost of the interpreter lock

Measured by `console::python::udf::tests::lock_cost`
(`cargo test -p oxhaven-server --features python --lib -- --ignored lock_cost --nocapture`),
once, on a 4-core machine that other builds shared, with Python 3.13 and a debug build:

| What | Time |
| --- | --- |
| The lock taken and released, no Python thread running | 0.19 µs |
| The lock taken and released, a Python thread computing (`while busy: pass`) | 0.46 ms on average |
| One row of `update('A = twice(ii)')`, 409600 rows | 0.56 µs |
| One row of `update('A = 2 * ii')` | 0.19 µs |
| A cycle that adds 41692 rows of `A = twice(ii)` (11 chunks), no Python thread | 77 ms |
| A cycle that adds 41504 rows of `A = 2 * ii`, no Python thread | 65 ms |
| A cycle that adds 54247 rows of `A = twice(ii)` (14 chunks), a Python thread computing | 157 ms |
| A cycle that adds 46853 rows of `A = 2 * ii`, a Python thread computing | 80 ms |

So: with no Python thread running, the lock is nothing against the calls of a chunk (0.19 µs
against about 1.5 ms of calls), and a call costs about 0.4 µs more per row than arithmetic.
With a Python thread that computes without pause, a cycle pays about 5 ms for each chunk,
which is the interpreter's switch interval (`sys.getswitchinterval()`, 5 ms by default): the
cycle thread asks for the lock and the running thread gives it up at the end of its
interval. The cost is per chunk, not per row, which is the reason for a chunk of 4096 rows
and against a smaller one; a cycle of one chunk is late by up to 5 ms. A console command
never competes with a cycle, because a command and a cycle exclude each other through the
graph lock; only threads the script started do. The figures are of a debug build and one
run; they order the costs and do not bound them.

## Refused

Each of these fails when the formula is compiled (`DHError` from the table method), unless it
says "the row fails", and names the Java method that would have done the work.

| What | Java | Named in the refusal |
| --- | --- | --- |
| A function without a return annotation, or with one that names no column type (`None`, `Any`, `object`, a union, `list`, an unsigned numpy type); also inside a cast, `(int) f(X)` | a column of `org.jpy.PyObject`; the cast converts it | `PyCallableWrapperJpyImpl#parseSignature` |
| A return annotation of a time type (`datetime`, `numpy.datetime64`, `pandas.Timestamp`, `dtypes.Instant`), and an `Instant` argument | converts to and from `Instant` | `deephaven._udf._ParsedReturnAnnotation#setup_return_converter`, `_ParsedParam#_setup_datetime_arg_converter` |
| A return or parameter annotation of an array type (`Sequence[...]`, `list[...]`, a numpy array, `bytes`), and an argument that is an array or vector | arrays both ways | `deephaven._udf#_udf_parser` |
| A return annotation of `numpy.uint16`, `dtypes.char` | a `char` column; the conversion failed for every value tried | `deephaven._udf._ParsedReturnAnnotation#setup_return_converter` |
| A character literal as an argument (`f('a')`) | fails when the row is computed (`ClassCastException`) | `PyCallableWrapperJpyImpl#call` |
| A call without arguments of a function that takes `*args` | fails to parse ("Expression changed!") | `QueryLanguageParser#visit(MethodCallExpr)` |
| A numba function, a numpy ufunc | vectorized calls | `PyCallableWrapperJpyImpl#prepareSignature` |
| In one operation, two calls of one function that give a parameter annotated `Optional[...]` numbers of different types | one converter per parameter is shared by the operation, so the type of the call compiled last decides which value is null for all of them | `deephaven._udf._ParsedParam#setup_arg_converter` |
| The row fails: a value that does not fit the annotation where Java's two ways of calling disagree: for `int`, a `bool`, a `float`, `None` or a numpy value; for `float`, a `bool` or `None`; for `bool`, anything but a `bool` or `None`; for `str`, anything but a `str` or `None`; an `int` beyond 64 bits | a value, a wrong value or a failure, depending on the arguments of the call | `PyCallableWrapperJpyImpl#call` |
| The row fails: a filter that is nothing but the call (`where("f(X)")`) and returns `None` | the row is dropped | `FilterKernelPythonChunkedFunction#filter` |

The last one fails with the engine's text for a null filter result
(`java.lang.NullPointerException encountered in filter`), which is what Java says for every
other filter that gives null (`where("f(X) && X > 0")`, `where("Z")` for a `Boolean` column).

## Not in the first form

- numba and `dh_vectorize`: a function is called once per row. A vectorized form would hand
  numpy arrays over the Arrow buffers to one call per chunk; the chunk callable is the place.
- Time types and arrays, as above.
- Keyword arguments in a query string, which the formula language does not have.
- Functions that return tables.
- Autocomplete and validation for gRPC clients: a client's formula cannot call Python at all.

## Alternatives rejected

- **Rewriting the call in the server before the expression compiler sees it.** The call can
  be anywhere in an expression, so its type has to be known to the type checker and its
  value to the evaluator; a rewrite would need a second evaluator for whatever surrounds the
  call.
- **Calling per row from Rust.** One interpreter lock round trip and one argument tuple per
  row from Rust costs more than a Python loop over lists, and the conversions of arguments
  and results are easier to state, and to change, in Python.
- **Giving the resolver to every compiler.** A gRPC client's formula would then call Python
  where Java refuses it, and the scope a call resolves in would not be a caller's.
- **A change to `FormulaCompiler`.** Not needed: the resolver is a property of the compiler
  value, and operations already take the compiler as a trait object.
