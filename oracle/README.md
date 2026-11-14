# Oracle

The oracle decides whether a server "matches Java". It runs the same scenario on two servers
through the Python client and compares every result, or compares one server with results
recorded from the Java reference. The reference is pinned in [REFERENCE.md](REFERENCE.md); what
"matches" means is in [PARITY.md](../PARITY.md).

| Path | Holds |
| --- | --- |
| `requirements.txt` | The pinned Python environment |
| `bin/start-java.sh`, `bin/stop-java.sh` | Start and stop the Java reference server |
| `bin/harness` | Runs the harness from any directory |
| `harness/scenario.py` | The scenario format, its validator, and the `--ops` filter |
| `harness/runner.py` | Runs a scenario against a server URL, returns tables or failures |
| `harness/rules.py` | **The comparison rules**, as data and code. Keep in step with `PARITY.md` |
| `harness/fuzz.py` | Scenario generator and shrinker |
| `harness/faults.py` | Fault injection, to prove the comparator sees differences |
| `harness/smoke.py` | Client smoke checks per protocol area |
| `harness/results.py` | Result sets and their recorded form |
| `scenarios/static/`, `scenarios/ticking/` | The scenarios, one JSON file each |
| `corpus/<scenario>/` | The recorded Java results of each scenario |
| `tests/` | Tests of the harness itself; they need no server |

The commands below run from the `oracle/` directory. From anywhere else, `oracle/bin/harness`
is the same as `.venv/bin/python -m harness`.

## Set up

Needs Python 3.13 and a JDK (`java` on `PATH` or `JAVA_HOME`; verified with OpenJDK 21). Docker
is not needed: the Java server is the one embedded in the `deephaven-server` wheel.

```bash
python3 -m venv .venv
.venv/bin/pip install -r requirements.txt
```

## Start and stop Java

```bash
bin/start-java.sh 10000      # prints the URL once the server answers a query
bin/stop-java.sh 10000
```

The server listens on `127.0.0.1` with the pre-shared key `oxhaven`, so its URL is
`dh+psk://oxhaven@127.0.0.1:10000`. `ORACLE_PSK` and `ORACLE_HEAP` (default `2g`) change the key
and the heap. The log and pid file are in `.run/`; when a request fails with
`Details Logged w/ID '...'`, the stack trace is in `.run/java-<port>.log`.

A server URL is `dh+psk://<key>@host:port`, or `dh://host:port` for anonymous authentication.
The Oxhaven server under test is named the same way.

## Compare two servers

```bash
URL=dh+psk://oxhaven@127.0.0.1:10000

# Java against Java: every scenario, run twice on one server, floats compared bit for bit ...
.venv/bin/python -m harness diff --a $URL --exact
# ... or on two servers (start a second one with bin/start-java.sh 10001).
.venv/bin/python -m harness diff --a $URL --b dh+psk://oxhaven@127.0.0.1:10001 --exact

# Java against a server under test, with the tolerance PARITY.md allows:
.venv/bin/python -m harness diff --a $URL --b dh://127.0.0.1:10100

# Only some scenarios, and looking at what a scenario returns:
.venv/bin/python -m harness diff --a $URL where_int ticking_agg_by
.venv/bin/python -m harness show --url $URL where_int
```

`--a` is the expected side (Java), `--b` the server under test. Each scenario prints `ok`,
`DIFF` followed by one line per mismatch, or `ERROR` when it could not be run. The exit status is
0 for no differences, 1 for differences, 2 when a scenario could not be run or none was selected.

A mismatch line names the scenario, table, step, column, row and both values:

```
scenario=agg_by_avg table=avg_k step=0 kind=value column=AvgD row=2 expected=0.875 actual=0.8750001
```

### Choosing scenarios by operation

`diff`, `record`, `replay` and `show` take `--ops a,b,c`: only scenarios that use at least one
of these operations. `--within a,b,c` keeps only scenarios that use nothing else, so a server
that implements a few operations can be checked on exactly those:

```bash
# Every scenario that sorts, whatever else it needs:
.venv/bin/python -m harness replay --url $URL --ops sort,head,tail
# Only those a server with sort, head, tail and input tables can run:
.venv/bin/python -m harness replay --url $URL --ops sort,head,tail --within sort,head,tail,input_table
```

The names are the `op` values of the scenario format (table below), plus `input_table` for
scenarios with ticking inputs, plus the gRPC names `unstructured_filter` (= `where`),
`aggregate` (= `agg_by`), `cross_join` (= `join`) and `aj_tables` (= `aj`). A name the harness
does not know is an error (exit status 2), never an empty selection. `--scenarios` takes
another scenario directory, or one scenario file.

## Record and replay

```bash
# Store the Java results. Run this against the Java reference only.
.venv/bin/python -m harness record --url $URL

# Compare a live server with the recordings. No Java needed for the server under test.
.venv/bin/python -m harness replay --url $URL
```

A recording is `corpus/<scenario>/manifest.json` plus one Arrow IPC file per result table and
step (`step<NNN>.<table>.arrow`); a result that failed is an entry in the manifest with its
status code and has no file. The manifest holds a hash of the scenario content; `replay`
refuses a recording whose scenario has changed since, so a changed scenario must be recorded
again. The recordings are committed. Re-recording changes the files byte for byte only where the
results changed.

## Fuzz

```bash
# 300 generated scenarios over every operation, Java against Java:
OPS=where,sort,head,tail,view,select,update,drop_columns,merge,agg_by,natural_join,exact_join,join,aj,update_by
.venv/bin/python -m harness fuzz --a $URL --ops $OPS --seeds 300 --exact

# Java against a server under test, 10000 scenarios that only filter, starting at seed 0:
.venv/bin/python -m harness fuzz --a $URL --b dh://127.0.0.1:10100 --ops where --seeds 10000
# One seed again:
.venv/bin/python -m harness fuzz --a $URL --b dh://127.0.0.1:10100 --ops where --seeds 1 --seed 4711
```

A scenario is a function of the seed and the set of operations, and of nothing else
(`harness/fuzz.py`): random schemas over every column type, cells with nulls, NaN, infinities,
negative zero and the limits of each integer type, static tables or keyed and append-only input
tables with steps that add rows, replace rows by key and delete them, and one to five operations
drawn from `--ops` with arguments that fit the schema. Generated scenarios use no operation
outside `--ops`. About 300 seeds over all operations take a minute and a half against a local
Java server.

`fuzz` stops at the first seed whose results differ. It prints the first mismatch, shrinks the
scenario (fewer steps, operations, rows, columns, arguments) while the two servers still differ,
and writes `fuzz_<seed>.json` and `fuzz_<seed>_min.json` to `.fuzz/` (`--out`), with the command
that reruns the minimal one:

```
DIFF  seed 18: 1 mismatches; the first:
      scenario=fuzz_18 table=o1 step=1 kind=row_count expected=4 actual=3
shrunk in 51 runs to 1 inputs, 3 rows, 2 columns, 1 operations, 1 steps:
...
rerun: python -m harness diff --a ... --b ... --scenarios .../.fuzz/fuzz_18_min.json
```

The exit status is 0 when every seed is clean, 1 at a difference, 2 when a scenario could not be
run. Operations that fail on the reference are part of the comparison (see the rules), so a
generated scenario with a failing operation is not an error.

### A faulty server to test against

`faulty:<fault>@<url>` names a server whose results are damaged on the way back: `<fault>` is
any fault of `harness/faults.py`, applied to every result where it applies.
`faulty:<fault>:<op>@<url>` damages only the results of one kind of operation, like a server
with a bug in that operator. It is accepted wherever a server URL is, and exists to show that
`diff`, `replay` and `fuzz` catch a difference:

```bash
.venv/bin/python -m harness fuzz --a $URL --b faulty:truncated_after_three:agg_by@$URL --ops $OPS --seeds 300
```

## Client smoke checks

```bash
.venv/bin/python -m harness client-smoke session --url $URL
```

Areas: `session`, `tables`, `flight`, `barrage`, `inputtable`. Each runs a handful of named
checks that do what a real client does in that area, through pydeephaven, a plain
`pyarrow.flight` client (`flight`), plain gRPC calls (`session`) and the Barrage subscription
client `pydeephaven-ticking` (`barrage`). Every check prints `ok` or `FAIL` with the reason; the
exit status is 0 only if all passed. The expectations, including status codes, are what the Java
server does; the module comment of `harness/smoke.py` lists what each area covers.

## Tests of the harness

```bash
.venv/bin/python -m pytest -q
# With the count of faults applied per kind:
.venv/bin/python -m pytest -q -s tests/test_faults.py
```

They need the recordings but no server.

- `tests/test_rules.py`: each rule, on hand-built tables.
- `tests/test_faults.py` damages results in each way listed in `harness/faults.py` and checks
  the comparator's verdict, on hand-built tables and on every result of the recorded corpus.
  Everything must be reported except: a computed float moved by less than its tolerance, the
  sign of a zero flipped in a computed column, and swapped rows in a result declared unordered.
- `tests/test_fuzz.py`: the generator is deterministic and stays within the requested
  operations, the shrinker reaches the smallest scenario for a known difference, and the `fuzz`
  command catches and shrinks a faulty target (with an in-process stand-in for the servers).
- `tests/test_scenario.py`: the scenario format, the `--ops` filter, the corpus, recordings.

## The comparison rules

They are in `harness/rules.py` and nowhere else; its module comment is the full statement and
cites the section of `PARITY.md` behind each rule. In short:

- Whether a result fails is compared, and the gRPC status code if it does; the message is not
  (Must match, item 7; A5).
- Schema: column count, names, order and Arrow types are exact, and so are the Deephaven field
  metadata (`deephaven:type`, `componentType`, the input table key flags and the others listed
  in `rules.py`) and the table attributes `IsFlat`, `AddOnly`, `AppendOnly` and `BlinkTable`
  in the schema metadata. Nullability flags are not compared.
- Row count is exact. Rows are compared by position, never by row key.
- Booleans, integers, strings and timestamps are exact. Null equals only null.
- Floats are compared bit for bit, except that every NaN equals every NaN. `0.0` and `-0.0`
  differ (A2 is proposed, not in force).
- The exception is A1: a float cell of a computed column (the result of `sum`, `avg` or
  `cum_sum`, or of a formula that calls an inexact library function) may differ by a relative
  `1e-9` or an absolute `1e-12` for `double`, and `1e-5` or `1e-6` for `float`. Null, NaN and
  infinity are never covered by the tolerance. `rules.computed_columns` derives the computed
  columns from the scenario's operations. `--exact` switches the tolerance off, for comparing
  two runs of the same server.
- A result that the scenario marks unordered is compared as a multiset. Everything else is
  compared in order.
- A column type the rules do not know is reported, never passed.

## Scenarios

A scenario is one JSON document. `harness/scenario.py` validates it and rejects unknown keys.

```json
{
  "format": 1,
  "name": "example",
  "description": "What this scenario is for.",
  "inputs": [
    {
      "name": "t",
      "columns": [{"name": "K", "type": "int32"}, {"name": "V", "type": "float64"}],
      "rows": [[1, 0.5], [2, null], [3, "NaN"]],
      "ticking": {"key_columns": ["K"]}
    }
  ],
  "operations": [
    {"name": "big", "op": "where", "source": "t", "filters": ["V > 0.1"]},
    {"name": "sorted", "op": "sort", "source": "big",
     "by": [{"column": "V", "direction": "descending"}]}
  ],
  "results": [{"table": "big"}, {"table": "sorted"}],
  "steps": [
    {"actions": [{"action": "add", "table": "t", "rows": [[4, 4.0], [1, 9.0]]}]},
    {"actions": [{"action": "delete", "table": "t", "rows": [[2]]}]}
  ]
}
```

- **inputs**: tables created on the server from Arrow data. Column types are `bool`, `int8`,
  `int16`, `int32`, `int64`, `float32`, `float64`, `string` and `timestamp` (nanoseconds since
  the epoch, UTC, written as an integer). A cell is a JSON value or `null`; the floats that JSON
  lacks are written `"NaN"`, `"Infinity"` and `"-Infinity"`. Without `ticking` the input is a
  static table. With it, the input is an input table: keyed when `key_columns` names columns,
  append-only when the list is empty.
- **operations**: each defines a named table from tables defined before it.

  | `op` | Other keys |
  | --- | --- |
  | `where` | `source`, `filters` |
  | `sort` | `source`, `by`: list of `{"column", "direction"}` with `ascending` or `descending` |
  | `head`, `tail` | `source`, `rows` |
  | `view`, `select`, `update` | `source`, `formulas` |
  | `drop_columns` | `source`, `columns` |
  | `merge` | `sources` |
  | `agg_by` | `source`, `by`, `aggs`: list of `{"agg", "columns"}` for `sum`, `min`, `max`, `first`, `last`, `avg`, or `{"agg": "count", "column"}` |
  | `natural_join` | `left`, `right`, `on`, optional `joins`, optional `type`: `error_on_duplicate` (default), `first_match`, `last_match`, `exactly_one_match` |
  | `exact_join`, `join`, `aj` | `left`, `right`, `on`, optional `joins` |
  | `update_by` | `source`, `by`, `ops`: list of `{"op": "cum_sum", "columns"}` |

  Filters, formulas and column pairs (`"Out = In"`, `"X >= Y"`) are Deephaven query strings and
  go to the server unchanged.
- **results**: the tables to compare; an input table may be named directly. A result is ordered
  unless it says `"ordered": false`, which also needs `"unordered_reason"`: a citation of the
  Java source showing that the reference does not guarantee the order. No scenario in the corpus
  needs it.
- **steps** (ticking scenarios): each step is a list of actions on input tables. `add` takes
  full rows; in a keyed input table a row with an existing key replaces that row. `delete` takes
  one cell per key column. Every result table is snapshotted before the first step (step 0)
  and after each step (steps 1, 2, ...).

To add a scenario, put the file under `scenarios/`, named `<name>.json`, then record it against
Java. To add an operation, add it to `OPERATIONS` in `harness/scenario.py`, to `apply_operation`
in `harness/runner.py`, to `computed_columns` in `harness/rules.py` if it computes floats, and
to the generator in `harness/fuzz.py`.

### Operations that fail

When the server refuses an operation, the table's result is a failure with the gRPC status code
(`INVALID_ARGUMENT`, `FAILED_PRECONDITION`, ...), and so is the result of every table defined
from it. When an operation is accepted and the snapshot of its table fails (a `view` whose
formula throws when it is read), the result of that step is a failure. `show` prints
`fails with <code>`. The `error_*` scenarios and `ticking_error_after_step` record such results.
An action of a step that the server refuses is not a result: the scenario is reported as
`ERROR`.

### How a ticking step is known to be finished

The Java server answers an input table change as soon as the input table has refreshed, while
derived tables may still be updating in the same cycle, so a snapshot taken right then can show
a derived table's previous state. After each step the runner therefore makes two further changes
to a private input table and waits for both; the second cannot be applied before the step's
cycle has completed. `_Barrier` in `harness/runner.py` gives the reasoning and the Java citation.

## Known limits

- Operations outside the table above have no scenario, no generator and no rule yet; `--ops`
  rejects their names.
- A ticking table that fails during an update cycle (a duplicate right key arriving in a
  `natural_join`, a formula that throws in `update`) is not compared after that. Java keeps
  answering snapshots of such a table, with contents that are not a consistent table (rows of
  the parent with stale or null values in the added columns). `ticking_error_after_step` defines
  such tables and compares only the tables around them.
- A formula over a computed column (`X = Sum / 3`) is compared exactly: `PARITY.md` A1 names
  aggregations and library functions, not arithmetic on their results.
- The fuzzer keeps float inputs small and few in kind, so sums in generated scenarios do not
  reach magnitudes where the tolerance of A1 is in question (`PARITY.md`, Q1).
- The canonical order used for unordered results can report a difference when two rows tie on
  every non-float column and differ in a float column by less than the tolerance. It cannot
  accept results that differ.
