# Oracle

The oracle decides whether a server "matches Java". It runs the same scenario on two servers
through the Python client and compares every result table, or compares one server with results
recorded from the Java reference. The reference is pinned in [REFERENCE.md](REFERENCE.md).

| Path | Holds |
| --- | --- |
| `requirements.txt` | The pinned Python environment |
| `bin/start-java.sh`, `bin/stop-java.sh` | Start and stop the Java reference server |
| `harness/scenario.py` | The scenario format and its validator |
| `harness/runner.py` | Runs a scenario against a server URL, returns Arrow tables |
| `harness/rules.py` | **The comparison rules**, as data and code. Keep in step with `PARITY.md` |
| `harness/faults.py` | Fault injection, to prove the comparator sees differences |
| `harness/results.py` | Result sets and their Arrow IPC form |
| `scenarios/static/`, `scenarios/ticking/` | The scenarios, one JSON file each |
| `corpus/<scenario>/` | The recorded Java results of each scenario |
| `tests/` | Tests of the harness itself; they need no server |

All commands below run from the `oracle/` directory.

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

## Run

```bash
URL=dh+psk://oxhaven@127.0.0.1:10000

# Gate A: Java against Java. Every scenario, run twice on one server ...
.venv/bin/python -m harness diff --a $URL
# ... or on two servers (start a second one with bin/start-java.sh 10001).
.venv/bin/python -m harness diff --a $URL --b dh+psk://oxhaven@127.0.0.1:10001

# Only some scenarios, and looking at what a scenario returns:
.venv/bin/python -m harness diff --a $URL where_int ticking_agg_by
.venv/bin/python -m harness show --url $URL where_int
```

`--a` is the expected side (Java), `--b` the server under test. Each scenario prints `ok`,
`DIFF` followed by one line per mismatch, or `ERROR` when it could not be run. The exit status is
0 for no differences, 1 for differences, 2 when a scenario could not be run.

A mismatch line names the scenario, table, step, column, row and both values:

```
scenario=agg_by_avg table=avg_k step=0 kind=value column=AvgD row=2 expected=0.875 actual=0.8750001
```

## Record and replay

```bash
# Store the Java results. Run this against the Java reference only.
.venv/bin/python -m harness record --url $URL

# Gate C: compare a live server with the recordings. No Java needed for the server under test.
.venv/bin/python -m harness replay --url $URL
```

A recording is `corpus/<scenario>/manifest.json` plus one Arrow IPC file per result table and
step (`step<NNN>.<table>.arrow`). The manifest holds a hash of the scenario content; `replay`
refuses a recording whose scenario has changed since, so a changed scenario must be recorded
again. The recordings are committed. Re-recording changes the files byte for byte only where the
results changed.

## Fault tests

```bash
# Gate B, and the tests of the scenario format and the rules.
.venv/bin/python -m pytest -q
# With the count of faults applied per kind:
.venv/bin/python -m pytest -q -s tests/test_faults.py
```

`tests/test_faults.py` damages results in each way listed in `harness/faults.py` (changed value,
swapped rows, null to value, value to null, dropped row, extra row, renamed column, changed
column type, changed engine type, float moved just beyond the tolerance, float moved just inside
it) and checks the comparator's verdict: everything is reported except the float inside the
tolerance, and swapped rows in a result declared unordered. It does this on hand-built tables
and on every table of the recorded corpus, so it needs the recordings but no server.

## The comparison rules

They are in `harness/rules.py` and nowhere else; its module comment is the full statement.
In short:

- Schema: column count, names, order and Arrow types are exact, and so is the engine type of
  each column (the `deephaven:type` field metadata). Nullability flags and schema-level
  metadata are not compared.
- Row count is exact. Rows are compared by position, never by row key.
- Booleans, integers, strings and timestamps are exact. Null equals only null.
- Floats: NaN equals NaN, infinities are equal with the same sign, finite values are equal
  within a relative tolerance of `1e-9` or an absolute difference of `1e-12`.
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
- **results**: the tables to compare. A result is ordered unless it says `"ordered": false`,
  which also needs `"unordered_reason"`: a citation of the Java source showing that the
  reference does not guarantee the order. No scenario in the starter corpus needs it.
- **steps** (ticking scenarios): each step is a list of actions on input tables. `add` takes
  full rows; in a keyed input table a row with an existing key replaces that row. `delete` takes
  one cell per key column. Every result table is snapshotted before the first step (step 0)
  and after each step (steps 1, 2, ...).

To add a scenario, put the file under `scenarios/`, named `<name>.json`, then record it against
Java. To add an operation, add it to `OPERATIONS` in `harness/scenario.py` and to
`apply_operation` in `harness/runner.py`.

### How a ticking step is known to be finished

The Java server answers an input table change as soon as the input table has refreshed, while
derived tables may still be updating in the same cycle, so a snapshot taken right then can show
a derived table's previous state. After each step the runner therefore makes two further changes
to a private input table and waits for both; the second cannot be applied before the step's
cycle has completed. `_Barrier` in `harness/runner.py` gives the reasoning and the Java citation.

## Known limits

- A scenario whose operation fails on the server is reported as `ERROR`; the harness does not
  yet compare error behavior.
- `float32` columns use the same tolerance as `float64`, which is tighter than one `float32`
  rounding step, so they are in effect compared exactly.
- The canonical order used for unordered results can report a difference when two rows tie on
  every non-float column and differ in a float column by less than the tolerance. It cannot
  accept results that differ.
