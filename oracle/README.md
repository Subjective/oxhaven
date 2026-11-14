# Oracle

The oracle decides whether a server "matches Java". It runs the same scenario on two servers
through the Python client and compares every result, or compares one server with results
recorded from the Java reference. The reference is pinned in [REFERENCE.md](REFERENCE.md); what
"matches" means is in [PARITY.md](../PARITY.md).

| Path | Holds |
| --- | --- |
| `requirements.txt` | The pinned Python environment |
| `requirements-replay.txt` | The same pins without the Java server: enough for `replay`, the smokes and the tests |
| `bin/start-java.sh`, `bin/stop-java.sh` | Start and stop the Java reference server |
| `bin/harness` | Runs the harness from any directory |
| `bin/ci-replay.sh` | **The replay gate** that CI runs: build, start, replay, smokes, compare with the baseline |
| `bin/check-baseline-change.sh` | Fails if a change to the baseline is not a stricter one |
| `replay-baseline.json` | The expected state of every scenario and smoke check against Oxhaven |
| `harness/gate.py`, `harness/baseline.py` | The gate's run and the baseline's rules |
| `harness/scenario.py` | The scenario format, its validator, and the `--ops` filter |
| `harness/runner.py` | Runs a scenario against a server URL, returns tables or failures |
| `harness/rules.py` | **The comparison rules**, as data and code. Keep in step with `PARITY.md` |
| `harness/fuzz.py` | Scenario generator and shrinker |
| `harness/faults.py` | Fault injection, to prove the comparator sees differences |
| `harness/smoke.py` | Client smoke checks per protocol area |
| `harness/results.py` | Result sets and their recorded form |
| `scenarios/static/`, `scenarios/ticking/` | The scenarios, one JSON file each |
| `corpus/<scenario>/` | The recorded Java results of each scenario |
| `harness/formulas/` | The `formulas` command: corpus format, classifier, grid, extractors |
| `corpus/formulas/` | The formula corpus: fixtures, cases and their recorded Java outcomes |
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

## The replay gate

CI runs one command on every pull request and on every push to `main` (the `oracle-replay` job):

```bash
oracle/bin/ci-replay.sh
```

It builds `oxhaven-server`, starts it on a free port, replays every scenario against the
recordings in `corpus/`, runs every client smoke area, stops the server, and compares what it
saw with `replay-baseline.json`. It needs no Java server: it talks only to the Oxhaven server it
started. To run it locally you need the Rust toolchain and a Python environment at
`oracle/.venv`; the one from `requirements.txt` works, and `requirements-replay.txt` is the same
pins without the Java server wheel, which is what CI installs:

```bash
python3 -m venv oracle/.venv
oracle/.venv/bin/pip install -r oracle/requirements-replay.txt
oracle/bin/ci-replay.sh
```

It takes about 40 seconds after the build. `ORACLE_GATE_PORT` fixes the port (other agents
share the machine), `ORACLE_GATE_OUT` moves the output directory (default `oracle/.run/gate`),
and `ORACLE_GATE_TIMEOUT` is the limit in seconds for the replay and the smokes (default 900).
The output directory holds `server.log` and `report.json`: every scenario and check with its
state, its difference kinds, each mismatch in full, and its duration. CI uploads the directory
as the artifact `oracle-replay-report` when the job fails. Against a server that is already
running, `bin/harness gate --url $URL` does the comparison alone.

The exit status is 0 when the outcome is the baseline's, 1 when it is not, and 2 when the server
did not build or start, exited during the run, or a scenario or check could not be run.

**The baseline.** `replay-baseline.json` has one line per scenario (section `replay`) and per
smoke check (sections `client-smoke <area>`):

```json
"where_int": {"state": "clean"},
"aj_int_keys": {"state": "differs", "kinds": ["failure_unexpected:UNIMPLEMENTED"], "tables": ["keyed", "strict", "unkeyed"], "why": "no handler for aj"},
```

`kinds` are the kinds of difference the scenario shows, `tables` the result tables that show
one, and `why` names the cause in words. A kind is the comparator's mismatch kind (the list is
in `harness/rules.py`), with the status code for a failure (`failure_unexpected:UNIMPLEMENTED`)
and the metadata key for a metadata difference (`table_metadata:AddOnly`,
`column_metadata:inputtable.isKey`). A smoke check that fails has the one kind `failed` and no
tables.

The gate is a ratchet in both directions. Each line of its verdict starts with what is wrong:

| Verdict | Meaning | What to do |
| --- | --- | --- |
| `REGRESSED` | Listed `clean`, now differs | Fix your change |
| `NEW KIND`, `NEW TABLE` | Listed `differs`, now differs in another way or in another result table | Fix your change |
| `PROMOTE` | Listed `differs`, now clean | Promote it, in the same pull request |
| `TIGHTEN` | Listed `differs`, now shows fewer kinds or tables | Tighten it, in the same pull request |
| `UNLISTED` | It ran and has no line | A new scenario or check comes with its line (protected) |
| `STALE` | It has a line and did not run | A removed scenario or check takes its line with it (protected) |
| `NOT RUN` | It could not be run | Find out why; the server log is next to the report |

**Promoting a scenario.** When your change makes a scenario or check clean, the gate fails with
`PROMOTE` and prints the line to write. Replace the entry's line in `replay-baseline.json` with
it (`"<name>": {"state": "clean"}`), keep the trailing comma as it was, and commit that in the
same pull request. `TIGHTEN` works the same way: the verdict prints the line with the kinds and
tables that remain. `report.json` also holds the whole observed outcome in baseline form under
`observed_as_baseline`. Nothing updates the baseline automatically, in CI or locally, so that
every change to it is a line somebody wrote and a reviewer sees.

**Weakening the baseline is a protected change.** A pull request may make the baseline stricter
without the `oracle-change` label: `differs` to `clean`, fewer kinds, fewer tables, a reworded
`why`. Everything else needs the label, which only the orchestrator applies: `clean` to
`differs`, another kind or table on a `differs` line, a new line, a removed line. The `rules`
CI job enforces this with `bin/check-baseline-change.sh <base>`, which you can run locally. If
you believe a `clean` line is wrong, stop and say so in the pull request (`PORTING.md`, "Never
weaken a check"). There is no state for a scenario that is sometimes clean: one that flakes
is a defect to find, in the server or in the harness.

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

## Formula corpus

`corpus/formulas/` holds formula and filter strings, each with the input table it runs against
and what the Java reference answers: the result column's type and values, the rows a filter
keeps, or the failure. It is separate from the scenarios because its unit is one expression, not
one query.

```bash
# Compare a server with every recorded outcome, or with the cases of one check.
bin/harness formulas --url $URL
bin/harness formulas --check values --url $URL
# Narrow further by tag, group, subset or id prefix; --verbose lists the clean cases too.
bin/harness formulas --check values --tag type:int --not-tag ternary --url $URL
bin/harness formulas --url $URL grid-binary/plus. grid-where/literal.lt.double

# Store the Java outcomes again. Run this against the Java reference only.
bin/harness formulas --record --url $URL

# Count the corpus (add --tags for the count per tag).
bin/harness formulas --stats
```

A case that differs prints `DIFF`, its id, kind and formula, and one line per mismatch in the
form the other commands use (`kind=value column=R row=10 expected=... actual=...`). The exit
status is 0 for no differences, 1 for differences, 2 when the arguments select nothing or a
selected case has no recording.

| `--check` | Cases |
| --- | --- |
| `minimal` | Column references, literals, `+ - * / %`, comparisons, `&& \|\| !`, parentheses, `isNull` (the tag `minimal`) |
| `accept-reject` | Operators, literals, casts and the ternary. Compares whether the formula is accepted, the status code when it is not, and the result type; not the values |
| `values` | The cases of `accept-reject`, with values |
| `functions` | Calls to the default-imported function library |
| `arrays` | Column arrays (`Col_`, indexing) and the row variables `i`, `ii`, `k` |
| `chains` | Accepted formulas from the tests and docs that run after other operations (`setup`) |
| `outside` | Formulas outside the accepted subset, as Java answers them |
| `all` | Every case; the default |

The first five take only cases inside the accepted subset that run directly against a fixture.

**Files.** `fixtures.json` holds the input tables by name. `cases/<group>.jsonl` holds one case
per line, with its outcome under `java`; a re-recording changes only the lines whose outcome
changed. `builtins.json` lists the functions and constants a formula imports by default, read
from the pinned jars. The module comment of `harness/formulas/corpus.py` defines every field.

```json
{"id": "grid-binary/plus.byte.short", "kind": "update", "fixture": "pairs",
 "formula": "B1 + S2", "subset": "accepted", "tags": ["arith", "minimal", "type:byte", "type:short"],
 "source": "grid", "java": {"type": "int", "arrow": "int32", "values": [null, -254, "..."]}}
```

- `kind` is `update` (`formula` is an expression; the harness sends `update("OxR<n> = <formula>")`,
  many per call) or `where` (`formula` is a filter string). The outcome of a `where` case is the
  positions kept, read from a `long` column `OxIdx` that the harness adds to every input table.
- `setup` (only in cases from the tests and docs) lists the operations applied to the fixture
  before the case.
- `subset` is `accepted` or `outside`: whether the formula can be written without arbitrary
  Java (method calls on values, `new`, class names, casts to classes, names that are neither a
  column nor a default import). `tags` name the features used, so a task can gate on what it
  implements. The module comment of `harness/formulas/tags.py` lists the tags and the rule.
- A failure is `{"error": {"status", "stage", "message", "cause"}}`. Only `status` is compared
  (`PARITY.md` A5). `message` and `cause` are there for the reader; they come from the Java
  server's log, which is why `--record` needs the log that `bin/start-java.sh` writes
  (`--java-log` when the server was started another way).

**Where the cases come from.** The `grid-*` groups are generated by `harness/formulas/grid.py`:
every binary operator over every ordered pair of column types (byte, short, int, long, float,
double, char, Boolean, String), operands and results at null, the type extremes, zero, NaN, the
infinities, both zeros and the empty string, then literals, unary operators, casts, the ternary,
`isNull` and the null constants, a sample of the function library, column arrays, and the same
comparisons as `where` filters against columns and against literals. The other groups are
extracted from the reference checkout by `harness/formulas/extract.py`, whose module comment
says how each source is read: `java-parser`, `java-formula-column`, `java-engine`, `py-server`
and `docs-python`.

```bash
# Rebuild the case list; outcomes of cases that did not change are kept. Without --reference
# only the grid is rebuilt. Record afterwards.
bin/harness formulas --generate --reference /path/to/deephaven-core-at-v42.9
```

**Comparison.** `harness/formulas/compare.py` hands every outcome to `harness/rules.py`: rule 0
for failures, `compare_tables` for values, the A1 tolerance only when the formula calls a
function in `INEXACT_FUNCTIONS`. `char` results (Arrow `uint16`) are compared as integers after
their type has been compared. Results of a type the rules do not know (arrays, `BigDecimal`,
dates) are compared for exact equality of their recorded form and counted as `uncompared`,
never as clean. Cases tagged `nondeterministic` (the clock, random numbers) record and compare
only the type. Cases whose `setup` fails in Java are counted as skipped.

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
- `tests/test_gate.py`: the baseline's ratchet (what fails the gate, what counts as weakening),
  its file format, the difference kinds, and that the checked-in baseline lists every scenario
  and every smoke check.
- `tests/test_formulas.py`: the formula corpus files, the classifier, the comparison of
  formula outcomes (each kind of difference must be reported), the checks, the Java log reader.

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

- The formula corpus runs every case on a static table. What a formula does on a refreshing
  table (which formulas Java refuses there, how column arrays propagate changes) is not in it.
- In the formula corpus an input column of type `byte[]`, a vector, `BigDecimal` or a boxed
  type cannot be built through the client, so formulas of the Java tests that need one are
  left out; `harness/formulas/extract.py` counts what it leaves out when it runs.
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
