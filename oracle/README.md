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
| `harness/fuzz_wide.py` | The generator of every operation and variant (`--variants all`, `--family`) |
| `harness/fuzz_second.py` | The generator of the second parity pass (`--variants second`, its families) |
| `harness/fuzz_times.py` | The generator for the date and time column types (`--family time_types`) |
| `harness/known.py`, `fuzz-known-findings.json` | Triaged differences that `fuzz` sets apart |
| `harness/faults.py` | Fault injection, to prove the comparator sees differences |
| `harness/smoke.py` | Client smoke checks per protocol area |
| `harness/results.py` | Result sets and their recorded form |
| `scenarios/static/`, `scenarios/ticking/` | The scenarios, one JSON file each |
| `corpus/<scenario>/` | The recorded Java results of each scenario |
| `harness/scripts.py` | Script scenarios: the format, the runner, the recordings |
| `harness/views.py` | Reading a view of a hierarchical table, for script scenarios |
| `scenarios/python/` | The script scenarios, one Python file each |
| `corpus/python/<scenario>/` | The recorded Java results of each script scenario |
| `scenarios/kafka/` | The Kafka scenarios: script scenarios that consume the topics of a broker |
| `corpus/kafka/` | The topics (`topics.json`) and the recorded Java results of each Kafka scenario |
| `harness/kafka.py`, `harness/kafka_topics.py` | The Kafka corpus: publishing the topics, the schema registry, the gate; the topics' definition |
| `bin/kafka-replay.sh` | **The Kafka gate** that CI runs: broker, registry, build, start, replay, compare with its baseline |
| `bin/kafka-record.sh`, `bin/kafka-services.sh` | Record the Kafka corpus from the Java reference; the broker and the registry of both |
| `kafka-baseline.json` | The expected state of every Kafka scenario against Oxhaven |
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

## Script scenarios

A script scenario runs Python on the server, as a user of the console or of `pydeephaven`'s
`run_script` does, and compares the tables the script leaves in its variables. It is the gate
of the `deephaven` package that Oxhaven writes anew (`docs/CONSOLE.md`): Java's answers are
recorded once, and every server with a Python console can be compared with them.

```bash
# Store what the Java reference answers. Run this against the Java reference only.
bin/harness record-scripts --url $URL
# Compare a server that has a Python console with the recordings; needs no Java.
bin/harness replay-scripts --url $URL
# Only some: a name, or the start of names.
bin/harness replay-scripts --url $URL --scripts agg_,table_sort
# What a script leaves behind, for writing one.
bin/harness show-scripts --url $URL --scripts agg_by
```

`replay-scripts` prints one line per scenario: `ok`, `DIFF` followed by one line per mismatch,
`MISSING` with what Python said when the script used a name the server's package does not
have, or `ERROR` when it could not be run. The exit status is 0 when every scenario is clean,
1 when one differs or is missing, 2 when one could not be run or `--scripts` selects nothing.

A scenario is one Python file, `scenarios/python/<name>.py`: the script, with comment lines
that say what to do with it.

```python
# What this scenario is for.
# oracle: results by_key, overall
# oracle: computed by_key.Sum, overall.Sum
from deephaven import agg, empty_table

source = empty_table(9).update(["K = i % 3", "V = i * 1.5"])
by_key = source.agg_by([agg.sum_("Sum = V")], by="K")
overall = source.agg_by([agg.sum_("Sum = V")])
# oracle: step
# oracle: fails
overall = source.agg_by([agg.sum_("Sum = NoSuchColumn")])
```

| Line | Meaning |
| --- | --- |
| `# oracle: results a, b` | The variables to compare. Each is read through its `s/<name>` ticket after every step |
| `# oracle: views v = r, w = r k A` | Views to compare, each under the name before the `=`: of the hierarchical table (a rollup) in the variable `r`, with its default expansions for `v`, and for `w` with those of the key table in the variable `k`, whose actions are in its column `A` (without `A` every row expands). Each is read after every step as a client reads it: `HierarchicalTableService/View`, then the first snapshot of a subscription to the first 2000 rows (`harness/views.py`). A view with two columns of one name, as a rollup with constituents has for its group-by columns, has the later one under `<name>#2` |
| `# oracle: computed a.Col` | A float column under the tolerance of A1, as an aggregation's sum is in a table scenario |
| `# oracle: step` | What follows is the script of the next step. A step without code only waits |
| `# oracle: fails` | The script of this step must fail on the reference |
| `# oracle: await t 12, u fails` | After this step's script, wait until the table in `t` has 12 rows or more and the one in `u` has failed, and only then read the results. For a table that something outside the update graph fills, as a Kafka consumer's thread does ("The Kafka corpus") |

- The text before the first `step` is the script of step 0. Each script is one `run_script`
  call, and keeps the line numbers of the file.
- After each script the runner waits until the server has finished an update cycle, with the
  rule of "How a ticking step is known to be finished", and then reads every result. So a
  time table, or an input table that a script adds rows to, is compared like a ticking
  scenario's tables.
- A variable that cannot be read is a failure with its status code, like the table of a
  refused operation: `NOT_FOUND` for a variable that was never assigned.
- The server has one Python session, for every client. After a scenario the runner removes
  every global variable that was not there before it, imported names included, so a scenario
  can use any name and must import what it uses.
- `record-scripts` refuses to store a script that fails without `fails`, or runs with it: a
  script that fails by mistake must not become the expectation.
- A name selects every scenario whose name starts with it, so name scenarios after the area
  they gate: `table_`, `agg_`, `updateby_`, `join_`, `dtypes_`, `time_`, `input_`,
  `convert_`, `udf_`, `listen_`.

A recording is `corpus/python/<name>/`: the manifest and Arrow files of every recording, and
`scripts.json` with the outcome of each step's script.

**How a script's outcome is compared.** When the reference's script failed, the other server's
must fail with the same Python exception type (`deephaven.dherror.DHError`, `TypeError`), and
for a `DHError` with the same message before the cause: `table update operation failed.` of
`table update operation failed. : Cannot find variable or class X`. The cause, the traceback
and the text of other exceptions are not compared (`PARITY.md` A5). Variables assigned before
the failure are compared like any other.

**Missing names.** A script that fails with `ModuleNotFoundError` or `ImportError` for a module
or name of `deephaven`, with `AttributeError` for an attribute of a `deephaven` module or of
an object that is not a builtin, or with `TypeError` for a keyword argument that a function
does not take, uses a name the server does not have yet. The scenario is then `missing`, not
different: its comparison ends at that step, and what was compared before it stands.
`rules.missing_name` is the rule.

## The replay gate

CI runs one command on every pull request and on every push to `main` (the `oracle-replay` job):

```bash
oracle/bin/ci-replay.sh
```

It builds `oxhaven-server` with its Python console (`--features python`), starts it on a free
port, replays every scenario and every script scenario against the recordings in `corpus/`,
runs every client smoke area, stops the server, and compares what it saw with
`replay-baseline.json`. It needs no Java server: it talks only to the Oxhaven server it
started. To run it locally you need the Rust toolchain and a Python environment at
`oracle/.venv`; the one from `requirements.txt` works, and `requirements-replay.txt` is the same
pins without the Java server wheel, which is what CI installs:

```bash
python3 -m venv oracle/.venv
oracle/.venv/bin/pip install -r oracle/requirements-replay.txt
oracle/bin/ci-replay.sh
```

It takes about 35 seconds after the build (88 scenarios, 78 script scenarios and 31 smoke
checks). `ORACLE_GATE_PORT` fixes the port (other agents
share the machine), `ORACLE_GATE_OUT` moves the output directory (default `oracle/.run/gate`),
and `ORACLE_GATE_TIMEOUT` is the limit in seconds for the replay and the smokes (default 900).
Fifteen seconds before that limit the harness writes the stack of every thread to stderr, so a
run that stalls shows what it was waiting for.
The output directory holds `server.log` and `report.json`: every scenario and check with its
state, its difference kinds, each mismatch in full, and its duration. CI uploads the directory
as the artifact `oracle-replay-report` when the job fails. Against a server that is already
running, `bin/harness gate --url $URL` does the comparison alone.

The exit status is 0 when the outcome is the baseline's, 1 when it is not, and 2 when the server
did not build or start, exited during the run, a scenario or check could not be run, or a
section was skipped.

**The Python console.** The script scenarios and the smoke area `console` run Python on the
server, so the gate builds the server with the cargo feature `python`. The server then embeds
the Python that PyO3 finds when it is built, `python3` on `PATH` or the one `PYO3_PYTHON`
names, and needs that Python's shared library when it runs; the gate adds its directory to
`LD_LIBRARY_PATH` (`docs/CONSOLE.md`, "Build and CI"). It imports the packages of
`oracle/.venv`, the environment that runs the harness: the gate sets `VIRTUAL_ENV` to it
unless `VIRTUAL_ENV` names another, so the server sees the package versions the recordings
were made with, and the venv must be made by the Python the server embeds (`docs/CONSOLE.md`,
"The same packages in CI and on a developer's machine"). On a machine without a Python to embed, where
`python3` does not run or `ORACLE_GATE_PYTHON=0` is set, the gate says so, builds the server
without the console, runs everything else, prints one `SKIPPED` line for each of the sections
`replay-scripts` and `client-smoke console`, and ends with exit status 2: the rest of the
outcome can be read, and the gate has not passed. `bin/harness gate --url $URL` against a
server without a Python console does the same.

**The baseline.** `replay-baseline.json` has one line per scenario (section `replay`), per
script scenario (section `replay-scripts`) and per smoke check (sections
`client-smoke <area>`):

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

A script scenario has the same kinds for its result tables, and these for its scripts, under
the table name `(script)`: `missing` when the script uses a name that the server's `deephaven`
package does not have yet, `script_failure_unexpected:<exception type>` when it fails only on
Oxhaven, `script_failure_expected` when it fails only on Java, `script_error_type:<exception
type>` and `script_error_message` when both fail in different ways ("Script scenarios"
below). A line with `missing` is how the baseline lists a part of the Python API that is not
written: when the name exists, the scenario is clean and the gate asks for the promotion, or
it differs in another way and the gate fails with `NEW KIND`.

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

## The Kafka corpus

What Deephaven makes of fixed topic contents, recorded once, and compared with what Oxhaven
makes of the same topics on a real broker. It is the script scenarios again, with what a
Kafka consumer needs around the server:

```bash
oracle/bin/kafka-replay.sh                       # the Kafka gate; needs Docker
oracle/bin/kafka-replay.sh kafka_json,kafka_avro # only some scenarios
```

`kafka-replay.sh` starts `apache/kafka` in Docker on `127.0.0.1` (a fresh broker every
time), publishes the topics, starts the schema registry, builds and starts Oxhaven with its
Python console, replays every scenario under `scenarios/kafka/` against the recordings under
`corpus/kafka/`, and judges the outcome against `kafka-baseline.json`. It needs no Java, and
the environment of the replay gate (`oracle/.venv` from `requirements-replay.txt`). CI runs
it in the `oracle-replay` job, after the replay gate, which has built the same server.
`ORACLE_KAFKA_PORT` (default 19192; the broker's controller takes the next port) and
`ORACLE_KAFKA_REGISTRY_PORT` (19194) move the two services, `ORACLE_KAFKA_OUT` the output
directory (default `oracle/.run/kafka`: `report.json`, the server's log, the paths the
registry was asked for), and `DOCKER_HOST` selects the Docker daemon.

| Piece | Where | What it is |
| --- | --- | --- |
| The topics | `corpus/kafka/topics.json`, written from `harness/kafka_topics.py` | Every topic with its partitions, and every record with its partition, timestamp, key, value and headers (text, or hexadecimal for bytes that are not text); the subjects of the schema registry with their versions and ids. `tests/test_kafka.py` checks that the file is what the definitions give |
| Publishing | `harness kafka-publish --broker <host:port>` (`kafka.publish`, with `kafka-python`) | Creates the topics and sends each record alone, to its partition and with its timestamp, and checks the offset it got: a broker that is not fresh is refused |
| The registry | `harness kafka-registry --port <n>` (`kafka.Registry`) | Answers `GET /subjects/<s>/versions/<n or latest>` and `GET /schemas/ids/<id>` from the file, with Confluent's bodies and error codes, and 404 for every other path. No registry image |
| A scenario | `scenarios/kafka/<name>.py` | A script scenario. Its script reads the broker's address and the registry's URL from `ORACLE_KAFKA_BROKER` and `ORACLE_KAFKA_REGISTRY` in the environment of the server that runs it, and waits for its tables with `await` |
| A recording | `corpus/kafka/<name>/` | As for every script scenario. `corpus/kafka/recorded-from.json` holds the hash of the topics file the recordings were made from; the gate refuses to run against another |
| The baseline | `kafka-baseline.json`, section `replay-kafka` | One line per scenario, with the states, kinds and verdicts of the replay gate (`baseline.py`). Every change to it is protected: `bin/check-baseline-change.sh` knows `replay-baseline.json` only |

**What makes a recording repeatable.** Nothing in a topic depends on the clock or on the
broker: every record has a timestamp given by the producer and a partition, and a fresh
broker gives each the offset of its position. The topics are created with
`retention.ms=-1`, because the timestamps are in 2023 and a broker's default retention
would delete the records within minutes. A consumer's thread fills its table outside the
update graph, so every scenario waits (`await`) for the rows it expects, or for the failure
it expects, before its tables are read. What Java leaves undefined is taken out in the
script, each with a comment there: tables of two partitions are sorted by partition and
offset (the order of two partitions' records within a poll, `PARITY.md` A3), the time a
record was received is reduced to whether it is there, a blink table is compared through
tables that keep its rows, and a consumer group is one that never commits.

**Recording.** Against the Java reference only:

```bash
oracle/bin/kafka-record.sh                      # every scenario; needs Docker and a JDK
ORACLE_KAFKA_HOLD=1 oracle/bin/kafka-record.sh  # start everything and wait, to write a scenario
```

It starts the same broker and registry and the Java reference (port 10020, or
`ORACLE_KAFKA_JAVA_PORT`), with `oracle/.venv` made from `requirements.txt`, and stores the
recordings. To change a topic, change `harness/kafka_topics.py`, write the file with
`bin/harness kafka-topics --write`, and record every scenario again: one scenario alone
cannot be recorded against topics other than those of the other recordings. While it
holds, `bin/harness show-scripts --url <url> --scenarios oracle/scenarios/kafka --scripts
<name>` prints what a scenario leaves.

**A difference.** When Oxhaven differs from a recording, the gate fails with `REGRESSED` or
`NEW KIND` and the lines of the mismatch. Fix Oxhaven; or, when the difference is one that
stays for now, its line in `kafka-baseline.json` says `differs` with its kinds, tables and
cause, as in the replay baseline, and the gate then fails when it differs in any other way
or no longer differs. The comparison itself is never changed for it.

**Not in the corpus.** Producing (`deephaven.stream.kafka.producer`); TLS and SASL, snappy,
lz4 and zstd, transactions and a broker that restarts, which the broker tests of
`crates/oxhaven-kafka` cover against Oxhaven alone; committed offsets of a consumer group
that commits; `kafka.topics`, which returns a list and not a table; records that arrive
while a table is open (every topic is filled before the server starts).

## Fuzz

```bash
# 300 generated scenarios over every operation the generator builds, Java against Java:
OPS=$(.venv/bin/python -c "from harness import fuzz; print(','.join(sorted(fuzz.OPERATIONS)))")
.venv/bin/python -m harness fuzz --a $URL --ops $OPS --seeds 300 --exact

# Java against a server under test, 10000 scenarios that only filter, starting at seed 0:
.venv/bin/python -m harness fuzz --a $URL --b dh://127.0.0.1:10100 --ops where --seeds 10000
# One seed again:
.venv/bin/python -m harness fuzz --a $URL --b dh://127.0.0.1:10100 --ops where --seeds 1 --seed 4711
# Every seed, whatever differs, with the differences grouped by cause:
.venv/bin/python -m harness fuzz --a $URL --b dh://127.0.0.1:10100 --ops $OPS --seeds 500 \
    --keep-going --out /tmp/fuzz-differences
# One family of operations with every variant of each (see "Variants and families"):
.venv/bin/python -m harness fuzz --a $URL --b dh://127.0.0.1:10100 --family update_by --seeds 1000 \
    --keep-going --out /tmp/fuzz-differences
```

A scenario is a function of the seed and the set of operations, and of nothing else
(`harness/fuzz.py`): random schemas over every column type, cells with nulls, NaN, infinities,
negative zero and the limits of each integer type, static tables or keyed and append-only input
tables with steps that add rows, replace rows by key and delete them, and one to five operations
drawn from `--ops` with arguments that fit the schema. Generated scenarios use no operation
outside `--ops`. About 300 seeds over all operations take a minute and a half against a local
Java server.

The generator builds the operations in `fuzz.OPERATIONS`: `where`, `filter` (structured
conditions: comparisons of numeric columns with literals and columns, `in`, the null test, and
`and`, `or`, `not` of them), `sort`, `head`, `tail`, `slice`, `reverse`, `flatten`, `view`,
`select`, `update`, `update_view`, `lazy_update`, `drop_columns`, `select_distinct`, `merge`,
`agg_by` (`sum`, `min`, `max`, `first`, `last`, `avg`, `count`), `head_by`, `tail_by`,
`natural_join`, `exact_join`, `join`, `aj` and `update_by` (`cum_sum`), and with the variants
`all` (below) also `raj`, `left_join`, `where_in`, `range_join`, `multi_join`, `agg_all_by`,
`group_by`, `ungroup`, `snapshot`, `snapshot_when` and `meta_table`. `--ops` with any other
name is refused. The scenarios for a seed depend on the set of operations asked for and on the
variants, so a seed over a set without the newer operations is the scenario it always was.

### Variants and families

There are two generators, chosen by `--variants`:

- `recorded` is the generator the seeds in `runlog/journal.md` were found with. It builds the
  first 24 operations of the list above in the variants named there, and it is not changed any
  more: `--ops aj --seed 39` is the scenario it was when it was written down
  (`tests/test_fuzz_wide.py` pins digests of it). It is the default whenever `--ops` names only
  operations it builds.
- `all` (`harness/fuzz_wide.py`) builds every operation, each with its variants: `update_by`
  with the cumulative operators, `forward_fill`, `delta`, the `em*` family and the rolling
  operators in ticks and in time, with windows, decays and bad-data controls; every
  aggregation, in `agg_by` and (but for `formula`) `agg_all_by`; joins on several columns
  and with all right columns; `raj`; `range_join` with its range rules; `multi_join`;
  `where_in`, also inverted; structured filters on strings, timestamps and patterns; absolute
  sorts; array columns from `group_by` and the `group` and `distinct` aggregations, into
  `ungroup` and whatever passes columns through. Its inputs have a key column of any type
  that can be one, a timestamp column that does not go backwards (for the time windows), and
  right tables whose key stays unique through the steps. It is the default when `--ops` names
  an operation only it builds. A seed means another scenario here than under `recorded`.

About one request in ten is one the reference refuses (a column of a type the operation does
not take, a window or percentile out of range, columns that do not match): a refusal is a
result to compare like any other. The generator keeps out requests that are valid when made
and fail the table in a later update cycle, because such a table cannot be compared (Known
limits); the module comment of `harness/fuzz_wide.py` lists them, and also what it does not
generate at all and why.

`--family NAME` is `--ops` with a named set and the variants `all`. The families
(`fuzz.FAMILIES`) are `update_by`, `aggregations` (`agg_by`, `agg_all_by`), `groups`
(`group_by`, `ungroup`, `head_by`, `tail_by`), `joins` (`natural_join`, `exact_join`, `join`,
`left_join`), `asof` (`aj`, `raj`), `range_join`, `multi_join`, `sets` (`where_in`,
`select_distinct`), `snapshots`, `shape` (`merge`, `reverse`, `flatten`, `slice`, `head`,
`tail`, `sort`), `filters`, `formulas` (the column operations and `meta_table`) and `all`. A
thousand seeds of one family take five to ten minutes against two local servers.

The variants `second` (`harness/fuzz_second.py`) are the wide generator with what the engine
gained after it was written; a seed means another scenario than under `all`, and the families
above keep theirs. It adds formulas and filters with the row variables `i`, `ii` and `k`,
column arrays and fixed-distance reads (`C_[i-1]`), arithmetic on array columns, and time
literals with a zone, durations and periods; `agg_by` with initial groups and
`preserve_empty`; `partition_merge` and `partition_get`; the script operations (Scenarios,
below) `ring_table`, `keyed_transpose`, `multi_join` without match columns and `range_join`
without aggregations; `update_by` with `rolling_formula_*`, `cum_count_where` and
`rolling_count_where_*`; and `column_statistics`. Its families (`fuzz.SECOND_FAMILIES`) are
`formulas2`, `initial_groups`, `partitions`, `ring`, `transpose`, `script_joins`,
`update_by2`, `statistics` and `all2`; `--family` with one of them, or `--ops` with one of
the new operations, selects these variants. `ring`, `transpose`, `script_joins` and `all2`
need servers with a Python console on both sides (Oxhaven built with `--features python`).
The module comment lists what it keeps out and why, and `fuzz_second.RESTRICTIONS` what it
steps around because of a finding in `tasks/4.20-fuzz-findings-2.md`.

The variants `times` (`harness/fuzz_times.py`, the family `time_types` of
`fuzz.TIME_FAMILIES`) are a generator of their own for the column types `LocalDate`,
`LocalTime`, `LocalDateTime`, `Duration`, `DayOfWeek`, `ZonedDateTime`, `Period` and
`ZoneId`; the families above keep their scenarios. Two inputs with a key, a number and an
instant (static, keyed or append-only) each get an `update` that makes columns of these
types with nulls, the `ZonedDateTime` as one instant in three zones. Over them it draws
sorts, match and range filters with the literal spellings of each type, natural, exact and
cross joins on these columns, `aj` and `raj` with all four rules on these stamps and keys
(the family names `aj`; `raj` is drawn as a form of it), `select_distinct`, and `agg_by`
of and by them (`min`, `max`, `first`, `last`, `count_distinct`, `unique`, `distinct`,
sorted first and last, `median`, percentile). The wire does not carry the zone of a
`ZonedDateTime`, so a `view` with `hourOfDay(Z, true)` shows it now and then; the `view`
also adds two formulas that convert or format these types (`toLocalDate`, `toLocalTime`
and `toLocalDateTime` of a `ZonedDateTime`, `getZone()`, `atTime`, `toZonedDateTime` of a
date, a time and a zone, `toString()` of a `ZonedDateTime` or a `Period`, `formatDate`,
`formatDateTime`), with method calls guarded against null. A table with
a `Period` column, which the rules cannot read, is compared through a view without it.
`fuzz_times.RESTRICTIONS` lists what it steps around because of a finding in
`tasks/4.24-fuzz-time-types.md` that is still open: sorted first or last by a `Period`,
the orderings of a `ZoneId`, an as-of join on a `Period` or `ZoneId` stamp, the
strict rules (`>`, `<`) of an as-of join whose right table is refreshing (a defect of the
reference's ticking result), and a range filter of a `DayOfWeek` on a backtick literal.
Task 4.25 lifted the filters on a `LocalDateTime`, the range comparisons with null and the
ordering aggregations of a `Period`; task 4.26 the as-of joins on the other six types. To
shrink a seed of this family by
hand when the shrinker ends at a table with a `Period` column, start from the scenario
`--keep-going` writes.

### What a run prints

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

With `--keep-going`, `fuzz` runs every seed and shrinks nothing, so that one known difference
does not hide the others. Each differing scenario is written to `--out` as `fuzz_<seed>.json`.
The differences are grouped by signature: the kind of mismatch, the metadata keys whose values
differ (for `table_metadata` and `column_metadata`), and the operation that defines the table
(`input_table` for an input). Names of generated tables and columns are not part of it. Each
group is printed with its seeds and one example, the most frequent first:

```
GROUP kind=table_metadata key=deephaven:attribute.AddOnly,deephaven:attribute.AppendOnly operation=where: 41 seeds, 96 mismatches; seeds 0, 3, 4, ...
      example, seed 0: scenario=fuzz_0 table=o2 step=0 kind=table_metadata expected={...} actual={...}
      scenario: /tmp/fuzz-differences/fuzz_0.json
fuzz: 500 seeds, 448 clean, 52 with differences in 3 groups, 0 not run
```

To shrink one of them, run that seed alone without `--keep-going`.

### Known findings

A difference that has been triaged and is not fixed yet would stop every run at the same
seed, or flood a `--keep-going` run. `fuzz-known-findings.json` lists such differences, each
with a class (`oxhaven-bug`, `allowed-difference`, `reference-defect`, `harness`), a reason
that cites the Java source, and a signature: the kind of mismatch, the operation, what made
the column (the aggregation or `update_by` operator), the metadata keys, both values, and
whether it shows at the first step or later. `harness/known.py` documents the fields.

`fuzz` sets the mismatches that fit an entry apart. A scenario counts as known only if every
one of its mismatches fits; it is then listed (`known seed 39: aj-reference-history`, or a
`KNOWN` line per finding with `--keep-going`), still written to `--out`, and counted in the
last line as `N more with known findings only (not counted as differences)`. A scenario with
one mismatch that fits no entry is a difference as before, reported and shrunk by the
mismatches that do not fit. So an entry cannot hide a difference with another signature, and
the narrower the signature, the less it can hide: give the aggregation and both values
wherever the cause fixes them. `--no-known` reports everything; `--known FILE` reads another
file. Only `fuzz` reads the file: `diff`, `replay` and the gate do not, and no comparison rule
changes.

An entry with `"requires": "reference_inconsistent"` is for a result where the reference
disagrees with itself: what it kept up to date through the steps is not what it answers when
the same operations are applied again to the inputs as they are then. `fuzz` checks that for
the scenario at hand before it counts the mismatch as known. The same check is a command:

```bash
# For each ticking scenario: the tables one server kept up to date against the same tables
# made again after every step. Clean on a server that agrees with itself.
.venv/bin/python -m harness selfcheck --url $URL --scenarios /tmp/fuzz-differences/fuzz_155.json
```

Remove an entry when its cause is fixed: nothing else notices that it no longer fits.

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

Areas: `session`, `tables`, `flight`, `barrage`, `inputtable` and `console` (which needs a
server with a Python console). Each runs a handful of named
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
- `tests/test_fuzz_wide.py`: the recorded generator still builds the scenarios it built (pinned
  digests); the wide one is deterministic, valid against its own predicted columns, mostly
  accepted requests, draws every operation and variant, and keeps its promises about
  timestamps and unique keys through the steps.
- `tests/test_fuzz_second.py`: the two older generators still build the scenarios they built
  (pinned digests); the second one is deterministic, valid, draws every operation and form it
  claims, and keeps its restrictions, each of which names a finding that is written up.
- `tests/test_fuzz_times.py`: the generator of the date and time types is deterministic
  (a pinned digest), valid, compares only tables the rules can read, draws every operation,
  type and aggregation it claims, and keeps its restrictions, each of which names a finding
  with a scenario file.
- `tests/test_known.py`: what a known finding fits and what it must not, that a scenario with
  any other mismatch is still reported and shrunk, and the comparison behind `selfcheck`.
- `tests/test_scenario.py`: the scenario format, the `--ops` filter, the corpus, recordings.
- `tests/test_gate.py`: the baseline's ratchet (what fails the gate, what counts as weakening),
  its file format, the difference kinds, and that the checked-in baseline lists every scenario
  and every smoke check.
- `tests/test_scripts.py`: the script scenario format, reading the exception out of what
  each server answers, the comparison of script outcomes (each kind must be reported, and
  what counts as a missing name), and that every script scenario has a recording made from
  its current content that agrees with its `fails` lines.
- `tests/test_formulas.py`: the formula corpus files, the classifier, the comparison of
  formula outcomes (each kind of difference must be reported), the checks, the Java log reader.

## The comparison rules

They are in `harness/rules.py` and nowhere else; its module comment is the full statement and
cites the section of `PARITY.md` behind each rule. In short:

- Whether a result fails is compared, and the gRPC status code if it does; the message is not
  (Must match, item 7; A5). A refreshing table that failed in an update cycle is a failure
  from that step on, with the status code of a new operation on it, whatever its snapshot
  still answers (A11).
- Schema: column count, names, order and Arrow types are exact, and so are the Deephaven field
  metadata (`deephaven:type`, `componentType`, the input table key flags and the others listed
  in `rules.py`) and the table attributes `IsFlat`, `AddOnly`, `AppendOnly` and `BlinkTable`
  in the schema metadata. Nullability flags are not compared.
- Row count is exact. Rows are compared by position, never by row key.
- Booleans, integers, strings and timestamps are exact. Null equals only null. So are
  dates, times of day and durations (`LocalDate`, `LocalTime` and `Duration` on the wire),
  compared as the integer count they hold, also inside arrays. The interval of months a
  `Period` is sent as is a type the rules do not know.
- Floats are compared bit for bit, except that every NaN equals every NaN. `0.0` and `-0.0`
  differ (A2 is proposed, not in force).
- The exception is A1: a float cell of a computed column may differ by a relative `1e-9` or an
  absolute `1e-12` for `double`, and `1e-5` or `1e-6` for `float`. Null, NaN and infinity are
  never covered by the tolerance. Computed columns are the results of the aggregations `sum`,
  `abs_sum`, `avg`, `weighted_avg`, `weighted_sum`, `var`, `std` and `median` (in `agg_by`,
  `agg_all_by` and `range_join`), of the `update_by` operators that accumulate (`cum_sum`,
  `cum_prod`, the `em*` family, rolling sum, avg, std and weighted avg), of a formula that
  calls an inexact library function, the columns `SUM`, `SUM_ABS`, `SUM_SQRD`, `AVG`,
  `AVG_ABS` and `STD_DEV` of `column_statistics`, and any column that passes such a column
  on (a `first` of a sum, a join that adds it, a snapshot of it). `rules.computed_columns`
  derives them from the scenario's operations. `--exact` switches the tolerance off, for
  comparing two runs of the same server.
- An array cell (the result of `group`, `distinct`, `rolling_group_*`, `group_by` and
  `range_join`) is compared element by element under the rule of its element type; a null
  array differs from an empty one. Arrays of structs and other nested types are not known.
- A result that the scenario marks unordered is compared as a multiset. Everything else is
  compared in order.
- A column type the rules do not know is reported, never passed.
- The script of a script scenario is compared by whether it fails, by the Python exception
  type, and for a `DHError` by the message before the cause ("Script scenarios").

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
  append-only when the list is empty. `"blink": true` next to an empty `key_columns` makes it
  a blink input table, which starts without rows and shows each added row for one cycle.
  `inputs` may be empty when every table comes from `empty_table` or `time_table`.
- **operations**: each defines a named table from tables defined before it.

  | `op` | Other keys |
  | --- | --- |
  | `op` | Request of the table service | Other keys |
  | --- | --- | --- |
  | `where` | `unstructured_filter` | `source`, `filters` |
  | `filter` | `filter` | `source`, `conditions`: list of structured conditions, see below |
  | `where_in` | `where_in` | `left`, `right`, `on`, optional `inverted` |
  | `sort` | `sort` | `source`, `by`: list of `{"column", "direction"}` with `ascending` or `descending`, optional `"absolute": true` |
  | `reverse` | `sort` with the `REVERSE` direction | `source` |
  | `head`, `tail` | `head`, `tail` | `source`, `rows` |
  | `slice` | `slice` | `source`, `start`, `stop` (negative counts from the end) |
  | `head_by`, `tail_by` | `head_by`, `tail_by` | `source`, `rows`, `by` |
  | `flatten` | `flatten` | `source` |
  | `view`, `select`, `update`, `update_view`, `lazy_update` | the same names | `source`, `formulas` |
  | `drop_columns` | `drop_columns` | `source`, `columns` |
  | `select_distinct` | `select_distinct` | `source`, `columns` (may be empty) |
  | `merge` | `merge` | `sources` |
  | `agg_by` | `aggregate` | `source`, `by`, `aggs`: list of aggregations, see below; optional `initial_groups` (a table with the `by` columns), `preserve_empty` |
  | `agg_all_by` | `aggregate_all` | `source`, `by`, `agg`: one aggregation without `columns` |
  | `group_by` | `aggregate_all` with `group` | `source`, `by` |
  | `ungroup` | `ungroup` | `source`, optional `columns`, `null_fill` |
  | `natural_join` | `natural_join` | `left`, `right`, `on`, optional `joins`, optional `type`: `error_on_duplicate` (default), `first_match`, `last_match`, `exactly_one_match` |
  | `exact_join`, `join`, `left_join` | `exact_join`, `cross_join`, `left_join` | `left`, `right`, `on`, optional `joins` |
  | `aj`, `raj` | `aj`, `raj` | `left`, `right`, `on` (the last entry is the as-of match), optional `joins` |
  | `range_join` | `range_join` | `left`, `right`, `on` (the last entry is the range match, such as `"<- Lo <= P < Hi"`), `aggs`; optional `script`: true to make it in the console, which also takes no aggregations |
  | `multi_join` | `multi_join` | `inputs`: list of `{"table", "on", optional "joins"}`; optional `script`: true to make it in the console, which also takes no match columns |
  | `partition_merge` | `partition_by` and `merge` of the partitioned table service | `source`, `by`, optional `drop_keys` |
  | `partition_get` | `partition_by` and `get_table` of the partitioned table service | `source`, `by`, `key` (a table with the `by` columns), optional `drop_keys`, `unique`: `not_set`, `require_unique`, `permit_multiple` |
  | `ring_table` | none: a script, `deephaven.ring_table` | `source`, `capacity`, optional `initialize` |
  | `keyed_transpose` | none: a script, `deephaven.table.keyed_transpose` | `source`, `aggs`, `row_by`, `col_by`, optional `initial_groups`, `new_columns`: `fail`, `ignore` |
  | `update_by` | `update_by` | `source`, `by`, `ops`: list of `{"op", "columns", optional "args"}` |
  | `snapshot` | `snapshot` | `source` |
  | `snapshot_when` | `snapshot_when` | `source`, `trigger`, optional `stamp_columns`, `initial`, `incremental`, `history` |
  | `time_table` | `time_table` | `period`, `start_time` (strings such as `PT1H` and `2024-01-01T00:00:00Z`), optional `blink` |
  | `empty_table` | `empty_table` | `size` |
  | `meta_table` | `meta_table` | `source` |
  | `column_statistics` | `column_statistics` | `source`, `column`, optional `unique_value_limit` |

  `create_input_table` has no `op`: every input with `ticking` is created with it.

  Filters, formulas and column pairs (`"Out = In"`, `"X >= Y"`) are Deephaven query strings and
  go to the server unchanged.

  An aggregation is `{"agg": name, "columns": [...], optional "args": {...}}`, or
  `{"agg": "count", "column": name}`. The names are `sum`, `abs_sum`, `avg`, `min`, `max`,
  `first`, `last`, `std`, `var`, `group`, `count`, `count_where` (`filters`), `count_distinct`
  (`count_nulls`), `distinct` (`include_nulls`), `unique` (`include_nulls`,
  `non_unique_sentinel`), `median` and `pct` (`percentile`, `average_evenly_divided`),
  `sorted_first` and `sorted_last` (`order_by`), `weighted_avg` and `weighted_sum` (`wcol`) and
  `formula` (`formula`, `formula_param`); the arguments in parentheses go in `args`.
  `scenario.AGGREGATIONS` is the list.

  An `update_by` operator is named after the function of `pydeephaven.updateby` that builds
  it (`cum_sum`, `delta`, `ema_tick`, `rolling_avg_time`, ...; `scenario.UPDATE_BY_OPS`), and
  `args` are that function's keyword arguments (`decay_ticks`, `rev_ticks`, `ts_col`, ...).
  `op_control` is written as `{"on_null": "SKIP", "on_nan": "POISON"}` (the names of
  `pydeephaven.updateby.BadDataBehavior`).

  A structured condition is an object with one key, shaped like the `Condition` message of
  `table.proto`: `{"and": [...]}`, `{"or": [...]}`, `{"not": condition}`,
  `{"compare": {"op": "<", "lhs": value, "rhs": value}}`,
  `{"in": {"target": value, "candidates": [value, ...]}}`, `{"is_null": column}`,
  `{"is_nan": column}`, `{"matches": {"column", "text"}}`, `{"contains": {"column", "text"}}`,
  `{"invoke": {"method", "arguments", optional "target"}}` and `{"search": {"text"}}`. A value
  is `{"column": name}`, `{"literal": string, number or boolean}`, `{"long": integer}` or
  `{"nanos": integer}`.
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
A ticking input that the server refuses to create (a blink input table on a server without
them) fails the same way: every table defined from it fails with that code, and the actions of
the steps on it are not sent. An action of a step on an input that was created and that the
server refuses is not a result: the scenario is reported as `ERROR`.

When writing a scenario, `ORACLE_TRACE=1` in the environment makes `show` print, for each
operation the server refuses, the status code and the server's message. Java often answers
`Details Logged w/ID '...'`; the reason is then next to that id in `.run/java-<port>.log`.

Refusals of Java 42.9 that the recordings hold and that are easy to take for harness faults:

- `left_join`: `UNIMPLEMENTED` for any tables (`raj_and_left_join`).
- `filter`: the NaN test, a NaN or infinite literal, and a string literal in `<`, `<=`, `>`,
  `>=` unless the text of the literal carries its own double quotes (`filter_structured`).
- `range_join`: any refreshing table, any aggregation other than `group`, and `<-` or `->`
  next to a strict `<` (`range_join`, `ticking_range_join`).
- `snapshot_when` with `history` together with `initial` or `incremental`: `UNIMPLEMENTED`
  (`snapshot_static`).
- `agg_all_by` with `sum`, `avg`, `std` and the like when a column that is not a key is a
  string, boolean or timestamp (`agg_all_by`).
- `head_by` and `tail_by` with zero rows, or with every column as a key (`head_by_tail_by`).
- `select_distinct` of a formula such as `Odd = K % 2`: `FAILED_PRECONDITION`
  (`select_distinct`).
- `rolling_formula_*` and the `formula` aggregation replace the parameter as text, so the
  parameter `x` in `max(x)` gives `maI(I)`; the scenarios name the parameter `each`.

### How a ticking step is known to be finished

The Java server answers an input table change as soon as the input table has refreshed, while
derived tables may still be updating in the same cycle, so a snapshot taken right then can show
a derived table's previous state. After each step the runner therefore makes two further changes
to a private input table and waits for both; the second cannot be applied before the step's
cycle has completed. `_Barrier` in `harness/runner.py` gives the reasoning and the Java citation.

## Known limits

- The script scenarios compare tables only. A variable that holds anything else (a number, a
  figure, a partitioned table) is not a result, and what a script prints is not compared.
- The scripts of one run share the server's one Python session. The runner removes a
  scenario's variables afterwards, but not what a script changes elsewhere: a module it
  patches, a listener it leaves registered.
- A script's third-party imports are those of the Python each server embeds. Against Oxhaven
  in the gate that is the build machine's `python3`, not `oracle/.venv`; `convert_pandas`
  imports `deephaven.pandas` before `pandas` so that it is `missing` there, whether pandas
  is installed or not, until the conversions exist.

- The formula corpus runs every case on a static table. What a formula does on a refreshing
  table (which formulas Java refuses there, how column arrays propagate changes) is not in it.
- In the formula corpus an input column of type `byte[]`, a vector, `BigDecimal` or a boxed
  type cannot be built through the client, so formulas of the Java tests that need one are
  left out; `harness/formulas/extract.py` counts what it leaves out when it runs.
- Five of the 41 batch operations of the table service have no operation name here:
  `combo_aggregate` (the older form of `aggregate`; the Python client does not send it),
  `as_of_join` (deprecated in `table.proto` in favour of `aj` and `raj`),
  `run_chart_downsample` and `apply_preview_columns` (services for the web client; the first
  needs a table already ordered on a time axis and returns a sample chosen by pixel count,
  the second rewrites columns into preview types the rules do not know), and `fetch_table`
  (it returns its source; every result is already fetched). `--ops` rejects these names and
  any other name outside the table above.
- The fuzzer builds only the operations listed under Fuzz. The table sources (`time_table`,
  `empty_table`) are covered by recorded scenarios only, and so is anything the module
  comments of `harness/fuzz_wide.py` and `harness/fuzz_second.py` list as not generated.
- A script operation (`scenario.SCRIPT_OPERATIONS`, and a join with `"script": true`) is made
  by a script in the server's Python console: the runner binds the tables it reads, runs the
  script and opens what it leaves. It needs a server with a console, and a script that fails
  has the one code `SCRIPT_FAILED`, whatever failed inside it.
- A `time_table` is only compared through operations whose result does not depend on the
  clock: a start time far in the past with `head`, `slice` or a `where` on the timestamp.
  Nothing compares when its rows arrive.
- A blink input can only be changed by adding rows, and its results are snapshotted between
  cycles, so the rows of one cycle are seen only through tables that keep them (`head`,
  `tail`, aggregations); the Barrage update stream of a blink table is not compared.
- The time-based `update_by` operators and `snapshot_when` with `history` are recorded on
  static tables and on input tables through adds and deletes; no scenario makes a timestamp
  go backwards.
- `faults.py` cannot add a row to, or change the attributes of, a table without columns
  (`empty_table`, `select_distinct` of no columns): pyarrow drops the rows. The comparison
  itself does see the row count of such a table.
- A ticking table that fails during an update cycle (a duplicate right key arriving in a
  `natural_join`, a formula that throws in `update`) is compared by its failure alone from
  that step on (`PARITY.md` A11): Java keeps answering snapshots of some such tables, with
  contents that are not a consistent table, so the runner asks each refreshing result for
  `where("true")` before its snapshot and records the refusal as the result. The recordings
  in `corpus/` were made before that rule: `ticking_error_after_step` defines such tables and
  lists only the tables around them, and a recording that holds a failed table's contents
  has to be made again to hold its failure.
- A formula over a computed column (`X = Sum / 3`) is compared exactly: `PARITY.md` A1 names
  aggregations and library functions, not arithmetic on their results.
- The fuzzer keeps float inputs small and few in kind, so sums in generated scenarios do not
  reach magnitudes where the tolerance of A1 is in question (`PARITY.md`, Q1).
- The canonical order used for unordered results can report a difference when two rows tie on
  every non-float column and differ in a float column by less than the tolerance. It cannot
  accept results that differ.
