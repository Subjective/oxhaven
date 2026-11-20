# Parity rules

This file defines what "matches Java" means for Oxhaven. The differential harness in `oracle/`
enforces it: a result that differs from the Java server's is a failure unless the difference is
listed under [Allowed differences](#allowed-differences).

## Purpose and who may change it

- The reference is the release pinned in `oracle/REFERENCE.md` (Deephaven Community Core 42.9,
  source tag `v42.9`). Every rule below describes that release.
- Parity is semantic, not bit-for-bit. Anything not listed as an allowed difference must match
  exactly.
- **Only the owner extends the allowed-differences list.** Agents never add, widen, or reinterpret
  an allowed difference, and never loosen a tolerance. An agent that believes a rule is wrong stops
  and says so in its pull request (`PORTING.md`, rule 3).
- This file is protected: CI rejects changes without the `oracle-change` label.
- A claim marked **UNVERIFIED** was not confirmed in the Java source. Treat it as a question, not a
  rule: confirm it against the source or the running oracle before depending on it, and do not
  encode it in the harness until it is confirmed.

### How to read the citations

Citations are `path: Class#method`, with paths relative to the root of the Java source tree at tag
`v42.9`. These prefixes are used:

| Prefix | Path |
| --- | --- |
| `ET/` | `engine/table/src/main/java/io/deephaven/engine/table/impl/` |
| `EU/` | `engine/table/src/main/java/io/deephaven/engine/util/` |
| `QC/` | `engine/query-constants/src/main/java/io/deephaven/util/` |
| `UC/` | `Util/src/main/java/io/deephaven/util/compare/` |
| `API/` | `table-api/src/main/java/io/deephaven/api/` |
| `BG/` | `extensions/barrage/src/main/java/io/deephaven/extensions/barrage/` |
| `DOC/` | `docs/python/` |

`QLFU` is `ET/lang/QueryLanguageFunctionUtils.java`, the functions that formula operators compile
to. Java has one class per primitive type for most kernels; a citation of the `Double` or `Int`
class stands for the whole family unless a row says otherwise.

A citation that starts with `FC/` is not Java source. It names an outcome recorded from the
reference server: `FC/` is `oracle/corpus/formulas/cases/` in this repository, and
`FC/grid-where.jsonl: literal.le.double.06` is the case with id `grid-where/literal.le.double.06`
in that file. A citation of a case family (`FC/grid-binary.jsonl: remainder.*`) stands for every
case whose id starts that way. In the grid cases the columns `B1`, `S1`, `I1`, `L1`, `F1`, `D1`,
`C1`, `Z1`, `T1` are `byte`, `short`, `int`, `long`, `float`, `double`, `char`, `Boolean` and
`String`.

## Must match

The harness compares the full table state of every exported result, after the initial snapshot and
after each update cycle.

1. **Schema.** Column names, column order, and column types. The type is the Deephaven type carried
   in the `deephaven:type` field metadata (and `deephaven:componentType` for arrays and vectors),
   plus the Arrow type of the field. See [Wire representation](#wire-representation).
2. **Row membership.** The multiset of rows in the result.
3. **Row order**, for every operation whose row in
   [Order guarantees](#order-guarantees-per-operation) says "defined".
4. **Values**, exactly, for: `byte`, `short`, `int`, `long`, `char`, `Boolean`, `String`,
   `Instant` and the other time types, `BigDecimal`, `BigInteger`. A null is equal only to a null.
5. **Floating point values** (`float`, `double`), exactly as bit patterns, wherever the value is
   passed through rather than computed. The computed cases are listed in
   [Allowed difference A1](#a1-floating-point-results-of-computation). In both cases:
   - null matches only null;
   - NaN matches only NaN (any NaN payload matches any other);
   - `+Infinity` and `-Infinity` match only themselves.
6. **Null, NaN and overflow behavior** of every expression and aggregation, as listed in
   [Null, NaN and overflow behavior](#null-nan-and-overflow-behavior). Overflow that wraps in Java
   must wrap to the same value in Oxhaven, including when the wrapped value is the null sentinel.
7. **Whether an operation fails.** If Java rejects a request or a formula throws, Oxhaven must also
   fail, with the same gRPC status code. If Java succeeds, Oxhaven must succeed. Only the message
   text is free (A5).
8. **Result types of aggregations**, as listed in
   [Aggregation result types](#aggregation-result-types).
9. **Array and vector cells**, confirmed by the owner on 2026-11-17: a null cell and an empty
   cell are different values; elements compare in order under the element type's rules;
   whether a column is a vector or an array is part of its type; on the wire, offsets start
   at 0 and a null cell contributes no elements; `isSortable` is absent for an array whose
   element type has no comparator.

## Allowed differences

Each entry states the exact rule the harness applies. Nothing else may differ.

### A1. Floating point results of computation

Applies to a `float` or `double` cell produced by one of:

- the aggregations `sum`, `abs_sum`, `avg`, `weighted_avg`, `weighted_sum`, `var`, `std`, and
  `median` when it averages two middle values;
- `update_by` operators that accumulate (cumulative sum and product, EMA family, rolling sum, avg,
  std, weighted avg);
- formula calls to library functions that are not correctly rounded by IEEE 754 (`exp`, `log`,
  `pow`, the trigonometric functions, and anything built on them).

Rule: with `a` the Java value and `b` the Oxhaven value, both finite and non-null, the cell matches
when

```
|a - b| <= max(REL * max(|a|, |b|), ABS)
```

| Column type | `REL` | `ABS` |
| --- | --- | --- |
| `double` | `1e-9` | `1e-12` |
| `float` | `1e-5` | `1e-6` |

Null, NaN and infinities are never covered by the tolerance: they must match as in Must match,
item 5. A cell that is finite on one side and not on the other is a failure.

The owner confirmed these numbers on 2026-11-17
([Q1](#questions-decided-by-the-owner)).

The tolerance does **not** apply to, and these must be bit-exact:

- columns passed through `where`, `sort`, `head`, `tail`, `merge`, joins, `ungroup`, `snapshot`,
  `select`/`view` of a plain column, and partition constituents;
- the aggregations `min`, `max`, `first`, `last`, `sorted_first`, `sorted_last`, `group`,
  `distinct`, `unique`, percentiles that select an element, and `median` when it selects an
  element, with one exception for the sign of zero (A2);
- formula `+`, `-`, `*`, `/`, `%`, unary minus, casts and `sqrt`. Each is a single correctly
  rounded IEEE 754 operation, so the result is fully determined by the operands.

Why a tolerance is needed at all: Java accumulates floating sums per chunk and then adds chunk
subtotals to a running total, and on ticking tables it subtracts removed values from that running
total (`ET/by/DoubleChunkedSumOperator.java: DoubleChunkedSumOperator#addChunk`, `#removeChunk`,
`#modifyChunk`). The result depends on chunk boundaries and on update history, neither of which
Oxhaven reproduces.

### A2. Sign of zero where Java treats `-0.0` and `0.0` as one value

**Proposed, not in force.** On 2026-11-17 the owner confirmed Q4 as written: this difference
stays a proposal, the harness compares the sign of zero exactly in these columns too, and a
mismatch is recorded as a finding, not waived.

Java's engine comparisons treat `-0.0` and `0.0` as equal (`UC/DoubleComparisons.java:
DoubleComparisons#compare`, `#eq`, `#hashCode`). Where an operation collapses equal values, the
surviving bit pattern is whichever Java happened to keep.

Rule: in the output columns of `min`, `max`, `distinct`, `unique`, `count_distinct`-family
operators and percentiles, a cell holding `0.0` matches a cell holding `-0.0`.

This does not apply to group-by key columns, `select_distinct` columns, or join key columns: there
Java keeps the key of the first row encountered, which is a defined rule that Oxhaven must follow.
See Q4.

### A3. Row order where Java defines none

Rule: for an operation whose row in [Order guarantees](#order-guarantees-per-operation) says
"not guaranteed", the harness compares the result as a multiset of rows. For every other operation
it compares rows by position.

In v42.9 every operation in the table has a defined order. One order outside the table is
covered, approved by the owner on 2026-11-17: the order of the rows of different partitions
that one poll brings into a Kafka consumer's table. Java follows the iteration order of a Java
`HashMap` keyed by topic and partition, which nothing documents; Oxhaven orders by partition
number (`docs/KAFKA.md`, "For the owner's ruling", item 4). The rule stays so that the owner
can move an operation here.

### A4. Row keys and delta sequences

- Row keys (the `long` keys of a table's row set) are never compared. Positions are.
- The sequence of Barrage update messages is not compared: not the number of messages, not how
  adds, removes, modifies and shifts are split, and not the modified-column sets. The harness
  applies each side's deltas to its own replica and compares replica state after each update
  cycle. State after every cycle must match under the rules of this file.
- A consequence: a row reported as modified by one side and as removed then added by the other is
  not a difference.

### A5. Error message text

The text of an error is not compared. Whether an error occurs and its gRPC status code are
(Must match, item 7).

### A6. Performance tables and internal metadata

Not compared:

- the contents of the performance and process tables: `updatePerformanceLog`,
  `updatePerformanceAncestorsLog`, `queryPerformanceLog`, `queryOperationPerformanceLog`,
  `processMetricsLog`, `processInfoLog`, `serverStateLog`
  (`ET/util/TableLoggers.java: TableLoggers`);
- Barrage performance logs (`BG/BarragePerformanceLog.java`);
- table attributes carried in schema metadata under `deephaven:attribute.*` and
  `deephaven:attribute_type.*`, **except** the ones in
  [Wire representation](#wire-representation) that clients act on;
- Arrow record batch boundaries and batch sizes;
- the bytes stored in a value buffer under a null validity bit.

### A7. Stopping a console command

Approved by the owner on 2026-11-15.

A console command whose `ExecuteCommand` call is cancelled, whose connection drops or whose
session ends is interrupted, and a queued command whose call was cancelled does not run. Java runs
both to their end.

The limits are those of `docs/CONSOLE.md`, "Stopping a running command":

- native code that does not return to the interpreter is not stopped;
- an engine operation in progress is finished, not abandoned;
- `CancelCommand` stays `UNIMPLEMENTED`.

### A8. Symbolic links that lead out of the storage directory

Approved by the owner on 2026-11-15.

Java's storage service checks the text of a client's path only: the path is resolved against
the storage directory, normalized, and has to start with that directory
(`server/src/main/java/io/deephaven/server/notebook/FilesystemStorageServiceGrpcImpl.java:
FilesystemStorageServiceGrpcImpl#resolveOrThrow`). A symbolic link inside the directory that
points out of it passes that check, and Java reads, writes, creates, moves and deletes where
the link leads.

Rule: after Java's check, Oxhaven's storage service also requires the real location of the
path, with links followed, to be inside the storage directory.

- A call whose path has its real location outside the directory is refused with
  `INVALID_ARGUMENT: Invalid path: <path>`, where Java carries the call out.
- `ListItems` lists a link that leads out of the directory as a file of size 0 with no etag,
  where Java lists what the link points to.

Nothing else of the storage service is covered. The recorded cases are `DIFFERENCES` in
`crates/oxhaven-server/src/storage/root_tests.rs`.

### A9. Symbolic links that lead out of a JS plugin's directory

Approved by the owner on 2026-11-16.

Java copies a JS plugin's directory when the plugin is registered and serves the copy without
login (`server/jetty-11/src/main/java/io/deephaven/server/jetty11/JsPluginsZipFilesystem.java:
JsPluginsZipFilesystem#add`). The copy follows symbolic links, so a link in the directory to
any file the server can read, outside the directory or in another plugin, is served.

Rule: Oxhaven copies a link only when its real location is inside the plugin's directory.

- A link that leads out of the directory is not copied: a request for it is answered 404 and
  a warning is logged when the plugin is registered, where Java serves the target.
- A plugin whose `main` is such a link is still listed in the manifest, and its main file is
  404.

Nothing else of the JS plugin routes is covered. The recorded case is
`a_link_of_a_js_plugin_is_followed_only_inside_its_directory` in
`crates/oxhaven-server/src/web/tests.rs`.

### A10. A `jpy` module without Java

Approved by the owner on 2026-11-16.

Python packages written for Java's server reach Java classes through the `jpy` module, and some
import it in a module body (`deephaven-plugin-plotly-express` does), so without a module of
that name they fail to import and the server does not start with them installed. Oxhaven has
no Java VM.

Rule: Oxhaven's Python holds a `jpy` module with the public names of jpy 2.2.0 and no Java
VM behind them (`crates/oxhaven-server/python/jpy.py`). The module carries out the Java
classes and members that `docs/CONSOLE.md` lists ("The jpy module", "Java arrays") and
refuses the rest.

- `jpy.get_type` answers a class for any name that is not empty, where jpy refuses a class
  Java does not have. The class can be held, compared and printed.
- Every other use of a class (calling it, reading a member, `jpy.array`, `jpy.cast` of an
  object, `jpy.convert`) raises `NotPorted` naming the Java class and member, where Java
  carries the call out.
- `jpy.has_jvm()` is false, `jpy.create_jvm` is refused, and `jpy.destroy_jvm()` does
  nothing, where on Java it ends the server's VM.

The call-by-call table is in `docs/CONSOLE.md`, "The jpy module" and "Java arrays", and
the recorded cases are in `jpy_tests.rs`, `jpy_array_tests.rs` and `calendar_tests.rs` of
`crates/oxhaven-server/src/console/python/`.

### A11. A table after it failed in an update cycle

Approved by the owner on 2026-11-17 (Q11, option (c)).

When a refreshing table fails in an update cycle (a formula that throws in `update`, a
duplicate right key arriving in a `natural_join`, an `update_by` that sees a timestamp
decrease), what Java serves afterwards depends on the operation and is not a consistent table:

- a failed natural or as-of join keeps answering snapshots, with the parent's rows and stale or
  null values in the added columns;
- a failed `update_by` refuses the snapshot with `INVALID_ARGUMENT`;
- a failed function-generated table cannot be fetched;
- a failed keyed transpose, partitioned transform or partitioned table proxy result keeps
  following its source's changes (`docs/PARTITIONED.md`, "Differences from Java");
- a failed chart downsample keeps its old rows.

Oxhaven does one thing for all of them: a failed table keeps serving the last rows it
published, and new operations on it fail.

Rule: from the cycle in which a table fails, what it serves is not compared, neither its rows
nor whether its snapshot answers. Both servers must still fail it in the same cycle, and every
table that has not failed is compared as before. The harness tells a failed table by asking
each refreshing result for `where("true")` after each step, which both servers refuse for a
failed table, and compares that refusal and its status code in place of the table's contents
(`oracle/README.md`, "The comparison rules"; `failed_in_cycle` in `oracle/harness/runner.py`).

Evidence: `tasks/4.17-fuzz-findings.md` (A1, A7 and Notes), `tasks/4.20-fuzz-findings-2.md`
(D5, J4, H1), `tasks/4.10-ide-table-ops.md`, `tasks/4.18-missing-engine-pieces.md`,
`tasks/4.19-table-sources.md`, `tasks/7.6.1-partitioned-transform.md`,
`tasks/7.6.2-partitioned-proxy.md` (item 12), and the scenario `ticking_error_after_step`.

### A12. Requests that end or hang Java's server

Approved by the owner on 2026-11-17.

Rule: where a request ends Java's server process or hangs it for good, Oxhaven does not
reproduce that. It fails the request or the table, or logs the error, and carries on serving.
The cases recorded:

| Request | Java | Oxhaven | Recorded in |
| --- | --- | --- | --- |
| `update_by` count where with a filter that compares two columns | a `NullPointerException`, then the server shuts down | refuses the request by name | `tasks/4.20-fuzz-findings-2.md` (J1) |
| Expanding a tree row that has no identifier, with every row expanded | a stack overflow ends the process | refuses the expansion by name (`TreeTableImpl#makeChildNodeIdLookup`) | `docs/HIERARCHICAL.md` ("Refused for now") |
| An `on_shutdown_callback` of a table publisher that raises when a failure is delivered | the update graph shuts the server down | logs the error | `tasks/7.4.2-stream-adapter.md` |
| `jpy.destroy_jvm()` from the console | destroys the server's Java VM | does nothing (A10) | `docs/CONSOLE.md` ("The jpy module") |
| A Python function that returns an integer beyond 64 bits | once stopped the server | fails the call | `tasks/6.11-python-udf.md` |
| A partitioned table proxy aggregation with a formula when an update cycle falls due during the call | hangs for good | makes the result on the calling thread | `tasks/7.6.2-partitioned-proxy.md` (item 9) |
| `function_generated_table` with source tables, called from a thread the command joins | never returns | makes the table | `tasks/4.19-table-sources.md` |

A new case of the same kind is covered too, and is added to this table when it is found.

### A13. As-of joins whose incremental answer contradicts Java's own

Approved by the owner on 2026-11-17; extended to `raj`, with its chain case restated, on
2026-11-20 (item 7.19 of the second batch of review items: "Do what's recommended").

On some ticking `aj` and `raj` results, what Java has kept up to date after a step differs
from what Java answers when the same join is made again from the same rows. The cases seen:

- a NaN left stamp joins a newly added NaN right row under `>`;
- the chain case: Java fails the join in an update cycle (`ArrayIndexOutOfBoundsException` in
  `ET/asofjoin/RightIncrementalHashedAsOfJoinStateManager.java:
  RightIncrementalHashedAsOfJoinStateManager#addToSequentialBuilder`) when the join has exact
  match columns, its left table is static, and its right table reports a change that touches
  neither a match column nor the stamp, as an as-of result does when only its joined columns
  change. This is a failure of Deephaven's: the same join made again from the same rows
  succeeds. It is what chains of three or four `aj` showed, first read as a kept match that
  is gone: that match was the last rows of the failed table (`tasks/12.1-fuzzer-coverage.md`,
  F1).

Rule: where Java's incremental `aj` or `raj` result, a failure included, differs from Java's
result for the same join made from scratch on the same rows, Oxhaven matches the result made
from scratch. Where Java's two answers agree, Oxhaven must match them. The fuzzer counts a
seed as this difference only when Java's two answers differ for it and Oxhaven's result is
the one made from scratch (`oracle/fuzz-known-findings.json`, `aj-reference-history`,
`aj-reference-history-rest`, `aj-reference-failure` and `raj-reference-failure`;
`tasks/4.4-asof-join.md`, Notes; `tasks/4.17-fuzz-findings.md`, J5).

### A14. Column types of `head_by` and `tail_by`

Approved by the owner on 2026-11-17.

Java's `head_by` and `tail_by` end with an `update_view` that casts each column to its raw
vector class (`ET/QueryTable.java: QueryTable#headOrTailBy`). For a vector column of strings,
Booleans or timestamps, such as the result of grouping a table that has a timestamp column,
Java's result column is `list<string>`, and after `ungroup` it is `java.lang.Object`.

Rule: Oxhaven keeps the element type: the column stays a vector of its strings, Booleans or
timestamps (`list<timestamp>` for timestamps), and `ungroup` of it gives a column of that
element type (`java.lang.String`, sortable, for strings, where Java gives `java.lang.Object`).
Nothing else of `head_by` and `tail_by` is covered. The case is A5 of `tasks/4.17-fuzz-findings.md`, with its scenario files
`F4-headby-*.json`; the fuzzer keeps it out (`build` in `oracle/harness/fuzz_wide.py`).

### A15. Refusals where Java's result is undefined, inconsistent or wrong

Approved by the owner on 2026-11-17.

Rule: Oxhaven refuses these by name (`UNIMPLEMENTED` over gRPC, `NotImplementedError` inside a
`DHError` in Python), or fails them, where Java answers, rather than reproduce a result that
is undefined, differs between runs or is silently wrong:

| Request | Java | Recorded in |
| --- | --- | --- |
| Initial groups that a refreshing table has no rows for, without `preserve_empty` | later shows the group with null key columns | `tasks/4.18-missing-engine-pieces.md`; `tasks/4.20-fuzz-findings-2.md` (R4) |
| `count_where` with no filter | counts an uninitialised chunk, and gave different answers for the same table on two runs | `tasks/4.18-missing-engine-pieces.md` |
| A function-generated table whose function later omits a column, or returns another reference type for it | keeps stale values, or stores the objects under the old declared type | `tasks/4.19-table-sources.md` |
| `SortedFirst` and `SortedLast` in a rollup with group-by columns, by a column that is not their own result | fails, or sorts the aggregated results rather than the source rows | `docs/HIERARCHICAL.md` ("Which aggregations", "Refused for now") |
| A rollup filter that reads row position (`i`, `ii`, `k`) | takes it for a filter of group-by columns and evaluates it on the rows of the finest level | `docs/HIERARCHICAL.md` ("Refused for now") |
| A tree walk deeper than 200 levels | has no limit and overflows its stack | `docs/HIERARCHICAL.md` ("Refused for now"); `tasks/7.9-tree.md` |
| An `Instant` scope variable outside the years 1677 to 2262 | silently turns the year 1600 into 2184 | `tasks/5.8-plot-formulas.md` ("For the owner's ruling") |
| Kafka: text that is not UTF-8 in a `String` column | replaces the bad bytes | `docs/KAFKA.md` ("For the owner's ruling", item 7) |
| Kafka: the records of a poll before a record that cannot be read | shows them for a cycle before the table fails; Oxhaven fails the table without them, and the failed table is the same | `docs/KAFKA.md` ("For the owner's ruling", item 5) |
| Kafka: a consumer property value that Java accepts and librdkafka does not | accepts it; Oxhaven fails the call rather than clamp the value | `docs/KAFKA.md` ("For the owner's ruling", item 10) |

The order of rows across Kafka partitions within one poll, the other Kafka item of this
ruling, is under A3.

### A16. The Python console's locks, execution contexts and table lifetimes

Approved by the owner on 2026-11-17.

Rule: Oxhaven's Python console differs from Java's in these four ways
(`docs/CONSOLE.md`, "Listeners, locks, liveness scopes and execution contexts"):

- **Execution context.** Java refuses formula operations on a Python thread, in a listener or
  in an executor task without an `ExecutionContext`. Oxhaven needs none and runs them, so a
  script that works on Oxhaven can fail on Java.
- **Locks.** A Java command holds the update graph's exclusive lock. An Oxhaven command holds
  the shared lock and reports it to scripts as exclusive, so another thread's `shared_lock` or
  table operation does not wait for the command. `auto_locking = False` has no effect.
- **`del` of a table.** A table made outside a liveness scope and then dropped by Python ends
  its stream in Oxhaven (a Kafka or publisher table stops); in Java the console session keeps
  it alive. A released table raises on every use, reads included, where Java still reads its
  rows.
- **`modified_prev()`** in a cycle that shifts rows returns the rows' true previous values in
  Oxhaven; Java returns other rows' values or nulls.

Recorded in `tasks/6.12-python-listeners-contexts.md` and `tasks/7.4.2-stream-adapter.md`
("Differences from Java, for the owner").

### A17. `NewSession`

Approved by the owner on 2026-11-17.

Java's `SessionService/NewSession` gives any caller a superuser session without credentials
(`server/src/main/java/io/deephaven/server/session/SessionServiceGrpcImpl.java:
SessionServiceGrpcImpl#newSession`), which bypasses the pre-shared key.

Rule: Oxhaven refuses every `NewSession` call with `UNAUTHENTICATED`. Nothing else of the
session service is covered. The recorded case is `new_session_without_credentials_is_refused`
in `crates/oxhaven-server/src/session/tests.rs`; `tasks/2.1-session-service.md`, Notes.

### A18. Column arrays at two distances on a refreshing table

Approved by the owner on 2026-11-20 (item 7.4 of the second batch of review items: "Do
what's recommended").

A formula or filter that reads column arrays at two different distances from the current
row, neither of them zero (`update("M = X_[i - 1] + X_[i + 1]")`, `X_[i - 1] - X_[i - 2]`,
`where("X_[i - 1] < X_[i + 1]")`), on a refreshing table: Java finds the rows a change
makes dirty by running its one-distance computation twice, the second time over row
positions as if they were row keys (`ET/ShiftedColumnOperation.java:
ShiftedColumnOperation#computeDirtyModifiedRowSetInPositionSpace`). Its `update` then keeps
old values in the rows next to a modified, added or removed row (60 where the same `update`
made again shows 61), and its `where` can keep a row that was removed.

Rule: Oxhaven's result is the one the same operation gives when it is made again from the
table's rows after each cycle, as A13 does for `aj`; a change marks as modified the rows
within the farthest distance on each side of it. Where Java's incremental result agrees
with Java's result made again, Oxhaven must match it; a static table, one distance, or one
distance and zero, are not covered. Recorded in `tasks/10.5-unported-select-arrays.md`
(owner question 8) and `two_distances_reach_both_neighbours_of_a_change` in
`crates/oxhaven-engine/src/ops/select/tests/arrays.rs`.

### A19. `update_by` buckets of vectors of `float` or `double`

Approved by the owner on 2026-11-20 (item 7.6 of the second batch of review items: "Do
what's recommended").

Java buckets `update_by` by a vector column with the vectors' `equals`, which depends on
their class: `DoubleVectorDirect#equals` compares two vectors made by `select()` with
`Arrays#equals`, for which `-0.0` and `0.0` differ, and any other pair (a vector of a
`group_by` result) element by element with `UC/DoubleComparisons.java:
DoubleComparisons#eq`, for which they are equal. `[0.0]` and `[-0.0]` share a bucket after
a `group_by` and are two after a `select()`.

Rule: a bucket column that is a vector of `float` or `double` buckets as Java's buckets
of a plain `double` column do: elements equal by `DoubleComparisons#eq`, so `-0.0` equals
`0.0` and every NaN equals every other, whatever made the vectors. Where Java's vectors
are of a `group_by` result, Oxhaven must match Java. Vectors of vectors of `float` or
`double` are not covered and stay refused. Recorded in `tasks/11.4-updateby-refusals.md`
(R4) and `a_bucket_column_of_floating_vectors_treats_zeros_as_equal` in
`crates/oxhaven-engine/src/ops/update_by/vector_tests.rs`.

### A20. Which of equal vectors of `float` or `double` a rolling minimum or maximum or a `distinct` shows

Approved by the owner on 2026-11-20 (items 7.10 and 7.13 of the second batch of review
items: "Do what's recommended").

Java orders vectors of `float` or `double` element by element with `-0.0` equal to `0.0`
(`DoubleVector#compareTo`), so two vectors that differ only in the sign of a zero are
equal, and which of them it shows depends on its internals:

- `rolling_min_tick`, `rolling_max_tick`, `rolling_min_time` and `rolling_max_time` keep
  the earlier until they evaluate the window again, and then the one the layout of their
  ring buffer favours (`ET/updateby/rollingminmax/ComparableRollingMinMaxOperator.java`).
  Seen: over 400 rows alternating `[-0.0]` and `[0.0]` with a window of three rows, the
  earliest vector of the window except at rows 128, 256 and 384.
- `agg.distinct` shows the one its sorted set kept, which on a ticking table depends on
  the order rows came and went (`ET/by/ssmcountdistinct/distinct/ObjectChunkedDistinctOperator.java`).

Rule: the rolling minimum and maximum of such a column keep the earliest of equal vectors
in the window. `agg.distinct` shows, of equal vectors, the one that first entered the
group's set of distinct values, for as long as an equal vector stays in the group, as it
does for plain doubles. A cell of these results may therefore differ from Java's only in
the sign of a zero inside a vector; everything else must match. Cumulative minimum and
maximum are not covered: they keep the first of equal vectors, as Java does. Recorded in
`tasks/11.4-updateby-refusals.md` (R3), `tasks/11.5-agg-refusals.md` (A9c),
`a_rolling_minimum_of_floating_vectors_keeps_the_earliest_of_equal_ones` in
`crates/oxhaven-engine/src/ops/update_by/vector_tests.rs` and
`the_distinct_vectors_of_doubles_show_the_first_of_equal_ones` in
`crates/oxhaven-engine/src/ops/agg/tests/vector_columns.rs`.

### A21. Chart downsampling by any `long` column

Approved by the owner on 2026-11-20 (item 7.15 of the second batch of review items: "Do
what's recommended").

Java's `RunChartDownsample` takes a `long` x column only when its source can be read as
`long` (`ColumnSource#allowsReinterpret`): a column held in memory, a sorted table's column
or a renamed one is taken, and a formula column of a view (`update_view("W = X * 2")`) is
refused with `Cannot use non-Instant, non-long x column`.

Rule: Oxhaven takes every `long` x column, and downsamples it as Java downsamples the same
values held in memory. Where Java takes the column, Oxhaven must match it. Recorded in
`tasks/11.7-hierarchical-server-refusals.md` (E12) and
`a_long_x_column_need_not_be_held_in_memory` in
`crates/oxhaven-engine/src/ops/ide/downsample_tests.rs`.

### A22. Vectors of a refreshing `group_by` result read as values

Approved by the owner on 2026-11-20 (item 7.19 of the second batch of review items: "Do
what's recommended").

In Java a vector cell of a refreshing `group_by` result (or of the `group` aggregation) is a
view of its group's rows, not a value: when the group changes, the vector an operator holds
changes under it. So Java fails, or keeps a stale value, where an operation holds such
vectors:

- as group-by columns of an aggregation, as `update_by` buckets or as `select_distinct`
  columns: the result fails in the cycle in which a group changes (`IllegalStateException:
  Missing value in probe`, `ET/by/typed/incopenagg/gen/IncrementalAggOpenHasherObject.java`);
- in `agg.distinct`: the result fails (`Failed to modify data`, `Failed to remove data`) or
  keeps the vector it had;
- as the input of `cum_min`, `cum_max`, `rolling_min_*` or `rolling_max_*`: the `update_by`
  result shows the group's new vector, but an operation that reads that result is not told
  of the change. Seen: a group going from `[-4, 8]` to `[8]`, the minimum shows `[8]` and a
  `count_where` of `len(U9) > 1` over it stays 1.

Rule: Oxhaven's vectors are values, and each of these results is the one the same
operations give when they are made again from the tables' rows after each cycle, as A13 does
for as-of joins. Where Java's incremental result agrees with Java's result made again,
Oxhaven must match it. Nothing else of vectors on refreshing tables is covered. Recorded in
`tasks/12.1-fuzzer-coverage.md` (F2, F8) with the scenario files
`F2_vector_of_a_refreshing_group_as_key.json` and
`F8_minimum_of_vectors_of_a_refreshing_group_read_on.json`; the fuzzer counts a seed as this
difference only when Java's two answers differ for it and Oxhaven's result is the one made
again (`oracle/fuzz-known-findings.json`, the entries `group-vector-*`).

### A23. Element types that Java loses

Approved by the owner on 2026-11-20 (item 7.20 of the second batch of review items: "Do
what's recommended").

- `cum_min`, `cum_max`, `rolling_min_*` and `rolling_max_*` of a column of vectors of
  strings: Java's result column is a vector without an element type. A client receives it as
  a list of strings either way; after `ungroup` Java's column is `java.lang.Object`, not
  sortable.
- An element of a condition between two vectors of objects, `(c ? S : S2)[0]`, in `update`
  and in rolling formulas (`(each.size() > 1 ? each : each)[0]`): Java types the column
  `java.lang.Object`, where `S[0]` alone is a `java.lang.String`. For vectors of `Instant` a
  client receives Java's column as text.

Rule: Oxhaven keeps the element type, as A14 does for `head_by` and `tail_by`: after
`ungroup` the column is `java.lang.String` (sortable), and the element of a condition is
`java.lang.String`, or `java.time.Instant` sent as a timestamp. The values must be the same;
only the column's type differs. Recorded in `tasks/12.1-fuzzer-coverage.md` (F3, F6) with
the scenario files `F3_ungroup_of_a_minimum_of_string_vectors.json` and
`F4_F6_conditions_between_windows_of_strings.json`.

### A24. Three `ungroup` requests that Java fails with internal errors

Approved by the owner on 2026-11-20 (item 7.21 of the second batch of review items: "Do
what's recommended").

| Request | Java | Oxhaven |
| --- | --- | --- |
| A second `ungroup` of a column that a first `ungroup` took out of the `distinct` aggregation of a vector column | refuses the request (`NullPointerException`) | the elements |
| `ungroup` with null fill of an aggregation grouped by a vector column, where a key vector has more elements than the row's other ungrouped column | makes the table and fails to read it (`Assertion failed: asserted first <= last`) | nulls for the missing elements |
| `ungroup` with null fill where the shorter column of a row is the vector of a `distinct` aggregation | makes the table and fails to read it (`ArrayIndexOutOfBoundsException` in `ET/ssms/IntSegmentedSortedMultiset.java: IntSegmentedSortedMultiset#get`) | nulls for the missing elements |

Rule: Oxhaven serves these three as the request asks: the elements, or nulls for the
missing elements as null fill gives for other columns. Without null fill both servers refuse
the last two alike (`Array sizes differ`). Nothing else of `ungroup` is covered. Recorded in
`tasks/12.1-fuzzer-coverage.md` (F7) with the scenario file
`F7_ungroup_after_aggregations_over_vectors.json`.

### A25. Twelve dates of the example business calendars

Approved by the owner on 2026-11-20 (item 7.22 of the second batch of review items: "Do
what's recommended").

Oxhaven's `USNYSE_EXAMPLE` and `USBANK_EXAMPLE` are its own files, written from public
sources (`tasks/12.5-shipped-calendars.md`; Deephaven's are under the Deephaven Community
License). On these dates Deephaven's files differ from the public record, and Oxhaven's
follow the record:

| Calendar | Date | Deephaven 42.9 | Oxhaven |
| --- | --- | --- | --- |
| `USNYSE_EXAMPLE` | 1999-11-26, 2000-07-03, 2000-11-24, 2001-07-03, 2001-11-23, 2001-12-24 | full days | early closes at 13:00 |
| `USNYSE_EXAMPLE` | 2022-06-20 (Juneteenth observed), 2025-01-09 (national day of mourning) | business days | closed |
| `USNYSE_EXAMPLE` | 2024-12-24, 2025-12-24 | 10:30 to 14:00 | 09:30 to 13:00 |
| `USBANK_EXAMPLE` | Thanksgiving 2019 | closed on 26 November, 28 November a business day | closed on 28 November, 26 November a business day |
| `USBANK_EXAMPLE` | 2009-01-19 (Martin Luther King, Jr. Day, before the valid range) | not listed | listed in the holidays |

Rule: every answer of these two calendars that depends on one of these dates follows the
public record (business days, hours, holidays, and the functions and figures built on them);
the calendars' descriptions are Oxhaven's own text. Every other date of the three calendars,
and all of `UTC`, must match. The sources for each date and the recorded comparison are in
`tasks/12.5-shipped-calendars.md` and `crates/oxhaven-expr/src/calendar/shipped_tests.rs`.

## Determinism

Oxhaven must give the same answer as itself on every run, for the same inputs and the same
sequence of update cycles. "Same" here is exact: bit-identical values, identical row order,
identical schema. The tolerances in A1 are for comparison against Java only; two Oxhaven runs must
not differ at all.

This requires (`PORTING.md`, rule 7):

- hash tables with fixed seeds, or results that do not depend on hash iteration order;
- parallel reductions that combine in a fixed order, independent of thread count and scheduling;
- no dependence on wall-clock time, addresses, or allocation order.

The harness runs each case twice against Oxhaven and fails on any difference between the runs.

## Reference tables

### Order guarantees per operation

"Source order" means the order of rows in the input table, by position.

| Operation | Order | Rule | Evidence |
| --- | --- | --- | --- |
| `sort` (ascending) | defined | Ascending under the engine comparison for the column type. Rows that compare equal keep source order (stable), in the initial result and after every update. | Stable timsort: `ET/sort/timsort/DoubleLongTimsortKernel.java` (class comment, `#doComparison`). Ticking ties broken by ascending input row key: `ET/SortListener.java` (comment above the listener's merge logic, "breaking ties by comparing input key indices ascending") |
| `sort` (descending) | defined | The comparison is negated; it is not a reversal. Rows that compare equal still keep source order. Null sorts last, NaN first. | `ET/sort/timsort/DoubleLongTimsortDescendingKernel.java: #doComparison` |
| `sort` (several columns) | defined | Lexicographic: first column decides, later columns break ties, source order breaks remaining ties. Each column has its own direction. | `ET/SortHelpers.java: SortHelpers#getSortMappingMulti`; `DOC/reference/table-operations/sort/sort.md` |
| `where` | defined | Source order. The result row set is a subset of the source row set. | `ET/QueryTable.java: QueryTable#where` (result built with `getSubTable`) |
| `head`, `tail`, `slice` | defined | Source order, restricted to the positions selected. | `ET/QueryTable.java: QueryTable#head`, `#tail`, `#slice` via `SliceLikeOperation#slice` |
| `reverse` | defined | Reverse of source order. | `ET/QueryTable.java: QueryTable#reverse` via `ReverseOperation` |
| `merge` | defined | All rows of the first table, then the second, and so on, in argument order; source order within each table. Holds while ticking. | `EU/TableTools.java: TableTools#merge` (javadoc: rows from the same table together, in the order specified as inputs) |
| `select_distinct` | defined | Order of first appearance of each distinct key. It is an aggregation with no operators, so every aggregation rule below applies. | `ET/QueryTable.java: QueryTable#selectDistinct` (calls `aggNoMemo` with `AggregationProcessor#forSelectDistinct`) |
| Aggregations (`agg_by`, `sum_by`, `count_by`, `group_by`, ...) | defined | One row per group, in encounter order: the order in which each key first appeared in the input. When ticking: a new key is appended at the end; a group that becomes empty is removed; a group that later gets rows again reappears at its original position relative to the other groups, not at the end. With `initial_groups`, encounter order starts with that table's keys. | Javadoc of `API/TableOperations.java: TableOperations#aggBy` ("ordered by the encounter order ... thereby ensuring that the row key for a given group never changes"); `ET/by/ChunkedOperatorAggregationHelper.java: ChunkedOperatorAggregationHelper#aggregation` (the `outputPosition` counter is the result row key) and its inner `KeyedUpdateContext#computeDownstreamIndicesAndCopyKeys` (reincarnated states are re-added at their old position); `DOC/reference/table-operations/group-and-aggregate/aggBy.md` |
| Aggregations, keys first seen in the same cycle | defined | Within one update cycle, keys introduced by modified rows whose key changed are encountered before keys introduced by added rows; within each of those, in row order. | `ET/by/ChunkedOperatorAggregationHelper.java: KeyedUpdateContext#computeDownstreamIndicesAndCopyKeys` (`doInserts` for key-change rows runs before `doInserts` for `upstream.added()`). See Q3. |
| Aggregation with no key columns | defined | At most one row. A static empty input gives zero rows unless `preserve_empty` is set. | `ET/by/ChunkedOperatorAggregationHelper.java: ChunkedOperatorAggregationHelper#noKeyAggregation` |
| `group_by` array contents | defined | Within each group, values are in source order. | **UNVERIFIED** in `ET/by/GroupByOperator.java`; follows from the group row set being a subset of the source row set |
| `natural_join`, `exact_join` | defined | Left table order. The result uses the left table's row set unchanged. | `ET/NaturalJoinHelper.java: NaturalJoinHelper#makeResult` |
| `join` (cross join) | defined | Ordered first by left row, then by right row: for each left row in left order, its matching right rows in right table order. | Javadoc of `API/TableOperations.java: TableOperations#join` ("The rows are ordered first by the this table then by the rightTable") |
| `aj`, `raj` | defined | Left table order. The result uses the left table's row set unchanged. | `ET/AsOfJoinHelper.java: AsOfJoinHelper#makeResult` |
| `aj`, `raj`: which right row matches | defined | `aj`: the right row with the closest stamp that does not exceed the left stamp; `raj`: the mirror image. Behavior for several right rows with the same stamp, and for null or NaN stamps, is **UNVERIFIED**. | `DOC/reference/table-operations/join/aj.md`; kernels under `ET/join/stamp/` not read |
| `ungroup` | defined | For each source row in source order, its array elements in array order. | `ET/UngroupOperation.java: UngroupOperation` (result key is the parent key shifted left by a base, plus the element offset) |
| `update_by` | defined | Source order; the result uses the source row set unchanged. Operators run over each group's rows in source order. | `ET/updateby/ZeroKeyUpdateByManager.java`, `ET/updateby/BucketedPartitionedUpdateByManager.java`, `ET/updateby/UpdateByBucketHelper.java` (each constructs the result on `source.getRowSet()`) |
| `update`, `update_view`, `select`, `view`, `drop_columns`, `rename_columns` | defined | Source order. | **UNVERIFIED** by direct reading for `select` on a static table, which may flatten; flattening changes row keys only, not positions (`ET/QueryTable.java: QueryTable#flatten`) |
| `snapshot` | defined | Source order at the time of the snapshot. | `ET/QueryTable.java: QueryTable#snapshot`; ordering of an incremental `snapshot_when` result is **UNVERIFIED** |
| `partition_by` constituents | defined | Constituents appear in encounter order of their keys, with the ticking behavior of an aggregation. Rows inside a constituent are in source order. | `ET/QueryTable.java: QueryTable#partitionBy` (implemented as `aggBy` with a `Partition` aggregation). Row order inside a constituent is **UNVERIFIED** by direct reading |

Notes:

- The Deephaven docs say that operations "should not assume anything about the order of data" in a
  standard (non-append-only) table (`DOC/conceptual/table-types.md`). That is advice to users
  about source tables. It does not relax the rules above: for a given input, v42.9 produces one
  order for each of these operations, and Oxhaven must produce the same one.
- Aggregation with a data index on the key columns takes the encounter order from the index table
  (`ET/by/ChunkedOperatorAggregationHelper.java: #aggregation`, indexed branch). The index is
  itself built in encounter order. The harness does not create data indexes; if it ever does, this
  needs re-checking.

### Null, NaN and overflow behavior

#### Null representation in the Java engine

Java stores nulls of primitive types as sentinel values. Oxhaven stores them as validity bits
(`PORTING.md`), but the sentinels remain observable and must be reproduced: an arithmetic result
that lands on a sentinel **is null** in Java.

| Type | Null sentinel | Citation |
| --- | --- | --- |
| `byte` | `Byte.MIN_VALUE` (-128) | `QC/QueryConstants.java: QueryConstants#NULL_BYTE` |
| `short` | `Short.MIN_VALUE` (-32768) | `#NULL_SHORT` |
| `int` | `Integer.MIN_VALUE` | `#NULL_INT` |
| `long` | `Long.MIN_VALUE` | `#NULL_LONG` |
| `char` | `Character.MAX_VALUE` (0xFFFF) | `#NULL_CHAR` |
| `float` | `-Float.MAX_VALUE` | `#NULL_FLOAT` |
| `double` | `-Double.MAX_VALUE` | `#NULL_DOUBLE` |
| `Boolean` | Java `null`; as a byte, -128 with false = 0 and true = 1 | `#NULL_BOOLEAN`; `QC/BooleanUtils.java: BooleanUtils#NULL_BOOLEAN_AS_BYTE` |
| `Instant` and other time types stored as `long` | `Long.MIN_VALUE` nanoseconds | `#NULL_LONG` |
| `String`, `BigDecimal`, other objects | Java `null` | — |

Rule for Oxhaven: any computation that in Java can produce a sentinel bit pattern must produce a
null. Concretely, after `int` or `long` arithmetic, narrowing casts, and wrapped sums, a result
equal to the type's sentinel is null. A `float` or `double` computation that yields exactly
`-MAX_VALUE` is null.

#### Ordering used by sort, min/max, range filters and sorted structures

| Case | Behavior | Citation |
| --- | --- | --- |
| Null in ascending sort | First. Null is less than every other value, including `-Infinity`. | `UC/DoubleComparisons.java: DoubleComparisons#compare`; `UC/CharComparisons.java: CharComparisons#compare`; `UC/ObjectComparisons.java: ObjectComparisons#compare` |
| Null in descending sort | Last (negated comparison). | `ET/sort/timsort/DoubleLongTimsortDescendingKernel.java: #doComparison` |
| NaN in ascending sort | Last. NaN is greater than every other value, including `+Infinity`. All NaNs are equal to each other. | `UC/DoubleComparisons.java: DoubleComparisons#compare` |
| NaN in descending sort | First. | as above, negated |
| `-0.0` and `0.0` in sort | Equal. They keep source order among themselves. | `UC/DoubleComparisons.java: DoubleComparisons#compare` |
| Full ascending order of a `double` column | null, `-Infinity`, finite values, `+Infinity`, NaN | as above |
| `Boolean` | null, false, true | `QC/BooleanUtils.java` (byte encoding -128, 0, 1) |
| `char` | null first, although its sentinel is the largest code unit | `UC/CharComparisons.java: CharComparisons#compare` |
| `String` and other objects | null first, then `Comparable#compareTo`. For `String` that is UTF-16 code unit order, not code point order and not locale order. | `UC/ObjectComparisons.java: ObjectComparisons#compare` |

#### Keys: group-by, `select_distinct`, `partition_by`, join match columns

| Case | Behavior | Citation |
| --- | --- | --- |
| Null key | A value like any other. All null keys fall in one group; a null left key matches a null right key in a join. | `ET/by/typed/staticopenagg/gen/StaticAggOpenHasherByteDouble.java` (keys compared with `ByteComparisons#eq`, `DoubleComparisons#eq`); `ET/naturaljoin/typed/staticopen/gen/StaticNaturalJoinHasherDouble.java` |
| NaN key | All NaNs are one key; NaN matches NaN in a join. | `UC/DoubleComparisons.java: DoubleComparisons#eq`, `#hashCode` |
| `-0.0` and `0.0` keys | One key. The key column shows the value of the first row encountered. | `UC/DoubleComparisons.java: DoubleComparisons#eq`, `#hashCode`; `engine/chunk/src/main/java/io/deephaven/chunk/util/hashing/DoubleChunkHasher.java: DoubleChunkHasher#hashInitialSingle` |
| Object keys | `Objects.equals`; null equals null. | `UC/ObjectComparisons.java: ObjectComparisons#eq` |
| Cross join and as-of join exact-match columns | Same equality as natural join is **UNVERIFIED** (their hashers were not read). | — |

#### Filters

Java compiles a filter string to one of three implementations, and they do not agree with each
other on NaN and signed zero. The harness must generate all three shapes.

| Shape | Implementation | Citation |
| --- | --- | --- |
| `Col == literal`, `Col != literal`, `Col in ...` | `MatchFilter` | `ET/select/WhereFilterFactory.java: WhereFilterFactory#getExpression` and the expression factories it dispatches to |
| `Col < literal`, `<=`, `>`, `>=` | `RangeFilter` | as above |
| anything else (column against column, arithmetic, function calls) | `ConditionFilter`, compiled through the formula operators below | as above |

| Case | Behavior | Citation |
| --- | --- | --- |
| Match filter, null literal | Matches null cells: `Col == null` and `Col in null` keep exactly the null rows, `Col != null` and `Col not in null` the others, for every column type. | `FC/grid-where.jsonl: literal.eq.int.00`, `literal.eq.double.00`, `literal.eq.string.00`, `literal.eq.boolean.00`, `literal.ne.int.00`, `extra.039` (`I1 in null`), `extra.041` (`I1 not in null`), `extra.073` (`Z1 in null`) |
| Match filter, NaN | `Col == NaN` matches nothing, and `Col != NaN` keeps every row. A `nanMatch` option makes NaN equal NaN, and the `in` forms set it: **`Col in NaN` keeps the NaN rows and `Col not in NaN` drops them.** | `ET/chunkfilter/DoubleChunkMatchFilterFactory.java: DoubleChunkMatchFilterFactory#makeFilter`; `ET/select/WhereFilterFactory.java: WhereFilterFactory#getExpression` (comment "When given an explicit list of values, we allow NaN-matching"); `FC/grid-where.jsonl: literal.eq.double.06`, `literal.ne.double.06`, `extra.063` (`D1 in NaN`), `extra.064` (`D1 not in NaN`), `extra.058` (`F1 in NaN`) |
| Match filter, zero | `-0.0` and `0.0` match each other. | same file, `#getBits` and the value filters |
| Range filter `Col > x`, `Col >= x`, x neither null nor NaN | Excludes NaN and null. | `ET/select/DoubleRangeFilter.java: DoubleRangeFilter#gt`, `#geq` (upper bound NaN, exclusive) |
| Range filter `Col < x`, `Col <= x`, x neither null nor NaN | **Includes null cells** (null is the lowest value). Excludes NaN. | `ET/select/DoubleRangeFilter.java: DoubleRangeFilter#lt`, `#leq` (lower bound `NULL_DOUBLE`, inclusive) |
| Range filter against the literal `null` | The literal is the lowest value. `Col > null` keeps every non-null row, `Col >= null` also the null rows; on `float` and `double` both drop NaN. `Col < null` keeps nothing; `Col <= null` keeps exactly the null rows. | `FC/grid-where.jsonl: literal.gt.int.00`, `literal.ge.int.00`, `literal.lt.int.00`, `literal.le.int.00`, `literal.gt.double.00`, `literal.ge.double.00`, `literal.lt.double.00`, `literal.le.double.00`, `literal.ge.string.00` |
| Range filter against the literal `NaN` | The literal is the highest value, so the NaN exclusion above does not hold. **`Col < NaN` keeps every row that is not NaN, null rows included; `Col <= NaN` keeps every row, NaN rows included.** `Col > NaN` and `Col >= NaN` keep nothing. This is the literal `NaN` only: `Col < NAN_DOUBLE` is a condition filter and keeps nothing, for all four operators. | `FC/grid-where.jsonl: literal.lt.double.06`, `literal.le.double.06`, `literal.gt.double.06`, `literal.ge.double.06`; `literal.lt.double.07` to `literal.ge.double.07` (`NAN_DOUBLE`) |
| Range filter, zero | `-0.0` and `0.0` are equal. | `ET/chunkfilter/DoubleRangeComparator.java` (uses `DoubleComparisons`) |

#### Formula operators

Binary operators in a formula compile to static functions named after the operator
(`ET/lang/QueryLanguageParser.java: QueryLanguageParser#getOperatorName`; `!=` becomes
`!eq(a, b)`).

| Case | Behavior | Citation |
| --- | --- | --- |
| Null in `+ - * / %` | If either operand is null the result is null, of the result type. | `QLFU: QueryLanguageFunctionUtils#plus(int,int)`, `#divide(double,double)`, `#remainder(int,int)` |
| Unary minus, casts | Null in, null out. | `QLFU: #negate(int)`, `#doubleCast(int)`, `#intCast(double)` |
| Unary minus on `int`, `long`, `float`, `double` | Result has the operand type. Negating the largest finite `float` or `double` gives the sentinel bit pattern and is therefore null. | `QLFU: #negate(float)`, `#negate(double)`; `FC/grid-unary.jsonl: negate.int`, `negate.long`, `negate.float`, `negate.double` |
| Unary minus on `byte`, `short`, `char` | **The formula fails to compile**, for a column and for a `char` literal. `QLFU` has `negate` overloads for these types that return `int`, but the parser asserts that the result type equals the operand type. | `ET/lang/QueryLanguageParser.java: QueryLanguageParser#visit(UnaryExpr, VisitArgs)`; `QLFU: #negate(char)`; `FC/grid-unary.jsonl: negate.byte`, `negate.short`, `negate.char`; `FC/grid-literal.jsonl: constant.062` (`-'a'`) |
| `null == null`, operands of the same type | true. | `QLFU: #eq(int,int)`, `#eq(double,double)`, `#eq(Object,Object)` |
| `null == null`, operands of different types | Between two primitive types: true, each operand is tested against the sentinel of its own type (`int` with `long`, `char` with `int`, `float` with `double`, `byte` with `double`). Between two object types: true (`String` with `Boolean`). **Between a primitive type and an object type: false**, and so is every other row: a null `int` does not equal a null `String`. The comparison compiles for every pair of types. | `FC/grid-null.jsonl: extra.00`, `extra.01`, `extra.02`; `FC/grid-binary.jsonl: eq.int.long`, `eq.char.int`, `eq.float.double`, `eq.byte.double`, `eq.string.boolean`, `eq.int.string`, `eq.double.string`, `eq.int.boolean` (first row of each: both operands null) |
| `null == x`, x not null | false. | as above |
| `null < x`, x not null and not NaN | **true**. Null is less than every non-null value in `<`, `<=`, `>`, `>=`. | `QLFU: #compareTo(int,int)`, `#less(int,int)`, `#compareTo(double,double)` |
| `NaN == NaN` | false (IEEE). This differs from sort, keys and min/max, where NaN equals NaN. | `QLFU: #eq(double,double)` |
| `<`, `<=`, `>`, `>=` with a NaN operand | false. | `QLFU: #less(double,double)`, `#lessEquals(double,double)`, `#greater(double,double)` |
| `-0.0 == 0.0` | true. | `QLFU: #eq(double,double)` |
| `-0.0 < 0.0` | **true**, and `0.0 <= -0.0` is false: after the NaN check these operators use `Double.compare`, which orders `-0.0` below `0.0`. This differs from the range filter, where the two are equal. See Q5. | `QLFU: #less(double,double)`, `#compareTo(double,double)` |
| `Boolean` `&`, `\|`, `^` with a null operand | null. | `QLFU: #binaryAnd(Boolean,Boolean)`, `#binaryOr`, `#xor` |
| `!` on a null `Boolean` | null. | `QLFU: #not(Boolean)` |
| `&&`, `\|\|` with a null `Boolean` operand | **The formula fails** (Java `NullPointerException`, status `INVALID_ARGUMENT`) when a null operand is evaluated. The operators short-circuit, so `Z != null && Z` succeeds. A null `Boolean` as the condition of `? :` and as the operand of a `(boolean)` cast fails the same way. | `FC/grid-binary.jsonl: and.boolean.boolean`, `or.boolean.boolean`; `FC/grid-where.jsonl: extra.014` (`Z1 != null && Z1`); `FC/grid-ternary.jsonl: boolean-column-condition.int`; `FC/grid-cast.jsonl: boolean.from-boolean` |
| `where` condition that evaluates to null | **The filter fails** (Java `NullPointerException`, status `INVALID_ARGUMENT`); the row is not dropped. This includes `where("Z")` on a `Boolean` column with a null, and the null results of `!`, `&`, `\|`, `^`. `Z == true` is a match filter and drops the null rows without failing. | `FC/grid-where.jsonl: extra.000` (`Z1`), `extra.001` (`!Z1`), `extra.004` (`Z1 & Z2`), `extra.005`, `extra.006`, `extra.009` (`Z1 == true`) |
| Object comparison | Null less than non-null; otherwise `compareTo`. | `QLFU: #compareTo(Comparable,Comparable)`, `#less(Comparable,Comparable)` |

#### Arithmetic: promotion, overflow, division

Result type of `a op b` for `+`, `-`, `*`, `%`:

| Operands | Result | Citation |
| --- | --- | --- |
| `byte`, `short`, `char`, `int` in any combination | `int` | `QLFU: #plus(byte,byte)`, `#plus(char,int)`, `#plus(int,int)` |
| any of the above with `long` | `long` | `QLFU: #plus(int,long)`, `#plus(long,long)` |
| any integer type with `float` | `float` (also `long` with `float`) | `QLFU: #plus(float,int)`, `#plus(float,long)` |
| anything with `double` | `double` | `QLFU: #plus(int,double)`, `#plus(float,double)` |
| `float` with `float` | `float` | `QLFU: #plus(float,float)` |
| `BigDecimal` or `BigInteger` with anything | `BigDecimal` for `/`; other operators **UNVERIFIED** | `QLFU: #divide(BigInteger,BigInteger)`, `#divide(int,BigDecimal)` |

| Case | Behavior | Citation |
| --- | --- | --- |
| `int` and `long` overflow in `+ - *` | Wraps (two's complement). No widening and no error. If the wrapped result equals the sentinel it is null. | `QLFU: #plus(int,int)`, `#plus(long,long)`, `#multiply(int,int)` |
| `byte + byte`, `short + short` | Computed as `int`; cannot overflow. | `QLFU: #plus(byte,byte)` |
| `/` | **Always floating point division, never integer division.** `7 / 2` is `3.5`. The operands are converted to the result type before dividing. | `QLFU: #divide(int,int)`, `#divide(double,double)` |
| `/` result type | `double`, except: `float / float` is `float`, and an integer type divided by a `float` is `float`. The rule is not symmetric: **`int / float` is `float` but `float / int` is `double`** (likewise for `byte`, `short`, `char`, `long`). | `QLFU: #divide(int,int)` (double), `#divide(long,long)` (double), `#divide(float,float)` (float), `#divide(int,float)` (float), `#divide(float,int)` (double) |
| Integer division by zero | No error. `1 / 0` is `+Infinity`, `-1 / 0` is `-Infinity`, `0 / 0` is NaN. | `QLFU: #divide(int,int)` (operands cast to `double` first) |
| Floating division by zero | IEEE: ±Infinity or NaN. | `QLFU: #divide(double,double)` |
| `%` on integer types | Java remainder: truncated toward zero, sign of the dividend. Result type follows the promotion table. | `QLFU: #remainder(int,int)` |
| Integer `%` by zero | The formula fails (Java `ArithmeticException`). Oxhaven must fail too. This holds only when **both** operands are integer types (`byte`, `short`, `char`, `int`, `long`, in any combination). | `QLFU: #remainder(int,int)` (plain `a % b`); `FC/grid-binary.jsonl: remainder.int.int`, `remainder.long.byte`, `remainder.char.short`; `FC/grid-literal.jsonl: remainder.int.zero-int`, `remainder.char.zero-long` |
| `%` on floating types | Java floating remainder (C `fmod`); `x % 0.0` is NaN. If either operand is `float` or `double` the remainder is floating: an integer zero divisor gives NaN, not a failure (`F % 0`, `D % L` with `L` zero, `I % 0.0`). | `QLFU: #remainder(double,double)`; `FC/grid-literal.jsonl: remainder.float.zero-int`, `remainder.double.zero-long`, `remainder.int.zero-double`; `FC/grid-binary.jsonl: remainder.float.int`, `remainder.double.long` |
| Cast between integer types | Java narrowing: keeps the low bits. A result equal to the target sentinel is null. | `QLFU: #intCast(long)`, `#byteCast(int)`, `#shortCast(int)` |
| Cast from floating to `int` or `long` | Java rules: truncate toward zero, saturate at the type's range, NaN becomes 0. `(int)` of a value at or below `Integer.MIN_VALUE` saturates to the sentinel and is therefore null; likewise `(long)`. So `-Infinity` casts to null and `+Infinity` to the largest value. | `QLFU: #intCast(double)`, `#longCast(double)`; `FC/grid-cast.jsonl: int.from-double`, `long.from-double`, `int.from-float`, `constant.006` (`(int)-1e10`), `constant.009` (`(int)(0.0/0)`) |
| Cast from floating to `byte`, `short` or `char` | **Does not saturate at the target's range.** Java converts to `int` first (truncating and saturating at the `int` range, NaN to 0) and then keeps the low bits. `(byte)` and `(short)` of `+Infinity` are -1 and of `-Infinity` are 0; `(byte)1e10` is -1. `(char)` of `-Infinity` is 0 and of `+Infinity` is 0xFFFF, the sentinel, so null; `(char)-2.5` is 65534. A result equal to the target sentinel is null. | `QLFU: #byteCast(double)`, `#shortCast(double)`, `#charCast(double)` (plain `(byte) a`); `FC/grid-cast.jsonl: byte.from-double`, `short.from-double`, `char.from-double`, `byte.from-float`, `constant.013`, `constant.014`, `constant.015`, `constant.016` |
| Cast from integer to floating | Null in, null out; otherwise the Java widening conversion (round to nearest). | `QLFU: #doubleCast(int)`, `#floatCast(int)` |
| Cast of a boxed or object value | Null in, null out; otherwise `Number#intValue` and friends. | `QLFU: #intCast(Object)` |

#### String concatenation

`+` with a `String` on either side is not a `QLFU` function. The parser prints it as Java's own
`+`, so each operand is converted with Java's string conversion and nothing is null-aware
(`ET/lang/QueryLanguageParser.java: QueryLanguageParser#visit(BinaryExpr, VisitArgs)`, the branch
for `String` with `PLUS`).

| Case | Behavior | Citation |
| --- | --- | --- |
| Result | `String`, never null. | `FC/grid-binary.jsonl: plus.string.*`, `plus.*.string` |
| Null `String` or null `Boolean` operand | The text `null`: a null `String` plus `a` is `nulla`, and two null strings give `nullnull`. | `FC/grid-binary.jsonl: plus.string.string`, `plus.boolean.string`, `plus.string.boolean` |
| Null operand of a primitive type | **The sentinel is printed**: `-128`, `-32768`, `-2147483648`, `-9223372036854775808`, `-3.4028235E38`, `-1.7976931348623157E308`, and for `char` the code unit 0xFFFF. | `FC/grid-binary.jsonl: plus.byte.string`, `plus.short.string`, `plus.int.string`, `plus.long.string`, `plus.float.string`, `plus.double.string`, `plus.char.string` (first rows: the primitive operand is null) |
| `float` and `double` operands | Java's `Float#toString` and `Double#toString`: `1.5`, `-0.0`, `NaN`, `Infinity`, `3.4028235E38`. | `FC/grid-binary.jsonl: plus.string.float`, `plus.string.double` |
| `char` operand | The character, not its number. | `FC/grid-binary.jsonl: plus.string.char` |
| `Boolean` with a number | Not concatenation, and there is no `plus` for it: the formula fails to compile. | `FC/grid-binary.jsonl: plus.boolean.int` |

#### Expressions the gRPC validator refuses

A formula or filter that arrives over gRPC is checked before the engine compiles it. An
expression that calls a method or constructor outside the server's allow list is refused with
status `INVALID_ARGUMENT`, whatever its operands
(`extensions/formula-validation/base/src/main/java/io/deephaven/server/table/validation/ParsingColumnExpressionValidator.java`,
the checks that throw "User expressions are not permitted to ..."). The allow list of the
reference configuration is the `ColumnExpressionValidator.allowedMethods.*` properties in
`props/configs/src/main/resources/dh-defaults.prop`, plus the annotated function library.
Must match, item 7 applies: Oxhaven must refuse what Java refuses.

| Case | Behavior | Citation |
| --- | --- | --- |
| Static methods of `java.lang.Math` and `java.lang.StrictMath` | Refused: `Math.abs(I)`, `Math.max(I, J)`, `Math.sqrt(D)`, `java.lang.Math.sin(X)`. The library functions `abs`, `sqrt`, `sin` are the accepted spelling. | `FC/grid-function.jsonl: java.20` to `java.23`; `FC/docs-python.jsonl: conceptual.ninja.L192.5.0`, `how-to-guides.built-in-functions.L103.9.1` |
| Methods and fields of the boxed primitive classes and of `String` | Accepted: `String.valueOf(I)`, `Integer.parseInt("12")`, `Integer.MAX_VALUE`, `Double.isNaN(D)`, `T.length()`, `T.toUpperCase()`. An instance method on a null receiver fails with `NullPointerException`. `String.valueOf` of a null `int` gives the sentinel's text. | `FC/grid-function.jsonl: java.18`, `java.19`, `java.24`, `java.26`, `java.00`, `java.01`, `java-guarded.00` |
| Constructors | `new String("a")` and `new Integer(I)` are accepted. `new java.net.URL(...)` and `new io.deephaven.vector.LongVectorDirect(...)` are refused. | `FC/grid-function.jsonl: java.28`, `java.29`; `FC/docs-python.jsonl: getting-started.crash-course.query-strings.L317.5.0`; `FC/java-engine.jsonl: QueryTableSelectUpdateTest.L1166.0.1` |
| Methods of `java.time.Duration` | Refused: `'PT1h'.multipliedBy(ii)`. `Duration` is not in the allow list; `Instant`, `LocalDate`, `LocalTime` and `ZonedDateTime` are. | `FC/docs-python.jsonl: conceptual.time-in-deephaven.L256.4.1` |

#### Aggregations

"Normal" means not null, not NaN, not infinite.

| Aggregation | Nulls | NaN and infinities | Empty or all-null group | Citation |
| --- | --- | --- | --- | --- |
| `count` | Counts rows; nulls are counted. | Counted. | 0 rows gives no group, or 0 with `preserve_empty`. | `ET/by/CountAggregationOperator.java` |
| `sum` (integer types) | Skipped. | — | null | `ET/by/SumIntChunk.java: SumIntChunk#sumIntChunk`; `ET/by/LongChunkedSumOperator.java: LongChunkedSumOperator#updateInternal` |
| `sum` (`float`, `double`) | Skipped. | Any NaN gives NaN. `+Infinity` and `-Infinity` together give NaN. Otherwise an infinity gives that infinity. | null | `ET/by/DoubleChunkedSumOperator.java: DoubleChunkedSumOperator#currentValueWithSum` |
| `sum` (`Boolean`) | Skipped. | — | null. Result is the count of true values. Result when all values are false is **UNVERIFIED**. | `ET/by/BooleanChunkedSumOperator.java` |
| `avg` (integer types) | Skipped; divides by the non-null count. | — | null | `ET/by/IntChunkedAvgOperator.java` |
| `avg` (`float`, `double`) | Skipped. | Same NaN and infinity rules as `sum`. | null | `ET/by/DoubleChunkedAvgOperator.java: DoubleChunkedAvgOperator#updateResultWithNewSum` |
| `var`, `std` | Skipped. | For `float` and `double`, a NaN or infinite input gives NaN. | No non-null value: null. **Exactly one non-null value: NaN**, not null and not 0. | `ET/by/IntChunkedVarOperator.java: IntChunkedVarOperator#addChunk`; `ET/by/FloatChunkedVarOperator.java` |
| `var`, `std` formula | Sample variance: `(sum2 - sum*sum/n) / (n - 1)`, accumulated in `double`. `std` is its square root. | — | — | `ET/by/IntChunkedVarOperator.java: IntChunkedVarOperator#addChunk` |
| `min`, `max` | Skipped. | **Any NaN in the group gives NaN, for both `min` and `max`.** Infinities are ordinary values. | null | `ET/by/ssmminmax/DoubleSetResult.java: DoubleSetResult#setResult`; `ET/by/DoubleChunkedAddOnlyMinMaxOperator.java` |
| `first`, `last` | Not skipped: the first or last row of the group in source order, whatever its value. | Passed through. | — | `ET/by/FirstOrLastChunkedOperator.java: FirstOrLastChunkedOperator#addChunk` |
| `median`, percentile | Skipped. | Any NaN in the group gives NaN. | null | `ET/by/ssmpercentile/DoublePercentileTypeMedianHelper.java`; `ET/by/ssmpercentile/DoublePercentileTypeHelper.java` |
| `median`, even count | `int`, `long`, `float`, `double`: mean of the two middle values. Other types: selects one element, no averaging. | — | — | `ET/by/ssmpercentile/SsmChunkedPercentileOperator.java` (constructor); `ET/by/ssmpercentile/IntPercentileTypeMedianHelper.java` |
| `count_distinct`, `distinct`, `unique` | Skipped unless the `countNulls` / `includeNulls` option is set. | Follow `DoubleComparisons` equality: **UNVERIFIED** by reading the operators. | — | `API/agg/spec/AggSpecCountDistinct.java`; `API/agg/spec/AggSpecDistinct.java`; `API/agg/spec/AggSpecUnique.java` |

Overflow in `sum`:

| Case | Behavior | Citation |
| --- | --- | --- |
| `sum` of `byte`, `short`, `int`, `char` | Accumulated in a `long`. Cannot overflow in practice. | `ET/by/SumIntChunk.java: SumIntChunk#sumIntChunk` |
| `sum` of `long` | Wraps silently. No widening, no error. | `ET/by/SumLongChunk.java: SumLongChunk#sumLongChunk` |
| `sum` of `long` wrapping to exactly `Long.MIN_VALUE` | The group shows null, and the next value added replaces the total instead of adding to it, because the running total is combined with a null-aware add. | `EU/NullSafeAddition.java: NullSafeAddition#plusLong`; `ET/by/LongChunkedSumOperator.java: LongChunkedSumOperator#updateInternal`. See Q6. |
| `sum` of `float` | Accumulated in a `double`, then narrowed to `float`. | `ET/by/FloatChunkedSumOperator.java` (fields `resultColumn`, `runningSum`) |
| `sum` of `BigDecimal`, `BigInteger` | Exact. | `ET/by/BigDecimalChunkedSumOperator.java`; `ET/by/BigIntegerChunkedSumOperator.java` |

### Aggregation result types

The output column type for each input column type. These are part of the schema and must match
exactly.

| Aggregation | `byte` | `short` | `int` | `long` | `char` | `float` | `double` | `Boolean` | `BigInteger` | `BigDecimal` | Other |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `count` | `long` | `long` | `long` | `long` | `long` | `long` | `long` | `long` | `long` | `long` | `long` |
| `sum`, `abs_sum` | `long` | `long` | `long` | `long` | `long` | `float` | `double` | `long` | `BigInteger` | `BigDecimal` | error |
| `avg` | `double` | `double` | `double` | `double` | `double` | `double` | `double` | error | `BigDecimal` | `BigDecimal` | error |
| `var`, `std` | `double` | `double` | `double` | `double` | `double` | `double` | `double` | error | `BigDecimal` | `BigDecimal` | error |
| `min`, `max` | input | input | input | input | input | input | input | input | input | input | input, if `Comparable` |
| `first`, `last` | input | input | input | input | input | input | input | input | input | input | input |
| `median` | input | input | `double` | `double` | input | `float` | `double` | input | input | input | input; `Instant` stays `Instant` |
| percentile | input | input | input | input | input | input | input | input | input | input | input |
| `count_distinct` | `long` | `long` | `long` | `long` | `long` | `long` | `long` | `long` | `long` | `long` | `long` |
| `weighted_avg` | `double` | `double` | `double` | `double` | `double` | `double` | `double` | — | — | — | — |
| `weighted_sum` | `long` if value and weight are both integer types, otherwise `double` | | | | | | | — | — | — | — |

Citations:

- `sum`, `abs_sum`: `ET/by/AggregationProcessor.java: AggregationProcessor#makeSumOperator`; result
  column fields of `ET/by/{Byte,Short,Int,Long,Char}ChunkedSumOperator.java` (`LongArraySource`),
  `FloatChunkedSumOperator.java` (`FloatArraySource`), `DoubleChunkedSumOperator.java`
  (`DoubleArraySource`), `BooleanChunkedSumOperator.java` (`LongArraySource`).
- `avg`: `AggregationProcessor#makeAvgOperator`; `ET/by/{Byte,Short,Int,Long,Char,Float,Double}ChunkedAvgOperator.java`
  (`DoubleArraySource`); `BigIntegerChunkedAvgOperator.java` and `BigDecimalChunkedAvgOperator.java`
  (`BigDecimal`).
- `var`, `std`: `AggregationProcessor#makeVarOrStdOperator`; `ET/by/*ChunkedVarOperator.java`.
- `min`, `max`: `AggregationProcessor#makeMinOrMaxOperator`; `ET/by/ssmminmax/SsmChunkedMinMaxOperator.java`.
- `median`, percentile: `ET/by/ssmpercentile/SsmChunkedPercentileOperator.java` (constructor).
- `count`: `ET/by/CountAggregationOperator.java`. `count_distinct`:
  `ET/by/ssmcountdistinct/count/IntChunkedCountDistinctOperator.java`.
- `weighted_avg`, `weighted_sum`: `ET/by/ChunkedWeightedAverageOperator.java`,
  `ET/by/LongChunkedWeightedSumOperator.java`, `ET/by/DoubleChunkedWeightedSumOperator.java`, and
  the weighted branch of `AggregationProcessor` that picks between them.

Notes:

- "error" means `makeSumOperator`, `makeAvgOperator` or `makeVarOrStdOperator` throws
  `UnsupportedOperationException` for that type. Whether `Instant`, `String` and `Boolean` reach
  that throw or are rejected earlier is **UNVERIFIED**; either way the request must fail.
- `avg` of `BigDecimal` divides the running sum by the count with `ROUND_HALF_UP` at the scale of
  the sum (`ET/by/BigDecimalChunkedAvgOperator.java: BigDecimalChunkedAvgOperator#removeChunk`).
  `var`/`std` of `BigDecimal` and `BigInteger` use the same rounding, and `std` takes a square root
  at scale 10 by default (`ET/by/BigDecimalChunkedVarOperator.java: BigDecimalChunkedVarOperator#doUpdate`,
  configuration property `BigDecimalStdOperator.scale`). These are decimals and so must match
  exactly, scale included. See Q7.
- The `weighted_avg` and `weighted_sum` rows for `Boolean` and the big-number types are
  **UNVERIFIED**.

### Wire representation

How results reach the harness. Oxhaven must be indistinguishable to the pinned clients.

| Topic | Behavior in v42.9 | Citation |
| --- | --- | --- |
| Nulls, default | **Arrow validity buffers.** The `useDeephavenNulls` option defaults to false for snapshots and subscriptions. | `BG/BarrageSnapshotOptions.java: BarrageSnapshotOptions#useDeephavenNulls`; `BG/BarrageSubscriptionOptions.java: BarrageSubscriptionOptions#useDeephavenNulls` |
| Validity buffer presence | Sent only when the batch has at least one null in that column; otherwise the buffer has length 0 and the field node reports a null count of 0. | `BG/chunk/BaseChunkWriter.java: BaseChunkWriter.BaseChunkInputStream#sendValidityBuffer`, `#nullCount` |
| Value under a null bit | The sentinel is written as-is in the value buffer. Arrow readers must not look at it, and the harness does not compare it (A6). | `BG/chunk/IntChunkWriter.java` (`drainTo` writes every chunk value) |
| Nulls with `useDeephavenNulls = true` | No validity buffer for primitive columns, null count reported as 0; the client must recognize the sentinels. Oxhaven must honor this option because it is in the protocol. | `BG/chunk/BaseChunkWriter.java` (constructor of `BaseChunkInputStream`); `BG/chunk/LongChunkReader.java` |
| Field nullability | A field declared non-nullable in the schema reports a null count of 0 regardless of content. | `BG/chunk/BaseChunkWriter.java: BaseChunkInputStream#nullCount` |

Arrow types (`BG/util/BarrageUtil.java: BarrageUtil#arrowTypeFor`):

| Deephaven type | Arrow type |
| --- | --- |
| `Boolean` | Bool (bit-packed) |
| `char` | UInt16 |
| `byte`, `short`, `int`, `long` | Int8, Int16, Int32, Int64 |
| `float`, `double` | Float32, Float64 |
| `String`, and any object type without its own mapping | Utf8 |
| `Instant`, `ZonedDateTime` | Timestamp, nanoseconds, time zone `UTC` |
| `Duration` | Duration, nanoseconds (`BarrageUtil#NANO_DURATION_TYPE`) |
| `LocalDate` | Date, milliseconds |
| `LocalTime` | Time, nanoseconds |
| `BigDecimal`, `BigInteger` | Binary, with the Deephaven type in metadata |
| `byte[]` | Binary (List when `columnsAsList` is set) |
| other arrays, vectors | List |

Schema metadata keys. All keys carry the prefix `deephaven:` (`BG/util/BarrageUtil.java:
BarrageUtil#ATTR_DH_PREFIX`, `#putMetadata`).

| Level | Key (after the prefix) | Meaning | Compared |
| --- | --- | --- | --- |
| Field | `type` | Canonical Java class name of the column type. Clients need it to tell `Instant` from `long`, `char` from an integer, `BigDecimal` from bytes. | yes |
| Field | `componentType` | Element type of an array or vector column. | yes |
| Field | `isPartitioning` | Column is a partitioning column. | yes |
| Field | `isSortable` | Column type can be sorted. | yes |
| Field | `isStyle`, `isRowStyle`, `isNumberFormat`, `isDateFormat`, `styleColumn`, `numberFormatColumn`, `dateFormatColumn` | Formatting columns used by the web UI. | yes |
| Field | `description` | Column description. | yes |
| Field | `inputtable.isKey`, `inputtable.isValue` | Input table roles. | yes |
| Schema | `attribute.<name>`, `attribute_type.<name>` | Table attributes with a string form. | only the list below |
| Schema | `unsent.attribute.<name>` | Marks an attribute that exists but was not sent. | no |
| Schema | `tableMetadata` | Base64 protobuf metadata for input tables. | **UNVERIFIED** contents; see Q8 |

Source: `BG/util/BarrageUtil.java: BarrageUtil#columnDefinitionsToFields`,
`#attributesToMetadata`, `#maybeAddInputTableMetadata`.

Schema-level attributes the harness compares, because clients change behavior on them: the flat
marker written by `#attributesToMetadata`, and the blink, add-only and append-only markers. The
exact attribute names for the last three and the full set clients read are **UNVERIFIED**; see Q8.

## Questions decided by the owner

These were open until the owner answered them on 2026-11-17. Q1 to Q10 were confirmed as
written, so the rules above stand as they were; Q11 became A11. Each question keeps its text,
with the decision after it.

**Q1. Tolerance values.** A1 proposes relative `1e-9` with absolute floor `1e-12` for `double`,
and `1e-5` / `1e-6` for `float`. A relative tolerance cannot bound the error of a sum with heavy
cancellation: Java's ticking `sum` and `avg` subtract removed values from a running total
(`DoubleChunkedSumOperator#removeChunk`), so after adding and removing `1e20` a group whose true
sum is `1.0` can read `0.0` in Java. Options: (a) accept the proposal and keep fuzz inputs within a
magnitude range where it holds; (b) require Oxhaven to port the running-total algorithm exactly so
that it accumulates the same error, and tighten the tolerance; (c) scale the absolute floor by the
largest magnitude that ever entered the group. The proposal assumes (a).

Decided 2026-11-17, confirmed by the owner: option (a), the values of A1.

**Q2. Variance formula.** Java computes `var` and `std` as `(sum2 - sum*sum/n) / (n - 1)` on
running totals (`IntChunkedVarOperator#addChunk`). This is numerically poor: it can lose all
precision and can go slightly negative, which makes `std` NaN. A better algorithm in Oxhaven would
differ by more than the tolerance on such inputs. Should Oxhaven reproduce the formula (the file
assumes yes), or is a deviation acceptable here?

Decided 2026-11-17, confirmed by the owner: Oxhaven reproduces Java's formula.

**Q3. Encounter order inside one cycle.** That keys from key-changing modifications are
encountered before keys from additions is a consequence of statement order in
`KeyedUpdateContext#computeDownstreamIndicesAndCopyKeys`, not a documented promise. The file
treats it as a rule because the result order is observable and stable in v42.9. Confirm, or move
"order among groups first seen in the same cycle" to A3.

Decided 2026-11-17, confirmed by the owner: it is a rule, as the order table states.

**Q4. Sign of zero.** A2 lets `min`, `max` and the distinct family return either zero. The
alternative is to require Oxhaven to reproduce which one Java's sorted multiset keeps
(`ET/ssms/DoubleSegmentedSortedMultiset.java`), which means porting its insertion behavior
exactly. For group keys the file already requires the first-encountered value. Confirm the split.

Decided 2026-11-17, confirmed by the owner: the split stands, and A2 stays proposed and not in force, so the sign of zero is compared exactly.

**Q5. Inconsistent comparison semantics.** Java has three behaviors for the same comparison:
`where("X < 0.0")` (range filter: `-0.0` is not less than `0.0`; nulls pass), a formula such as
`where("X < Y")` (`-0.0` is less than `0.0`; nulls pass), and sort or keys (NaN equals NaN). These
look incidental, especially `0.0 <= -0.0` being false in formulas. The file requires Oxhaven to
reproduce all of them. Confirm that incidental behavior is in scope, in particular the signed-zero
case.

Decided 2026-11-17, confirmed by the owner: all three behaviours are in scope, the signed-zero case included.

**Q6. `long` sum wrapping onto the null sentinel.** A `long` sum that wraps to exactly
`Long.MIN_VALUE` reads as null and then restarts from the next value
(`NullSafeAddition#plusLong`). This is a bug-shaped corner. The file requires parity; the owner
may prefer to exclude it from fuzzing instead.

Decided 2026-11-17, confirmed by the owner: Oxhaven reproduces it.

**Q7. Decimal equality.** "Decimals must match exactly" is read here as value and scale
(`1.10` differs from `1.1`), because `BigDecimal` travels as its own type and scale is visible to
clients. Confirm, or relax to numeric equality.

Decided 2026-11-17, confirmed by the owner: value and scale.

**Q8. Schema metadata scope.** Which schema-level `deephaven:attribute.*` entries must match was
not settled from source. The file compares field-level metadata fully and schema-level attributes
only for flat, blink, add-only and append-only. The pinned Python client's use of these keys was
not read. Confirm the list, and whether `tableMetadata` for input tables is in scope.

Decided 2026-11-17, confirmed by the owner: the schema-level attributes compared are flat, blink, add-only and append-only, as written.

**Q9. Status codes.** Must match item 7 requires the same gRPC status code on failure. The mapping
from Java exceptions to status codes was not researched for this file. If only "fails or not"
should be compared, say so and item 7 narrows.

Decided 2026-11-17, confirmed by the owner: the status code is compared (Must match, item 7).

**Q10. Unverified rows.** These claims are marked UNVERIFIED above and need a source reading or an
oracle run before the harness encodes them:

- `aj` / `raj` match selection for duplicate right stamps, and for null and NaN stamps;
- key equality (null, NaN, signed zero) in cross join and in the exact-match columns of as-of join;
- row order inside `group_by` arrays and inside `partition_by` constituents;
- row order of an incremental `snapshot_when` result, and of static `select` when it flattens;
- `sum` of `Boolean` when every value is false;
- NaN and signed-zero handling in `count_distinct`, `distinct`, `unique`;
- result types of `+ - * %` involving `BigDecimal` or `BigInteger`, and of `weighted_avg` / `weighted_sum` for non-primitive types;
- whether unsupported aggregation input types fail in `AggregationProcessor` or earlier;
- the schema-level attribute names clients read.

Formula library functions (the numeric, string and time functions callable from formulas) and
`update_by` null and NaN behavior are not covered by this file at all. Each needs its own section
before the tasks that port them start.

Decided 2026-11-17, confirmed by the owner: these claims stay **UNVERIFIED** and are not
encoded in the harness until a reading of the source or an oracle run confirms them.

**Q11. A ticking table that failed during an update cycle.** When a ticking table fails in a cycle
(a formula that throws in `update`, a duplicate right key arriving in a `natural_join`), Java
keeps answering snapshots of it, with contents that are not a consistent table: rows of the
parent with stale or null values in the added columns. This file does not say what Oxhaven must
answer after the failure. The harness records the failure at the step where it happens and does
not compare such a table afterwards (`oracle/README.md`, "Known limits"; scenario
`oracle/corpus/ticking_error_after_step`). Options: (a) a failed table refuses every later
request; (b) Oxhaven reproduces what Java serves; (c) the state of a table after it failed is
left out of the comparison, which would be a new allowed difference.

Decided 2026-11-17 by the owner: option (c), allowed difference
[A11](#a11-a-table-after-it-failed-in-an-update-cycle).
