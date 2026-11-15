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

These numbers are a proposal; see [Open questions](#open-questions-for-the-owner), Q1.

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

**Proposed, not in force.** The owner has not approved this difference. Until Q4 is answered the
harness compares the sign of zero exactly in these columns too, and a mismatch is recorded as a
finding, not waived.

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

In v42.9 every operation in the table has a defined order, so this rule currently covers nothing.
It stays so that the owner can move an operation here.

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

## Open questions for the owner

**Q1. Tolerance values.** A1 proposes relative `1e-9` with absolute floor `1e-12` for `double`,
and `1e-5` / `1e-6` for `float`. A relative tolerance cannot bound the error of a sum with heavy
cancellation: Java's ticking `sum` and `avg` subtract removed values from a running total
(`DoubleChunkedSumOperator#removeChunk`), so after adding and removing `1e20` a group whose true
sum is `1.0` can read `0.0` in Java. Options: (a) accept the proposal and keep fuzz inputs within a
magnitude range where it holds; (b) require Oxhaven to port the running-total algorithm exactly so
that it accumulates the same error, and tighten the tolerance; (c) scale the absolute floor by the
largest magnitude that ever entered the group. The proposal assumes (a).

**Q2. Variance formula.** Java computes `var` and `std` as `(sum2 - sum*sum/n) / (n - 1)` on
running totals (`IntChunkedVarOperator#addChunk`). This is numerically poor: it can lose all
precision and can go slightly negative, which makes `std` NaN. A better algorithm in Oxhaven would
differ by more than the tolerance on such inputs. Should Oxhaven reproduce the formula (the file
assumes yes), or is a deviation acceptable here?

**Q3. Encounter order inside one cycle.** That keys from key-changing modifications are
encountered before keys from additions is a consequence of statement order in
`KeyedUpdateContext#computeDownstreamIndicesAndCopyKeys`, not a documented promise. The file
treats it as a rule because the result order is observable and stable in v42.9. Confirm, or move
"order among groups first seen in the same cycle" to A3.

**Q4. Sign of zero.** A2 lets `min`, `max` and the distinct family return either zero. The
alternative is to require Oxhaven to reproduce which one Java's sorted multiset keeps
(`ET/ssms/DoubleSegmentedSortedMultiset.java`), which means porting its insertion behavior
exactly. For group keys the file already requires the first-encountered value. Confirm the split.

**Q5. Inconsistent comparison semantics.** Java has three behaviors for the same comparison:
`where("X < 0.0")` (range filter: `-0.0` is not less than `0.0`; nulls pass), a formula such as
`where("X < Y")` (`-0.0` is less than `0.0`; nulls pass), and sort or keys (NaN equals NaN). These
look incidental, especially `0.0 <= -0.0` being false in formulas. The file requires Oxhaven to
reproduce all of them. Confirm that incidental behavior is in scope, in particular the signed-zero
case.

**Q6. `long` sum wrapping onto the null sentinel.** A `long` sum that wraps to exactly
`Long.MIN_VALUE` reads as null and then restarts from the next value
(`NullSafeAddition#plusLong`). This is a bug-shaped corner. The file requires parity; the owner
may prefer to exclude it from fuzzing instead.

**Q7. Decimal equality.** "Decimals must match exactly" is read here as value and scale
(`1.10` differs from `1.1`), because `BigDecimal` travels as its own type and scale is visible to
clients. Confirm, or relax to numeric equality.

**Q8. Schema metadata scope.** Which schema-level `deephaven:attribute.*` entries must match was
not settled from source. The file compares field-level metadata fully and schema-level attributes
only for flat, blink, add-only and append-only. The pinned Python client's use of these keys was
not read. Confirm the list, and whether `tableMetadata` for input tables is in scope.

**Q9. Status codes.** Must match item 7 requires the same gRPC status code on failure. The mapping
from Java exceptions to status codes was not researched for this file. If only "fails or not"
should be compared, say so and item 7 narrows.

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

**Q11. A ticking table that failed during an update cycle.** When a ticking table fails in a cycle
(a formula that throws in `update`, a duplicate right key arriving in a `natural_join`), Java
keeps answering snapshots of it, with contents that are not a consistent table: rows of the
parent with stale or null values in the added columns. This file does not say what Oxhaven must
answer after the failure. The harness records the failure at the step where it happens and does
not compare such a table afterwards (`oracle/README.md`, "Known limits"; scenario
`oracle/corpus/ticking_error_after_step`). Options: (a) a failed table refuses every later
request; (b) Oxhaven reproduces what Java serves; (c) the state of a table after it failed is
left out of the comparison, which would be a new allowed difference.
