# Partitioned tables

How Oxhaven holds a table of tables, keeps its constituents up to date, and hands it to
clients. Written by task 7.6 (`tasks/7.6-partitioned-tables.md`), which also built the first
slice. The follow-on tasks, 7.6.1 to 7.6.6, are sized in that file; 7.6.1
(`tasks/7.6.1-partitioned-transform.md`) wrote "Transforms"; 7.6.3
(`tasks/7.6.3-partitioned-factories.md`) wrote "Made from tables"; 7.6.4
(`tasks/7.6.4-columns-of-tables.md`) wrote "Columns of tables in filters, aggregations and
formulas".

What Java does was read in `PartitionByChunkedOperator`, `PartitionedTableImpl`,
`PartitionedTableServiceGrpcImpl` and `PartitionedTableTypePlugin`, and observed on Java 42.9
with scripts sent through `run_script` and with plain gRPC calls. Where this document says
"Java", it means that observation; the cases that are kept are the script scenarios
`oracle/scenarios/python/partitioned_*.py` and the handler tests of
`crates/oxhaven-server/src/partitioned`.

## Decision

1. **A partitioned table is an ordinary table with a column of tables, plus five facts about
   it**, as in Java: `oxhaven_engine::ops::partitioned::PartitionedTable` holds the underlying
   table, the key column names, whether keys are unique, the constituent column's name, the
   constituents' definition, and whether the set of constituents can change. Every operation
   on a partitioned table is an operation on the underlying table or on its constituents.
2. **The contracts get one column type, `ColumnType::Table`**, and nothing else changes in
   them. A cell is a 64-bit handle, the chunk an Arrow `Int64` array, so every operator that
   moves cells (`where`, `sort`, `head`, `where_in`, a join's right side) carries a column
   of tables with no change. See "The column type".
3. **`partition_by` is the partition aggregation**, as in Java
   (`QueryTable#partitionBy` is `aggBy(Partition)`): one operator of the aggregation
   framework, `ops::partitioned::Partition`. The framework already finds each row's group,
   keeps the rows of each group, and adds and removes result rows as groups appear and
   empty; the operator turns that into constituent tables. `agg_by` with a partition
   aggregation is the same operator.
4. **A constituent is a view of the parent**: a `QueryTable` with the parent's column sources
   (without the key columns when they are dropped) and the group's rows as its row set, in
   the parent's key space. No value is copied. The partition operator notifies it during
   the cycle with the part of the parent's change that concerns it.
5. **`merge` is a union whose parts can come and go**: `ops::partitioned::merge` listens to
   the underlying table and to every constituent it currently shows.
6. **Clients get it as Java's object type `PartitionedTable`**: `ListFields` names the type,
   `ObjectService/FetchObject` and `MessageStream` answer with Java's
   `PartitionedTableDescriptor` and one exported reference, the underlying table, and the
   three calls of `PartitionedTableService` do the rest. A client is sent the constituent
   column as Java sends it: strings, `QueryTable` in every cell.
7. **The Python `PartitionedTable` wraps a handle** to the engine's `PartitionedTable`, as
   `Table` wraps a handle to a table. Its operations are native functions that run the same
   code as the gRPC handlers.

## The column type

`ColumnType::Table(TableClass)` is the type of a column whose cells are tables.
`TableClass` is the Java class the column's definition names, which clients are told and
`meta_table` shows: `QueryTable` for the column the partition aggregation makes
(`io.deephaven.engine.table.impl.QueryTable`), `Table` for the column a transform makes
(`io.deephaven.engine.table.Table`). Both were observed; they differ in nothing else.

Arrow has no array of references, so a cell is a **handle**: an `Int64` that
`ops::partitioned::resolve` turns back into the table. A null cell is Java's `null`. The
handles are numbers from a process-wide counter and mean nothing to a client, which never
sees them.

- **Who owns the tables.** The column source that the partition operator (or a transform)
  makes owns its constituents strongly, and takes their handles out of the registry when it
  is dropped. The registry itself refers to them weakly. Operators that pass a column on
  share or wrap the source, or hold the table that has it, so the handles of their cells
  stay good.
- **A copied cell keeps its table.** An operation that copies values into columns of its
  own (`select`, `snapshot`, a static `update`) copies handles, and the column source it
  stores them in (`FlatSource`, `WritableSource`) keeps the tables they name
  (`oxhaven_column::tables`), as Java's object sources refer to their tables. So the
  constituent of a copied cell can be read after the partitioned table is gone; as in
  Java, nothing updates it any more. A `WritableSource` keeps the tables of the handles it
  stores until it compacts its storage, so a table can outlive the cell a little, never
  the other way. The source that makes the tables (the partition, a transform) keeps them
  itself and decides when to give them up.
- **Keys.** Two cells are equal when they are the same table, which is when their handles
  are equal, so a column of tables is a group key and a join key with the code for
  `Int64`. A sort by it is refused, as Java refuses it; what else is refused is in
  "Refused for now".
- **Why not less.** Without a column type the underlying table could not be a `Table` of
  this engine at all: `pt.table`, `filter` and `sort` of a partitioned table, and the key
  subscription of the web client all are operations on it, and its schema names the column.
- **Why not more.** A method on `ColumnSource` that returns tables would have to be
  forwarded by every wrapping source in the engine; a cell that holds the `Arc` itself is
  not an Arrow array.

This is a contract change: `crates/oxhaven-contracts/src/schema.rs`, one variant and one
enum. `ColumnType` is `#[non_exhaustive]`, so no other crate had to change to compile.

## Constituents as groups change

The underlying table has one row per key that has rows, at row key = the group's slot, in
the order keys were first seen, then the constituent column. This is the aggregation
framework's result and is not new.

Per cycle, in Java's order (`PartitionByChunkedOperator#propagateUpdates`):

| The group | Its constituent | The underlying table |
| --- | --- | --- |
| is new | is made with the group's rows; it does not notify on this cycle | row added |
| lost all its rows | is told that its rows were removed, and stays empty | row removed |
| has rows again | **the same table** is told that rows were added | row added, at its old place |
| changed otherwise | is told what was added, removed, modified and shifted | nothing: a change inside a constituent is not a change of its cell |

- A row whose key changed leaves one constituent as a removal and joins the other as an
  addition. A modified row that stays is a modification only when a column other than a
  key column changed; the modified columns are the parent's, less the keys.
- Shifts are the parent's, cut down to the rows of the constituent.
- When the parent fails, every constituent in the result fails.
- The work is by group, never by row: the rows of a cycle are sorted into groups once, and
  each touched group costs a few row set operations.

**Ownership and `satisfied`.** The result table owns the operator, the operator's column
owns the constituents, and a constituent refers to the result weakly: it is satisfied for a
step once it has notified or the result is. A constituent handed out
(`constituent_tables`, `get_constituent`, `GetTable`) is wrapped so that it keeps the
partitioned table alive; otherwise a client that keeps only the constituent would see it
stop. `QueryTable` gets one constructor for this, `QueryTable::dependent`: a table that
another operator notifies.

## Transforms

`PartitionedTable::transform` and `PartitionedTable::partitioned_transform`
(`ops/partitioned/transform.rs`), observed on Java 42.9 and kept as the script scenarios
`partitioned_transform*`.

**The result** has the rows and the other columns of the table it was made from; only the
constituent column is its own, typed `Table`. The definition of its constituents is what
the function answers for a constituent without rows, which is the last call when the
result is made. It keeps `unique_keys`, and whether constituents can change.

**When the function is called.**

| What happens to a row of the underlying table | The function |
| --- | --- |
| it is there when the result is made | is called with its constituent, in row order |
| it is added in a cycle: a new key, or a key that has rows again | is called in the cycle, with the constituent; for a key that comes back that is the table it was called with before, with its rows of now |
| it is removed | is not called. What it had made for the row is given up one change later, so that a listener can still read what the row held |
| its constituent cell is modified | is called again |
| anything else is modified, or the rows shift | is not called |
| rows change inside a constituent | is not called: the change reaches what the function made of the constituent, as any table operation follows its parent |

One operator does Java's `update` with a `TableTransformationColumn` and the
`ValidateConstituents` that Java's constructor adds. In a cycle the function runs when the
underlying table has finished, so every constituent it can be given has: a table made of a
constituent in the cycle starts from the constituent's rows of now and misses nothing.

**`dependencies`** are the refreshing tables (for a partitioned table, its underlying
table) that the function reads besides the constituent. The function is not called in a
cycle before each has finished. With at least one, the transform of a partitioned table
that is not refreshing is refreshing, nothing ever changes its rows, and its constituents
may be refreshing; without one a refreshing answer is refused, with Java's text. A
dependency that is not refreshing, or is not a table, counts for nothing, as in Java's
Python package.

**`partitioned_transform`** is, as in Java, the natural join of the two underlying tables
on the key columns (by name when both have the same names, by position otherwise; the
types must match), less the rows that found no constituent, then the transform with a
function of two tables. So the result has the rows of the first table whose key the
second has, in the first one's order, and a pair appears and goes as either side gets or
loses the key. Constituents can change when they can in either. That is for a partitioned
table with unique keys; for one without (`from_partitioned_table` without arguments,
`from_constituent_tables`), as in Java, it is the `join` of the two: each row with every
row of the second that has its key, in the second's order, and with no key columns every
pair. A first partitioned table with unique keys and a second without fails in the
natural join, with Java's "Natural Join found duplicate right key for ...".

**What the function may not answer**, when the result is made (the transform fails) and
in a cycle (the result fails: its underlying table and every merge of it):

| The function | Java |
| --- | --- |
| raises | the `update` fails |
| answers `None` for a constituent | `IllegalStateException`: "Encountered null constituent" |
| answers `None` for the table without rows | `NullPointerException` (only when the result is made) |
| answers something that is not a table | `ClassCastException` |
| answers a table with other columns | `IncompatibleTableDefinitionException` |
| answers a refreshing table, in a result that is not refreshing | `IllegalStateException`: "Static partitioned tables cannot contain refreshing constituents..." |
| makes a formula or a filter | no query scope, in a cycle as when the result is made |

The partitioned table that was transformed, and other transforms of it, go on.

**Python in the cycle.** The function is called on the thread that runs the cycle, which
takes the interpreter lock for the call; a table operation in the function gives the lock
up while the engine works, as everywhere. This is the lock order of `docs/PYTHON_UDF.md`.
The tests `console::python::partitioned::tests` run cycles that call a function beside
commands and a Python thread that transform, and fail when the native function keeps the
interpreter lock.

**The merge of a transform** of a partitioned table that is not refreshing, made
refreshing by a dependency, has the attributes its constituents share, as in Java
(`PartitionedTableImpl#computeSharedAttributes`): a merge of `head`s of static tables is
append-only.

## Made from tables

`PartitionedTable::of`, `of_table` and `of_tables` (`ops/partitioned/factory.rs`), Python's
`PartitionedTable.from_partitioned_table` (with every argument, or with none) and
`from_constituent_tables`, as Java's `PartitionedTableFactory`. Every case was observed
on Java 42.9; the table of `tasks/7.6.3-partitioned-factories.md` holds them.

- **With every argument**, the key columns and the constituent column must be columns of
  the table, and the constituent column one of tables. Constituents can change only when
  the table refreshes, whatever the caller says.
- **With none**, the key columns are the columns that do not hold tables, the keys are not
  unique, the one column of tables is the constituent column (none or two are refused),
  the constituents have the columns of the first one, and they can change when the table
  refreshes. A first cell that is null counts as no constituent.
- **From tables**, the underlying table has one column `__CONSTITUENT__` of type `Table`
  and a flat row per table that is not `None`, no key columns, constituents that cannot
  change, and the columns of the definition given or of the first table. When one of the
  tables refreshes, the underlying table is refreshing too, as Java's `addParentReference`
  makes it, though its rows never change.
- **The constituents are checked** when it is made and, for a refreshing table, in every
  cycle for the rows added and modified, as Java's `ValidateConstituents` does: none is
  null, none refreshes in a static table, each has the columns of the definition (Java's
  `checkMutualCompatibility` and its text). A check that fails in a cycle fails the
  underlying table. Java checks only the first 2048 rows it is given (one chunk); so does
  this, for the owner's ruling.
- `get_constituent([])` of a partitioned table without key columns matches every row, as
  Java's filter by no key does, and fails when there are two.

## `merge`

The result shows the constituents in the order of the underlying table's rows, each with its
rows in its own order, as `UnionSourceManager` does for a partitioned table. Each
constituent gets a range of the result's key space as in the plain `merge`
(`ops/merge/union.rs`); when a constituent appears, disappears or outgrows its range, the
ranges after it move and their rows shift.

When neither the underlying table nor a constituent is refreshing, it is the plain `merge`
of the constituents. Otherwise the result has a listener of its own kind, with one recorder
on the underlying table and one per constituent, which runs once per step after all of
them are satisfied; recorders are added and dropped as rows of the underlying table come
and go.

## The service and the object type

- `PartitionBy`: the table, the key column names (none is allowed: one constituent),
  `drop_keys`. The result export holds the `PartitionedTable` as an object.
- `Merge`: an export of the merged table.
- `GetTable`: Java's two modes. With one key row and one match, the constituent itself;
  with `PERMIT_MULTIPLE_KEYS`, the merge of the constituents of the keys that match, which
  follows the key table. Both are `where_in` on the underlying table, as in Java.
- `ObjectService`: `FetchObject`, and `MessageStream` for a fetch-only type: the first
  message of a stream is answered with the object, and the stream then ends, as Java's
  does. `PartitionedTable` is the built-in object type; the service, the registry of types
  and the types of plugins are task 7.10's (`docs/OBJECTS.md`).
- `Aggregate` with a partition aggregation makes the column of tables in an ordinary table.
- The statuses and messages of the cases probed are Java's
  (`crates/oxhaven-server/src/partitioned/tests.rs`).

## Python

`deephaven.table.PartitionedTable` with `table`, `key_columns`, `unique_keys`,
`constituent_column`, `constituent_table_columns`, `constituent_changes_permitted`,
`is_refreshing`, `update_graph`, `keys`, `constituent_tables`, `get_constituent`, `merge`,
`filter`, `sort`, `transform`, `from_partitioned_table`, `from_constituent_tables`, and
`Table.partition_by`, `agg.partition` and
`Table.partitioned_agg_by`.

**`transform` and `partitioned_transform`.** See "Transforms" below. The function is called
for each constituent when the result is made, and inside the update cycle for each
constituent that appears.

**No query scope inside the function.** Java runs the function in the partitioned table's
execution context, which has no query scope, so everything that compiles a formula or a
filter fails there with "No ExecutionContext registered, or current ExecutionContext has
no QueryScope": `update`, `view`, `where` with a string, `where_one_of`, `head_by`,
`tail_by`, `agg.count_where`, `agg.formula`. Renames and column copies, `head`, `tail`,
`sort`, joins and the other aggregations work. All 56 operations probed fail or work here
as they do in Java: while the function runs, a flag on the thread makes the formula
compiler (`ScopedCompiler`) and the Python methods that Java checks earlier refuse with
Java's text. A function that has to compile a formula enters an execution context in Java
(`deephaven.execution_context`), which this package does not have yet (task 6.12).

**`partitioned_agg_by`** is `agg_by` with a partition aggregation added, then the
partitioned table of that result (`PartitionedTable::of_aggregation`), with
`preserve_empty` and `initial_groups` as `agg_by` takes them.

**The proxy** (`PartitionedTableProxy`, from `PartitionedTable.proxy`) is a transform per
table operation, with key checks for joins. See "The proxy" below.

## The proxy

`PartitionedTable.proxy(require_matching_keys=True, sanity_check_joins=True)` answers a
`PartitionedTableProxy` with `target`, the two flags, `is_refreshing`, `update_graph` (the
target's, refused as in the table below) and the 37 table operations of Java's Python
class: `head`, `tail`, `reverse`, `snapshot`, `snapshot_when`, `sort`, `sort_descending`,
`where`, `where_in`, `where_not_in`, `view`, `update_view`, `update`, `select`,
`select_distinct`, `natural_join`, `exact_join`, `join`, `aj`, `raj`, `group_by`, `agg_by`,
`agg_all_by`, `count_by`, `first_by`, `last_by`, `min_by`, `max_by`, `sum_by`,
`abs_sum_by`, `weighted_sum_by`, `avg_by`, `weighted_avg_by`, `median_by`, `std_by`,
`var_by` and `update_by`. Each answers a proxy with the flags of the one it was called on.
Everything below was observed on Java 42.9 before it was written; the scenarios
`partitioned_proxy_*` hold it.

**An operation is a transform.** With no other table, or with a table, it is `transform`
of the target (`PartitionedTableProxyImpl#basicTransform`); with another proxy it is
`partitioned_transform` of the two targets (`#complexTransform`). The result has the key
columns of the target, unique keys, and the constituent column `__CONSTITUENT__`. A
refreshing table that the operation reads besides (the other table of a join, a filter
table, a trigger) is a dependency of the transform: with one, the result of a partitioned
table that does not refresh is refreshing, its constituents may not change, and the
constituents themselves refresh.

**How it runs here.** The Python method runs the `Table` method once, on a table without
rows that has the columns of a constituent (two such tables for two proxies), and records
the operations that the method sends to the engine (`deephaven/_engine.py`, `Recording`).
The function of the transform is those operations run again on a constituent
(`console/python/proxy.rs`): Rust, with no Python in the update cycle beyond what a
formula calls. So the proxy does what `Table` does, operation by operation, and a method
of `Table` that changes needs no second implementation.

**The variables of a formula are those of the call.** Java's proxy runs in an execution
context that has a query scope, so `where`, `update` and the others with formulas work,
unlike in the function of `transform`. The variables are bound when the proxy operation
is called: a constituent that appears in a later cycle is given the values of then, also
of a local of a function that has returned, and a global that was assigned since has its
old value there. Here the recording keeps a copy of the caller's variables
(`_oxhaven_bootstrap.scope_snapshot`), and every run compiles its formulas with it.

**Consistent key columns** (`PartitionedTableImpl#getConsistentKeyColumnNames`) are the key
columns that every constituent is known to hold, with the key of its row in every row.
`partition_by` without `drop_keys` and `partitioned_agg_by` make them; `filter` keeps
them; `sort` of a partitioned table, `transform` and `partitioned_transform` leave none.
A proxy operation keeps these:

| Operation | Keeps |
| --- | --- |
| `head`, `tail`, `reverse`, `snapshot`, `snapshot_when`, `sort`, `sort_descending`, `where`, `where_in`, `where_not_in`, the joins, `select_distinct()` | all |
| `view`, `select`, `select_distinct` with columns | those named as they are; `"K = K"` is not one |
| `update`, `update_view` | those no formula assigns |
| `update_by` | those no operator writes |
| the aggregations, also `group_by` and `count_by`, also when they group by the key | none |

`view([])` and `update_view([])` keep all.

**A filter of key columns is a filter of the underlying table.** `where` applies each
filter that reads nothing but consistent key columns to the underlying table, which
leaves fewer constituents, and the others to every constituent
(`PartitionedTableProxyImpl#where`, `#isValidAgainstKeyColumns`). A list is split filter by
filter; `"K = 1 && V > 0"` is one filter, of the constituents. A filter that reads no
column (`"true"`, `"1 > 2"`) goes to the underlying table. `where_in` and `where_not_in`
whose columns are all consistent key columns filter the underlying table as well. Without
consistent key columns a filter of a key column is one of the constituents, which fails
where they have no such column (`drop_keys`).

**The checks of two proxies.** Only the flags of the proxy the method is called on are
read. The key columns of the two are matched by position, as `partitioned_transform`
matches them.

- `require_matching_keys`: the two have the same keys (`#matchingKeysValidation`). The
  call fails if not. In a cycle after which they have not, the result fails: its
  underlying table and its merge. The two partitioned tables go on. Without the flag the
  result has the keys that both have.
- `sanity_check_joins`, for `natural_join`, `exact_join`, `join`, `aj` and `raj`: no value
  of the columns that the join matches exactly is in two constituents, in each of the two
  (`#nonOverlappingJoinKeysValidation`). It fails the call, or in a cycle the result, in
  the same way. A row that moves to another constituent in one cycle takes its value with
  it and does not fail the check.

Both are tables that follow the underlying tables (`ops/partitioned/validate.rs`): the
underlying table of each side is replaced by a copy that fails in the cycle in which the
check does, so what is made of it fails.

**What Java does that looks like a defect, done here in the same way.** None of these is
decided; they are listed in `tasks/7.6.2-partitioned-proxy.md` for the owner's ruling.

- `sort` of the Python class does not make a sequence of one name: a string is read as
  the sequence of its characters. `sort("V")` sorts by `V`, `sort("VV")` by `V` twice,
  and `sort("Nope")` fails for the column `N`. `sort_descending` takes a string as one
  name.
- `select()`, `select(None)` and `update([])` fail with an `IndexError` inside `DHError`;
  `view([])` and `update_view([])` work.
- `weighted_avg_by(wcol)` without group columns is `avg_by(wcol)`: the average of every
  other column for each value of the weight column.
- The message of a failure names another method for `abs_sum_by` and `weighted_sum_by`
  ("sum_by operation"), `weighted_avg_by` ("avg_by operation"), `group_by` ("group-by
  operation"), `update_by` ("update-by operation"), `aj` ("as-of join operation") and
  `raj` ("reverse as-of join operation").
- A filter that reads a column as an array (`"V_[i - 1] > 0"`, `"K_[i] = 1"`) fails
  through a proxy with "No ExecutionContext registered, or current ExecutionContext has
  no QueryScope"; the same filter of a table works.
- A filter that reads key columns and `i`, `ii` or `k` (`"K > i"`) is applied to the
  underlying table, where the row number is that of the constituent's row in it.
- `sanity_check_joins` with one match column fails with `NullPointerException` when a
  constituent has a null in it; with two or more match columns nulls pass.
- `sanity_check_joins` of a join without exact match columns (`join` with no `on`, `aj`
  and `raj` on the as-of column alone) fails whenever two constituents have rows.
- `natural_join` and the others with a partitioned table that is not a proxy fail with
  "Unexpected TableOperations input null"; here the text names what it was given.

## Refused for now

Each names the Java method in its error.

| What | Java | Task |
| --- | --- | --- |
| A transform function that compiles a formula inside an execution context it enters | `deephaven.execution_context` | 6.12 |
| `update_graph` | `Table#getUpdateGraph` | 6.12 |
| `partitioned_agg_by` and `agg_by` of a refreshing table with `initial_groups` it has no rows for, without `preserve_empty` | `ChunkedOperatorAggregationHelper#copyKeyColumns` | 4.18, needs a ruling |
| `get_constituent` with a key value of a class this package has no stand-in for (a `java.time` value for a column of numbers) | `PartitionedTableImpl#constituentFor` | none |
| A match filter on a column of tables with a query scope variable (`C == t`) | `MatchFilter.ColumnTypeConvertor#convertValue` | 6.12 |
| In a formula: a method of a table (`C.size()`), its text (`C + "x"`), a cast (`(Object) C`) | `BaseTable#toString`, the method's class | none |
| `min`, `max`, `unique`, `distinct`, `count_distinct` of a column of tables: Java fails as well, with another text | `AggregationProcessor` | none |
| `OpenMessageStream`, `NextMessageStream` | `ObjectServiceGrpcImpl#openMessageStream`, `#nextMessageStream` | 7.6.5 |

Java refuses as well, and the text here is Java's: a second partition aggregation in one
`agg_by`, `partition_by` of a blink table, a sort or a filter of a partitioned table by its
constituent column, and `head_by` or `tail_by` of a table with a column of tables that is
not grouped by (`ViewColumnLayer#checkResultType`: Java casts each such column in an
`updateView`, which does not make columns of tables). Java refuses a sort of a table by a
column of tables with `NotSortableColumnException`; here it is refused as not ported,
naming `SortOperation#SortOperation`.

A column of tables works as a group key (`count_by`, `agg_by`, `head_by` by it) and as the
column a `natural_join` adds, as in Java; both are in `partitioned_static`.

## Columns of tables in filters, aggregations and formulas

Task 7.6.4 (`tasks/7.6.4-columns-of-tables.md`, whose "Probes" hold every case run on
Java 42.9).

- **`get_constituent`** filters the underlying table by one match filter per key column,
  given the Python values as jpy gives them to Java (`bool` a `Boolean`, `int` the narrowest
  of `Byte` to `Long`, `float` a `Double`, `str` a `String`): a column of numbers takes any
  number through Java's narrowing (`1.9` finds `1`, `257` finds the byte `1`), a NaN finds
  nothing, a column of `char` takes no value but `None`, any other column compares with
  `Objects.equals`, so a value of another class finds nothing. Anything else fails with
  Java's `ClassCastException`, but only when rows are left for that filter: the filters
  run in order and stop at the first that leaves no row. A Python value jpy cannot convert
  fails as jpy does. All of it is in `deephaven/table.py` (`_match_value`).
- **Match filters** on a column of tables hold only `null` (`C == null`, `C != null`,
  `in null`, `icase in null`), as Java's convertor of a class without one of its own;
  another literal fails to convert and the filter becomes its formula.
- **`where_in` and `where_not_in`** compare tables by identity, that is by handle. A column
  of `Table` against one of `QueryTable` is refused, as Java refuses it.
- **Aggregations** `first`, `last`, `sorted_first` and `sorted_last` copy the cell; the
  result's source keeps the table (`oxhaven_column::tables`). `group_by` makes a vector of
  tables as before.
- **Formulas** hold a cell as an object: `==` and `!=` compare by identity (a table equals
  no value of another class, and `null` equals `null`), `isNull` tests it, a ternary and
  `D = C` pass it on, and `update` and `select` store it. `view` and `update_view` refuse a
  formula that makes tables, with Java's text (`ViewColumnLayer#checkResultType`).

## Differences from Java

- Row keys of a merged table (PARITY A4).
- A constituent that a client holds keeps the partitioned table alive here; in Java it is
  the other way round as well. Not observable.
- The text of a constituent cell is `QueryTable` on both.
- The bytes of the schema inside the `PartitionedTableDescriptor` are laid out differently
  by the two flatbuffer writers; the schema they decode to is the same.
- Columns of tables (task 7.6.4), none of these decided:
  - Java answers the same partitioned table for `partition_by` of the same table and
    columns twice, so `where_in` of the cells of one with the other matches; here the two
    calls make other tables, and nothing matches.
  - Java refuses `view` and `update_view` of a column of tables named on its own (`view("C")`)
    when the column's source is one it does not preserve (that of a `natural_join`, or a
    column an `update` passed on); here such a column is passed on. Which sources Java
    preserves was not ported.
  - A table in a `ClassCastException` text is `QueryTable` here; Java names its class
    (`InMemoryTable` for `new_table`).
  - A refused formula fails here with "unsupported in formulas"; Java computes it
    (method calls, text, casts) or fails with a compilation error (`C > null`).
- The texts of the factories name every table `QueryTable`; Java names some tables
  otherwise (`FilteredTable(null)` for a `where`).
- When a transform is made, Java may call the function for several constituents at once,
  on threads of its own; here the calls are one after the other in row order. In a cycle
  both call on the thread of the cycle.
- A transform that failed in a cycle (`PARITY.md` A11, ruled on 2026-11-17: what a failed
  table serves afterwards is not compared, and this behaviour stays): Java's underlying table
  keeps following the rows of the table it was made from, with a null cell for each row
  it had no table for, `merge` goes on answering the failed merge, and `constituent_tables`
  fails with `LivenessStateException`. Here the underlying table keeps the rows it had, a new
  `merge` says that the table has failed, and `constituent_tables` answers the tables it
  held. On both, `get_constituent`, `keys` and another transform fail. The scenario
  `partitioned_transform_ticking_failures` compares that the tables fail, and nothing of
  them afterwards.
- A function that reads a refreshing table it did not name as a dependency is not safe in
  Java, which may call it before that table has finished. Here it is called in the same
  way, and what it makes then starts from that table's rows of the moment and follows it.
- The proxy (task 7.6.2, none of these decided):
  - Java calls the operation for several constituents at once when it makes the result,
    so which constituent's failure is reported (the key in "no mappings for key" of an
    `exact_join`) is not fixed there; here it is the first in row order.
  - Java was seen to hang for good in a proxy aggregation of a refreshing partitioned
    table (`agg_by` with `agg.formula`, which is slow to compile) when an update cycle fell
    due during the call: the caller holds the update graph's lock and the threads that
    make the constituents wait in `QueryTable#aggBy`. Here the operation is made under
    the lock on the calling thread and does not wait for a cycle.
  - Java's join key check keeps the owner of each key and takes the claims of a cycle as
    the constituents report them. Read from
    `PartitionedTableProxyImpl#validateConstituentJoinKeys`, and not observed: a key that
    moves to a constituent that appears in the same cycle can fail the check depending on
    the order of the reports. Here the check reads the keys of all constituents at the
    end of the cycle, which is what Java was observed to do for a move between
    constituents that exist.
  - A result that failed in a cycle is as a transform that failed (`PARITY.md` A11, above):
    Java's `merge` of it answers a table and `constituent_tables` fails; here `merge`
    fails and `constituent_tables` answers.
  - Two proxies of the same partitioned table and flags are equal on both. In Java
    `partition_by` of the same table and columns answers the same partitioned table
    twice, so the proxies of two such calls are equal; here two calls make two
    partitioned tables, and their proxies are not.
  - The text of an error after "... operation on the PartitionedTableProxy failed." is
    the engine's own here, not Java's exception.

## Outside the task's paths

- `crates/oxhaven-contracts/src/schema.rs`: the column type.
- `crates/oxhaven-engine/src/ops/agg/`: the hooks the partition operator needs (a result
  row's group, the result table once it is made). `ops/merge/`: the ranges of the union,
  shared with the merge of a partitioned table. `ops/head_by/`: the refusal above.
  `table/query_table.rs`: `QueryTable::dependent`.
- Task 7.6.1: `ops/merge/`: the attributes that the tables of a merge share, now also
  read by the merge of a partitioned table. `deephaven/table.py` and
  `console/python/mod.rs`: `Table.is_failed`, which the failure scenario asserts with.
- `crates/oxhaven-server/src/object/` (task 7.10's directory): the object service with the
  one built-in type. `schema.rs`, `wire_columns.rs`: the Java type names and the strings a
  client is sent. `table/ops/aggregate.rs`: the partition aggregation.
  `console/session.rs`, `app/scope.rs`, `console/python/`: the type name of a variable, the
  missing query scope, the native functions.
- `oracle/scenarios/python/partitioned_*.py`, their recordings and their baseline lines;
  `agg_by` in the baseline is narrowed to the one table that still differs.
- Task 7.6.2: `deephaven/_engine.py` (`Recording`; `table_op` and `local_op` record
  while one is entered), `_oxhaven_bootstrap.py` (`scope_snapshot`),
  `console/python/ops.rs` (`local_operation`, a local operation by its name),
  `console/python/mod.rs` (the module `proxy`).
- Task 7.6.3: `crates/oxhaven-column` (`tables.rs`, the registry of handles, moved from
  `ops/partitioned/cells.rs`; `FlatSource` and `WritableSource` keep the tables of the
  handles they store).
- Task 7.6.4: `ops/filter/` (the convertor and cells of a column of tables, a range filter
  refused for it), `ops/distinct/where_in.rs` (the key class), `ops/agg/` (first, last and
  sorted copy the cell), `ops/select/mod.rs` (the refusal of `view`), `crates/oxhaven-expr`
  (a cell of tables as an object), `deephaven/table.py` (`get_constituent`).

## Gate

`cargo test -p oxhaven-engine partitioned`, `cargo test -p oxhaven-server partitioned`
(with and without `--features python`), and the script scenarios `partitioned_*` and
`agg_by` in the replay gate. Task 7.6.1 adds the full seed budget for the engine tests
(`OXHAVEN_TEST_SEEDS=full`) and the scenarios `partitioned_transform_ticking*` and
`partitioned_transform_pair*`. Task 7.6.2 adds
`cargo test -p oxhaven-server --features python console::python::proxy` and the scenarios
`partitioned_proxy_*`, one for each family of operations and one for refreshing tables.
Task 7.6.3 adds the engine tests `partitioned::factory_tests` and
`cargo test -p oxhaven-server --features python console::python::partitioned`; it adds no
scenario. Task 7.6.4 adds the engine tests `partitioned::column_tests` and the Python test
`get_constituent_converts_key_values_as_java_does`; it adds no scenario either (`oracle/`
is not its to change).
