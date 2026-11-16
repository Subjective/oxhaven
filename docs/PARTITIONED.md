# Partitioned tables

How Oxhaven holds a table of tables, keeps its constituents up to date, and hands it to
clients. Written by task 7.6 (`tasks/7.6-partitioned-tables.md`), which also built the first
slice. The follow-on tasks, 7.6.1 to 7.6.6, are sized in that file.

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
- **What copies cells loses the tables.** An operation that copies values into columns of
  its own (`select`, `snapshot`, a static `update`) copies handles, not ownership. When the
  table the cells came from is gone, such a handle resolves to nothing, and reading the
  constituent fails by name. Java keeps such a table reachable but no longer updates it.
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
`filter`, `sort`, `transform`, and `Table.partition_by`, `agg.partition` and
`Table.partitioned_agg_by`.

**`transform` and `partitioned_transform`.** Java calls the function once per constituent,
when the result is made and, for a partitioned table whose constituents can change, again
inside the update cycle for each constituent that appears. The first slice does the first
half: on a partitioned table that is not refreshing, the function is called for each
constituent and the result is a partitioned table of the answers, with the constituent
column typed `Table` as in Java. The result keeps the rows of the underlying table and its
other columns; only the constituent column is new
(`PartitionedTable::transformed`). The constituents' definition is what the function
answers for an empty constituent, as Java finds it. On a refreshing partitioned table
`transform` is refused. Calling Python inside a cycle is what `docs/PYTHON_UDF.md` already
does for formulas, with its lock order; what is missing is making tables inside a cycle
from a table that was made in the same cycle.

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

**The proxy** (`PartitionedTableProxy`) is a transform per table operation, with a key
check for joins. It follows `transform` and is not in the first slice.

## Refused for now

Each names the Java method in its error.

| What | Java | Task |
| --- | --- | --- |
| `transform` on a refreshing partitioned table | `PartitionedTableImpl#transform` | 7.6.1 |
| A transform function that answers `None` for a constituent | `TableTransformationColumn` | 7.6.1 |
| `partitioned_transform` | `PartitionedTableImpl#partitionedTransform` | 7.6.1 |
| `proxy` | `PartitionedTableImpl#proxy` | 7.6.2 |
| `PartitionedTable.from_partitioned_table`, `from_constituent_tables` | `PartitionedTableFactory#of`, `#ofTables` | 7.6.3 |
| `update_graph` | `Table#getUpdateGraph` | 6.12 |
| `partitioned_agg_by` and `agg_by` of a refreshing table with `initial_groups` it has no rows for, without `preserve_empty` | `ChunkedOperatorAggregationHelper#copyKeyColumns` | 4.18, needs a ruling |
| Reading a constituent whose partitioned table is gone (a copied cell) | `ObjectArraySource#get` | 7.6.3 |
| `get_constituent` with a key value that is not of the key column's type | `PartitionedTableImpl#constituentFor` | 7.6.4 |
| `where("C != null")` and other match filters on a column of tables | `MatchFilter.ColumnTypeConvertorFactory#getConvertor` | 7.6.4 |
| `where_in` on a column of tables | `DynamicWhereFilter#beginOperation` | 7.6.4 |
| A column of tables in a formula | the formula compiler: "unsupported in formulas" | 7.6.4 |
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

## Differences from Java

- Row keys of a merged table (PARITY A4).
- A constituent that a client holds keeps the partitioned table alive here; in Java it is
  the other way round as well. Not observable.
- The text of a constituent cell is `QueryTable` on both.
- The bytes of the schema inside the `PartitionedTableDescriptor` are laid out differently
  by the two flatbuffer writers; the schema they decode to is the same.
- The rows of the table above marked 7.6.3 and 7.6.4 work in Java and are refused here.

## Outside the task's paths

- `crates/oxhaven-contracts/src/schema.rs`: the column type.
- `crates/oxhaven-engine/src/ops/agg/`: the hooks the partition operator needs (a result
  row's group, the result table once it is made). `ops/merge/`: the ranges of the union,
  shared with the merge of a partitioned table. `ops/head_by/`: the refusal above.
  `table/query_table.rs`: `QueryTable::dependent`.
- `crates/oxhaven-server/src/object/` (task 7.10's directory): the object service with the
  one built-in type. `schema.rs`, `wire_columns.rs`: the Java type names and the strings a
  client is sent. `table/ops/aggregate.rs`: the partition aggregation.
  `console/session.rs`, `app/scope.rs`, `console/python/`: the type name of a variable, the
  missing query scope, the native functions.
- `oracle/scenarios/python/partitioned_*.py`, their recordings and their baseline lines;
  `agg_by` in the baseline is narrowed to the one table that still differs.

## Gate

`cargo test -p oxhaven-engine partitioned`, `cargo test -p oxhaven-server partitioned`
(with and without `--features python`), and the script scenarios `partitioned_*` and
`agg_by` in the replay gate.
