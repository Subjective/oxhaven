# Hierarchical tables

How Oxhaven holds a rollup, answers a client that looks at part of it, and what a tree
will reuse. Written by task 7.8 (`tasks/7.8-rollup.md`), which also built the first slice;
the follow-on tasks, 7.8.1 to 7.8.5, are sized in that file, and `tasks/7.9-tree.md` says
what this means for a tree.

What Java does was read in `RollupTableImpl`, `HierarchicalTableImpl`,
`AggregationProcessor` (the two rollup converters), `HierarchicalTableServiceGrpcImpl`,
`HierarchicalTableViewSubscription` and `HierarchicalTableSchemaUtil`, and observed on Java
42.9: the IDE's calls with the tracing of `oracle/ide`
(`oracle/ide/traces/http-console-rollup.txt`), the views with the script scenarios
`oracle/scenarios/python/rollup_*.py`, the statuses with the same calls made to both
servers.

## Decision

1. **A rollup is one aggregation per level of the key hierarchy, plus who is whose
   child.** For `by = [K1 .. Kn]`, level `d` (0 to n) is the source aggregated by
   `K1 .. Kd`: level 0 is the one row of the grand total, level n the finest groups. The
   children of a row of level `d` are the rows of level `d + 1` with the same `K1 .. Kd`:
   level `d + 1` partitioned by `K1 .. Kd`, which is `partition_by` of
   `docs/PARTITIONED.md`. Nothing else is stored. `ops::hierarchical::RollupTable`.
2. **Every level aggregates the source itself.** Java aggregates the source once, by all
   keys, and makes each coarser level by aggregating the level below it with a second
   set of operators (a sum of sums, a first of firsts by carried row key, an average from
   carried sums and counts). The results are the same, floating point sums aside (PARITY
   A1), and the order of rows is the same, because both give a group its row when it is
   first seen. Aggregating the source needs no second operator set, so every aggregation
   `ops::agg` has is available to a rollup as it is; the price is n + 1 aggregations of
   every source change in place of one. Re-aggregation is a later task with a benchmark
   (7.8.5); it changes nothing a client sees.
3. **A rollup is not a table and the contracts do not change.** It is held by exports and
   variables as an object, as a partitioned table is.
4. **A client never gets the rollup; it gets a snapshot of a view.** A view is a rollup
   and a key table that says which nodes are expanded. A snapshot of rows `a..b` of a
   view is the rows `a..b` of the depth-first walk of the expanded nodes. The walk visits
   expanded nodes only and reads values only for `a..b`: a collapsed node costs nothing,
   an expanded one a few row set operations. See "The snapshot".
5. **The walk is written against a trait, `Hierarchy`,** which a rollup implements and a
   tree will: the root, the node of a key, its parent's key, the table of its children,
   its row among them, whether rows of a level can be expanded, and the columns of a
   node. Directives, linking, the walk, the view, the service, the subscription and the
   object type know nothing about rollups.
6. **Clients are served as Java serves them:** the object type `HierarchicalTable`, the
   calls of `HierarchicalTableService`, and a Barrage subscription to a view whose every
   message is a whole snapshot of the viewport.

## Which aggregations

Java allows in a rollup: `AbsSum`, `Avg`, `Count`, `CountDistinct`, `CountWhere`,
`Distinct`, `First`, `Last`, `Max`, `Min`, `SortedFirst`, `SortedLast`, `Std`, `Sum`,
`Unique`, `Var`, `WAvg`, `WSum`, `Group` and the formula aggregation `AggFormula`. It
refuses with `Agg<Name> is not supported for rollup()`: `FirstRowKey`, `LastRowKey`,
`ApproximatePercentile`, `Freeze`, the per-column `Formula`, `Median`, `Percentile`,
`TDigest`; a partition aggregation is refused too (`AggregationProcessor`'s
`UnsupportedRollupAggregations` and `RollupBaseConverter`). The refusals are Java's here,
with Java's text. Of the allowed ones the first slice has all but `CountWhere`, `Group`
and `AggFormula`, which are refused as not ported: a group's vectors and the formula's
`__FORMULA_DEPTH__` and `__FORMULA_KEYS__` need work of their own (7.8.4).

The columns of a level are the group-by columns, then the aggregation results. A level
shows null in the group-by columns it does not group by, in the column's type.

Three things follow from how Java makes the coarser levels, and are kept:

- A result named like a group-by column, and a group-by column named twice, are refused
  with `Duplicate output columns found: <name> used <n> times`: the coarser level has the
  null column of the group-by column and the result under one name.
- A rollup without aggregations is made, and its levels have the group-by columns alone.
  `ops::agg` has no aggregation without aggregations, so the levels count their rows in a
  column that is dropped.
- `SortedFirst` and `SortedLast` sort a coarser level by the columns of the level below
  it, not by the source's. With group-by columns Java therefore fails with a
  `NoSuchColumnException` unless every sort column is a result of the same aggregation
  under its own name, and then sorts the results. Sorting the source is the same when the
  sort columns are such results, and that case is made here; with a sort column that is
  not, the rollup is refused as not ported
  (`AggregationProcessor.RollupReaggregatedConverter#reaggregateSortedFirstOrLastOperator`)
  until it is ruled whether Java's failure is to be reproduced (7.8.4). Without group-by
  columns there is one level and nothing to sort again.

## Constituents

With `include_constituents` the rows of the finest groups can be expanded into the source
rows of the group, with the source's columns: one more level whose children are the
partition of the source by all keys, and one more set of columns in the snapshot schema,
null for aggregated rows as the aggregated columns are null for constituent rows. The
`Hierarchy` trait has what this needs (a node's columns depend on its level). Not in the
first slice (7.8.3).

## Node keys and the key table

Java's node key is the depth and the key values: depth 0 is the root (its one child is
the grand total row), depth 1 the grand total (key width 0), depth `d` a row of level
`d - 1` with `d - 1` key values. A key table has an int column `__DEPTH__` and every
group-by column, in the type the rollup has it (`RollupTableImpl#makeNodeKeySource`; a
table without one of them is refused when the view is first read, not when it is made);
a row's values beyond its width are ignored. An optional byte column
names the action: 1 expand, 3 expand with all descendants, 4 contract, `0x70 + n - 1`
expand `n` levels. Without a key table, and before its rows, the root and the grand
total are expanded (`RollupTableImpl#getDefaultExpansionNodeKeys`).

The rows are read in order; a later row for a key replaces the earlier one and takes its
place at the end (`HierarchicalTableImpl#accumulateKeyTableDirective`). They are then
linked into a tree of directives, each key under its parent's, parents that have no row
as linkage; a key whose node does not exist now is dropped
(`#linkKeyTableNodeDirectives`).

## The snapshot

`View::snapshot` (`ops/hierarchical/walk.rs`) is `HierarchicalTableImpl#snapshot`:

- Walk from the root. At a node, the rows are the node's child table in its order. The
  child directives that expand, sorted by their row in that table, cut the rows into
  runs: the rows up to and including an expanded child, then that child's subtree, then
  on. Under "expand all" every row that is not contracted is an expanded child.
- A run is counted, or skipped while the viewport has not begun, or its part inside the
  viewport is read: per requested column, one bulk read of those rows from the level's
  column source. `__DEPTH__` is the level; `__EXPANDED__` is true for the last row of a
  run that is followed by its subtree, false for the others, null where the level cannot
  be expanded.
- The answer is the total number of rows of the walk and the column data of the
  viewport. The total is needed for the scroll bar; this is why expanded nodes are
  visited beyond the viewport, as in Java. Their rows are not read.

**Finding a node.** The walk goes by directive, not by row, so it needs the row of a
key in its level and the child table of a key. Both are a `KeyIndex`: a hash map from
the encoded key to the row key in a table (a level, or the underlying table of a level's
partition), filled lazily at snapshot time for the rows not indexed yet, in bulk. A slot
of an aggregation is the key's for good, so an entry never goes stale; whether the row
is there now is one row set lookup. The update cycle does none of this work.

**Consistency.** A snapshot runs under the shared lock of the update graph, so no cycle
runs during it and current values are read. Java reads without the lock, using previous
values when a cycle is running, and retries. The result is the same.

## The service

`HierarchicalTableService`, observed and read:

- `Rollup`: source ticket, aggregations (the messages of `AggregateRequest`),
  `include_constituents`, group-by column names; the result export holds the rollup.
- `View`: the rollup, or an existing view to take its rollup from, and optionally a key
  table with the name of its action column. The result export holds the view. The IDE
  makes a new view for every change of expansion: it uploads a key table with `DoPut`
  (the group-by columns, `__DEPTH__`, `__action__`), calls `View`, subscribes, and
  releases the old view and key table.
- `ExportSource`: an export of the source table.
- `Apply`: filters, sorts, and format and update views per node type, giving a new
  rollup. Java applies them in the order update views, formats, filters, sorts. A filter
  may use only group-by columns and filters the finest level, from which the coarser
  ones are aggregated again (`RollupTableImpl#withFilter`); here that is a `where` on
  the source in front of every level, which is the same rows. A sort is applied to each
  node's child table when it is first walked and kept with the view; sorts of aggregated
  nodes are translated for constituent nodes. Formats and update views add columns to a
  node's table. Refused in the first slice (7.8.1, 7.8.2); the checks before the first
  operation are Java's (the fields, the input, `No operations specified`).
- `Tree`: task 7.9.

**The object type.** `ListFields` and the console name the type `HierarchicalTable`.
`FetchObject` and `MessageStream` answer with `HierarchicalTableDescriptor`: the snapshot
schema (`__DEPTH__` int, `__EXPANDED__` Boolean, then the node columns, each with the
`hierarchicalTable.*` and `rollupTable.*` metadata of `HierarchicalTableSchemaUtil`) and
`is_static`. There are no exported references.

**The subscription.** `DoExchange` with a `BarrageSubscriptionRequest` whose ticket is a
view: columns, a viewport, `min_update_interval_ms`. The viewport must be one range and
not reversed. The server sends the schema, then a snapshot message whenever the viewport
or columns change, and, at most once per interval, when the source or the key table
changed: `is_snapshot`, `table_size` = the rows of the whole walk, the effective viewport
and columns, and the column data of the rows of the viewport that exist. The requested
columns must include `__DEPTH__` and `__EXPANDED__`. There are no deltas. A
`BarrageSnapshotRequest` on a view is refused, as in Java. `ViewSubscription` is the
port of `HierarchicalTableViewSubscription`: one snapshot at a time, the next one
scheduled at the interval after the last.

**Uploads from a browser.** The web client uploads the key table with
`BrowserFlightService/OpenDoPut` and `NextDoPut`, whatever its transport, so the first
slice has these two calls (`flight/browser.rs`, on the browser streams the console
already had). `OpenHandshake`, `NextHandshake`, `OpenDoExchange` and `NextDoExchange`
are refused as not ported; the web client makes those calls on the Flight service.

## Python

`Table.rollup(aggs, by, include_constituents)` and `deephaven.table.RollupTable` with
`aggs`, `by`, `include_constituents`. `node_operation_recorder`, `with_node_operations`,
`with_filters` and `with_update_view` follow `Apply` and are refused by name until then,
as `Table.tree` is. `RollupTable` wraps a handle, as `PartitionedTable` does; a variable
that holds one is listed as a `HierarchicalTable`.

## What a tree reuses

Everything but the `Hierarchy` implementation: key table directives and linking, the
walk, `KeyIndex`, the view and its snapshot state, `View` and `ExportSource`, the object
type and descriptor (with the `treeTable.*` metadata), the subscription, the harness's
`views` directive and the IDE route's helpers. A tree's `Hierarchy` has one node table
per parent identifier (the source partitioned by the parent column), levels that are
expandable row by row, and a node key of one value. `__EXPANDED__` is null for a row
without children; `Hierarchy::rows_expandable` answers for a whole level today (`All` or
`None`, which is all a rollup needs), and a tree adds the answer per row.

## Refused for now

Each names the Java method in its error.

| What | Java | Task |
| --- | --- | --- |
| `Apply`: sorts | `RollupNodeOperationsRecorder#sort` | 7.8.1 |
| `Apply`: filters | `RollupTableImpl#withFilter` | 7.8.1 |
| `Apply`: format and update views | `RollupNodeOperationsRecorder#formatColumns`, `#updateView` | 7.8.2 |
| `include_constituents` | `RollupTableImpl#makeRollup` (constituents) | 7.8.3 |
| `CountWhere`, `Group`, `AggFormula` in a rollup | `AggregationProcessor.RollupBaseConverter#visit` | 7.8.4 |
| `SortedFirst`, `SortedLast` by a column that is not their own result, with group-by columns | `AggregationProcessor.RollupReaggregatedConverter#reaggregateSortedFirstOrLastOperator` | 7.8.4 |
| A rollup of a blink table | `AggregationProcessor#forRollupBase` | 7.8.4 |
| `Tree`, Python `Table.tree` | `HierarchicalTableServiceGrpcImpl#tree`, `QueryTable#tree` | 7.9 |
| Python `node_operation_recorder`, `with_node_operations` | `RollupTableImpl#makeNodeOperationsRecorder`, `#withNodeOperations` | 7.8.1, 7.8.2 |
| Python `with_filters`, `with_update_view` | `RollupTableImpl#withFilter`, `#withUpdateView` | 7.8.1, 7.8.2 |
| The browser streams of `Handshake` and `DoExchange` | `BrowserFlightServiceGrpcBinding#bindService` | 2.6 |

## Where Java does otherwise

- Java reads a snapshot without the update graph's lock and retries; here it is read
  under the shared lock. A client sees the same rows.
- Java's coarser levels are aggregations of the level below; here of the source. Floating
  point sums may differ in the last place (PARITY A1). Nothing else was seen to differ in
  the recorded scenarios or in a probe of every aggregation both servers accept.
- The order of the keys of a field's metadata in the snapshot schema differs; the keys
  and values are the same.
- `SortedFirst` and `SortedLast`: see "Which aggregations". This one needs a ruling.

## Contracts

No change.

## Gate

`cargo test -p oxhaven-engine hierarchical`, `cargo test -p oxhaven-server hierarchical`
(with and without `--features python`), the script scenarios `rollup_*` in the replay
gate, and `node oracle/ide/ide-test.js --routes rollup` on Java and on Oxhaven.

The script scenarios compare views: the directive `# oracle: views <name> = <rollup>
[<key table> [<action column>]]` (`oracle/README.md`) reads the first rows of a view of
a rollup with the key table's expansions on both servers, after every step.
