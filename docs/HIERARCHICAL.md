# Hierarchical tables

How Oxhaven holds a rollup and a tree, answers a client that looks at part of one, and
what a tree reuses. Written by task 7.8 (`tasks/7.8-rollup.md`), which also built the first slice;
the follow-on tasks, 7.8.1 to 7.8.5, are sized in that file, and `tasks/7.9-tree.md` says
what this means for a tree. Constituents are task 7.8.3
(`tasks/7.8.3-rollup-constituents.md`).

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

With `include_constituents` the rows of the finest groups can be expanded into the rows of
the source that are in the group. Read from `RollupTableImpl` and
`AggregationProcessor#forRollupBase`, and observed on Java 42.9 with the script scenarios
`oracle/scenarios/python/rollup_constituents*.py`, the same `Apply` calls made to both
servers, and the IDE route `rollup`, whose panel includes constituents by default.

- **One more level.** The children of a row of the last level are the rows of the source
  with its keys, in the order of the source: the source partitioned by all the group-by
  columns (`partition_by`), found by key with a `KeyIndex` as the children of the other
  levels are. Java adds this partition to the aggregations of the last level. Without
  group-by columns the last level is the grand total and its children are the source
  itself.
- **Depths.** With `n` group-by columns a row of the source is at depth `n + 2`, cannot be
  expanded (`__EXPANDED__` is null), and a finest group, at depth `n + 1`, can. A key
  table names a finest group with depth `n + 1` and every key value; a deeper key is
  refused as before (`Invalid depth`). Expanding everything, or to a depth, goes down to
  the rows of the source.
- **Default expansions.** The root and the grand total, also without group-by columns:
  such a rollup shows the rows of the source at once
  (`RollupTableImpl#getDefaultExpansionNodeKeys`).
- **Columns.** A snapshot has the structural columns, the columns of the aggregated nodes,
  then every column of the source, group-by columns included: two columns have one name
  wherever a result or a group-by column is named like a column of the source, which with
  group-by columns is always. A row of the source is null in the aggregated columns and a
  row of a level null in the source's (`RollupTableImpl#makeOrFillChunkSourceArray`).
  `Hierarchy::columns` is therefore a list of columns and not a `TableDefinition`, which
  holds no name twice. The fields of the source's columns have
  `rollupTable.isConstituentNodeColumn`, and `rollupTable.isGroupByColumn` where they are
  group-by columns; they are not expand-by columns and have no description. Java describes
  them with the rollup's attributes, so a restriction of the source's sorting marks them
  as it marks the aggregated ones.
- **Sorts.** The rows of the source under a group have sorts of their own, a second list
  kept with the rollup (`RollupTable::with_constituent_sorts`). The Python recorder for
  `NodeType.CONSTITUENT` records them, checked against the columns of the source; a
  recorder for `NodeType.AGGREGATED` sorts the levels alone. `Apply` sorts both: the
  request's sorts are the levels', and each is translated for the rows of the source
  (`RollupTableImpl#translateAggregatedNodeOperationsForConstituentNodes`): a result
  becomes the column it aggregates, a group-by column is left out, since the rows under
  one group are equal by it, a result without an input (a count) is left out, and an
  absolute value stays one where the input is a number. A sort by a column the source
  alone has is refused by `Apply` as a column the levels do not have.
- **Filters.** As without constituents: the rows of the source under a group are those
  the filters accept. A filter of group-by columns keeps or drops whole groups, so the
  partition of the filtered table is what Java shows, which filters the last level and
  keeps its partition.
- **Ticking.** The partition follows the source; a snapshot reads it under the lock as it
  reads the levels. An expanded finest group that has no rows now is left out, or fails
  the snapshot of a rollup whose levels are sorted, as any expanded node does ("Sorted
  nodes").

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
as linkage; a key the rollup never had a node for is dropped
(`#linkKeyTableNodeDirectives`). A key whose group was there and has no rows now keeps
its directive, as in Java, whose lookup is the aggregation's and forgets no key; the walk
finds no row for it and leaves it out, or fails (see "Sorted nodes").

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

**Sorted nodes.** `Hierarchy::order` gives the rows of a node's child table in the order
they are shown, or nothing for the order of the table. A rollup with sorts sorts the
rows of the child table by them at every visit of the node (`ops::sort::sorted_rows`: a
stable sort of the rows, nothing kept between snapshots); a sort by a group-by column
that the level does not have sorts nothing there, as Java's sort by its column of nulls.
The walk then goes by position in that order: the child directives are placed by the
position of their rows, and a run of the viewport is read in the order of its row keys
and put in the shown order afterwards. Java keeps one sorted table per visited node with
the snapshot state of a view and sorts only where it still fills the viewport; here every
visited node is sorted again for every snapshot, which is the cost task 7.8.5 measures.

Java reproduced: while the viewport is not full, the walk of a sorted node looks up the
row of every child directive it acts on in the sorted table, and a directive whose group
is known and has no rows now fails the snapshot (`AssertionFailure: Failed to perform
sort reverse lookup`, `HierarchicalTableImpl.LinkedDirective#setRowKeyInParentSorted`);
the client gets `INVALID_ARGUMENT` for the view until the group has rows again or the key
leaves the key table. A rollup that is not sorted leaves that node out. This looks like
a defect of Java and is reproduced because a difference is the owner's to decide. One
case differs: `KeyIndex` learns keys when a snapshot looks one up, so a group that came
and went between two snapshots of a rollup is not known here, and its directive is
dropped where Java fails.

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
  rollup; the input is left as it was. Java reads the request in this order, and so does
  `hierarchical::apply`: the conditions of the filters (one that cannot be read is an
  internal error); the sorts (a direction that is not ascending or descending is
  `INVALID_ARGUMENT`, `Unsupported or unknown sort direction: <name>`; a column outside
  the source's `SortableColumns`, where it has that attribute, is `INVALID_ARGUMENT`,
  `Sorting attempted on restricted column(s): ...`); the update views, then the format
  views; then it applies the update views, the format views, the filters, then the
  sorts. See "Update views and formats of nodes".
  - **Filters.** A filter is checked against the columns of the source and may read any
    of them that no aggregation reads; one that reads an aggregated column or a column
    array is `INVALID_ARGUMENT`, `Invalid filter found: ...`, and one that reads a column
    the source does not have, a result of the rollup among them, is an internal error.
    Filters that read group-by columns only filter the finest level in Java, from which
    the coarser ones are aggregated again; the others filter the source, and the filtered
    table becomes the rollup's source (`RollupTableImpl#withFilter`, `#rebase`). Here
    both are a `where` in front of every level, which is the same rows; `ExportSource`
    gives the unfiltered source in the first case and the filtered one in the second, as
    in Java. A filter that leaves nothing leaves no grand total either.
  - **Sorts.** The sorts of a rollup are a list kept with it. New sorts go in front of
    the earlier ones, and a column that is named again keeps its first place in the new
    list (`BaseNodeOperationsRecorder#sort`); the absolute value of a column is a column
    of its own, `__ABS__<name>`. A sort names a column of the nodes, not `__DEPTH__` or
    `__EXPANDED__`; anything else is an internal error. See "Sorted nodes".
- `Tree`: source ticket, identifier column, parent identifier column, `promote_orphans`;
  the result export holds the tree. A name that is empty is a field that is missing
  (`GrpcErrorHelper#checkHasField`). See "Trees".

**The object type.** `ListFields` and the console name the type `HierarchicalTable`.
`FetchObject` and `MessageStream` answer with `HierarchicalTableDescriptor`: the snapshot
schema (`__DEPTH__` int, `__EXPANDED__` Boolean, then the node columns, each with the
`hierarchicalTable.*` and `rollupTable.*` metadata of `HierarchicalTableSchemaUtil`) and
`is_static`. There are no exported references. A rollup of a table whose sorts are
restricted (`restrict_sort_to`) has the attribute in the schema's metadata
(`attribute.SortableColumns`, the names joined by commas, and its
`attribute_type`), and `isSortable` on those fields only.

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
`aggs`, `by`, `include_constituents`. `RollupTable` wraps a handle, as `PartitionedTable`
does; a variable that holds one is listed as a `HierarchicalTable`.

`node_operation_recorder(NodeType.AGGREGATED)` gives a `RollupNodeOperationsRecorder`
whose `sort` and `sort_descending` record sorts, checked when they are recorded (the
error is Java's `RuntimeError`, not a `DHError`); `with_node_operations(recorders)`
applies them, later recorders after earlier ones; `with_filters(filters)` filters, with
the checks of `Apply`. A recorder for `NodeType.CONSTITUENT` records sorts of the rows of
the source, checked against the source's columns; it is refused for a rollup without
constituents, and so is applying one to such a rollup, as in Java. `with_node_operations` does not check that a
recorder was made by this rollup; neither does Java. The recorder's `update_view`,
`format_column`, `format_row_where` and `format_column_where` record update views and
formats, and `RollupTable.with_update_view` is the update view of a recorder for the
aggregated nodes, applied (`RollupTableImpl#withUpdateView`). A recorder's errors are
Java's `RuntimeError`; those of `with_node_operations` and `with_update_view` are
`DHError`s with Java's messages.

## Update views and formats of nodes

Task 7.8.2. `ops::hierarchical::NodeOperations` is the recorder: the columns of the
nodes of one type (without `__DEPTH__` and `__EXPANDED__`), and in the order recorded
the update views, the formats and the sorts (`BaseNodeOperationsRecorder`). An update
view and a format are compiled when they are recorded, against the columns of the nodes
and what the recorder added before them: so a format can read a column an update view of
the same recorder adds, and a later update view cannot name a column the recorder has.
A format is `Column = format` of `ops::select::format`, as `Table.format_columns` takes
it: a hidden column `<Column>__TABLE_STYLE_FORMAT`, `__TABLE_NUMBER_FORMAT` or
`__TABLE_DATE_FORMAT` (`__ROW__TABLE_STYLE_FORMAT` for `*`).

`RollupTable::with_node_operations(constituents, recorder)` gives a rollup whose nodes
of that type have the recorder's columns after their own, and its sorts in front of
theirs. A node's table is then a table that computes the added columns when they are read
(`node_ops::node_table`, no listener and no stored values): its update views, then its
formats, a format of a column that has one replacing it in place. A row of a level has
null in the group-by columns that level does not group by, as Java's update view reads
them. The snapshot schema has the added columns after the columns of their node type,
with the metadata of `HierarchicalTableSchemaUtil` (an aggregated node's column has
`rollupTable.aggregationInputColumnName` empty, a constituent's has none) and the format
links of `schema.rs` (`styleColumn`, `isStyle`, ...).

Java keeps one recorder per node type and merges a second one into it only when both
give the nodes the same columns (`RollupNodeOperationsRecorder#withOperations`). So a
recorder that adds columns is refused for nodes that have operations of any kind, sorts
included (`Incompatible operation recorders; ...`), and taken for nodes that have none;
one that adds none (sorts, or formats again by a recorder made of the rollup that has
them) is always taken. The same here.

`Apply` checks every update view and every format view against the columns of the
rollup's source before it reads its node type (`translateAndValidateUpdateViews`): an
expression that reads a result of an aggregation that is not a column of the source is
refused, and so are a pattern (`Decimal(..)`, `Date(..)`) and the row (`*`). A node type
that is neither is `INVALID_ARGUMENT`, `Unsupported or unknown node type: <name>`. The
views of the constituent nodes of a rollup without them are dropped. Every other refusal
is an internal error with an error id, as in Java.

## What a tree reuses

Everything but the `Hierarchy` implementation: key table directives and linking, the
walk, `KeyIndex`, the view and its snapshot state, `View` and `ExportSource`, the object
type and descriptor (with the `treeTable.*` metadata), the subscription, the harness's
`views` directive and the IDE route's helpers. Of `Apply`: `hierarchical::apply` and its
order of checks, `translate_sorts`, `ops::hierarchical::merged_sorts`,
`Hierarchy::order` with the walk by shown position, and `ops::filter::CheckedFilters`,
which says what a filter reads before it is applied; a tree's filters are of another
kind (they keep the ancestors of a row that passes) and its sorts are per node as here. A tree's `Hierarchy` has one node table
per parent identifier (the source partitioned by the parent column), levels that are
expandable row by row, and a node key of one value. `__EXPANDED__` is null for a row
without children; `Hierarchy::rows_expandable` answers for a whole level today (`All` or
`None`, which is all a rollup needs), and a tree adds the answer per row.

## Trees

Task 7.9 (`tasks/7.9-tree.md`). Read from `TreeTableImpl`, `TreeTableFilter`,
`TreeSourceRowLookup`, `TreeTable#promoteOrphans` and `HierarchicalTableImpl`; every
behaviour below was observed on Java 42.9 with the script scenarios
`oracle/scenarios/python/tree_*.py`, the same calls made to both servers, and the IDE
route `tree` (`oracle/ide/traces/http-console-tree.txt`).

**What a tree is.** `ops::hierarchical::tree(source, identifier, parent)` is the source
partitioned by the parent column: one node table per parent identifier, whose rows keep
the row keys of the source, so a node is a row of the source and the root is the node of
the rows without a parent. A `KeyIndex` over the partition finds the children of an
identifier. `RowLookup` finds the row of an identifier (`TreeSourceRowLookup`): a map
that a listener of the source builds again in every cycle that adds, removes or shifts
rows or changes the identifier column. Java keeps it as an aggregation, incrementally.
The two columns must be of one type; the messages for a missing column and for two types
are Java's.

**What a view shows.**

- By default the root alone is expanded. `__EXPANDED__` is null for a row that is no
  one's parent (`Expandable::ByRow`), and `__DEPTH__` is 1 for the rows of the root.
- A key table names a node by the identifier column, in the type the tree has it; a null
  identifier is the root. It needs no `__DEPTH__` column. A column of another type or no
  such column fails the view when it is read, `INVALID_ARGUMENT`.
- A row whose parent is no row is not shown. With `promote_orphans` it is a child of the
  root for as long as that is so: the source is joined with itself and the parent
  replaced by null where nothing matched (`TreeTable#promoteOrphans`). The sortable
  columns of the source survive the promotion, in the order of the set Java's `view`
  collects them in.
- Rows that are their own ancestors are not reached from the root and not shown.
- A key that names a row which has or had children and whose parent is no row fails the
  snapshot: `Failed to find ancestor for existing key table directive`,
  `INVALID_ARGUMENT`. A key of such a row that never had children is dropped. Keys that
  name two rows that are each other's ancestors fail it with an assertion of
  `HierarchicalTableImpl#removeOldChildren`. Both are reproduced.
- A tree by one column as identifier and parent has no rows.

**Two rows with one identifier.** Of a static source the tree is refused
(`UniqueRowKeyChunkedOperator`). Of a ticking source that gets such a row later, Java's
lookup aggregation fails alone and nothing tells a client: views keep being answered,
with the lookup as it was before that cycle. Reproduced: `RowLookup` stops at that cycle
and keeps its map.

**Sorts and filters.** `Apply` and the Python recorder sort the children of every node,
with the list of sorts and `__ABS__<name>` as for a rollup; the order is computed at a
snapshot. Unlike a sorted rollup, a sorted tree with an expanded node that is gone
answers. A filter keeps the rows that pass and their ancestors (`TreeTableFilter`): the
filtered tree is a tree of that table, which here is computed again from the source in
every cycle the source or the filter's result changes, and Java maintains
incrementally. A filter of a filtered tree filters that table, so an ancestor the first
filter kept may pass the second. A filter that nothing passes leaves no rows.

**The schema.** The fields have `treeTable.isNodeColumn`, the identifier column also
`treeTable.isIdentifierColumn` and `hierarchicalTable.isExpandByColumn`, the parent
column `treeTable.isParentIdentifierColumn`; `__DEPTH__` is not an expand-by column, as
it is in a rollup. The schema has no `ColumnDescriptions` attribute.

**Python.** `Table.tree(id_col, parent_col, promote_orphans)` and
`deephaven.table.TreeTable` with `id_col`, `parent_col`, `node_operation_recorder()`,
`with_node_operations(recorder)` and `with_filters(filters)`;
`TreeNodeOperationsRecorder` has `sort` and `sort_descending`.

## Refused for now

Each names the Java method in its error.

| What | Java | Task |
| --- | --- | --- |
| A filter that reads the position of a row (`i`, `ii`, `k`), which Java takes for a filter of group-by columns and evaluates on the rows of the finest level | `RollupTableImpl#withFilter` | needs a ruling |
| A sort by a column of vectors or arrays (the result of `Distinct`), the absolute value of a `char` column | `SortOperation#SortOperation`, `QueryLanguageParser#visit(MethodCallExpr)`: the refusals of the engine's sort | none yet |
| `CountWhere`, `Group`, `AggFormula` in a rollup | `AggregationProcessor.RollupBaseConverter#visit` | 7.8.4 |
| `SortedFirst`, `SortedLast` by a column that is not their own result, with group-by columns | `AggregationProcessor.RollupReaggregatedConverter#reaggregateSortedFirstOrLastOperator` | 7.8.4 |
| A rollup of a blink table | `AggregationProcessor#forRollupBase` | 7.8.4 |
| The expansion of a row of a tree that has no identifier | `TreeTableImpl#makeChildNodeIdLookup` | needs a ruling |
| A walk of a tree more than 200 levels deep | `HierarchicalTableImpl#visitExpandedNode` | needs a ruling |
| The tree recorder's `where`, `format_column`, `format_row_where`, `format_column_where` | `TreeNodeOperationsRecorder#where`, `#formatColumns`, `#formatRowWhere`, `#formatColumnWhere` | none yet |
| A colour as the value of a formula (`AK = RED`, `new Color(..)`), the colours of `io.deephaven.engine.util.ColorUtil.RED`, `colorRGB(int, boolean)` | `io.deephaven.gui.color.Color`, `ColorUtilImpl` | 4.21 |
| A tree of a blink table | `TreeTableImpl#makeTree` | none yet |
| The refusal of an identifier and a parent of two types when one is of a type whose Java description is not written here | `ColumnDefinition#describeForCompatibility` | none yet |
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
- The sorted order of a node is computed at every snapshot; Java keeps a sorted table per
  visited node. A client sees the same rows.
- A view of a sorted rollup with an expanded node that is gone: reproduced but for one
  case, see "Sorted nodes".
- Java cannot describe a rollup of a table whose sorts are restricted to no column
  (`restrict_sort_to` without names): `FetchObject` fails with a `NullPointerException`
  in `BarrageUtil#columnDefinitionsToFields`. Here it is described, with no sortable
  column. Not reproduced: the place is the schema code every table shares.
- The order of the keys of the schema's metadata differs, as that of a field's.
- A tree's lookup of rows by identifier and the table of a filtered tree are computed
  again in a cycle that changes them; Java maintains both incrementally. A client sees
  the same rows; a cycle costs the size of the source.
- A row of a tree without an identifier can be expanded in Java, and its children are
  the rows of the root, itself among them. Expanded to a depth, Java then shows rows of
  the root with depths that are not theirs; with every row expanded its stack overflows
  and the server process ends (observed: the reference server had to be started again).
  Here the expansion of such a row is refused by name. The default view, in which the row
  is shown and not expanded, is the same. Needs a ruling.
- Java's walk has no limit of depth and overflows its stack on a tree that is deep
  enough; here a walk more than 200 levels deep fails the snapshot, by name. Needs a
  ruling on the limit.
- Java's `Apply` does not read the update views and format views of a request for a
  tree: a request with only those gives the tree back, whatever they say (one that does
  not compile too). Reproduced: they are not read here either. Listed for a ruling, as a
  client is told nothing of the views it asked for.
- Java refuses a second set of operations that add columns for the nodes of a rollup
  that have operations (`RollupNodeOperationsRecorder#withOperations`): two update views
  in a row, `with_update_view` twice, formats after a sort, update views and formats of
  one node type in one `Apply`. Reproduced. Listed for a ruling.
- Java's Python `TreeNodeOperationsRecorder` has `where` (a filter of the rows of a
  node); it is refused by name here, and `TreeTable#withNodeFilterColumns` is not
  offered.
- Java's natural join copies the sortable columns of its left table; `ops::join` does
  not yet, and the promotion of orphans restores them itself.

## Contracts

No change.

## Gate

`cargo test -p oxhaven-engine hierarchical`, `cargo test -p oxhaven-server hierarchical`
(with and without `--features python`), the script scenarios `rollup_*` in the replay
gate, and `node oracle/ide/ide-test.js --routes rollup` on Java and on Oxhaven. The route
makes its rollup in the IDE's Rollup Rows panel, which sends `first` of every column that
is not grouped by, a new `Rollup` for every column added, and `include_constituents`
unless its option is unchecked; the route leaves it checked and expands a finest group.

The script scenarios compare views: the directive `# oracle: views <name> = <rollup>
[<key table> [<action column>]]` (`oracle/README.md`) reads the first rows of a view of
a rollup with the key table's expansions on both servers, after every step. Where a view
has two columns of one name, the harness names the later one `<name>#2`
(`harness/views.py`).
