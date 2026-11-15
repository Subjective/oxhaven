# partition_by of a static table: the underlying table, whose constituent column a client is
# sent as text, the keys, the constituents, one constituent by its key, and merge; with the
# keys dropped, by two columns and by none; then a filter and a sort of the partitioned table.
# oracle: results underlying, keys, merged, first, third, found, dropped_underlying, dropped_first, dropped_found, dropped_merged, whole_underlying, whole, filtered_underlying, filtered_keys, filtered_merged, ordered_underlying, ordered_merged, meta, joined, counted
from deephaven import SortDirection, empty_table

source = empty_table(9).update(["K = i % 3", "S = `s` + (i % 2)", "V = i * 1.5"])
pt = source.partition_by("K")
underlying = pt.table
meta = pt.table.meta_table
keys = pt.keys()
merged = pt.merge()
first, second, third = pt.constituent_tables
found = pt.get_constituent(1)
assert pt.get_constituent(7) is None
assert list(pt.key_columns) == ["K"]
assert pt.unique_keys
assert not pt.constituent_changes_permitted
assert not pt.is_refreshing
assert pt.constituent_column == "__CONSTITUENT__"
assert [column.name for column in pt.constituent_table_columns] == ["K", "S", "V"]
assert not first.is_refreshing and first.size == 3

dropped = source.partition_by(["K", "S"], drop_keys=True)
assert list(dropped.key_columns) == ["K", "S"]
assert [column.name for column in dropped.constituent_table_columns] == ["V"]
dropped_underlying = dropped.table
dropped_first = dropped.constituent_tables[0]
dropped_found = dropped.get_constituent([2, "s0"])
dropped_merged = dropped.merge()

everything = source.partition_by([])
assert list(everything.key_columns) == []
assert len(everything.constituent_tables) == 1
whole_underlying = everything.table
whole = everything.merge()

filtered = pt.filter("K > 0")
assert filtered.unique_keys
filtered_underlying = filtered.table
filtered_keys = filtered.keys()
filtered_merged = filtered.merge()
assert filtered.get_constituent(0) is None

ordered = pt.sort("K", SortDirection.DESCENDING)
ordered_underlying = ordered.table
ordered_merged = ordered.merge()
assert [table.size for table in pt.sort(["K"]).constituent_tables] == [3, 3, 3]

# the column of tables in other operations
joined = source.natural_join(pt.table, "K", "__CONSTITUENT__").head(4)
counted = pt.table.count_by("N", "__CONSTITUENT__")
# oracle: step
# oracle: fails
first_per_key = pt.table.head_by(1, "K")
