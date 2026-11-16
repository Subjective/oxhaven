# transform with dependencies: the refreshing tables the function reads besides the
# constituent. With one, the transform of a partitioned table that is not refreshing is
# refreshing and may hold refreshing tables, which it may not without; dependencies that
# are not refreshing, or not tables, count for nothing. In a cycle the function is called
# after its dependencies have finished, so a constituent that appears is joined with what
# the other table holds after the cycle's change.
# oracle: results rows, look, static_joined, static_underlying, ticking_joined, ticking_underlying, heads
from deephaven import empty_table, input_table, new_table
from deephaven.column import int_col


def change(ids, keys, values):
    return new_table([int_col("Id", ids), int_col("K", keys), int_col("V", values)])


def weights(keys, values):
    return new_table([int_col("K", keys), int_col("W", values)])


rows = input_table(init_table=change([1, 2, 3], [1, 2, 1], [10, 20, 30]), key_cols="Id")
look = input_table(init_table=weights([1, 2], [100, 200]), key_cols="K")
static = empty_table(6).update(["K = i % 3", "V = i"]).partition_by("K")
pt = rows.partition_by("K")


def joined(table):
    return table.natural_join(look, "K", "W")


with_look = static.transform(joined, dependencies=[look])
assert with_look.is_refreshing and with_look.table.is_refreshing
assert not with_look.constituent_changes_permitted
assert [table.is_refreshing for table in with_look.constituent_tables] == [True, True, True]
assert [column.name for column in with_look.constituent_table_columns] == ["K", "V", "W"]
static_joined = with_look.merge()
static_underlying = with_look.table

heads = static.transform(lambda table: table.head(1), dependencies=look)
assert heads.is_refreshing and not heads.constituent_changes_permitted
assert [table.is_refreshing for table in heads.constituent_tables] == [False, False, False]
heads = heads.merge()
assert heads.is_refreshing

for nothing in ([static, empty_table(1)], [1], None, []):
    plain = static.transform(lambda table: table.head(1), dependencies=nothing)
    assert not plain.is_refreshing
by_partitioned = static.transform(lambda table: table.head(1), dependencies=[pt])
assert by_partitioned.is_refreshing

ticking = pt.transform(joined, dependencies=[look])
assert ticking.is_refreshing and ticking.constituent_changes_permitted
ticking_joined = ticking.merge()
ticking_underlying = ticking.table
# oracle: step
look.add(weights([1, 3], [101, 300]))
# oracle: step
rows.add(change([4], [3], [40]))
# oracle: step
look.delete(weights([2], [0]).view("K"))
# oracle: step
# oracle: fails
refreshing_results = static.transform(joined)
# oracle: step
# oracle: fails
static_dependency = static.transform(joined, dependencies=[static])
