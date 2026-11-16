# A PartitionedTableProxy of a refreshing partitioned table. The operation is applied in
# the update cycle to a constituent that appears, with the variables that the formulas
# had when the proxy operation was called: a variable that changed since, or a local of a
# function that has returned, has its value of then. A table the operation reads besides
# is a dependency of the result. The checks of a join of two proxies are made in every
# cycle: when the keys stop matching, or a join key comes to be in two constituents, the
# result fails (its underlying table and its merge), and the partitioned tables it was
# made of go on.
# oracle: results rows, filtered, updated, updated_local, key_filtered, heads, sorted_rows, counted, with_static, with_ticking, static_with_ticking, pairs_common, by_value_unchecked
from deephaven import DHError, empty_table, input_table, new_table
from deephaven.column import int_col


def change(ids, keys, values):
    return new_table([int_col("Id", ids), int_col("K", keys), int_col("V", values)])


def fails(call, text):
    try:
        call()
    except DHError as error:
        assert text in str(error), str(error)
        return
    raise AssertionError("did not fail")


rows = input_table(init_table=change([1, 2, 3, 4], [1, 2, 1, 3], [10, 20, 30, 40]), key_cols="Id")
pt = rows.partition_by("K")
px = pt.proxy()
assert px.is_refreshing

a = 15
made = px.where("V > a")
assert made.is_refreshing and made.target.constituent_changes_permitted
assert made.target.unique_keys and made.target.key_columns == ["K"]
filtered = made.target.merge()
updated = px.update("X = V + a").target.merge()


def with_local():
    b = 100
    return px.update("X = V + b")


updated_local = with_local().target.merge()
by_key = px.where("K >= 2")
assert by_key.target.table.size == 2 and by_key.target.constituent_changes_permitted
key_filtered = by_key.target.merge()
heads = px.head(1).target.merge()
sorted_rows = px.sort_descending("V").target.merge()
counted = px.count_by("N").target.merge()

static = empty_table(3).update(["K = i + 1", "S = i * 1000"])
with_static = px.natural_join(static, "K", "S").target.merge()
other = input_table(
    init_table=new_table([int_col("K", [1, 2, 3]), int_col("S", [1, 2, 3])]), key_cols="K"
)
with_ticking = px.natural_join(other, "K", "S").target.merge()
# a partitioned table that does not refresh, and a table that does
fixed = empty_table(6).update(["K = i % 3 + 1", "V = i"]).partition_by("K")
joined = fixed.proxy().natural_join(other, "K", "S")
assert joined.is_refreshing and joined.target.table.is_refreshing
assert not joined.target.constituent_changes_permitted
assert all(table.is_refreshing for table in joined.target.constituent_tables)
static_with_ticking = joined.target.merge()

# two refreshing partitioned tables
rows2 = input_table(init_table=change([1, 2, 3], [1, 2, 3], [100, 200, 300]), key_cols="Id")
pt2 = rows2.partition_by("K")
px2 = pt2.proxy()
second = px2.view(["Id2 = Id", "V2 = V"])
pair = px.natural_join(second, "Id = Id2", "V2")
assert pair.is_refreshing and pair.target.table.size == 3
pair_merged = pair.target.merge()
assert pair_merged.size == 4
common = pt.proxy(False, True).natural_join(second, "Id = Id2", "V2")
pairs_common = common.target.merge()
# the values of V are each in one constituent
values = px2.view(["VV = V", "Id2 = Id"])
by_value = pt.proxy(False, True).natural_join(values, "V = VV", "Id2")
by_value_merged = by_value.target.merge()
loose = pt.proxy(False, False).natural_join(values, "V = VV", "Id2")
by_value_unchecked = loose.target.merge()
# oracle: step
a = 0
rows.add(change([5, 6], [5, 1], [50, 5]))
other.add(new_table([int_col("K", [5, 2]), int_col("S", [55, 22])]))
# oracle: step
# the first table has the key 5 and the second does not
assert pair.target.table.is_failed and pair_merged.is_failed
assert not common.target.table.is_failed and not pairs_common.is_failed
assert common.target.table.size == 3
assert not pt.table.is_failed and not pt2.table.is_failed
assert not filtered.is_failed and not updated.is_failed and not updated_local.is_failed
assert by_key.target.table.size == 3
fails(
    lambda: px.natural_join(second, "Id = Id2", "V2"),
    "natural_join operation on the PartitionedTableProxy failed.",
)
rows2.add(change([9], [5], [900]))
# oracle: step
assert common.target.table.size == 4
matched = px.natural_join(second, "Id = Id2", "V2")
assert matched.target.table.size == 4 and matched.target.merge().size == 6
# a row moves to another constituent and takes its value with it: no value is in two
rows.add(change([3], [2], [30]))
# oracle: step
assert not by_value.target.table.is_failed and not by_value_merged.is_failed
assert not matched.target.table.is_failed
# the value 10 is in the constituents of the keys 1 and 2
rows.add(change([20], [2], [10]))
# oracle: step
assert by_value.target.table.is_failed and by_value_merged.is_failed
assert not loose.target.table.is_failed and not by_value_unchecked.is_failed
assert not pt.table.is_failed and not matched.target.table.is_failed
fails(
    lambda: px.natural_join(values, "V = VV", "Id2"),
    "natural_join operation on the PartitionedTableProxy failed.",
)
rows.delete(change([20], [0], [0]).view("Id"))
# oracle: step
assert px.natural_join(values, "V = VV", "Id2").target.table.size == 4
