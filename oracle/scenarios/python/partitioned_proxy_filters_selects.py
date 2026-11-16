# The filters and the selects of a PartitionedTableProxy: each is the table operation
# applied to every constituent. A filter that reads only key columns the constituents are
# known to have is applied to the underlying table instead, which leaves fewer
# constituents; which key columns are known that way after each operation is asserted
# through such a filter. The variables of a formula are those of the caller.
# oracle: results filtered, filtered_local, key_filtered, key_and_value, one_string, no_filter, in_table, not_in_table, in_keys, in_keys_underlying, not_in_keys, viewed, update_viewed, updated, selected, distinct, distinct_all, key_assigned
from deephaven import DHError, empty_table, new_table
from deephaven.column import int_col
from deephaven.table import PartitionedTableProxy

src = empty_table(12).update(["K = i % 3", "V = i", "W = i % 4"])
pt = src.partition_by("K")
px = pt.proxy()
assert isinstance(px, PartitionedTableProxy)
assert px.target is px.target and px.require_matching_keys and px.sanity_check_joins
assert not px.is_refreshing
assert not pt.proxy(False).require_matching_keys and pt.proxy(False).sanity_check_joins
assert not pt.proxy(sanity_check_joins=False).sanity_check_joins


def fails(call, text):
    try:
        call()
    except DHError as error:
        assert text in str(error), str(error)
        return
    raise AssertionError("did not fail")


def rows_for_key(proxy):
    """The rows of the underlying table after a filter of the key column: 1 when the
    filter was applied to the underlying table, 3 when to every constituent."""
    return proxy.where("K = 1").target.table.size


def columns(proxy):
    return [column.name for column in proxy.target.constituent_table_columns]


a = 5
made = px.where("V > a")
assert isinstance(made, PartitionedTableProxy)
assert made.require_matching_keys and made.sanity_check_joins
assert made.target.key_columns == ["K"] and made.target.unique_keys
assert not made.target.constituent_changes_permitted and not made.target.is_refreshing
assert made.target.constituent_column == "__CONSTITUENT__"
assert made.target.table.size == 3
filtered = made.target.merge()


def with_local():
    b = 7
    return px.where("V > b")


filtered_local = with_local().target.merge()

by_key = px.where("K = 1")
assert by_key.target.table.size == 1
key_filtered = by_key.target.merge()
mixed = px.where(["K = 1", "V > 5"])
assert mixed.target.table.size == 1
key_and_value = mixed.target.merge()
# one filter that reads a key column and another column is a filter of the constituents
single = px.where("K = 1 && V > 5")
assert single.target.table.size == 3
one_string = single.target.merge()
assert px.where().target.table.size == 3 and px.where([]).target.table.size == 3
no_filter = px.where().target.merge()
assert rows_for_key(px.where("V > 5")) == 1

# a partitioned table whose constituents do not have the key columns
dropped = src.partition_by("K", drop_keys=True).proxy()
fails(lambda: dropped.where("K = 1"), "where operation on the PartitionedTableProxy failed.")
assert dropped.where("V > 5").target.table.size == 3
fails(lambda: px.where("Nope > 1"), "where operation on the PartitionedTableProxy failed.")

others = empty_table(4).update(["W = i", "R = i * 100"]).where("W < 2")
in_table = px.where_in(others, "W").target.merge()
not_in_table = px.where_not_in(others, "W").target.merge()
assert px.where_in(others, "W").target.table.size == 3
keys = new_table([int_col("K", [1, 2])])
kept = px.where_in(keys, "K")
assert kept.target.table.size == 2
in_keys = kept.target.merge()
in_keys_underlying = kept.target.table.view("K")
assert px.where_in(keys.view("Q = K"), "K = Q").target.table.size == 2
assert px.where_in(keys.view("V = K"), ["K = V"]).target.table.size == 2
assert px.where_not_in(keys, "K").target.table.size == 1
not_in_keys = px.where_not_in(keys, "K").target.merge()
fails(lambda: px.where_in(px, "K"), "where_in operation on the PartitionedTableProxy failed.")

viewed = px.view(["K", "X = V + a"]).target.merge()
assert columns(px.view(["K", "X = V + a"])) == ["K", "X"]
assert rows_for_key(px.view(["K", "X = V + a"])) == 1
fails(
    lambda: px.view(["X = V + a"]).where("K = 1"),
    "where operation on the PartitionedTableProxy failed.",
)
assert columns(px.view([])) == ["K", "V", "W"] and rows_for_key(px.view([])) == 1
update_viewed = px.update_view(["X = V * 2"]).target.merge()
assert rows_for_key(px.update_view(["X = V"])) == 1
assert rows_for_key(px.update_view([])) == 1
# a key column that a formula assigns is not known to hold the key any more
assert rows_for_key(px.update_view(["K = V"])) == 3
updated = px.update("X = V + a").target.merge()
assert rows_for_key(px.update("X = V + a")) == 1
reassigned = px.update("K = K + 1")
assert rows_for_key(reassigned) == 3
key_assigned = reassigned.where("K = 1").target.merge()
fails(lambda: px.update("X = Nope"), "update operation on the PartitionedTableProxy failed.")
fails(lambda: px.update([]), "update operation on the PartitionedTableProxy failed.")

selected = px.select(["K", "V"]).target.merge()
assert rows_for_key(px.select(["K", "V"])) == 1
assert columns(px.select(["K = K", "V"])) == ["K", "V"]
assert rows_for_key(px.select(["K = K", "V"])) == 3
fails(
    lambda: px.select(["K2 = K", "V"]).where("K = 1"),
    "where operation on the PartitionedTableProxy failed.",
)
fails(lambda: px.select(), "select operation on the PartitionedTableProxy failed.")
fails(lambda: px.select(None), "select operation on the PartitionedTableProxy failed.")

distinct = px.select_distinct("W").target.merge()
fails(
    lambda: px.select_distinct("W").where("K = 1"),
    "where operation on the PartitionedTableProxy failed.",
)
assert rows_for_key(px.select_distinct("K")) == 1
assert columns(px.select_distinct()) == ["K", "V", "W"]
assert rows_for_key(px.select_distinct()) == 1
distinct_all = px.select_distinct().target.merge()
