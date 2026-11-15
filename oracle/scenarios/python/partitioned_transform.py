# transform of a partitioned table that is not refreshing: the function is applied to every
# constituent, and the result keeps the rows and the keys of the underlying table. The
# function runs where Java has no query scope, so a formula or a filter in it fails. Then
# what a partitioned table refuses.
# oracle: results heads_underlying, heads, heads_found, counted, counted_keys, renamed, joined, refiltered
from deephaven import SortDirection, agg, empty_table, time_table

source = empty_table(9).update(["K = i % 3", "S = `s` + (i % 2)", "V = i * 2"])
other = empty_table(2).update(["S = `s` + i", "X = i * 10"])
pt = source.partition_by("K")

top = pt.sort("K", SortDirection.DESCENDING).transform(lambda table: table.head(1))
assert [column.name for column in top.constituent_table_columns] == ["K", "S", "V"]
assert list(top.key_columns) == ["K"]
assert top.unique_keys and not top.constituent_changes_permitted and not top.is_refreshing
heads_underlying = top.table
heads = top.merge()
heads_found = top.get_constituent(1)

count = pt.transform(lambda table: table.count_by("N", "S"))
assert [column.name for column in count.constituent_table_columns] == ["S", "N"]
counted = count.merge()
counted_keys = count.keys()
renamed = pt.transform(lambda table: table.view(["Key = K", "V"]).sort_descending("V")).merge()
joined = pt.transform(lambda table: table.natural_join(other, "S", "X").agg_by(agg.sum_("X"), "S")).merge()
refiltered = count.filter("K != 1").transform(lambda table: table.tail(1)).merge()
# oracle: step
# oracle: fails
formula = pt.transform(lambda table: table.update("W = V * 2"))
# oracle: step
# oracle: fails
condition = pt.transform(lambda table: table.where("V > 2"))
# oracle: step
# oracle: fails
counting = pt.transform(lambda table: table.agg_by(agg.count_where("N", "V > 2"), "S"))
# oracle: step
# oracle: fails
first_rows = pt.transform(lambda table: table.head_by(1, "S"))
# oracle: step
# oracle: fails
nothing = pt.transform(lambda table: None)
# oracle: step
# oracle: fails
not_a_table = pt.transform(lambda table: 1)
# oracle: step
# oracle: fails
too_many = pt.get_constituent([1, 2])
# oracle: step
# oracle: fails
by_tables = pt.sort("__CONSTITUENT__")
# oracle: step
# oracle: fails
no_column = source.partition_by("Nope")
# oracle: step
# oracle: fails
not_a_bool = source.partition_by("K", drop_keys=1)
# oracle: step
# oracle: fails
no_filter_column = pt.filter("Nope > 1")
# oracle: step
# oracle: fails
blink = time_table("PT1s", blink_table=True).update("K = ii % 2").partition_by("K")
# oracle: step
still_scoped = source.where("V > 2").update("W = V * 2")
