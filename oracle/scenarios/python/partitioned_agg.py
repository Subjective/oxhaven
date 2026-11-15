# The partition aggregation: beside other aggregations in agg_by, where the result is a
# table with a column of tables, and through partitioned_agg_by, which adds one when the
# aggregations have none and takes the name and the columns of the constituents from one
# they have.
# oracle: results raw, totals, totals_keys, totals_merged, totals_first, named, named_merged, overall, overall_merged, ticking, ticking_merged
from deephaven import agg, empty_table, input_table, new_table
from deephaven.column import int_col

source = empty_table(9).update(["K = i % 3", "S = `s` + (i % 2)", "V = i * 2"])
raw = source.agg_by([agg.count_("N"), agg.partition("Rows"), agg.sum_("Total = V")], by="K")

with_totals = source.partitioned_agg_by([agg.sum_("Total = V")], by="K")
assert with_totals.constituent_column == "__CONSTITUENT__"
assert list(with_totals.key_columns) == ["K"]
assert [column.name for column in with_totals.constituent_table_columns] == ["K", "S", "V"]
totals = with_totals.table
totals_keys = with_totals.keys()
totals_merged = with_totals.merge()
totals_first = with_totals.get_constituent(0)

with_name = source.partitioned_agg_by(
    [agg.partition("Rows", include_by_columns=False), agg.count_("N")], by=["K", "S"]
)
assert with_name.constituent_column == "Rows"
assert [column.name for column in with_name.constituent_table_columns] == ["V"]
named = with_name.table
named_merged = with_name.merge()

everything = source.partitioned_agg_by(agg.max_("Most = V"))
overall = everything.table
overall_merged = everything.merge()

rows = input_table(init_table=new_table([int_col("K", [1, 2, 1]), int_col("V", [1, 2, 3])]))
counted = rows.partitioned_agg_by([agg.count_("N"), agg.last("Last = V")], by="K")
assert counted.is_refreshing and counted.constituent_changes_permitted
ticking = counted.table
ticking_merged = counted.merge()
# oracle: step
rows.add(new_table([int_col("K", [3, 1]), int_col("V", [4, 5])]))
