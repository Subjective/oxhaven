# The aggregations of a PartitionedTableProxy and its update_by: each is the table
# operation applied to every constituent. After an aggregation no key column is known to
# hold the key of its constituent, also where the aggregation groups by it: a filter of a
# key column is then applied to every constituent. update_by keeps the key columns that
# its operators do not write. `weighted_avg_by` of the Python proxy without group
# columns was observed to be `avg_by` of the weight column's name.
# oracle: results grouped, grouped_by, agg, agg_none, agg_formula, agg_all, agg_all_none, counted, counted_by, first, last, minimum, maximum, total, absolute, average, median, deviation, variance, weighted_sum, weighted_sum_by, weighted_average, weighted_average_by, running, running_by, running_key
# oracle: computed deviation.V, variance.V, average.V, weighted_average.V, weighted_average.K, weighted_average_by.V, agg_all_none.V, agg_all_none.W, agg_all_none.K
from deephaven import DHError, agg as aggs, empty_table
from deephaven.table import PartitionedTableProxy
from deephaven.updateby import cum_sum

src = empty_table(12).update(["K = i % 3", "V = i - 4", "W = i % 4"])
pt = src.partition_by("K")
px = pt.proxy()


def fails(call, text):
    try:
        call()
    except DHError as error:
        assert text in str(error), str(error)
        return
    raise AssertionError("did not fail")


def columns(proxy):
    return [column.name for column in proxy.target.constituent_table_columns]


def checked(proxy, names, key_rows=3):
    """The merge of the result. `key_rows` is the number of rows of the underlying table
    after a filter of the key column: 3 when the filter is one of the constituents,
    None when the constituents have no column K."""
    assert isinstance(proxy, PartitionedTableProxy)
    assert proxy.target.table.size == 3 and proxy.target.key_columns == ["K"]
    assert proxy.target.unique_keys and not proxy.target.constituent_changes_permitted
    assert columns(proxy) == names, columns(proxy)
    if key_rows is None:
        fails(lambda: proxy.where("K = 1"), "where operation on the PartitionedTableProxy failed.")
    else:
        assert proxy.where("K == 1").target.table.size == key_rows
    return proxy.target.merge()


grouped = checked(px.group_by("K"), ["K", "V", "W"])
grouped_by = checked(px.group_by(["W"]), ["W", "K", "V"], None)
assert columns(px.group_by()) == ["K", "V", "W"]
agg = checked(px.agg_by(aggs.sum_("V"), "K"), ["K", "V"])
agg_none = checked(px.agg_by([aggs.sum_("V"), aggs.count_("N")]), ["V", "N"], None)
agg_formula = checked(px.agg_by([aggs.formula("X = sum(V)")], "W"), ["W", "X"], None)
assert columns(px.agg_by([aggs.partition("P")], "W")) == ["W", "P"]
agg_all = checked(px.agg_all_by(aggs.sum_(), "K"), ["K", "V", "W"])
agg_all_none = checked(px.agg_all_by(aggs.avg()), ["K", "V", "W"])
counted = checked(px.count_by("N"), ["N"], None)
counted_by = checked(px.count_by("N", ["W"]), ["W", "N"], None)
assert columns(px.count_by("N", "K")) == ["K", "N"]

first = checked(px.first_by("W"), ["W", "K", "V"])
last = checked(px.last_by(["K", "W"]), ["K", "W", "V"])
minimum = checked(px.min_by(), ["K", "V", "W"])
maximum = checked(px.max_by("K"), ["K", "V", "W"])
total = checked(px.sum_by("K"), ["K", "V", "W"])
absolute = checked(px.abs_sum_by("K"), ["K", "V", "W"])
average = checked(px.avg_by("K"), ["K", "V", "W"]).view(["K", "V"])
median = checked(px.median_by("K"), ["K", "V", "W"]).view(["K", "V"])
deviation = checked(px.std_by("K"), ["K", "V", "W"]).view(["K", "V"])
variance = checked(px.var_by("K"), ["K", "V", "W"]).view(["K", "V"])
weighted_sum = checked(px.weighted_sum_by("W"), ["K", "V"])
weighted_sum_by = checked(px.weighted_sum_by("W", "K"), ["K", "V"])
weighted_average = checked(px.weighted_avg_by("W"), ["W", "K", "V"])
weighted_average_by = checked(px.weighted_avg_by("W", "K"), ["K", "V"])

running = checked(px.update_by(cum_sum("S = V")), ["K", "V", "W", "S"], 1)
running_by = checked(px.update_by([cum_sum("S = V")], "W"), ["K", "V", "W", "S"], 1)
running_key = checked(px.update_by([cum_sum("K = V")], "W"), ["K", "V", "W"], 3)

fails(lambda: px.count_by("N", "Nope"), "count_by operation on the PartitionedTableProxy failed.")
fails(lambda: px.agg_by([]), "agg_by operation on the PartitionedTableProxy failed.")
fails(lambda: px.group_by("Nope"), "group-by operation on the PartitionedTableProxy failed.")
fails(lambda: px.first_by("Nope"), "first_by operation on the PartitionedTableProxy failed.")
fails(lambda: px.abs_sum_by("Nope"), "sum_by operation on the PartitionedTableProxy failed.")
fails(lambda: px.weighted_sum_by("W", "Nope"), "sum_by operation on the PartitionedTableProxy failed.")
fails(lambda: px.weighted_avg_by("W", "Nope"), "avg_by operation on the PartitionedTableProxy failed.")
fails(
    lambda: px.update_by(cum_sum("S = Nope")),
    "update-by operation on the PartitionedTableProxy failed.",
)
fails(lambda: px.agg_all_by(aggs.sum_(), "Nope"), "agg_all_by operation on the PartitionedTableProxy failed.")
