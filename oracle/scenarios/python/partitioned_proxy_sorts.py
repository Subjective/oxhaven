# The operations of a PartitionedTableProxy that order or cut the rows of every
# constituent: head, tail, reverse, sort, sort_descending and snapshot. All of them keep
# the key columns as they are, so a filter of a key column after them is applied to the
# underlying table. A partitioned table that was sorted itself is not known to have key
# columns in its constituents any more: its proxy applies such a filter to every
# constituent. `sort` of the Python proxy does not take one column name as a sequence of
# one: it reads a string as the sequence of its characters.
# oracle: results heads, tails, reversed_rows, sorted_rows, sorted_two, sorted_directions, sorted_descending, sorted_descending_two, snapshot, one_character, sorted_partitions
from deephaven import DHError, SortDirection, empty_table
from deephaven.table import PartitionedTableProxy

src = empty_table(12).update(["K = i % 3", "V = 11 - i", "W = i % 4", "S = i % 2"])
pt = src.partition_by("K")
px = pt.proxy()


def fails(call, text):
    try:
        call()
    except DHError as error:
        assert text in str(error), str(error)
        return
    raise AssertionError("did not fail")


def rows_for_key(proxy):
    return proxy.where("K = 1").target.table.size


def checked(proxy):
    assert isinstance(proxy, PartitionedTableProxy)
    assert proxy.target.table.size == 3 and proxy.target.key_columns == ["K"]
    assert proxy.target.unique_keys and not proxy.target.constituent_changes_permitted
    assert [column.name for column in proxy.target.constituent_table_columns] == ["K", "V", "W", "S"]
    assert rows_for_key(proxy) == 1
    return proxy.target.merge()


heads = checked(px.head(2))
tails = checked(px.tail(1))
reversed_rows = checked(px.reverse())
sorted_rows = checked(px.sort(["V"]))
sorted_two = checked(px.sort(["W", "V"]))
sorted_directions = checked(px.sort(["S", "V"], [SortDirection.DESCENDING, SortDirection.ASCENDING]))
assert checked(px.sort(["W"], SortDirection.DESCENDING)).size == 12
sorted_descending = checked(px.sort_descending("V"))
sorted_descending_two = checked(px.sort_descending(["S", "W"]))
snapshot = checked(px.snapshot())
assert px.head(0).target.merge().size == 0

# "V" is a sequence of one name; "Nope" is the names N, o, p and e
one_character = checked(px.sort("V"))
fails(lambda: px.sort("Nope"), "sort operation on the PartitionedTableProxy failed.")
# "VV" is the name V twice, where there is no column VV
assert px.sort("VV").target.merge().size == 12
fails(
    lambda: px.sort(["W", "V"], [SortDirection.DESCENDING]),
    "sort operation on the PartitionedTableProxy failed.",
)
fails(lambda: px.sort(["V"], ["x"]), "sort operation on the PartitionedTableProxy failed.")
fails(lambda: px.head(-1), "head operation on the PartitionedTableProxy failed.")
fails(lambda: px.tail(-1), "tail operation on the PartitionedTableProxy failed.")
fails(
    lambda: px.sort_descending("Nope"),
    "sort_descending operation on the PartitionedTableProxy failed.",
)

# the partitioned table itself, sorted or filtered, and then a proxy of it
by_key = pt.sort("K", SortDirection.DESCENDING).proxy()
assert rows_for_key(by_key) == 3
assert by_key.where("K > 0").target.table.size == 3
sorted_partitions = by_key.where("K > 0").target.merge()
assert rows_for_key(pt.filter("K > 0").proxy()) == 1
