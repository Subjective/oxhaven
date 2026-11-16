# The joins of a PartitionedTableProxy. With a table, every constituent is joined with
# the table. With another proxy, the constituents of the two partitioned tables that have
# the same key are joined. Then `require_matching_keys` of the first proxy asks that the
# two have the same keys, and its `sanity_check_joins` asks of each partitioned table
# that no value of the columns the join matches exactly is in two constituents; the flags
# of the second proxy are not read. The checks were observed to fail also where the join
# itself would not: with one match column, a null in it fails the check, and a join
# without exact match columns fails it whenever two constituents have rows.
# oracle: results natural, natural_renamed, natural_two, exact, cross, cross_all, as_of, reverse_as_of, snapshot, pair_natural, pair_keys, pair_unchecked, pair_common, pair_other_name, pair_exact, pair_cross, pair_as_of, pair_snapshot, pair_null_two
from deephaven import DHError, empty_table
from deephaven.table import PartitionedTableProxy

src = empty_table(12).update(["K = i % 3", "V = i", "W = i % 4"])
pt = src.partition_by("K")
px = pt.proxy()
unchecked = pt.proxy(True, False)
right = empty_table(4).update(["W = i", "R = i * 100"])


def fails(call, text):
    try:
        call()
    except DHError as error:
        assert text in str(error), str(error)
        return
    raise AssertionError("did not fail")


def columns(proxy):
    return [column.name for column in proxy.target.constituent_table_columns]


def checked(proxy, names, rows=3, flags=(True, True)):
    assert isinstance(proxy, PartitionedTableProxy)
    assert proxy.target.table.size == rows and proxy.target.key_columns == ["K"]
    assert proxy.target.unique_keys and not proxy.target.constituent_changes_permitted
    assert (proxy.require_matching_keys, proxy.sanity_check_joins) == flags
    assert columns(proxy) == names, columns(proxy)
    # a join leaves the key columns of the first table as they are
    assert proxy.where("K = 1").target.table.size == 1
    return proxy.target.merge()


natural = checked(px.natural_join(right, "W", "R"), ["K", "V", "W", "R"])
assert columns(px.natural_join(right, "W")) == ["K", "V", "W", "R"]
natural_renamed = checked(px.natural_join(right, "W", "R2 = R"), ["K", "V", "W", "R2"])
natural_two = checked(
    px.natural_join(right.update("K = 1"), ["W", "K"], ["R"]), ["K", "V", "W", "R"]
)
exact = checked(px.exact_join(right, "W", "R"), ["K", "V", "W", "R"])
cross = checked(px.join(right, "W", "R"), ["K", "V", "W", "R"])
cross_all = checked(px.join(right.view("R").head(2)), ["K", "V", "W", "R"])
assert px.join(right.view("R").head(2), [], "R2 = R").target.merge().size == 24
as_of = checked(px.aj(right, "W >= W", "R"), ["K", "V", "W", "R"])
assert columns(px.aj(right, "W", "R")) == ["K", "V", "W", "R"]
reverse_as_of = checked(px.raj(right.view(["U = W + 1", "R"]), "W <= U"), ["K", "V", "W", "U", "R"])
snapshot = checked(px.snapshot_when(empty_table(1).update("Z = 1")), ["K", "V", "W", "Z"])
fails(
    lambda: px.natural_join(empty_table(4).update(["W = 1", "R = i"]), "W", "R"),
    "natural_join operation on the PartitionedTableProxy failed.",
)
fails(lambda: px.exact_join(right.head(2), "W", "R"), "exact_join operation on the PartitionedTableProxy failed.")
fails(lambda: px.natural_join("x", "W", "R"), "natural_join operation on the PartitionedTableProxy failed.")
fails(lambda: px.join(right, "Nope", "R"), "join operation on the PartitionedTableProxy failed.")
fails(lambda: px.aj(right, "Nope", "R"), "as-of join operation on the PartitionedTableProxy failed.")
fails(lambda: px.raj(right, "Nope", "R"), "reverse as-of join operation on the PartitionedTableProxy failed.")

# two partitioned tables with the same keys
src2 = empty_table(8).update(["K = i % 3", "W = i % 4", "R = i * 100"])
pt2 = src2.partition_by("K")
px2 = pt2.proxy()
# W is in more than one constituent of both
fails(lambda: px.natural_join(px2, "W", "R"), "natural_join operation on the PartitionedTableProxy failed.")
fails(
    lambda: px.natural_join(pt2.proxy(True, False), "W", "R"),
    "natural_join operation on the PartitionedTableProxy failed.",
)
pair_unchecked = checked(
    unchecked.natural_join(px2, "W", "R"), ["K", "V", "W", "R"], flags=(True, False)
)
pair_keys = checked(px.natural_join(px2, ["K", "W"], "R"), ["K", "V", "W", "R"])
src3 = empty_table(6).update(["K = i % 3", "U = i", "R = i * 100"]).where("U < 3")
px3 = src3.partition_by("K").proxy()
pair_natural = checked(px.natural_join(px3, "V = U", "R"), ["K", "V", "W", "R"])
fails(lambda: px.exact_join(px3, "V = U", "R"), "exact_join operation on the PartitionedTableProxy failed.")
pair_exact = checked(px.head(1).exact_join(px3, "V = U", "R"), ["K", "V", "W", "R"])
pair_cross = checked(px.join(px3, "V = U", "R"), ["K", "V", "W", "R"])
# no exact match column: the check fails when two constituents have rows
fails(lambda: px.join(px3.view("R")), "join operation on the PartitionedTableProxy failed.")
fails(lambda: px.aj(px3, "V >= U", "R"), "as-of join operation on the PartitionedTableProxy failed.")
fails(lambda: px.raj(px3, "V <= U", "R"), "reverse as-of join operation on the PartitionedTableProxy failed.")
assert unchecked.join(px3.view("R")).target.merge().size == 12
pair_as_of = checked(
    px.aj(px3.view(["K", "U", "R"]), ["K", "V >= U"], "R"), ["K", "V", "W", "U", "R"]
)
pair_snapshot = checked(px.snapshot_when(px3.view(["U2 = U"])), ["K", "V", "W", "U2"])

# keys that do not match
src4 = empty_table(6).update(["K = i % 2", "U = i", "R = i * 100"])
pt4 = src4.partition_by("K")
fails(
    lambda: px.natural_join(pt4.proxy(), "V = U", "R"),
    "natural_join operation on the PartitionedTableProxy failed.",
)
fails(
    lambda: px.natural_join(pt4.proxy(False, True), "V = U", "R"),
    "natural_join operation on the PartitionedTableProxy failed.",
)
pair_common = checked(
    pt.proxy(False, True).natural_join(pt4.proxy(), "V = U", "R"),
    ["K", "V", "W", "R"],
    rows=2,
    flags=(False, True),
)
# key columns of other names are matched by position; another number of them is refused
src5 = empty_table(6).update(["Q = i % 3", "U = i", "R = i * 100"])
pair_other_name = checked(
    px.natural_join(src5.partition_by("Q").proxy(), "V = U", "R"), ["K", "V", "W", "R"]
)
fails(
    lambda: px.natural_join(src5.partition_by(["Q", "U"]).proxy(), "V = U", "R"),
    "natural_join operation on the PartitionedTableProxy failed.",
)
# the other argument is a table or a proxy
fails(lambda: px.natural_join(pt2, "W", "R"), "natural_join operation on the PartitionedTableProxy failed.")
fails(lambda: px.where_in(px3, "V = U"), "where_in operation on the PartitionedTableProxy failed.")

# a null in the one match column fails the check; in one of two it does not
nulls = empty_table(6).update(["K = i % 3", "V = i == 4 ? null : i", "S = i == 4 ? null : `s` + i"])
others = empty_table(6).update(["K = i % 3", "U = i", "R = i * 100", "T = `s` + i"])
with_nulls = nulls.partition_by("K").proxy()
with_others = others.partition_by("K").proxy()
fails(
    lambda: with_nulls.natural_join(with_others, "V = U", "R"),
    "natural_join operation on the PartitionedTableProxy failed.",
)
fails(
    lambda: with_nulls.natural_join(with_others, "S = T", "R"),
    "natural_join operation on the PartitionedTableProxy failed.",
)
pair_null_two = with_nulls.natural_join(with_others, ["K", "V = U"], "R").target.merge()
assert nulls.partition_by("K").proxy(True, False).natural_join(with_others, "V = U", "R").target.merge().size == 6
