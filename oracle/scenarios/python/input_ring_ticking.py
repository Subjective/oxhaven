# Ring tables that tick: of an append-only input table, started with its rows and
# without, and given more rows in one cycle than the ring holds; of a keyed input table,
# whose deletes the ring does not follow; and of a static table.
# oracle: results ring, later, wide, of_keyed, of_static, shapes
from deephaven import empty_table, input_table, new_table, ring_table
from deephaven.column import bool_col, int_col, string_col


def rows(keys):
    texts = [None if k % 3 == 0 else str(k) for k in keys]
    return new_table([int_col("K", keys), string_col("S", texts), bool_col("B", [k % 2 == 0 for k in keys])])


source = input_table(init_table=rows([1, 2, 3]))
ring = ring_table(source, 2)
later = ring_table(source, 2, initialize=False)
wide = ring_table(source, 5)
keyed = input_table(init_table=rows([1, 2, 3]), key_cols="K")
of_keyed = ring_table(keyed, 2)
of_static = ring_table(empty_table(5).update("X = i"), 2)
shapes = new_table(
    [
        string_col("Table", ["ring", "of_static"]),
        bool_col("Refreshing", [ring.is_refreshing, of_static.is_refreshing]),
        bool_col("Flat", [ring.is_flat, of_static.is_flat]),
        int_col("Attributes", [len(ring.attributes()), len(of_static.attributes())]),
    ]
)
# oracle: step
source.add(rows([4]))
keyed.delete(rows([3]).view("K"))
# oracle: step
source.add(rows([5, 6, 7, 8, 9, 10, 11]))
keyed.add(rows([7]))
# oracle: step
source.add(rows([12]))
keyed.delete(rows([1, 2]).view("K"))
# oracle: step
# oracle: fails
ring_table(source, 0)
# oracle: step
# oracle: fails
ring_table(empty_table(1).update("X = i"), 2, initialize=False)
