# Formats and filters of the nodes of a tree, of a ticking table: formats of columns, of
# the rows and of a column where a condition holds, a filter of the rows of every node
# (a row none of whose children pass cannot be expanded), a filter that reads a format,
# a format again by a recorder of the formatted tree, a filter of the source of a
# formatted tree, and a key table that expands a row the filter drops, of nodes that are
# sorted too, of a ticking and of a static table. And what Java
# refuses: formats for nodes that have operations, a sort by a formatting column, a
# filter or a format of no column.
# oracle: views v_fmt = fmt all_keys A, v_rows = rows_fmt all_keys A, v_cols = cols_fmt all_keys A
# oracle: views v_where = big all_keys A, v_two = two all_keys A, v_pos = later all_keys A
# oracle: views v_by_fmt = by_fmt all_keys A, v_refmt = refmt all_keys A, v_src = src all_keys A
# oracle: views v_sorted = fmt_sorted all_keys A, v_chosen = over_four chosen, v_default = fmt
# oracle: views v_hidden_sorted = over_four_sorted chosen, v_static_hidden_sorted = static_sorted chosen
from deephaven import input_table, new_table
from deephaven.column import byte_col, int_col, string_col


def rows(ids, parents, names, values):
    return new_table([
        int_col("Id", ids),
        int_col("Parent", parents),
        string_col("Name", names),
        int_col("V", values),
    ])


source = input_table(
    init_table=rows(
        [1, 2, 3, 4, 5, 6, 7],
        [None, None, 1, 1, 3, 3, 2],
        ["a", "b", "c", "d", "e", "f", "g"],
        [5, 9, 3, 8, -7, 2, 1],
    ),
    key_cols="Id",
)
t = source.tree("Id", "Parent")
r = t.node_operation_recorder()
fmt = t.with_node_operations(r.format_column(["V = V > 2 ? RED : BLUE", "Name = Decimal(`0.0`)"]))
rows_fmt = t.with_node_operations(r.format_row_where("V > 2", "PINK"))
cols_fmt = t.with_node_operations(r.format_column_where("Name", "V > 2", "bgfg(BLUE, WHITE)"))
big = t.with_node_operations(r.where("V > 2"))
two = t.with_node_operations(r.where(["V > 2", "V < 9"]))
later = t.with_node_operations(r.where("i > 0"))
by_fmt = t.with_node_operations(r.format_column("V = V > 2 ? RED : NO_FORMATTING").where("V__TABLE_STYLE_FORMAT != 0"))
refmt = fmt.with_node_operations(fmt.node_operation_recorder().format_column("V = BLUE").where("V < 9"))
src = fmt.with_filters("V > 4")
fmt_sorted = fmt.with_node_operations(fmt.node_operation_recorder().sort_descending("V"))
over_four = t.with_node_operations(r.where("V > 4"))
# a key of a row the filter drops, of sorted nodes: Java fails the view of a ticking
# table, and passes the key over for a static one
over_four_sorted = t.with_node_operations(r.where("V > 4").sort_descending("V"))
static = rows([1, 2, 3, 4, 5, 6, 7], [None, None, 1, 1, 3, 3, 2], ["a", "b", "c", "d", "e", "f", "g"], [5, 9, 3, 8, -7, 2, 1]).tree("Id", "Parent")
static_sorted = static.with_node_operations(static.node_operation_recorder().where("V > 4").sort_descending("V"))
all_keys = new_table([int_col("Id", [None]), byte_col("A", [3])])
chosen = new_table([int_col("Id", [1, 3])])
# oracle: step
source.add(rows([8, 3], [4, 1], ["h", "c2"], [6, 10]))
# oracle: step
source.delete(rows([6], [None], [""], [0]).view("Id"))
# oracle: step
# oracle: fails
# formats for nodes that are sorted
t.with_node_operations(r.sort("V")).with_node_operations(r.format_column("V = RED"))
# oracle: step
# oracle: fails
# a recorder of the tree without formats, for the formatted one
fmt.with_node_operations(r.sort("V"))
# oracle: step
# oracle: fails
# formats for nodes that are filtered
big.with_node_operations(r.format_column("V = RED"))
# oracle: step
# oracle: fails
r.where("Nope > 1")
# oracle: step
# oracle: fails
r.format_column("Nope = RED")
# oracle: step
# oracle: fails
fmt.node_operation_recorder().sort("V__TABLE_STYLE_FORMAT")
# oracle: step
again = big.with_node_operations(r.where("V < 9"))
