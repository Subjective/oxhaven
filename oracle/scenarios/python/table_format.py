# Table formats: a colour or a style per column and for the row, formats that depend on
# the row, a format of a table that has formats, and what Java refuses. A format is a
# hidden column the schema links to the column it formats.
# oracle: results colours, styles, conditional, by_row, by_column, again, row_first
from deephaven import empty_table

source = empty_table(8).update([
    "X = i",
    "Y = (i % 4 == 3) ? NULL_DOUBLE : i * 1.5",
    "S = (i % 3 == 0) ? null : `s` + i",
    "N = (i % 2 == 0) ? NULL_INT : i",
])
colours = source.format_columns(["X = RED", "Y = bgfg(BLUE, WHITE)", "S = `GREEN`"])
styles = source.format_columns([
    "* = bg(`#102030`)",
    "X = Color(`blue`)",
    "Y = fgo(RED) | bgo(BLUE)",
    "S = fg(colorRGB(1, 2, 3))",
    "N = bgfga(200, 100, 50)",
])
conditional = source.format_columns([
    "X = X > 2 ? RED : NO_FORMATTING",
    "Y = isNull(Y) ? bg(VIVID_YELLOW) : fg(colorHSL(120f, 50f, 50f))",
    "S = N",
])
by_row = source.format_row_where("X % 2 == 0", "bgfga(GREEN)")
by_column = source.format_column_where("Y", "Y > 3", "PINK")
# the format of a column that has one replaces it, where it was
again = colours.format_columns(["X = BLUE"]).format_row_where("X > 5", "`salmon`")
row_first = by_row.format_columns("X = 5")
# oracle: step
# oracle: fails
source.format_columns("Nope = RED")
# oracle: step
# oracle: fails
source.format_columns("X RED")
# oracle: step
# oracle: fails
source.format_columns("X = NoSuchColour")
# oracle: step
# oracle: fails
source.format_column_where("X", "Nope > 1", "RED")
# oracle: step
# oracle: fails
source.format_row_where("X > 1", "Y")
# oracle: step
# oracle: fails
colours.format_columns("X__TABLE_STYLE_FORMAT = RED")
