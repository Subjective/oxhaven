# Number and date formats, which are patterns in hidden columns, beside colours, and
# heat maps, whose colour is between two by where a value is in a range.
# oracle: results patterns, mixed, heat, heat_text
from deephaven import empty_table

source = empty_table(8).update([
    "X = i",
    "Y = (i % 4 == 3) ? NULL_DOUBLE : i * 1.5",
    "N = (i % 2 == 0) ? NULL_INT : i",
    "T = epochNanosToInstant(1700000000000000000L + i * 3600000000000L)",
])
patterns = source.format_columns(["Y = Decimal(`0.00`)", "X = Decimal(`#,##0`)", "T = Date(`yyyy-MM-dd`)"])
mixed = patterns.format_columns(["X = RED", "Y = Decimal(`0.0`)"])
heat = source.format_columns([
    "X = heatmap(X, 0, 7, RED, GREEN)",
    "Y = heatmap(Y, 2, 6, BLUE, WHITE)",
    "N = heatmap(N, 1, 5, RED, BLUE)",
])
heat_text = source.format_columns(["X = heatmapFg(X, 0, 7, RED, GREEN)", "Y = heatmapForeground(Y, 0, 10, BLACK, WHITE)"])
# oracle: step
# oracle: fails
# a pattern is no value of a formula
source.format_columns("Y = Y > 3 ? Decimal(`0.0`) : Decimal(`0.000`)")
