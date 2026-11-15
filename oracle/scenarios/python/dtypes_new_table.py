# new_table from one input column of each type the rules compare (not char: Arrow uint16),
# with nulls written as the null constants, and a column with an explicit dtype.
# oracle: results typed, with_nulls, explicit
from deephaven import dtypes, new_table
from deephaven.column import (
    InputColumn, bool_col, byte_col, double_col, float_col, int_col, long_col,
    short_col, string_col,
)
from deephaven.constants import NULL_DOUBLE, NULL_INT, NULL_LONG

typed = new_table([
    bool_col("Bool", [True, False]),
    byte_col("Byte", [1, -1]),
    short_col("Short", [300, -300]),
    int_col("Int", [70000, -70000]),
    long_col("Long", [5000000000, -5000000000]),
    float_col("Float", [1.5, -1.5]),
    double_col("Double", [2.25, -2.25]),
    string_col("String", ["x", "y"]),
])
with_nulls = new_table([
    int_col("Int", [1, NULL_INT]),
    long_col("Long", [NULL_LONG, 2]),
    double_col("Double", [NULL_DOUBLE, 0.5]),
    string_col("String", [None, "s"]),
])
explicit = new_table([InputColumn("Short", dtypes.short, input_data=[1, 2, 3])])
