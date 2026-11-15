# new_table from one input column of each type the engine has, with nulls written as None and
# as the null constants, values beyond their type, a column with an explicit dtype, array
# columns, and a name given twice.
# oracle: results typed, with_none, with_nulls, wrapped, explicit, arrays, twice
from deephaven import dtypes, new_table
from deephaven.column import (
    InputColumn, bool_col, byte_col, char_col, datetime_col, double_col, float_col, int_col,
    long_col, short_col, string_col,
)
from deephaven.constants import (
    NULL_BYTE, NULL_CHAR, NULL_DOUBLE, NULL_FLOAT, NULL_INT, NULL_LONG, NULL_SHORT,
)

typed = new_table([
    bool_col("Bool", [True, False]),
    byte_col("Byte", [1, -1]),
    char_col("Char", "a\u00e9"),
    short_col("Short", [300, -300]),
    int_col("Int", [70000, -70000]),
    long_col("Long", [5000000000, -5000000000]),
    float_col("Float", [1.5, -1.5]),
    double_col("Double", [2.25, -2.25]),
    string_col("String", ["x", "y"]),
    datetime_col("Instant", ["2024-01-02T03:04:05.123456789 UTC", 1]),
])
with_none = new_table([
    bool_col("Bool", [None, True]),
    byte_col("Byte", [None, 1]),
    char_col("Char", [None, 97]),
    short_col("Short", [1, None]),
    int_col("Int", [None, 1]),
    long_col("Long", [1, None]),
    float_col("Float", [None, 1]),
    double_col("Double", [1, None]),
    string_col("String", [None, ""]),
    datetime_col("Instant", [None, "2024-01-02T03:04:05 ET"]),
])
with_nulls = new_table([
    byte_col("Byte", [NULL_BYTE, 1]),
    char_col("Char", [NULL_CHAR, 1]),
    short_col("Short", [1, NULL_SHORT]),
    int_col("Int", [1, NULL_INT]),
    long_col("Long", [NULL_LONG, 2]),
    float_col("Float", [NULL_FLOAT, 0.5]),
    double_col("Double", [NULL_DOUBLE, 0.5]),
    string_col("String", [None, "s"]),
])
wrapped = new_table([
    bool_col("Bool", [0, "text", [], 2.5]),
    byte_col("Byte", [200, -129, 2**40, True]),
    short_col("Short", [2**16 + 5, -32769, 2**40 + 7, False]),
    int_col("Int", [2**32 + 1, -2**31 - 1, 2**63 - 1, True]),
    float_col("Float", [1e40, -1e40, 2**24 + 1, True]),
    double_col("Double", [2**53 + 1, 10**30, -0.0, False]),
])
explicit = new_table([
    InputColumn("Short", dtypes.short, input_data=[1, 2, 3]),
    InputColumn("Text", dtypes.string, input_data=["a", None, "c"]),
    InputColumn("Flag", dtypes.bool_, input_data=[1, 0, None]),
])
arrays = new_table([
    InputColumn("Ints", dtypes.int32_array, input_data=[[1, None, 3], None, []]),
    InputColumn("Longs", dtypes.long_array, input_data=[[2**40], [], None]),
    InputColumn("Doubles", dtypes.double_array, input_data=[None, [0.5, None], [1, 2]]),
    InputColumn("Strings", dtypes.string_array, input_data=[["a", None], None, []]),
])
twice = new_table([int_col("A", [1]), string_col("B", ["b"]), double_col("A", [2.5])])
