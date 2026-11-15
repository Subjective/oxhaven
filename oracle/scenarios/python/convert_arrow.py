# A table to a pyarrow table and back, with nulls, instants and arrays; a pyarrow table
# built in Python to a table. The asserts say what the pyarrow table holds.
# oracle: results round_trip, formulas, from_arrow, lists, part, chunked, no_rows, grouped
import pyarrow as pa

from deephaven import empty_table
from deephaven.arrow import to_arrow, to_table

source = empty_table(5).update([
    "B = (byte)(i == 1 ? null : i)", "H = (short)(i == 1 ? null : i)", "I = i == 1 ? null : i",
    "L = i == 1 ? null : (long)i * 5000000000L", "F = (float)(i == 1 ? null : i * 0.5)",
    "D = i == 1 ? null : i * 0.25", "C = i == 1 ? null : (char)(65 + i)",
    "S = i == 1 ? null : `s` + i", "Z = i == 1 ? null : i % 2 == 0",
    "T = i == 1 ? null : '2024-01-01T00:00:00Z' + i * SECOND",
])
arrow = to_arrow(source)
assert isinstance(arrow, pa.Table) and arrow.num_rows == 5
assert arrow.column_names == ["B", "H", "I", "L", "F", "D", "C", "S", "Z", "T"]
assert [str(field.type) for field in arrow.schema] == [
    "int8", "int16", "int32", "int64", "float", "double", "uint16", "string", "bool",
    "timestamp[ns, tz=UTC]"]
assert all(field.nullable for field in arrow.schema)
assert [field.metadata[b"deephaven:type"] for field in arrow.schema] == [
    b"byte", b"short", b"int", b"long", b"float", b"double", b"char", b"java.lang.String",
    b"java.lang.Boolean", b"java.time.Instant"]
assert [column.null_count for column in arrow.columns] == [1] * 10
values = arrow.to_pydict()
assert values["I"] == [0, None, 2, 3, 4] and values["C"] == [65, None, 67, 68, 69]
assert values["L"] == [0, None, 10000000000, 15000000000, 20000000000]
assert values["D"] == [0.0, None, 0.5, 0.75, 1.0] and values["F"] == [0.0, None, 1.0, 1.5, 2.0]
assert values["S"] == ["s0", None, "s2", "s3", "s4"] and values["Z"] == [True, None, True, False, True]
assert arrow["T"].cast(pa.int64()).to_pylist() == [
    1704067200000000000, None, 1704067202000000000, 1704067203000000000, 1704067204000000000]
round_trip = to_table(arrow)

with_formulas = to_arrow(source, cols=["S", "Twice = I * 2"])
assert with_formulas.column_names == ["S", "Twice"]
assert with_formulas.to_pydict()["Twice"] == [0, None, 4, 6, 8]
formulas = to_table(with_formulas)

from_arrow = to_table(pa.table({
    "Bool": pa.array([True, None, False]),
    "Byte": pa.array([1, None, -128], pa.int8()),
    "Short": pa.array([1, None, -32768], pa.int16()),
    "Int": pa.array([1, None, -2**31], pa.int32()),
    "Long": pa.array([1, None, -2**63], pa.int64()),
    "Char": pa.array([65, None, 65535], pa.uint16()),
    "Float": pa.array([1.5, None, float("nan")], pa.float32()),
    "Double": pa.array([1.5, None, -1.7976931348623157e+308], pa.float64()),
    "String": pa.array(["a", None, ""]),
    "Naive": pa.array([1, None, 1704067200000000000], pa.timestamp("ns")),
    "Utc": pa.array([1, None, 1704067200000000000], pa.timestamp("ns", tz="UTC")),
}))


def elements(kind):
    # the element field named as Java names it
    return pa.list_(pa.field("", kind))


lists = to_table(pa.table({
    "Ints": pa.array([[1, None], None, []], elements(pa.int32())),
    "Strings": pa.array([["a", None], None, ["b"]], elements(pa.string())),
    "Doubles": pa.array([[1.5], None, [2.5, 3.5]], elements(pa.float64())),
    "Bools": pa.array([[True, None], None, [False]], elements(pa.bool_())),
    "Times": pa.array([[1, None], None, [2]], elements(pa.timestamp("ns", tz="UTC"))),
}))
# a table made from Arrow keeps the name the element field had
named = pa.table({"X": pa.array([[1]], pa.list_(pa.field("x", pa.int32())))})
assert str(to_arrow(to_table(named)).schema.field("X").type) == "list<x: int32>"
nested = pa.table({"Nested": pa.array([[[1], None], None, [[2, 3]]], pa.list_(pa.list_(pa.int32())))})
assert to_arrow(to_table(nested)).to_pydict() == nested.to_pydict()
letters = pa.table({"A": [1, 2, 3], "B": ["x", "y", "z"]})
part = to_table(letters, cols=["B"])
chunked = to_table(pa.concat_tables([letters, letters.slice(1, 2)]))
no_rows = to_table(to_arrow(source.head(0)))
assert to_arrow(source.head(0)).num_rows == 0 and to_arrow(empty_table(3)).num_rows == 3
assert to_table(pa.table({})).size == 0

# a vector column is a list, and comes back as an array
groups = to_arrow(source.update("K = i % 2").group_by("K").view(["K", "I", "S"]))
assert [str(field.type) for field in groups.schema] == ["int32", "list<: int32>", "list<: string>"]
assert groups.to_pydict() == {
    "K": [0, 1], "I": [[0, 2, 4], [None, 3]], "S": [["s0", "s2", "s4"], [None, "s3"]]}
grouped = to_table(groups)
