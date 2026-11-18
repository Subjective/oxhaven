//! `ColumnsToRowsTransform.columnsToRows` through `jpy`, as `deephaven.plot.express`'s
//! `dx.data.marketing` calls it (`python/deephaven/_columns_to_rows.py`).
//!
//! Every expected value, key, column type and message is what Deephaven 42.9 answered to
//! the same code in its Python console.

use super::tests::Python;
use oxhaven_contracts::graph::UpdateGraph;

const SETUP: &str = r#"
import jpy
from random import Random
from deephaven import empty_table
from deephaven.table import Table

C = jpy.get_type("io.deephaven.engine.table.impl.util.ColumnsToRowsTransform")

def raised(call):
    try:
        call()
    except BaseException as e:
        return type(e).__name__, str(e).split("\n")[0]
    return None

def shape(table):
    return [(c.name, c.data_type.j_name) for c in table.columns]
"#;

/// The body of `dx.data.marketing` of `deephaven-plugin-plotly-express` 0.22.0
/// (`data/data_generators.py`), which the test environment does not have.
const MARKETING: &str = r#"
def marketing(ticking=True):
    _ColsToRowsTransform = jpy.get_type(
        "io.deephaven.engine.table.impl.util.ColumnsToRowsTransform"
    )

    def weighted_selection(prob: float, index: int) -> bool:
        return Random(index).uniform(0, 1) < prob

    marketing_query_strings = [
        "VisitedWebsite = true",
        "Downloaded = VisitedWebsite ? weighted_selection(0.45, ii) : false",
        "PotentialCustomer = Downloaded ? weighted_selection(0.77, ii + 1) : false",
        "RequestedPrice = PotentialCustomer ? weighted_selection(0.82, ii + 2) : false",
        "InvoiceSent = RequestedPrice ? weighted_selection(0.24, ii + 3) : false",
    ]

    marketing_table = empty_table(100).update(marketing_query_strings)

    if ticking:
        from deephaven import merge, time_table
        marketing_table = merge(
            [
                marketing_table,
                time_table("PT1s")
                .update(marketing_query_strings)
                .drop_columns("Timestamp"),
            ]
        )

    return Table(
        _ColsToRowsTransform.columnsToRows(
            marketing_table.sum_by().j_table,
            "Stage",
            "Count",
            "VisitedWebsite",
            "Downloaded",
            "PotentialCustomer",
            "RequestedPrice",
            "InvoiceSent",
        )
    )
"#;

/// `dx.data.marketing(ticking=False)` has Java's rows, and the ticking one is made and
/// grows.
#[tokio::test]
async fn marketing_data_is_made() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(MARKETING);
    python.run(
        r#"
static = marketing(ticking=False)
assert shape(static) == [('Stage', 'java.lang.String'), ('Count', 'long')], shape(static)
assert not static.is_refreshing and not static.is_flat
assert static.size == 5
"#,
    );
    let table = python.table("static");
    assert_eq!(table.row_set().iter().collect::<Vec<_>>(), [0, 1, 2, 3, 4]);
    let snapshot = oxhaven_testkit::TableSnapshot::current(&*table).unwrap();
    let stages = snapshot.columns[0]
        .as_any()
        .downcast_ref::<arrow::array::StringArray>()
        .unwrap();
    let stages: Vec<&str> = stages.iter().map(Option::unwrap).collect();
    assert_eq!(
        stages,
        [
            "VisitedWebsite",
            "Downloaded",
            "PotentialCustomer",
            "RequestedPrice",
            "InvoiceSent"
        ]
    );
    let counts = snapshot.columns[1]
        .as_any()
        .downcast_ref::<arrow::array::Int64Array>()
        .unwrap();
    assert_eq!(counts.values().to_vec(), [100, 49, 39, 33, 10]);

    python.run(
        r#"
ticking = marketing()
assert ticking.is_refreshing
assert shape(ticking) == [('Stage', 'java.lang.String'), ('Count', 'long')]
"#,
    );
    let table = python.table("ticking");
    let counts = |table: &dyn oxhaven_contracts::table::Table| -> Vec<i64> {
        let snapshot = oxhaven_testkit::TableSnapshot::current(table).unwrap();
        let counts = snapshot.columns[1]
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
        counts.values().to_vec()
    };
    let before = counts(&*table);
    // the test server's graph runs a cycle when asked: let the time table add rows
    std::thread::sleep(std::time::Duration::from_millis(1100));
    python.server.state.graph.run_cycle();
    let after = counts(&*table);
    // every row of the time table visited the website; the other stages may stay
    assert!(after[0] > before[0], "{before:?} {after:?}");
    assert!(
        after.iter().zip(&before).all(|(a, b)| a >= b),
        "{before:?} {after:?}"
    );
    assert!(before
        .iter()
        .zip([100, 49, 39, 33, 10])
        .all(|(b, s)| *b >= s));
    assert_eq!(table.row_set().iter().collect::<Vec<_>>(), [0, 1, 2, 3, 4]);
}

/// The three overloads, and how jpy chooses among them.
#[tokio::test]
async fn the_overloads_are_chosen_as_jpy_chooses() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
t = empty_table(2).update(["Sym = ii==0 ? `AAPL` : `SPY`", "Val1 = (int)(ii+1)", "D1 = 7.7 + ii*1.1", "D2=9.9+ii", "Val2=(int)(ii+3)", "Val3=(int)(ii+5)", "D3=11.11+ii"])
r1 = Table(C.columnsToRows(t.j_table, "Name", "IV", "Val1", "Val2", "Val3"))
r2 = Table(C.columnsToRows(t.j_table, "Name", "IV", ["Apple","Banana","Canteloupe"], ["Val1","Val2","Val3"]))
r3 = Table(C.columnsToRows(t.j_table, "Name", ["IV","DV"], ["Apple","Banana","Canteloupe"], [["Val1","Val2","Val3"],["D1","D2","D3"]]))
assert shape(r1) == [('Sym', 'java.lang.String'), ('D1', 'double'), ('D2', 'double'), ('D3', 'double'), ('Name', 'java.lang.String'), ('IV', 'int')], shape(r1)
assert shape(r2) == shape(r1)
assert shape(r3) == [('Sym', 'java.lang.String'), ('Name', 'java.lang.String'), ('IV', 'int'), ('DV', 'double')], shape(r3)

u = empty_table(2).update(["A = (int)(ii+1)", "B = (int)(ii+10)", "C=(int)(ii+20)", "D=(int)(ii+30)", "AB=(int)(ii+40)", "CD=(int)(ii+50)", "x=(int)ii", "y=(int)ii"])
j = u.j_table
def made(result):
    table = Table(result)
    return [c.name for c in table.columns], table.size

# with five arguments a string is an array of its characters
assert made(C.columnsToRows(j, "L", "V", "AB", "CD")) == (['A', 'B', 'AB', 'CD', 'x', 'y', 'L', 'V'], 4)
assert made(C.columnsToRows(j, "L", "V", ["x"], "C")) == (['A', 'B', 'D', 'AB', 'CD', 'x', 'y', 'L', 'V'], 2)
assert made(C.columnsToRows(j, "L", "V", ("x", "y"), ("A", "C"))) == (['B', 'D', 'AB', 'CD', 'x', 'y', 'L', 'V'], 4)
assert made(C.columnsToRows(j, "L", ["V"], ["x", "y"], ["AB"])) == (['C', 'D', 'AB', 'CD', 'x', 'y', 'L', 'V'], 4)
assert made(C.columnsToRows(j, "L", "VW", ["x"], [["A"], ["B"]])) == (['C', 'D', 'AB', 'CD', 'x', 'y', 'L', 'V', 'W'], 2)
assert made(C.columnsToRows(j, "L", ["V"], ["x"], "A")) == (['B', 'C', 'D', 'AB', 'CD', 'x', 'y', 'L', 'V'], 2)
assert made(C.columnsToRows(j, "L", ["V", "W"], ["x"], ["A", ["B"]])) == (['C', 'D', 'AB', 'CD', 'x', 'y', 'L', 'V', 'W'], 2)
# with four, the fourth is one column, or the array of them
assert made(C.columnsToRows(j, "L", "V", "AB")) == (['A', 'B', 'C', 'D', 'CD', 'x', 'y', 'L', 'V'], 2)
assert made(C.columnsToRows(j, "L", "V", ["A", "B", "C"])) == (['D', 'AB', 'CD', 'x', 'y', 'L', 'V'], 6)
assert made(C.columnsToRows(j, "L", "V", ("A", "B"))) == (['C', 'D', 'AB', 'CD', 'x', 'y', 'L', 'V'], 4)

no_match = ('RuntimeError', 'no matching Java method overloads found')
for call in [
    lambda: C.columnsToRows(j, "L", "V", "A", ["C"], "D"),
    lambda: C.columnsToRows(j, "L", "V", 1, 2),
    lambda: C.columnsToRows(u, "L", "V", "A", "B", "C"),
    lambda: C.columnsToRows(j, "L", "V", ["x", None], ["A", "C"]),
    lambda: C.columnsToRows(j, "L", "V", ["A", None]),
    lambda: C.columnsToRows(j, "L", [None], ["x"], [["A"]]),
    lambda: C.columnsToRows(j, "L"),
    lambda: C.columnsToRows(j, "L", "V", b"A", b"C"),
    lambda: C.columnsToRows(j, "L", "V", [["x"]], [["A"]]),
    lambda: C.columnsToRows(j, "L", "V", True, "A"),
    lambda: C.columnsToRows(j, 1, "V", "A", "B", "C"),
]:
    assert raised(call) == no_match, raised(call)
assert raised(lambda: C.columnsToRows(j, "L", None, None, None)) == ('RuntimeError', 'ambiguous Java method call, too many matching method overloads found')
assert raised(lambda: C.columnsToRows(j, "L", None, ["x"], None)) == ('RuntimeError', 'ambiguous Java method call, too many matching method overloads found')
assert raised(lambda: C.columnsToRows(j, "L", ["V"], ["x"], [[1]])) == ('ValueError', "cannot convert a Python 'int' to a Java 'java.lang.String'")
assert raised(lambda: C.columnsToRows(j, "L", ["V"], ["x"], [1.5])) == ('ValueError', "cannot convert a Python 'float' to a Java array of type 'java.lang.String'")
"#,
    );
}

/// Java's refusals, its `null`s among them, with Java's texts.
#[tokio::test]
async fn bad_arguments_are_refused_as_java_refuses_them() {
    let python = Python::start().await;
    python.run(SETUP);
    python.run(
        r#"
u = empty_table(2).update(["A = (int)(ii+1)", "B = (int)(ii+10)", "C=(int)(ii+20)", "D=(long)ii", "x=(int)ii", "y=(int)ii"])
j = u.j_table
npe = 'java.lang.NullPointerException: '
iae = 'java.lang.IllegalArgumentException: '
available = 'available column names are [A, B, C, D, x, y]'
for call, expected in [
    (lambda: C.columnsToRows(j, "L", "V", "D", "A", "Nope"), 'io.deephaven.engine.table.impl.NoSuchColumnException: Unknown column names [Nope], ' + available),
    (lambda: C.columnsToRows(j, "L", "V", "A", None, "C"), 'io.deephaven.engine.table.impl.NoSuchColumnException: Unknown column names [null], ' + available),
    (lambda: C.columnsToRows(j, "L", ["V"], ["x"], [[None]]), 'io.deephaven.engine.table.impl.NoSuchColumnException: Unknown column names [null], ' + available),
    (lambda: C.columnsToRows(j, "L", ["V", "W"], ["x", "y"], [["A", "D"], ["B", "Nope"]]), 'io.deephaven.engine.table.impl.NoSuchColumnException: Unknown column names [Nope], ' + available),
    (lambda: C.columnsToRows(j, "L", "V", "A", "D", "B"), iae + 'Incompatible transpose types A is int, D is long'),
    (lambda: C.columnsToRows(j, "L", "V", "Nope", "D"), iae + '4 labels defined, but 1 transpose columns defined for V.'),
    (lambda: C.columnsToRows(j, "L", [], [], []), iae + 'No columns to transpose defined!'),
    (lambda: C.columnsToRows(j, "L", ["V", "W"], ["x", "y"], [["A", "C"]]), iae + 'Inconsistent transpose column definition, 2 names defined, 1 columns defined.'),
    (lambda: C.columnsToRows(j, "L", ["V", "W"], ["x", "y"], [["A", "C"], ["D"]]), iae + '2 labels defined, but 1 transpose columns defined for W.'),
    (lambda: C.columnsToRows(j, "L", ["V", "W"], ["x"], [["A", "B"], None]), iae + '1 labels defined, but 2 transpose columns defined for V.'),
    (lambda: C.columnsToRows(j, "L", "V"), 'io.deephaven.base.verify.AssertionFailure: Assertion failed: asserted newNumShiftBits < 63, instead newNumShiftBits == 64, 63 == 63.'),
    (lambda: C.columnsToRows(j, "L", "V", [], []), 'io.deephaven.base.verify.AssertionFailure: Assertion failed: asserted newNumShiftBits < 63, instead newNumShiftBits == 64, 63 == 63.'),
    (lambda: C.columnsToRows(j, "L", ["V", "W"], ["x", "y"], [["A", "C"], ["A", "C"]]), npe + 'Cannot invoke "java.lang.Class.isPrimitive()" because "type" is null'),
    (lambda: C.columnsToRows(None, "L", "V", "A", "C"), npe + 'Cannot invoke "io.deephaven.engine.table.Table.coalesce()" because "source" is null'),
    (lambda: C.columnsToRows(j, "L", None, ["x"], ["A"]), npe + 'Column names cannot be null'),
    (lambda: C.columnsToRows(j, None, "V", "A", "B", "C"), npe + 'Column names cannot be null'),
    (lambda: C.columnsToRows(j, "L", "V", None), npe + 'Cannot read the array length because "labels" is null'),
    (lambda: C.columnsToRows(j, "L", "V", "A", None), npe + 'Cannot read the array length because "transposeColumns[cc]" is null'),
    (lambda: C.columnsToRows(j, "L", ["V"], None, None), npe + 'Cannot read the array length because "transposeColumns" is null'),
    (lambda: C.columnsToRows(j, "L", None, None, [["A"]]), npe + 'Cannot read the array length because "valueColumns" is null'),
    (lambda: C.columnsToRows(j, "L", ["V", "W"], None, [["A"]]), iae + 'Inconsistent transpose column definition, 2 names defined, 1 columns defined.'),
    (lambda: C.columnsToRows(j, "L", ["V", "W"], ["x"], [None, ["A", "B"]]), npe + 'Cannot read the array length because "transposeColumns[cc]" is null'),
]:
    assert raised(call) == ('RuntimeError', expected), (raised(call), expected)

# what Java does not refuse
assert [c.name for c in Table(C.columnsToRows(j, "1 bad", "", "A", "C", "B")).columns][-2:] == ['1 bad', '']

# a column Java reads as the type of another: not ported
kind, text = raised(lambda: C.columnsToRows(j, "L", ["V", "W"], ["x", "y"], [["A", "B"], ["B", "D"]]))
assert kind == 'NotImplementedError' and text.startswith('not ported: io.deephaven.engine.table.impl.util.ColumnsToRowsTransform#columnsToRows'), text
# other members of the class are still refused
kind, text = raised(lambda: C.getClass)
assert kind == 'NotPorted' and text == 'not ported: io.deephaven.engine.table.impl.util.ColumnsToRowsTransform#getClass', text
"#,
    );
}
