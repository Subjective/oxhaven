//! Column arrays: `Col_`, the whole of a column as a vector over the table's rows.
//!
//! Expected values were seen on Java 42.9 unless a test says otherwise.

use super::*;
use std::sync::Mutex;

/// Four rows: `X` with a null, a `Boolean` column `Z` and a `String` column `S`.
fn table() -> TestTable {
    TestTable::new([
        ("X", ints([Some(10), Some(20), None, Some(40)])),
        ("Z", booleans([None, Some(false), Some(true), None])),
        ("S", strings([Some("a"), None, Some("c"), Some("d")])),
        ("N", ints([Some(0), None, Some(3), Some(-1)])),
        ("D", doubles([Some(0.0), Some(1.0), Some(2.0), Some(3.0)])),
    ])
}

/// The rows at keys 1 and 3 of [`table`]: positions 0 and 1.
fn sparse() -> TestTable {
    let mut table = table();
    table.rows = Arc::new(RowSet::from_keys([1, 3]));
    table
}

/// The cells of a list column, each as its elements.
fn lists(values: &ArrayRef) -> Vec<String> {
    let cells = values.as_list::<i32>().iter();
    cells
        .map(|cell| render(&cell.expect("a vector")).join(", "))
        .collect()
}

fn compile(table: &TestTable, formula: &str) -> Arc<dyn oxhaven_contracts::expr::Formula> {
    let expr = Compiler.parse(formula).expect("parses");
    Compiler
        .compile(&expr, &table.definition)
        .unwrap_or_else(|e| panic!("{formula} did not compile: {e}"))
}

/// Behavior: `IntVectorColumnWrapper#get`: the value at a position among the table's rows,
/// and null for a position that is not a row.
#[test]
fn an_index_reads_the_row_at_that_position_and_null_outside() {
    let table = table();
    table.assert("X_[i - 1]", ints([None, Some(10), Some(20), None]));
    table.assert("X_[i + 1]", ints([Some(20), None, Some(40), None]));
    table.assert("X_[ii + 2]", ints([None, Some(40), None, None]));
    table.assert("X_[1]", ints([Some(20); 4]));
    table.assert("X_[-1]", ints([None; 4]));
    table.assert("X_[4]", ints([None; 4]));
    table.assert("X_.get(i - 1L)", ints([None, Some(10), Some(20), None]));
    table.assert("X_[3 - i]", ints([Some(40), None, Some(20), Some(10)]));
    table.assert("S_[i - 1]", strings([None, Some("a"), None, Some("c")]));
    // An element is a value like any other.
    table.assert("X_[i - 1] + X_[i + 1]", ints([None, None, Some(60), None]));
    table.assert("isNull(X_[i - 1])", flags([true, false, false, true]));
}

/// A null index, and an index from a column, name no row or the row they say.
#[test]
fn an_index_from_a_column_is_read_per_row() {
    table().assert("X_[N]", ints([Some(10), None, Some(40), None]));
}

/// Positions are counted among the table's rows, not row keys.
#[test]
fn positions_are_counted_among_the_tables_rows() {
    let table = sparse();
    table.assert("X_[i - 1]", ints([None, Some(20)]));
    table.assert("X_[i + 1]", ints([Some(40), None]));
    table.assert("X_[0]", ints([Some(20), Some(20)]));
    table.assert("X_[2]", ints([None, None]));
    table.assert("X_.size()", longs([Some(2), Some(2)]));
    table.assert("sum(X_)", longs([Some(60), Some(60)]));
}

/// Behavior: `ColumnVectors#of`: the vector of the column's type, the same in every row.
#[test]
fn the_whole_column_is_a_vector_in_every_row() {
    let table = table();
    let (column_type, values) = table.evaluate("X_").unwrap();
    assert_eq!(column_type, ColumnType::vector(ColumnType::Int));
    assert_eq!(lists(&values), ["10, 20, null, 40"; 4]);
    table.assert("X_.size()", longs([Some(4); 4]));
    table.assert("sum(X_)", longs([Some(70); 4]));
    table.assert("X_.size() - i", longs([Some(4), Some(3), Some(2), Some(1)]));
    table.assert("sum(X_) + X", longs([Some(80), Some(90), None, Some(110)]));
    let (column_type, values) = table.evaluate("X_.subVector(i, i + 2)").unwrap();
    assert_eq!(column_type, ColumnType::vector(ColumnType::Int));
    assert_eq!(
        lists(&values),
        ["10, 20", "20, null", "null, 40", "40, null"]
    );
    let (_, values) = table.evaluate("X_.subVector(1, 3)").unwrap();
    assert_eq!(lists(&values), ["20, null"; 4]);
    let (column_type, values) = table.evaluate("S_").unwrap();
    assert_eq!(column_type, ColumnType::vector(ColumnType::String));
    assert_eq!(lists(&values), ["\"a\", null, \"c\", \"d\""; 4]);
}

/// Java declares a column array with its element type in a filter too, so an element of a
/// `Boolean` column is a `Boolean`: `where("Z_[1]")`, `update("P = Z_[1] && X > 5")`.
#[test]
fn an_element_of_a_boolean_column_array_is_a_boolean() {
    let table = table();
    table.assert("Z_[i - 1]", booleans([None, None, Some(false), Some(true)]));
    table.assert("Z_[2] && X > 15", flags([false, true, false, true]));
    table.assert("!Z_[1]", flags([true; 4]));
}

#[test]
fn a_formula_reports_the_column_arrays_it_reads_apart_from_its_columns() {
    let table = table();
    let shifted = compile(&table, "X_[i - 1] + Z_.size() + X_[0]");
    assert_eq!(shifted.column_arrays(), ["X", "Z"]);
    assert!(shifted.columns().is_empty());
    assert!(shifted.uses_row_variables());

    let both = compile(&table, "X_[1] + X + N");
    assert_eq!(both.column_arrays(), ["X"]);
    assert_eq!(both.columns(), ["X", "N"]);
    assert!(!both.uses_row_variables());
}

/// A previous-value read sees the previous values of the other rows.
#[test]
fn previous_values_are_read_for_a_previous_value_read() {
    let table = TestTable::with_previous([(
        "X",
        ints([Some(10), Some(20), Some(30)]),
        ints([Some(1), Some(2), Some(3)]),
    )]);
    let formula = compile(&table, "X_[i - 1] + sum(X_)");
    let now = formula.evaluate(&table, &table.rows, false).unwrap();
    assert_eq!(render(&now), ["null", "70", "80"]);
    let before = formula.evaluate(&table, &table.rows, true).unwrap();
    assert_eq!(render(&before), ["null", "7", "8"]);
}

/// A column that records how many values each read asked for.
struct Recording {
    inner: Arc<dyn ColumnSource>,
    reads: Arc<Mutex<Vec<u64>>>,
}

impl ColumnSource for Recording {
    fn column_type(&self) -> &ColumnType {
        self.inner.column_type()
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.reads.lock().unwrap().push(rows.size());
        self.inner.chunk(rows)
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.reads.lock().unwrap().push(rows.size());
        self.inner.prev_chunk(rows)
    }
}

/// A table of `size` rows with the int column `X = 0, 1, ...`, and the sizes of the reads
/// of that column.
fn recording(size: i32) -> (TestTable, Arc<Mutex<Vec<u64>>>) {
    let values: ArrayRef = Arc::new(Int32Array::from_iter_values(0..size));
    let mut table = TestTable::with_previous([(
        "X",
        (ColumnType::Int, values.clone()),
        (ColumnType::Int, values),
    )]);
    let reads = Arc::new(Mutex::new(Vec::new()));
    table.columns[0] = Arc::new(Recording {
        inner: table.columns[0].clone(),
        reads: reads.clone(),
    });
    (table, reads)
}

/// An index reads the rows it names and no others, and anything made of the whole column
/// reads the column once, whatever the number of rows evaluated.
#[test]
fn a_column_array_is_not_read_once_per_row() {
    let (table, reads) = recording(1000);
    let some = RowSet::from_keys([5, 6, 7, 500]);
    let taken = |reads: &Arc<Mutex<Vec<u64>>>| std::mem::take(&mut *reads.lock().unwrap());

    let shifted = compile(&table, "X_[i - 1]");
    let values = shifted.evaluate(&table, &some, false).unwrap();
    assert_eq!(render(&values), ["4", "5", "6", "499"]);
    assert_eq!(taken(&reads), [4]);

    // The same position for every row is one value read.
    let fixed = compile(&table, "X_[999] + X_[i]");
    let values = fixed.evaluate(&table, &some, false).unwrap();
    assert_eq!(render(&values), ["1004", "1005", "1006", "1499"]);
    assert_eq!(taken(&reads), [1, 4]);

    let whole = compile(&table, "sum(X_) + X_.size() + i");
    let values = whole.evaluate(&table, &some, false).unwrap();
    assert_eq!(render(&values), ["500505", "500506", "500507", "501000"]);
    assert_eq!(taken(&reads), [1000]);

    // A part of the column that depends on the row needs the column, once.
    let part = compile(&table, "sum(X_.subVector(i, i + 2))");
    let values = part.evaluate(&table, &some, false).unwrap();
    assert_eq!(render(&values), ["11", "13", "15", "1001"]);
    assert_eq!(taken(&reads), [1000]);
}

/// A list array holds at most `i32::MAX` elements; Java holds one vector and a reference
/// to it per row.
#[test]
fn more_elements_than_a_column_can_hold_are_refused() {
    let (table, reads) = recording(50_000);
    let failure = table.evaluate("X_").unwrap_err();
    assert!(
        failure
            .message
            .contains("io.deephaven.engine.table.vectors.ColumnVectors#of"),
        "{failure}"
    );
    // A few rows of it fit.
    let few = compile(&table, "X_");
    let values = few
        .evaluate(&table, &RowSet::from_keys([1, 2]), false)
        .unwrap();
    assert_eq!(values.as_list::<i32>().values().len(), 100_000);
    assert_eq!(reads.lock().unwrap().as_slice(), [50_000, 50_000]);
}

/// A column that is itself named `X_` is that column.
#[test]
fn a_column_of_the_name_hides_the_column_array() {
    let table = TestTable::new([
        ("X", ints([Some(1), Some(2)])),
        ("X_", longs([Some(7), Some(8)])),
    ]);
    table.assert("X_ + 1", longs([Some(8), Some(9)]));
    let formula = compile(&table, "X_ + 1");
    assert_eq!(formula.columns(), ["X_"]);
    assert!(formula.column_arrays().is_empty());
}

#[test]
fn column_arrays_that_do_not_compile() {
    let table = table();
    for formula in ["Nope_[i - 1]", "Nope_"] {
        assert!(
            matches!(table.compile_error(formula), FormulaError::Type(_)),
            "{formula}: {:?}",
            table.compile_error(formula)
        );
    }
    // Java has no `get(double)`, and a vector is not a number. The refusal is the one a
    // vector column gets.
    for formula in ["X_[D]", "X_[S]", "X_[i - 1].size()"] {
        table.compile_error(formula);
    }
    // Java sends the column array of a `Boolean` or `Instant` column as strings.
    assert!(matches!(
        table.compile_error("Z_"),
        FormulaError::Unsupported(_)
    ));
    assert!(matches!(
        table.compile_error("X > 15 ? Z_ : Z_"),
        FormulaError::Unsupported(_)
    ));
    table.assert("Z_.size() + (isNull(Z_[0]) ? 1 : 0)", longs([Some(5); 4]));
    // The column array of a column of vectors is a vector of vectors.
    let grouped = TableDefinition::new(vec![ColumnDefinition::new(
        "V",
        ColumnType::vector(ColumnType::Int),
    )])
    .unwrap();
    let expr = Compiler.parse("V_[i - 1]").unwrap();
    assert!(matches!(
        Compiler.compile(&expr, &grouped),
        Err(FormulaError::Unsupported(_))
    ));
}
