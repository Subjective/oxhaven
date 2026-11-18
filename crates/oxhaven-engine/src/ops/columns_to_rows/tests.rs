//! `columns_to_rows`. The expected values, keys, updates and messages are what Deephaven
//! 42.9 answered for the same calls through `jpy` in its Python console.

use super::{columns_to_rows, ColumnsToRowsError};
use crate::ops::sort::testing::{
    cells, check_random, keys, keys_of, operation, shifts, static_table, Ticking,
};
use arrow::array::{Array, TimestampNanosecondArray, UInt16Array};
use arrow::array::{ArrayRef, BooleanArray, Float32Array, Int16Array, Int8Array, StringArray};
use arrow::compute::interleave;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_testkit::{
    doubles, ints, list_of, longs, strings, ReferenceTable, TableChange, TableSnapshot,
};
use std::sync::Arc;

fn names(values: &[&str]) -> Vec<String> {
    values.iter().map(|value| value.to_string()).collect()
}

/// `columnsToRows(source, label, value_columns, labels, groups)`.
fn transform(
    source: &Arc<dyn Table>,
    label: &str,
    value_columns: &[&str],
    labels: &[&str],
    groups: &[&[&str]],
) -> Result<Arc<dyn Table>, ColumnsToRowsError> {
    let groups: Vec<Vec<String>> = groups.iter().map(|group| names(group)).collect();
    columns_to_rows(
        source,
        label,
        &names(value_columns),
        &names(labels),
        &groups,
    )
}

/// `columnsToRows(source, label, value, columns...)`: the column names are the labels.
fn named(
    source: &Arc<dyn Table>,
    label: &str,
    value: &str,
    columns: &[&str],
) -> Result<Arc<dyn Table>, ColumnsToRowsError> {
    transform(source, label, &[value], columns, &[columns])
}

/// The names and types of the columns of `table`.
fn columns(table: &dyn Table) -> Vec<(String, ColumnType)> {
    let declared = table.definition().columns().iter();
    declared
        .map(|column| (column.name.clone(), column.column_type.clone()))
        .collect()
}

fn column_names(table: &dyn Table) -> Vec<String> {
    columns(table).into_iter().map(|(name, _)| name).collect()
}

/// The values of the column `name`, as text.
fn column(table: &dyn Table, name: &str) -> Vec<String> {
    cells(table, table.definition().index_of(name).unwrap())
}

/// The table of the example of Java's documentation of the class.
fn example() -> Arc<dyn Table> {
    static_table(&[
        ("Sym", ColumnType::String, strings(["AAPL", "SPY"])),
        ("Val1", ColumnType::Int, ints([1, 2])),
        ("D1", ColumnType::Double, doubles([7.7, 8.8])),
        ("D2", ColumnType::Double, doubles([9.9, 10.9])),
        ("Val2", ColumnType::Int, ints([3, 4])),
        ("Val3", ColumnType::Int, ints([5, 6])),
        ("D3", ColumnType::Double, doubles([11.11, 12.11])),
    ])
}

/// The error's text as the Python console shows Java's: the class and the message.
fn java_text(error: ColumnsToRowsError) -> String {
    format!("{}: {error}", error.java_exception())
}

#[test]
fn two_value_columns_make_a_row_per_label() {
    let result = transform(
        &example(),
        "Name",
        &["IV", "DV"],
        &["Apple", "Banana", "Canteloupe"],
        &[&["Val1", "Val2", "Val3"], &["D1", "D2", "D3"]],
    )
    .unwrap();
    assert_eq!(
        columns(&*result),
        [
            ("Sym".to_string(), ColumnType::String),
            ("Name".to_string(), ColumnType::String),
            ("IV".to_string(), ColumnType::Int),
            ("DV".to_string(), ColumnType::Double),
        ]
    );
    assert_eq!(keys_of(&*result), [0, 1, 2, 4, 5, 6]);
    assert_eq!(
        column(&*result, "Sym"),
        ["AAPL", "AAPL", "AAPL", "SPY", "SPY", "SPY"]
    );
    let labels = ["Apple", "Banana", "Canteloupe"];
    assert_eq!(column(&*result, "Name"), [labels, labels].concat());
    assert_eq!(column(&*result, "IV"), ["1", "3", "5", "2", "4", "6"]);
    assert_eq!(
        column(&*result, "DV"),
        ["7.7", "9.9", "11.11", "8.8", "10.9", "12.11"]
    );
    assert!(!result.is_refreshing());
    assert!(!result.is_flat());
}

#[test]
fn the_column_names_are_the_labels_by_default() {
    let result = named(&example(), "Name", "IV", &["Val1", "Val2", "Val3"]).unwrap();
    assert_eq!(
        column_names(&*result),
        ["Sym", "D1", "D2", "D3", "Name", "IV"]
    );
    assert_eq!(column(&*result, "Name")[..3], ["Val1", "Val2", "Val3"]);
    assert_eq!(column(&*result, "IV"), ["1", "3", "5", "2", "4", "6"]);
    assert_eq!(
        column(&*result, "D1"),
        ["7.7", "7.7", "7.7", "8.8", "8.8", "8.8"]
    );
}

#[test]
fn row_keys_leave_room_for_the_next_power_of_two() {
    let source = static_table(&[
        ("A", ColumnType::Int, ints([1, 2])),
        ("B", ColumnType::Int, ints([10, 11])),
        ("C", ColumnType::Int, ints([20, 21])),
        ("x", ColumnType::Int, ints([0, 1])),
        ("y", ColumnType::Int, ints([0, 1])),
        ("D", ColumnType::Long, longs([0, 1])),
    ]);
    // one label: the parent's keys, and its columns as they are
    let one = named(&source, "L", "V", &["A"]).unwrap();
    assert_eq!(keys_of(&*one), [0, 1]);
    assert_eq!(column(&*one, "V"), ["1", "2"]);
    let two = named(&source, "L", "V", &["A", "B"]).unwrap();
    assert_eq!(keys_of(&*two), [0, 1, 2, 3]);
    let five = named(&source, "L", "V", &["A", "B", "C", "x", "y"]).unwrap();
    assert_eq!(keys_of(&*five), [0, 1, 2, 3, 4, 8, 9, 10, 11, 12]);
    assert_eq!(column_names(&*five), ["D", "L", "V"]);
    assert_eq!(
        column(&*five, "V"),
        ["1", "10", "20", "0", "0", "2", "11", "21", "1", "1"]
    );

    // sparse parent keys
    let sparse: Arc<dyn Table> = ReferenceTable::new_static(
        source.definition().clone(),
        RowSet::from_keys([3, 9]),
        (0..6)
            .map(|c| TableSnapshot::current(&*source).unwrap().columns[c].clone())
            .collect(),
    );
    let three = named(&sparse, "L", "V", &["A", "B", "C"]).unwrap();
    assert_eq!(keys_of(&*three), [12, 13, 14, 36, 37, 38]);
    assert_eq!(column(&*three, "V"), ["1", "10", "20", "2", "11", "21"]);
}

#[test]
fn a_name_the_result_has_replaces_that_column_in_place() {
    let source = static_table(&[
        ("Sym", ColumnType::String, strings(["AAPL", "SPY"])),
        ("A", ColumnType::Int, ints([1, 2])),
        ("B", ColumnType::Long, longs([10, 11])),
        ("C", ColumnType::Int, ints([20, 21])),
        ("D", ColumnType::Double, doubles([7.5, 8.5])),
        ("S", ColumnType::String, strings(["s0", "s1"])),
    ]);
    // label named as a column that is not transposed
    let result = transform(&source, "Sym", &["V"], &["A"], &[&["C"]]).unwrap();
    assert_eq!(column_names(&*result), ["Sym", "A", "B", "D", "S", "V"]);
    assert_eq!(column(&*result, "Sym"), ["A", "A"]);
    assert_eq!(column(&*result, "V"), ["20", "21"]);
    // value column named so
    let result = transform(&source, "L", &["Sym"], &["A"], &[&["C"]]).unwrap();
    assert_eq!(column_names(&*result), ["Sym", "A", "B", "D", "S", "L"]);
    assert_eq!(columns(&*result)[0].1, ColumnType::Int);
    assert_eq!(column(&*result, "Sym"), ["20", "21"]);
    // label and value of one name: the value column
    let result = transform(&source, "X", &["X"], &["A"], &[&["C"]]).unwrap();
    assert_eq!(column_names(&*result), ["Sym", "A", "B", "D", "S", "X"]);
    assert_eq!(column(&*result, "X"), ["20", "21"]);
    // value column named as a transposed column
    let result = transform(&source, "L", &["A"], &["A"], &[&["C"]]).unwrap();
    assert_eq!(column_names(&*result), ["Sym", "A", "B", "D", "S", "L"]);
    assert_eq!(column(&*result, "A"), ["20", "21"]);
    // two value columns of one name: the last
    let result = transform(
        &source,
        "L",
        &["V", "V"],
        &["x", "y"],
        &[&["A", "C"], &["B", "B"]],
    );
    let result = result.unwrap();
    assert_eq!(column_names(&*result), ["Sym", "D", "S", "L", "V"]);
    assert_eq!(column(&*result, "V"), ["10", "10", "11", "11"]);
    // a column twice in one group, and a column in two groups of one type
    let result = named(&source, "L", "V", &["A", "A", "C"]).unwrap();
    assert_eq!(column(&*result, "L"), ["A", "A", "C", "A", "A", "C"]);
    assert_eq!(column(&*result, "V"), ["1", "1", "20", "2", "2", "21"]);
    let result = transform(&source, "L", &["V", "W"], &["x"], &[&["A"], &["A"]]).unwrap();
    assert_eq!(column(&*result, "W"), ["1", "2"]);
    // names Java does not check
    let result = transform(&source, "", &["1 bad"], &["A"], &[&["C"]]).unwrap();
    assert_eq!(
        column_names(&*result),
        ["Sym", "A", "B", "D", "S", "", "1 bad"]
    );
}

#[test]
fn every_column_type_is_transposed_and_repeated() {
    let instants = |values: [Option<i64>; 2]| -> ArrayRef {
        Arc::new(TimestampNanosecondArray::from(values.to_vec()).with_timezone("UTC"))
    };
    let lists = |column_type: &ColumnType, lengths: [Option<usize>; 2], values: &[i64]| {
        list_of(column_type, lengths, longs(values.iter().copied()))
    };
    let array = ColumnType::array(ColumnType::Long);
    let vector = ColumnType::vector(ColumnType::Long);
    let typed: Vec<(ColumnType, ArrayRef, ArrayRef)> = vec![
        (
            ColumnType::Boolean,
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            Arc::new(BooleanArray::from(vec![Some(false), Some(true)])),
        ),
        (
            ColumnType::Byte,
            Arc::new(Int8Array::from(vec![Some(1), None])),
            Arc::new(Int8Array::from(vec![Some(2), Some(3)])),
        ),
        (
            ColumnType::Char,
            Arc::new(UInt16Array::from(vec![Some(99), None])),
            Arc::new(UInt16Array::from(vec![Some(100), Some(101)])),
        ),
        (
            ColumnType::Short,
            Arc::new(Int16Array::from(vec![Some(1), None])),
            Arc::new(Int16Array::from(vec![Some(2), Some(3)])),
        ),
        (ColumnType::Int, ints([1, 2]), ints([3, 4])),
        (ColumnType::Long, longs([1, 2]), longs([3, 4])),
        (
            ColumnType::Float,
            Arc::new(Float32Array::from(vec![Some(1.5), None])),
            Arc::new(Float32Array::from(vec![Some(2.5), Some(-0.0)])),
        ),
        (
            ColumnType::Double,
            doubles([1.5, 2.5]),
            doubles([3.5, -0.0]),
        ),
        (
            ColumnType::String,
            Arc::new(StringArray::from(vec![Some("a"), None])),
            Arc::new(StringArray::from(vec![Some("b"), Some("")])),
        ),
        (
            ColumnType::Instant,
            instants([Some(1), None]),
            instants([Some(2), Some(3)]),
        ),
        (
            array.clone(),
            lists(&array, [Some(2), None], &[1, 5]),
            lists(&array, [Some(0), Some(1)], &[2]),
        ),
        (
            vector.clone(),
            lists(&vector, [Some(1), None], &[1]),
            lists(&vector, [None, Some(2)], &[3, 4]),
        ),
    ];
    for (column_type, first, second) in typed {
        let source = static_table(&[
            ("K", column_type.clone(), first.clone()),
            ("P", column_type.clone(), first.clone()),
            ("Q", column_type.clone(), second.clone()),
        ]);
        let result = named(&source, "L", "V", &["P", "Q"]).unwrap();
        assert_eq!(columns(&*result)[2], ("V".to_string(), column_type.clone()));
        let values = TableSnapshot::current(&*result).unwrap().columns;
        let parts: [&dyn Array; 2] = [first.as_ref(), second.as_ref()];
        let expected = interleave(&parts, &[(0, 0), (1, 0), (0, 1), (1, 1)]).unwrap();
        assert_eq!(values[2].as_ref(), expected.as_ref(), "{column_type:?}");
        let expected = interleave(&parts, &[(0, 0), (0, 0), (0, 1), (0, 1)]).unwrap();
        assert_eq!(values[0].as_ref(), expected.as_ref(), "{column_type:?}");
        // and read by key, with a null key, as a join reads them
        let read = result.column(2).chunk_by_keys(&[3, -1, 0], false).unwrap();
        let expected = interleave(&parts, &[(1, 1), (0, 1), (0, 0)]).unwrap();
        assert_eq!(read.len(), 3);
        assert!(read.is_null(1), "{column_type:?}");
        assert_eq!(read.slice(0, 1).as_ref(), expected.slice(0, 1).as_ref());
        assert_eq!(read.slice(2, 1).as_ref(), expected.slice(2, 1).as_ref());
    }
}

#[test]
fn bad_arguments_are_refused_with_java_messages() {
    let source = static_table(&[
        ("I", ColumnType::Int, ints([1])),
        ("L", ColumnType::Long, longs([1])),
        ("S", ColumnType::String, strings(["s"])),
        (
            "Ia",
            ColumnType::array(ColumnType::Int),
            list_of(&ColumnType::array(ColumnType::Int), [Some(1)], ints([1])),
        ),
        (
            "Iv",
            ColumnType::vector(ColumnType::Int),
            list_of(&ColumnType::vector(ColumnType::Int), [Some(1)], ints([1])),
        ),
        (
            "B",
            ColumnType::Boolean,
            Arc::new(BooleanArray::from(vec![true])),
        ),
        ("Ch", ColumnType::Char, Arc::new(UInt16Array::from(vec![1]))),
        ("I2", ColumnType::Int, ints([2])),
    ]);
    let refused = |result: Result<Arc<dyn Table>, ColumnsToRowsError>| {
        java_text(result.err().expect("a refusal"))
    };
    assert_eq!(
        refused(transform(&source, "L", &[], &[], &[])),
        "java.lang.IllegalArgumentException: No columns to transpose defined!"
    );
    assert_eq!(
        refused(transform(
            &source,
            "L",
            &["V", "W"],
            &["x", "y"],
            &[&["I", "I2"]]
        )),
        "java.lang.IllegalArgumentException: Inconsistent transpose column definition, 2 \
         names defined, 1 columns defined."
    );
    assert_eq!(
        refused(transform(
            &source,
            "L",
            &["V", "W"],
            &["x", "y"],
            &[&["I", "I2"], &["L"]]
        )),
        "java.lang.IllegalArgumentException: 2 labels defined, but 1 transpose columns \
         defined for W."
    );
    assert_eq!(
        refused(transform(&source, "L", &["V"], &[], &[&[]])),
        "io.deephaven.base.verify.AssertionFailure: Assertion failed: asserted \
         newNumShiftBits < 63, instead newNumShiftBits == 64, 63 == 63."
    );
    assert_eq!(
        refused(named(&source, "Lab", "V", &["L", "I", "Nope"])),
        "io.deephaven.engine.table.impl.NoSuchColumnException: Unknown column names [Nope], \
         available column names are [I, L, S, Ia, Iv, B, Ch, I2]"
    );
    // the first in the parent's order
    assert_eq!(
        refused(named(&source, "Lab", "V", &["I2", "I", "L"])),
        "java.lang.IllegalArgumentException: Incompatible transpose types I is int, L is long"
    );
    for (name, class) in [
        ("S", "class java.lang.String"),
        ("Ia", "class [I"),
        ("Iv", "interface io.deephaven.vector.IntVector"),
        ("B", "class java.lang.Boolean"),
        ("Ch", "char"),
    ] {
        assert_eq!(
            refused(named(&source, "Lab", "V", &[name, "I2", "I2"])),
            format!(
                "java.lang.IllegalArgumentException: Incompatible transpose types {name} is \
                 {class}, I2 is int"
            )
        );
    }
    // a value column whose columns are all in an earlier group, with more than one label
    assert_eq!(
        refused(transform(
            &source,
            "Lab",
            &["V", "W"],
            &["x", "y"],
            &[&["I", "I2"], &["I", "I2"]]
        )),
        "java.lang.NullPointerException: Cannot invoke \"java.lang.Class.isPrimitive()\" \
         because \"type\" is null"
    );
}

#[test]
fn a_column_read_as_another_type_is_not_ported() {
    let source = static_table(&[
        ("A", ColumnType::Int, ints([1])),
        ("B", ColumnType::Int, ints([10])),
        ("D", ColumnType::Long, longs([0])),
    ]);
    // Java makes W a long column that reads the int column B
    let error = transform(
        &source,
        "L",
        &["V", "W"],
        &["x", "y"],
        &[&["A", "B"], &["B", "D"]],
    );
    let error = error.err().unwrap();
    assert!(matches!(error, ColumnsToRowsError::NotPorted(_)), "{error}");
    assert!(error.to_string().starts_with(
        "not ported: io.deephaven.engine.table.impl.util.ColumnsToRowsTransform#columnsToRows"
    ));
}

#[test]
fn row_keys_beyond_the_last_are_not_ported() {
    let source: Arc<dyn Table> = ReferenceTable::new_static(
        oxhaven_testkit::definition(&[("A", ColumnType::Int), ("B", ColumnType::Int)]),
        RowSet::from_keys([0, (RowKey::MAX >> 1) + 1]),
        vec![ints([1, 2]), ints([3, 4])],
    );
    let error = named(&source, "L", "V", &["A", "B"]).err().unwrap();
    assert!(matches!(error, ColumnsToRowsError::NotPorted(_)), "{error}");
    let fits: Arc<dyn Table> = ReferenceTable::new_static(
        oxhaven_testkit::definition(&[("A", ColumnType::Int), ("B", ColumnType::Int)]),
        RowSet::from_keys([0, RowKey::MAX >> 1]),
        vec![ints([1, 2]), ints([3, 4])],
    );
    let result = named(&fits, "L", "V", &["A", "B"]).unwrap();
    assert_eq!(keys_of(&*result), [0, 1, RowKey::MAX - 1, RowKey::MAX]);
    assert_eq!(column(&*result, "V"), ["1", "3", "2", "4"]);
}

/// A refreshing source of `K`, `X`, `A`, `B` and `C`, as Java's probe had them.
fn ticking(build: impl Fn(&Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync + 'static) -> Ticking {
    Ticking::new(
        [0, 1, 2],
        &[
            ("K", ColumnType::String, strings(["k1", "k2", "k3"])),
            ("X", ColumnType::Int, ints([10, 20, 30])),
            ("A", ColumnType::Int, ints([1, 1, 1])),
            ("B", ColumnType::Int, ints([2, 2, 2])),
            ("C", ColumnType::Int, ints([3, 3, 3])),
        ],
        build,
    )
}

const A: usize = 2;
const B: usize = 3;
const C: usize = 4;

/// The update as Java's listener printed it: added, removed, modified, shifts and the
/// names of the modified columns.
type Shown = (
    Vec<RowKey>,
    Vec<RowKey>,
    Vec<RowKey>,
    Vec<(RowKey, RowKey, i64)>,
    Vec<String>,
);

fn shown(result: &dyn Table, update: TableUpdate) -> Shown {
    let declared = result.definition().columns();
    let modified_columns = match &update.modified_columns {
        ModifiedColumnSet::All => vec!["ALL".to_string()],
        ModifiedColumnSet::Columns(columns) => {
            columns.iter().map(|&c| declared[c].name.clone()).collect()
        }
    };
    (
        keys(&update.added),
        keys(&update.removed),
        keys(&update.modified),
        shifts(&update),
        modified_columns,
    )
}

fn modify(column: usize, key: RowKey, value: i32) -> TableChange {
    TableChange::new().modify([key], [(column, ints([value]))])
}

#[test]
fn a_change_of_a_transposed_column_modifies_the_rows_of_its_label() {
    let rig = ticking(|source| named(source, "L", "V", &["A", "B", "C"]).unwrap());
    let step = |change| shown(&*rig.result, rig.step(change).unwrap());
    assert_eq!(
        step(modify(A, 1, 100)),
        (vec![], vec![], vec![4], vec![], names(&["V"]))
    );
    assert_eq!(
        step(modify(B, 1, 200)),
        (vec![], vec![], vec![5], vec![], names(&["V"]))
    );
    assert_eq!(
        step(modify(C, 1, 300)),
        (vec![], vec![], vec![6], vec![], names(&["V"]))
    );
    // a column that is not transposed: every row of the parent row
    let change = TableChange::new().modify([2], [(0, strings(["k3"])), (1, ints([31]))]);
    assert_eq!(
        step(change),
        (vec![], vec![], vec![8, 9, 10], vec![], names(&["K", "X"]))
    );
    // two of three labels
    let change = TableChange::new().modify([0], [(A, ints([7])), (C, ints([8]))]);
    assert_eq!(
        step(change),
        (vec![], vec![], vec![0, 2], vec![], names(&["V"]))
    );
    // every label
    let change = TableChange::new().modify([0], [(A, ints([9])), (B, ints([9])), (C, ints([9]))]);
    assert_eq!(
        step(change),
        (vec![], vec![], vec![0, 1, 2], vec![], names(&["V"]))
    );
}

#[test]
fn adds_removes_and_shifts_cover_every_row_of_a_parent_row() {
    let rig = ticking(|source| named(source, "L", "V", &["A", "B", "C"]).unwrap());
    let step = |change| shown(&*rig.result, rig.step(change).unwrap());
    // as Java's flattened table when a row is added before the others
    let add = TableChange::new().shift(0, 2, 1).add(
        [0],
        vec![strings(["k0"]), ints([5]), ints([1]), ints([2]), ints([3])],
    );
    assert_eq!(
        step(add),
        (vec![0, 1, 2], vec![], vec![], vec![(0, 11, 4)], vec![])
    );
    // and when the second is removed
    let remove = TableChange::new().remove([1]).shift(2, 3, -1);
    assert_eq!(
        step(remove),
        (vec![], vec![4, 5, 6], vec![], vec![(8, 15, -4)], vec![])
    );
    assert_eq!(keys_of(&*rig.result), [0, 1, 2, 4, 5, 6, 8, 9, 10]);
    assert_eq!(
        column(&*rig.result, "K"),
        ["k0", "k0", "k0", "k2", "k2", "k2", "k3", "k3", "k3"]
    );
}

#[test]
fn one_label_and_a_label_in_place_of_a_column() {
    // one label: the parent's keys
    let rig = ticking(|source| transform(source, "L", &["V"], &["a"], &[&["B"]]).unwrap());
    let step = |change| shown(&*rig.result, rig.step(change).unwrap());
    assert_eq!(
        step(modify(A, 1, 100)),
        (vec![], vec![], vec![1], vec![], names(&["A"]))
    );
    assert_eq!(
        step(modify(B, 1, 200)),
        (vec![], vec![], vec![1], vec![], names(&["V"]))
    );
    // the label column in place of X changes with X
    let rig = ticking(|source| {
        transform(source, "X", &["V"], &["a", "b", "c"], &[&["A", "B", "C"]]).unwrap()
    });
    let step = |change| shown(&*rig.result, rig.step(change).unwrap());
    assert_eq!(
        step(modify(A, 1, 100)),
        (vec![], vec![], vec![4], vec![], names(&["V"]))
    );
    assert_eq!(
        step(modify(1, 2, 31)),
        (vec![], vec![], vec![8, 9, 10], vec![], names(&["X"]))
    );
}

#[test]
fn a_value_column_replaced_by_one_of_its_name_still_reports_changes() {
    // the first group's V is gone, but Java names V when its columns change
    let rig = ticking(|source| {
        transform(
            source,
            "L",
            &["V", "V"],
            &["x", "y"],
            &[&["A", "B"], &["X", "C"]],
        )
        .unwrap()
    });
    let step = |change| shown(&*rig.result, rig.step(change).unwrap());
    assert_eq!(
        step(modify(A, 1, 100)),
        (vec![], vec![], vec![2], vec![], names(&["V"]))
    );
    assert_eq!(
        step(modify(C, 1, 100)),
        (vec![], vec![], vec![3], vec![], names(&["V"]))
    );
}

#[test]
fn a_failed_parent_fails_the_result() {
    let rig = ticking(|source| named(source, "L", "V", &["A", "B"]).unwrap());
    assert_eq!(rig.fail("broken").message, "broken");
}

#[test]
fn inconsistent_java_updates_are_not_ported_for_a_refreshing_parent() {
    let rig = ticking(|source| source.clone());
    let source = rig.result.clone();
    for (labels, groups) in [
        (&["a", "b"][..], &[&["B", "A"][..]][..]),
        (&["a", "b", "c"][..], &[&["A", "A", "C"][..]][..]),
        (&["a", "b"][..], &[&["A", "B"][..], &["C", "A"][..]][..]),
    ] {
        let values: Vec<String> = (0..groups.len()).map(|g| format!("V{g}")).collect();
        let values: Vec<&str> = values.iter().map(String::as_str).collect();
        let error = transform(&source, "L", &values, labels, groups)
            .err()
            .unwrap();
        assert!(matches!(error, ColumnsToRowsError::NotPorted(_)), "{error}");
        // a static copy is fine
        let copy = crate::ops::sort::testing::frozen(&*source);
        transform(&copy, "L", &values, labels, groups).unwrap();
    }
}

#[test]
fn random_updates_match_a_transform_of_a_static_copy() {
    let columns = [
        ("K", ColumnType::String),
        ("P", ColumnType::Long),
        ("Q", ColumnType::Long),
        ("R", ColumnType::Long),
        ("M", ColumnType::Long),
        ("N", ColumnType::Long),
        ("S", ColumnType::Double),
        ("T", ColumnType::Double),
    ];
    check_random(
        0..60,
        30,
        1,
        &columns,
        &[
            operation("three", |s| {
                named(&s[0], "L", "V", &["P", "Q", "R"]).unwrap()
            }),
            operation("two groups", |s| {
                let groups: &[&[&str]] = &[&["P", "R"], &["S", "T"]];
                transform(&s[0], "L", &["V", "W"], &["x", "y"], groups).unwrap()
            }),
            operation("five", |s| {
                named(&s[0], "L", "V", &["P", "Q", "R", "M", "N"]).unwrap()
            }),
            operation("one, label in place of K", |s| {
                transform(&s[0], "K", &["V"], &["a"], &[&["Q"]]).unwrap()
            }),
            operation("value in place of K", |s| {
                transform(
                    &s[0],
                    "L",
                    &["K"],
                    &["a", "b", "c", "d"],
                    &[&["P", "Q", "M", "N"]],
                )
                .unwrap()
            }),
        ],
    );
}
