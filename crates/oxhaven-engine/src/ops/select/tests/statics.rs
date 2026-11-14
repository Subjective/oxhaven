//! Results on static tables, what is shared and what is stored, and refused requests.

use super::*;
use crate::table::{empty_table, in_memory_table};
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use oxhaven_testkit::{definition, longs, strings};

fn sym_a_b() -> TableDefinition {
    definition(&[
        ("Sym", ColumnType::String),
        ("A", ColumnType::Long),
        ("B", ColumnType::Long),
    ])
}

fn columns(size: usize) -> Vec<ArrayRef> {
    let syms = ["a", "b", "c"];
    vec![
        strings((0..size).map(|i| syms[i % 3])),
        longs((0..size).map(|i| 10 * (i as i64 + 1))),
        longs((0..size).map(|i| i as i64 + 1)),
    ]
}

/// A static table at the given row keys with `A` 10, 20, ... and `B` 1, 2, ..., that does
/// not declare itself flat.
fn keyed(keys: &[i64]) -> Arc<dyn Table> {
    let rows = RowSet::from_keys(keys.iter().copied());
    let values = ReferenceTable::new_static(sym_a_b(), rows.clone(), columns(keys.len()));
    // The reference table makes a new source on every call; holding one of each lets the
    // tests see which sources a result shares.
    let sources = (0..3).map(|index| values.column(index)).collect();
    QueryTable::new_static(TableSpec::new(sym_a_b(), rows, sources))
}

/// The same values in a flat in-memory table.
fn flat(size: usize) -> Arc<dyn Table> {
    let fields: Vec<Field> = sym_a_b()
        .columns()
        .iter()
        .map(|c| Field::new(&c.name, c.column_type.arrow_type(), true))
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns(size)).unwrap();
    in_memory_table(sym_a_b(), &[batch]).unwrap()
}

fn same_source(left: &Arc<dyn ColumnSource>, right: &Arc<dyn ColumnSource>) -> bool {
    std::ptr::addr_eq(Arc::as_ptr(left), Arc::as_ptr(right))
}

fn column(table: &Arc<dyn Table>, name: &str) -> Arc<dyn ColumnSource> {
    table.column(table.definition().index_of(name).unwrap())
}

#[test]
fn view_has_the_listed_columns_computed_from_the_parent() {
    let parent = keyed(&[3, 7, 9]);
    let result = view(&parent, &["B", "Sum = A + B", "Sym2 = Sym"], &Compiler).unwrap();
    assert_eq!(names(&*result), ["B", "Sum", "Sym2"]);
    assert_eq!(values(&*result, "Sum"), ["11", "22", "33"]);
    assert_eq!(values(&*result, "Sym2"), ["a", "b", "c"]);
    assert_eq!(keys(&*result), [3, 7, 9]);
    assert!(!result.is_flat() && !result.is_refreshing());
    // A column that only names another is that column.
    assert!(same_source(&column(&result, "B"), &column(&parent, "B")));
    assert!(same_source(
        &column(&result, "Sym2"),
        &column(&parent, "Sym")
    ));
}

#[test]
fn update_view_keeps_the_parent_columns_and_replaces_in_place() {
    let parent = flat(2);
    let result = update_view(&parent, &["A = A + 1", "C = A * 2"], &Compiler).unwrap();
    assert_eq!(names(&*result), ["Sym", "A", "B", "C"]);
    assert_eq!(values(&*result, "A"), ["11", "21"]);
    // `C` reads the `A` that replaced the parent's.
    assert_eq!(values(&*result, "C"), ["22", "42"]);
    assert!(result.is_flat());
    assert!(same_source(&column(&result, "B"), &column(&parent, "B")));
}

#[test]
fn no_columns_gives_the_parent_itself_for_views_and_drops() {
    let parent = flat(2);
    let none: [&str; 0] = [];
    assert!(Arc::ptr_eq(
        &view(&parent, &none, &Compiler).unwrap(),
        &parent
    ));
    assert!(Arc::ptr_eq(
        &update_view(&parent, &none, &Compiler).unwrap(),
        &parent
    ));
    assert!(Arc::ptr_eq(&drop_columns(&parent, &none).unwrap(), &parent));
    // The storing and remembering operations make a new table with the same contents.
    for result in [
        update(&parent, &none, &Compiler).unwrap(),
        lazy_update(&parent, &none, &Compiler).unwrap(),
        select(&parent, &none, &Compiler).unwrap(),
    ] {
        assert!(!Arc::ptr_eq(&result, &parent));
        assert_eq!(names(&*result), ["Sym", "A", "B"]);
        assert_eq!(rendered(&*result), rendered(&*parent));
    }
}

#[test]
fn later_columns_read_earlier_ones_and_the_columns_they_replace() {
    let parent = flat(3);
    let result = select(&parent, &["X = A + 1", "Y = X * 2", "Z = Y - X"], &Compiler).unwrap();
    assert_eq!(names(&*result), ["X", "Y", "Z"]);
    assert_eq!(values(&*result, "Z"), ["11", "21", "31"]);

    // `A = A + 1` reads the parent's A; `B = A + B` reads the new A and the parent's B.
    let result = update(&parent, &["A = A + 1", "B = A + B"], &Compiler).unwrap();
    assert_eq!(names(&*result), ["Sym", "A", "B"]);
    assert_eq!(values(&*result, "A"), ["11", "21", "31"]);
    assert_eq!(values(&*result, "B"), ["12", "23", "34"]);
}

#[test]
fn a_name_assigned_twice_keeps_its_first_place_and_its_last_value() {
    let parent = flat(2);
    let result = update(&parent, &["X = 1", "X = 2"], &Compiler).unwrap();
    assert_eq!(names(&*result), ["Sym", "A", "B", "X"]);
    assert_eq!(values(&*result, "X"), ["2", "2"]);

    let result = select(&parent, &["X = A", "Y = X + 1", "X = Y + 1"], &Compiler).unwrap();
    assert_eq!(names(&*result), ["X", "Y"]);
    assert_eq!(values(&*result, "X"), ["12", "22"]);
    assert_eq!(values(&*result, "Y"), ["11", "21"]);

    let result = view(&parent, &["X = A", "X = B"], &Compiler).unwrap();
    assert_eq!(names(&*result), ["X"]);
    assert_eq!(values(&*result, "X"), ["1", "2"]);
}

#[test]
fn select_of_a_flat_table_is_flat_and_stores_its_formulas() {
    let parent = flat(3);
    let counting = Counting::new();
    let result = select(&parent, &["A", "X = A + B"], &counting).unwrap();
    assert_eq!(counting.take(), 3);
    assert!(result.is_flat() && !result.is_refreshing());
    assert_eq!(keys(&*result), [0, 1, 2]);
    assert_eq!(values(&*result, "X"), ["11", "22", "33"]);
    // Reading the stored column does not evaluate the formula again.
    assert_eq!(counting.take(), 0);
    assert!(same_source(&column(&result, "A"), &column(&parent, "A")));

    // A view evaluates on every read.
    let result = view(&parent, &["X = A + B"], &counting).unwrap();
    assert_eq!(counting.take(), 0);
    values(&*result, "X");
    values(&*result, "X");
    assert_eq!(counting.take(), 6);
}

#[test]
fn static_select_gets_new_row_keys_unless_it_shares_a_parent_column() {
    let parent = keyed(&[3, 7, 9]);

    // Nothing shared: positions become the row keys.
    let computed = select(&parent, &["X = A + 1"], &Compiler).unwrap();
    assert_eq!(keys(&*computed), [0, 1, 2]);
    assert!(computed.is_flat());
    assert_eq!(values(&*computed, "X"), ["11", "21", "31"]);

    // A parent column passed through is shared, which keeps the parent's row keys.
    let shared = select(&parent, &["B2 = B", "X = A + 1"], &Compiler).unwrap();
    assert_eq!(keys(&*shared), [3, 7, 9]);
    assert!(!shared.is_flat());
    assert!(same_source(&column(&shared, "B2"), &column(&parent, "B")));
    assert_eq!(values(&*shared, "X"), ["11", "21", "31"]);
    assert_eq!(values(&*shared, "B2"), ["1", "2", "3"]);

    // `update` always keeps the parent's columns and so its row keys.
    let updated = update(&parent, &["X = A + 1"], &Compiler).unwrap();
    assert_eq!(keys(&*updated), [3, 7, 9]);
    assert!(!updated.is_flat());
    assert_eq!(values(&*updated, "X"), ["11", "21", "31"]);
}

#[test]
fn a_renumbered_select_reads_parent_and_earlier_columns_of_the_same_row() {
    let parent = keyed(&[3, 7, 9]);
    // Z reads X by position and A by the parent's row key.
    let result = select(&parent, &["X = A + 1", "Z = X + A", "X2 = X"], &Compiler).unwrap();
    assert_eq!(keys(&*result), [0, 1, 2]);
    assert_eq!(values(&*result, "Z"), ["21", "41", "61"]);
    // A column that names an earlier stored column shares it.
    assert!(same_source(&column(&result, "X"), &column(&result, "X2")));
}

#[test]
fn a_static_result_with_flat_rows_is_marked_flat() {
    // The parent's rows are 0..3 but it does not say it is flat.
    let parent = keyed(&[0, 1, 2]);
    assert!(!parent.is_flat());
    for result in [
        select(&parent, &["A"], &Compiler).unwrap(),
        update(&parent, &["X = A + 1"], &Compiler).unwrap(),
    ] {
        assert!(result.is_flat());
        assert_eq!(keys(&*result), [0, 1, 2]);
    }
    // The views share the parent's rows and its word on flatness.
    assert!(!view(&parent, &["A"], &Compiler).unwrap().is_flat());
}

#[test]
fn select_of_a_view_column_stores_it_once_per_name() {
    let parent = keyed(&[3, 7, 9]);
    let counting = Counting::new();
    let viewed = view(&parent, &["V = A + 1", "B"], &counting).unwrap();
    let result = select(&viewed, &["V", "V2 = V", "W = V2 + 1"], &Compiler).unwrap();
    // One copy of V was made, with one evaluation per row, and both names share it.
    assert_eq!(counting.take(), 3);
    assert!(same_source(&column(&result, "V"), &column(&result, "V2")));
    assert!(!same_source(&column(&result, "V"), &column(&viewed, "V")));
    assert_eq!(values(&*result, "W"), ["12", "22", "32"]);
    values(&*result, "V");
    assert_eq!(counting.take(), 0);
    // No parent column is shared, so the result is renumbered.
    assert_eq!(keys(&*result), [0, 1, 2]);
    assert!(result.is_flat());

    // Sharing the view's own pass-through column keeps its row keys.
    let result = select(&viewed, &["V", "B"], &Compiler).unwrap();
    assert_eq!(keys(&*result), [3, 7, 9]);
    assert_eq!(values(&*result, "V"), ["11", "21", "31"]);
}

#[test]
fn a_copy_is_not_reused_once_its_name_is_assigned_again() {
    let parent = keyed(&[3, 7]);
    let viewed = view(&parent, &["V = A + 1"], &Compiler).unwrap();
    let result = select(&viewed, &["P = V", "V = P * 2", "Q = V"], &Compiler).unwrap();
    assert_eq!(values(&*result, "P"), ["11", "21"]);
    assert_eq!(values(&*result, "Q"), ["22", "42"]);
    assert!(same_source(&column(&result, "Q"), &column(&result, "V")));
}

#[test]
fn select_of_a_sparse_static_table_flattens_and_copies_everything() {
    // Three rows spread over three blocks of 1024 keys.
    let parent = keyed(&[0, 5000, 100_000]);
    let result = select(&parent, &["A", "X = A + B"], &Compiler).unwrap();
    assert_eq!(keys(&*result), [0, 1, 2]);
    assert!(result.is_flat());
    assert!(!same_source(&column(&result, "A"), &column(&parent, "A")));
    assert_eq!(values(&*result, "A"), ["10", "20", "30"]);
    assert_eq!(values(&*result, "X"), ["11", "22", "33"]);

    // `update` keeps the parent's keys however sparse they are.
    let result = update(&parent, &["X = A + B"], &Compiler).unwrap();
    assert_eq!(keys(&*result), [0, 5000, 100_000]);
    assert_eq!(values(&*result, "X"), ["11", "22", "33"]);
}

#[test]
fn sparse_storage_is_wasteful_when_rows_use_more_blocks_than_a_tenth_extra() {
    use super::super::materialize::sparse_storage_wasteful as wasteful;
    assert!(!wasteful(&RowSet::empty()));
    assert!(!wasteful(&RowSet::flat(5000)));
    // Two rows in one block need one block.
    assert!(!wasteful(&RowSet::from_keys([3, 900])));
    // Two rows in two blocks need one block and use two.
    assert!(wasteful(&RowSet::from_keys([3, 1024])));
    // 10240 rows need ten blocks and may use eleven.
    let mut rows = RowSet::from_range(0, 10_238);
    rows.insert(&RowSet::from_keys([11_000]));
    assert!(!wasteful(&rows));
    rows.insert(&RowSet::from_keys([20_000]));
    rows.remove(&RowSet::from_keys([0]));
    assert!(wasteful(&rows));
}

#[test]
fn row_variables_see_the_parent_rows_in_views_and_in_stored_columns() {
    let parent = keyed(&[3, 7, 9]);
    let result = view(&parent, &["K = k", "I = i"], &WithRowVariables).unwrap();
    assert_eq!(values(&*result, "K"), ["3", "7", "9"]);
    assert_eq!(values(&*result, "I"), ["0", "1", "2"]);

    let result = update(&parent, &["K = k", "I = ii"], &WithRowVariables).unwrap();
    assert_eq!(values(&*result, "K"), ["3", "7", "9"]);
    assert_eq!(values(&*result, "I"), ["0", "1", "2"]);

    // A renumbered select evaluates a formula over parent columns at the parent's keys.
    let result = select(&parent, &["K = k"], &WithRowVariables).unwrap();
    assert_eq!(keys(&*result), [0, 1, 2]);
    assert_eq!(values(&*result, "K"), ["3", "7", "9"]);

    // A flattened select evaluates everything at positions.
    let sparse = keyed(&[0, 5000, 100_000]);
    let result = select(&sparse, &["K = k", "S = A + K"], &WithRowVariables).unwrap();
    assert_eq!(values(&*result, "K"), ["0", "1", "2"]);
    assert_eq!(values(&*result, "S"), ["10", "21", "32"]);
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer#createContext"
)]
fn a_row_variable_beside_an_earlier_column_in_a_renumbered_select_is_unported() {
    let parent = keyed(&[3, 7, 9]);
    let _ = select(&parent, &["X = A + 1", "Y = X + k"], &WithRowVariables);
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.ShiftedColumnsFactory#getShiftedColumnDefinitions"
)]
fn a_formula_that_reads_another_row_is_unported() {
    let _ = update(&flat(3), &["P = A_[i - 1]"], &WithRowVariables);
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.select.DhFormulaColumn#generateAppropriateGetMethod"
)]
fn a_lazy_formula_with_a_row_variable_is_unported() {
    let _ = lazy_update(&flat(3), &["K = k"], &WithRowVariables);
}

#[test]
fn constants_are_evaluated_once_whatever_rows_there_are() {
    let counting = Counting::new();
    let result = update(&flat(3), &["One = 1", "Three = 1 + 2"], &counting).unwrap();
    assert_eq!(counting.take(), 2);
    assert_eq!(values(&*result, "Three"), ["3", "3", "3"]);
    assert_eq!(counting.take(), 0);

    // A constant is computed for a table without rows too, and so can fail one.
    let empty: Arc<dyn Table> = empty_table(0);
    let result = update(&empty, &["Three = 1 + 2"], &counting).unwrap();
    assert_eq!(counting.take(), 1);
    assert_eq!(
        result.definition().columns()[0].column_type,
        ColumnType::Int
    );
    assert!(matches!(
        update(&empty, &["X = 7 % 0"], &Compiler),
        Err(SelectError::Failed(_))
    ));
    assert!(matches!(
        view(&empty, &["X = 7 % 0"], &Compiler),
        Err(SelectError::Failed(_))
    ));
    // The same remainder over a column is a formula, evaluated for no row.
    let none = keyed(&[]);
    assert!(update(&none, &["X = 7 % A"], &Compiler).is_ok());
    // A negative literal is not a constant.
    let result = view(&flat(2), &["X = -1"], &counting).unwrap();
    assert_eq!(counting.take(), 0);
    assert_eq!(values(&*result, "X"), ["-1", "-1"]);
    assert_eq!(counting.take(), 2);
}

#[test]
fn a_constant_view_column_is_shared_by_a_later_select() {
    let parent = keyed(&[3, 7]);
    let viewed = view(&parent, &["C = 5"], &Compiler).unwrap();
    let result = select(&viewed, &["C"], &Compiler).unwrap();
    assert!(same_source(&column(&result, "C"), &column(&viewed, "C")));
    assert_eq!(keys(&*result), [3, 7]);
}

#[test]
fn lazy_update_computes_each_distinct_input_once() {
    let parent = ReferenceTable::new_static(
        sym_a_b(),
        RowSet::from_keys([2, 4, 6, 8, 10]),
        vec![
            strings(["a", "b", "a", "b", "c"]),
            longs([1, 2, 1, 2, 1]),
            longs([5, 5, 5, 6, 5]),
        ],
    );
    let parent: Arc<dyn Table> = parent;
    let counting = Counting::new();
    let result = lazy_update(&parent, &["X = A + B", "Y = X * 2", "C = 4"], &counting).unwrap();
    assert_eq!(names(&*result), ["Sym", "A", "B", "X", "Y", "C"]);
    assert_eq!(counting.take(), 1, "only the constant is computed up front");

    // (1, 5) three times, (2, 5) and (2, 6) once each.
    assert_eq!(values(&*result, "X"), ["6", "7", "6", "8", "6"]);
    assert_eq!(counting.take(), 3);
    assert_eq!(values(&*result, "X"), ["6", "7", "6", "8", "6"]);
    assert_eq!(counting.take(), 0);
    // Y reads X, which is remembered, and computes its own three distinct inputs.
    assert_eq!(values(&*result, "Y"), ["12", "14", "12", "16", "12"]);
    assert_eq!(counting.take(), 3);
    // A read of other rows with inputs seen before computes nothing, and neither does a
    // previous-value read.
    let some = RowSet::from_keys([6, 10]);
    let x = column(&result, "X");
    assert_eq!(cells(&x.chunk(&some).unwrap()), ["6", "6"]);
    assert_eq!(cells(&x.prev_chunk(&some).unwrap()), ["6", "6"]);
    assert_eq!(cells(&x.chunk(&RowSet::empty()).unwrap()).len(), 0);
    assert_eq!(counting.take(), 0);
}

#[test]
fn a_lazy_formula_that_fails_is_not_remembered() {
    let parent = keyed(&[1, 2, 3]);
    let counting = Counting::new();
    // B is 1, 2, 3: the remainder by B - 2 fails for the second row.
    let result = lazy_update(&parent, &["X = A % (B - 2)"], &counting).unwrap();
    let x = column(&result, "X");
    assert!(x.chunk(&result.row_set()).is_err());
    assert_eq!(counting.take(), 3);
    assert!(x.chunk(&result.row_set()).is_err());
    assert_eq!(counting.take(), 3);
    let good = RowSet::from_keys([1, 3]);
    assert_eq!(cells(&x.chunk(&good).unwrap()), ["0", "0"]);
    assert_eq!(counting.take(), 2);
    assert_eq!(cells(&x.chunk(&good).unwrap()), ["0", "0"]);
    assert_eq!(counting.take(), 0);
}

#[test]
fn drop_columns_removes_the_named_columns_and_shares_the_rest() {
    let parent = keyed(&[3, 7]);
    let result = drop_columns(&parent, &["A", "Sym", "A"]).unwrap();
    assert_eq!(names(&*result), ["B"]);
    assert_eq!(keys(&*result), [3, 7]);
    assert!(same_source(&column(&result, "B"), &column(&parent, "B")));
    assert!(drop_columns(&flat(2), &["B"]).unwrap().is_flat());

    let all = drop_columns(&parent, &["A", "B", "Sym"]).unwrap();
    assert!(all.definition().columns().is_empty());
    assert_eq!(all.row_set().size(), 2);

    match drop_columns(&parent, &["B", "Nope", "Zip"]) {
        Err(SelectError::NoSuchColumn(message)) => {
            assert!(message.contains("Nope, Zip"), "{message}")
        }
        other => panic!(
            "expected a missing column, got {:?}",
            other.map(|t| names(&*t))
        ),
    }
}

#[test]
fn requests_that_do_not_compile_are_errors() {
    let parent = flat(2);
    let error = |columns: &[&str]| match update(&parent, columns, &Compiler) {
        Err(error) => error,
        Ok(table) => panic!("{columns:?} gave columns {:?}", names(&*table)),
    };
    assert!(matches!(
        error(&["X = A +"]),
        SelectError::Formula {
            error: FormulaError::Syntax(_),
            ..
        }
    ));
    assert!(matches!(
        error(&["X = noSuchFunction(A)"]),
        SelectError::Formula { .. }
    ));
    assert!(matches!(
        error(&["X = Sym * 2"]),
        SelectError::Formula { .. }
    ));
    // The compiler's refusal of what it does not support yet comes through as it is.
    for unsupported in ["X = i", "X = k + A", "X = A_"] {
        match error(&[unsupported]) {
            SelectError::Formula { column, error } => {
                assert_eq!(column, unsupported);
                assert!(matches!(error, FormulaError::Unsupported(_)), "{error:?}");
            }
            other => panic!("{unsupported} gave {other:?}"),
        }
    }
    // A name on its own must be a column; after `=` an unknown name is a formula's error.
    assert!(matches!(error(&["Nope"]), SelectError::NoSuchColumn(_)));
    assert!(matches!(error(&["X = Nope"]), SelectError::Formula { .. }));
    // A column can only read what is listed before it.
    assert!(matches!(
        error(&["X = Y + 1", "Y = A"]),
        SelectError::Formula { .. }
    ));
    for reserved in ["i = A", "k = A + 1", "in = A", "class = A", "true = A"] {
        assert!(
            matches!(error(&[reserved]), SelectError::InvalidName(_)),
            "{reserved}"
        );
    }
    // The same checks guard every operation.
    assert!(view(&parent, &["Nope"], &Compiler).is_err());
    assert!(update_view(&parent, &["X = A +"], &Compiler).is_err());
    assert!(lazy_update(&parent, &["X = Nope + 1"], &Compiler).is_err());
    assert!(select(&parent, &["X = Sym * 2"], &Compiler).is_err());
}

#[test]
fn a_failing_formula_fails_a_stored_column_at_once_and_a_view_when_read() {
    // B - 2 is zero in the second row.
    let parent = keyed(&[1, 2, 3]);
    for stored in [
        update(&parent, &["X = A % (B - 2)"], &Compiler),
        select(&parent, &["X = A % (B - 2)"], &Compiler),
    ] {
        assert!(matches!(stored, Err(SelectError::Failed(_))));
    }
    let result = view(&parent, &["X = A % (B - 2)", "B"], &Compiler).unwrap();
    let x = column(&result, "X");
    assert!(x.chunk(&result.row_set()).is_err());
    assert!(TableSnapshot::current(&*result).is_err());
    // Only a read of the bad row fails.
    assert_eq!(
        cells(&x.chunk(&RowSet::from_keys([1, 3])).unwrap()),
        ["0", "0"]
    );
    assert_eq!(values(&*result, "B"), ["1", "2", "3"]);
    // Storing the view's column runs into the failure.
    assert!(matches!(
        select(&result, &["X"], &Compiler),
        Err(SelectError::Failed(_))
    ));
}

#[test]
fn static_results_carry_no_attributes_except_through_drop_columns() {
    let mut attributes = TableAttributes::default();
    attributes.add_only = true;
    attributes.append_only = true;
    attributes.blink = true;
    let parent: Arc<dyn Table> = QueryTable::new_static(
        TableSpec::new(
            definition(&[("A", ColumnType::Long), ("B", ColumnType::Long)]),
            RowSet::flat(2),
            vec![
                Arc::new(oxhaven_column::FlatSource::new(ColumnType::Long, longs([1, 2])).unwrap()),
                Arc::new(oxhaven_column::FlatSource::new(ColumnType::Long, longs([3, 4])).unwrap()),
            ],
        )
        .with_attributes(attributes),
    );
    for result in [
        view(&parent, &["A"], &Compiler).unwrap(),
        update_view(&parent, &["X = A + 1"], &Compiler).unwrap(),
        lazy_update(&parent, &["X = A + 1"], &Compiler).unwrap(),
        select(&parent, &["A"], &Compiler).unwrap(),
        update(&parent, &["X = A + 1"], &Compiler).unwrap(),
    ] {
        assert_eq!(result.attributes(), TableAttributes::default());
    }
    assert_eq!(
        drop_columns(&parent, &["B"]).unwrap().attributes(),
        attributes
    );
}
