//! Reading a whole table and comparing two tables.

use arrow::array::{Array, ArrayRef};
use arrow::util::display::array_value_to_string;
use oxhaven_contracts::graph::locked;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::table::{Table, TableFailure};

/// The rows and values of a table at one instant. Element `i` of each column belongs to the
/// `i`th key of `rows`.
#[derive(Clone, Debug)]
pub struct TableSnapshot {
    pub rows: RowSet,
    pub columns: Vec<ArrayRef>,
}

impl TableSnapshot {
    /// Reads the current state of `table`, under its graph's shared lock.
    pub fn current(table: &dyn Table) -> Result<Self, TableFailure> {
        with_lock(table, || {
            let rows = table.row_set();
            let columns = read_columns(table, &rows, false)?;
            Ok(Self {
                rows: (*rows).clone(),
                columns,
            })
        })
    }

    /// Reads the state of `table` as of the end of the previous cycle.
    pub fn previous(table: &dyn Table) -> Result<Self, TableFailure> {
        with_lock(table, || {
            let rows = table.prev_row_set();
            let columns = read_columns(table, &rows, true)?;
            Ok(Self {
                rows: (*rows).clone(),
                columns,
            })
        })
    }
}

pub(crate) fn with_lock<T>(table: &dyn Table, f: impl FnOnce() -> T) -> T {
    match table.update_graph() {
        Some(graph) => locked(&*graph, f),
        None => f(),
    }
}

/// Every column's values at `rows`, current or previous.
pub(crate) fn read_columns(
    table: &dyn Table,
    rows: &RowSet,
    prev: bool,
) -> Result<Vec<ArrayRef>, TableFailure> {
    (0..table.definition().columns().len())
        .map(|c| {
            let column = table.column(c);
            if prev {
                column.prev_chunk(rows)
            } else {
                column.chunk(rows)
            }
        })
        .collect()
}

/// Whether two arrays hold the same type, nulls and values. Floating point values are equal
/// when their bits are, as with Java's `TableDiff.DiffItems.DoublesExact`: a NaN equals the
/// same NaN, and `0.0` differs from `-0.0`.
///
/// The cells of a vector or array column are equal when both are null, or neither is and
/// they have the same number of elements, equal one by one under the rule for cells of the
/// element type. A null cell differs from an empty one. What lies under a null cell and
/// where the elements lie in the child array make no difference
/// (`ColumnType::list_element`).
pub(crate) fn arrays_equal(a: &ArrayRef, b: &ArrayRef) -> bool {
    a.to_data() == b.to_data()
}

/// Describes the first element at which two arrays differ, or `None` if they are equal.
/// `place` names an element by its position, for example as a row key.
pub(crate) fn array_difference(
    expected: &ArrayRef,
    actual: &ArrayRef,
    place: impl Fn(usize) -> String,
) -> Option<String> {
    if arrays_equal(expected, actual) {
        return None;
    }
    if expected.data_type() != actual.data_type() {
        return Some(format!(
            "expected type {:?} but found {:?}",
            expected.data_type(),
            actual.data_type()
        ));
    }
    if expected.len() != actual.len() {
        return Some(format!(
            "expected {} values but found {}",
            expected.len(),
            actual.len()
        ));
    }
    let show = |array: &ArrayRef, i: usize| {
        if array.is_null(i) {
            "null".to_string()
        } else {
            array_value_to_string(array, i).unwrap_or_else(|e| e.to_string())
        }
    };
    (0..expected.len())
        .find(|&i| !arrays_equal(&expected.slice(i, 1), &actual.slice(i, 1)))
        .map(|i| {
            format!(
                "{}: expected {} but found {}",
                place(i),
                show(expected, i),
                show(actual, i)
            )
        })
}

/// Describes the first difference between two tables, or `None` if they hold the same data.
///
/// Tables are equal when they have the same definition, the same number of rows, and the same
/// values row by row in row-set order. Row keys are not compared, so an incrementally
/// maintained table equals its from-scratch recompute even when the two address their rows
/// differently.
///
/// Behavior: io.deephaven.engine.testutil.TstUtils#assertTableEquals with
/// `TableDiff.DiffItems.DoublesExact`.
pub fn diff_tables(expected: &dyn Table, actual: &dyn Table) -> Option<String> {
    if expected.definition() != actual.definition() {
        return Some(format!(
            "definitions differ: expected {:?} but found {:?}",
            expected.definition().columns(),
            actual.definition().columns()
        ));
    }
    let read = |table: &dyn Table, name: &str| {
        TableSnapshot::current(table).map_err(|e| format!("cannot read the {name} table: {e}"))
    };
    let (expected_data, actual_data) = match (read(expected, "expected"), read(actual, "actual")) {
        (Ok(e), Ok(a)) => (e, a),
        (Err(message), _) | (_, Err(message)) => return Some(message),
    };
    if expected_data.rows.size() != actual_data.rows.size() {
        return Some(format!(
            "expected {} rows but found {}",
            expected_data.rows.size(),
            actual_data.rows.size()
        ));
    }
    let columns = expected.definition().columns();
    columns.iter().enumerate().find_map(|(c, column)| {
        let place = |i| format!("position {i}");
        array_difference(&expected_data.columns[c], &actual_data.columns[c], place)
            .map(|difference| format!("column {}, {difference}", column.name))
    })
}

/// Panics with the first difference if [`diff_tables`] finds one.
#[track_caller]
pub fn assert_tables_equal(expected: &dyn Table, actual: &dyn Table) {
    if let Some(difference) = diff_tables(expected, actual) {
        panic!("tables differ: {difference}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{definition, doubles, list_of, longs, strings, ReferenceTable};
    use arrow::array::{Float64Array, Int64Array, ListArray};
    use arrow::buffer::OffsetBuffer;
    use arrow::datatypes::DataType;
    use oxhaven_contracts::schema::ColumnType;
    use std::sync::Arc;

    fn table(keys: [i64; 2], syms: [&str; 2], prices: [f64; 2]) -> Arc<ReferenceTable> {
        ReferenceTable::new_static(
            definition(&[("Sym", ColumnType::String), ("Price", ColumnType::Double)]),
            RowSet::from_keys(keys),
            vec![strings(syms), doubles(prices)],
        )
    }

    #[test]
    fn tables_compare_by_position_not_by_row_key() {
        let a = table([0, 1], ["x", "y"], [1.0, 2.0]);
        let b = table([10, 40], ["x", "y"], [1.0, 2.0]);
        assert_eq!(diff_tables(&*a, &*b), None);
        assert_tables_equal(&*a, &*b);
    }

    #[test]
    fn first_differing_value_is_named() {
        let a = table([0, 1], ["x", "y"], [1.0, 2.0]);
        let b = table([0, 1], ["x", "y"], [1.0, 2.5]);
        assert_eq!(
            diff_tables(&*a, &*b).unwrap(),
            "column Price, position 1: expected 2.0 but found 2.5"
        );
        let c = table([0, 1], ["x", "z"], [1.0, 2.0]);
        assert_eq!(
            diff_tables(&*a, &*c).unwrap(),
            "column Sym, position 1: expected y but found z"
        );
    }

    #[test]
    #[should_panic(expected = "tables differ: column Sym, position 0: expected x but found q")]
    fn assert_panics_on_a_difference() {
        let a = table([0, 1], ["x", "y"], [1.0, 2.0]);
        let b = table([0, 1], ["q", "y"], [1.0, 2.0]);
        assert_tables_equal(&*a, &*b);
    }

    #[test]
    fn size_and_definition_differences_are_reported() {
        let a = table([0, 1], ["x", "y"], [1.0, 2.0]);
        let short = ReferenceTable::new_static(
            a.definition().clone(),
            RowSet::flat(1),
            vec![strings(["x"]), doubles([1.0])],
        );
        assert_eq!(
            diff_tables(&*a, &*short).unwrap(),
            "expected 2 rows but found 1"
        );
        let other = ReferenceTable::new_static(
            definition(&[("Sym", ColumnType::String), ("Price", ColumnType::Long)]),
            RowSet::flat(2),
            vec![strings(["x", "y"]), longs([1, 2])],
        );
        assert!(diff_tables(&*a, &*other)
            .unwrap()
            .starts_with("definitions differ"));
    }

    #[test]
    fn floats_compare_by_bits_and_nulls_ignore_the_value_underneath() {
        let array = |values: Vec<Option<f64>>| -> ArrayRef { Arc::new(Float64Array::from(values)) };
        let nan = array(vec![Some(f64::NAN)]);
        assert!(arrays_equal(&nan, &array(vec![Some(f64::NAN)])));
        assert!(!arrays_equal(
            &array(vec![Some(0.0)]),
            &array(vec![Some(-0.0)])
        ));
        assert_eq!(
            array_difference(
                &array(vec![Some(1.0), None]),
                &array(vec![Some(1.0), Some(0.0)]),
                |i| format!("row key {}", 10 * i)
            ),
            Some("row key 10: expected null but found 0.0".into())
        );

        // Two null slots are equal whatever value sits under the validity bit.
        let ints = |values: Vec<i64>, valid: Vec<bool>| -> ArrayRef {
            Arc::new(Int64Array::new(values.into(), Some(valid.into())))
        };
        let (a, b) = (
            ints(vec![5, 1], vec![false, true]),
            ints(vec![9, 1], vec![false, true]),
        );
        assert!(arrays_equal(&a, &b));
        assert!(!arrays_equal(&a, &ints(vec![5, 1], vec![true, true])));
    }

    fn double_lists(lengths: &[Option<usize>], elements: Vec<Option<f64>>) -> ArrayRef {
        list_of(
            &ColumnType::vector(ColumnType::Double),
            lengths.iter().copied(),
            Arc::new(Float64Array::from(elements)),
        )
    }

    #[test]
    fn list_cells_compare_element_by_element_like_cells_of_the_element_type() {
        // [1.0, null], null, [], [NaN]
        let lengths = [Some(2), None, Some(0), Some(1)];
        let cells = |elements| double_lists(&lengths, elements);
        let expected = cells(vec![Some(1.0), None, Some(f64::NAN)]);
        assert!(arrays_equal(
            &expected,
            &cells(vec![Some(1.0), None, Some(f64::NAN)])
        ));

        let place = |i| format!("row key {}", 10 * i);
        let differs = |actual: ArrayRef| array_difference(&expected, &actual, place);
        // An element differs, also only in the sign of zero or in being null.
        assert_eq!(
            differs(cells(vec![Some(1.5), None, Some(f64::NAN)])),
            Some("row key 0: expected [1.0, ] but found [1.5, ]".into())
        );
        assert_eq!(
            differs(cells(vec![Some(1.0), Some(0.0), Some(f64::NAN)])),
            Some("row key 0: expected [1.0, ] but found [1.0, 0.0]".into())
        );
        assert_eq!(
            differs(cells(vec![Some(1.0), None, Some(0.0)])),
            Some("row key 30: expected [NaN] but found [0.0]".into())
        );
        let zeros = |zero| double_lists(&[Some(1)], vec![Some(zero)]);
        assert!(!arrays_equal(&zeros(0.0), &zeros(-0.0)));

        // The same elements cut into other cells, and the order of a cell's elements.
        assert_eq!(
            differs(double_lists(
                &[Some(1), None, Some(1), Some(1)],
                vec![Some(1.0), None, Some(f64::NAN)]
            )),
            Some("row key 0: expected [1.0, ] but found [1.0]".into())
        );
        let pair = |a, b| double_lists(&[Some(2)], vec![Some(a), Some(b)]);
        assert!(!arrays_equal(&pair(1.0, 2.0), &pair(2.0, 1.0)));

        // A null cell is not an empty cell, in either direction.
        assert_eq!(
            differs(double_lists(
                &[Some(2), Some(0), Some(0), Some(1)],
                vec![Some(1.0), None, Some(f64::NAN)]
            )),
            Some("row key 10: expected null but found []".into())
        );
        assert_eq!(
            differs(double_lists(
                &[Some(2), None, None, Some(1)],
                vec![Some(1.0), None, Some(f64::NAN)]
            )),
            Some("row key 20: expected [] but found null".into())
        );

        // A vector and an array of one element type hold the same Arrow values; the
        // definitions tell them apart.
        let array = list_of(
            &ColumnType::array(ColumnType::Double),
            lengths,
            Arc::new(Float64Array::from(vec![Some(1.0), None, Some(f64::NAN)])),
        );
        assert!(arrays_equal(&expected, &array));
    }

    #[test]
    fn list_cells_are_equal_whatever_lies_under_a_null_cell_and_wherever_the_elements_lie() {
        let DataType::List(field) = ColumnType::vector(ColumnType::Long).arrow_type() else {
            unreachable!()
        };
        let lists = |offsets: Vec<i32>, elements: Vec<i64>, valid: Vec<bool>| -> ArrayRef {
            Arc::new(ListArray::new(
                field.clone(),
                OffsetBuffer::new(offsets.into()),
                Arc::new(Int64Array::from(elements)),
                Some(valid.into()),
            ))
        };
        // [7], null, [8, 9]: the null cell spans no elements, or two that mean nothing,
        // and the elements start at the front of the child or after an unused one.
        let plain = lists(vec![0, 1, 1, 3], vec![7, 8, 9], vec![true, false, true]);
        let padded = lists(
            vec![1, 2, 4, 6],
            vec![0, 7, -1, -2, 8, 9],
            vec![true, false, true],
        );
        assert!(arrays_equal(&plain, &padded));
        assert_eq!(array_difference(&plain, &padded, |i| i.to_string()), None);
        // A slice of a longer array equals the same cells on their own.
        let longer = lists(
            vec![0, 2, 3, 3, 5],
            vec![5, 6, 7, 8, 9],
            vec![true, true, false, true],
        );
        assert!(arrays_equal(&plain, &longer.slice(1, 3)));
        assert!(!arrays_equal(&plain, &longer.slice(0, 3)));
    }

    #[test]
    fn tables_with_a_vector_and_an_array_of_one_element_type_differ() {
        let table = |column_type: ColumnType| {
            let cells = list_of(&column_type, [Some(1)], longs([4]));
            ReferenceTable::new_static(
                definition(&[("V", column_type)]),
                RowSet::flat(1),
                vec![cells],
            )
        };
        let vector = table(ColumnType::vector(ColumnType::Long));
        let array = table(ColumnType::array(ColumnType::Long));
        assert_eq!(diff_tables(&*vector, &*vector), None);
        assert!(diff_tables(&*vector, &*array)
            .unwrap()
            .starts_with("definitions differ"));
    }
}
