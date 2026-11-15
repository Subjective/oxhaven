use super::*;
use crate::table::holds_values;
use arrow::array::{Int32Array, Int64Array};
use oxhaven_contracts::rowset::NULL_ROW_KEY;
use oxhaven_contracts::schema::ColumnDefinition;
use oxhaven_contracts::table::Table;
use std::sync::Mutex;

/// A location of `size` rows with the columns `Row` (long: `base` plus the row) and `Odd`
/// (int: null on even rows). It remembers what was read.
struct Counted {
    name: String,
    base: i64,
    size: u64,
    reads: Mutex<Vec<(usize, Range<u64>)>>,
    /// What `read` does wrong, if anything.
    fault: Option<Fault>,
}

#[derive(Clone, Copy)]
enum Fault {
    Fails,
    WrongType,
    TooShort,
}

impl Counted {
    fn new(name: &str, base: i64, size: u64) -> Arc<Self> {
        Self::faulty(name, base, size, None)
    }

    fn faulty(name: &str, base: i64, size: u64, fault: Option<Fault>) -> Arc<Self> {
        Arc::new(Self {
            name: name.to_string(),
            base,
            size,
            reads: Mutex::new(Vec::new()),
            fault,
        })
    }

    fn reads(&self) -> Vec<(usize, Range<u64>)> {
        self.reads.lock().unwrap().clone()
    }
}

impl TableLocation for Counted {
    fn name(&self) -> &str {
        &self.name
    }

    fn size(&self) -> u64 {
        self.size
    }

    fn read(&self, column: usize, rows: Range<u64>) -> Result<ArrayRef, TableFailure> {
        self.reads.lock().unwrap().push((column, rows.clone()));
        let rows = match self.fault {
            Some(Fault::Fails) => return Err(failure(format!("{} is gone", self.name))),
            Some(Fault::TooShort) => rows.start..rows.end - 1,
            _ => rows,
        };
        let longs = || Int64Array::from_iter_values(rows.clone().map(|row| self.base + row as i64));
        Ok(match (column, self.fault) {
            (0, Some(Fault::WrongType)) => Arc::new(Int32Array::from_iter_values(
                rows.clone().map(|row| row as i32),
            )),
            (0, _) => Arc::new(longs()),
            _ => Arc::new(Int32Array::from_iter(
                rows.clone().map(|row| (row % 2 == 1).then_some(row as i32)),
            )),
        })
    }
}

fn definition() -> TableDefinition {
    TableDefinition::new(vec![
        ColumnDefinition::new("Row", ColumnType::Long),
        ColumnDefinition::new("Odd", ColumnType::Int),
    ])
    .unwrap()
}

fn longs(values: &ArrayRef) -> Vec<Option<i64>> {
    let values = values.as_any().downcast_ref::<Int64Array>().unwrap();
    values.iter().collect()
}

fn ints(values: &ArrayRef) -> Vec<Option<i32>> {
    let values = values.as_any().downcast_ref::<Int32Array>().unwrap();
    values.iter().collect()
}

const REGION: RowKey = 1 << 43;

#[test]
fn each_location_with_rows_is_a_region_of_row_keys() {
    let locations: Vec<Arc<dyn TableLocation>> = vec![
        Counted::new("a", 100, 3),
        Counted::new("empty", 200, 0),
        Counted::new("c", 300, 2),
    ];
    let table = stored_table(definition(), locations).unwrap();
    // the empty location has no region: the third location is region 1, not 2
    let expected = RowSet::from_ranges([RowRange::new(0, 2), RowRange::new(REGION, REGION + 1)]);
    assert_eq!(*table.row_set(), expected);
    assert!(!table.is_refreshing());
    assert!(!table.is_flat());
    assert!(table.attributes().add_only);
    assert!(!table.attributes().append_only);

    let all = table.column(0).chunk(&table.row_set()).unwrap();
    let expected = [100, 101, 102, 300, 301].map(Some);
    assert_eq!(longs(&all), expected);
    let odd = table.column(1).chunk(&table.row_set()).unwrap();
    assert_eq!(ints(&odd), [None, Some(1), None, None, Some(1)]);
}

#[test]
fn a_table_without_rows_has_its_columns() {
    let locations: Vec<Arc<dyn TableLocation>> = vec![Counted::new("empty", 0, 0)];
    let table = stored_table(definition(), locations).unwrap();
    assert!(table.row_set().is_empty());
    assert!(table.attributes().add_only);
    let none = table.column(0).chunk(&RowSet::empty()).unwrap();
    assert_eq!(none.data_type(), &ColumnType::Long.arrow_type());
    assert!(none.is_empty());
}

#[test]
fn only_the_rows_asked_for_are_read() {
    let first = Counted::new("a", 100, 10);
    let second = Counted::new("b", 200, 10);
    let table = stored_table(definition(), vec![first.clone(), second.clone()]).unwrap();
    assert!(first.reads().is_empty(), "making the table reads nothing");

    let rows = RowSet::from_ranges([
        RowRange::new(2, 3),
        RowRange::new(7, 7),
        RowRange::new(REGION + 4, REGION + 5),
    ]);
    let values = table.column(0).chunk(&rows).unwrap();
    assert_eq!(longs(&values), [102, 103, 107, 204, 205].map(Some));
    assert_eq!(first.reads(), [(0, 2..4), (0, 7..8)]);
    assert_eq!(second.reads(), [(0, 4..6)]);
}

#[test]
fn keys_in_any_order_and_null_keys_are_read() {
    let locations: Vec<Arc<dyn TableLocation>> =
        vec![Counted::new("a", 100, 3), Counted::new("b", 200, 3)];
    let table = stored_table(definition(), locations).unwrap();
    let keys = [REGION + 2, 0, NULL_ROW_KEY, 0, REGION];
    let values = table.column(0).chunk_by_keys(&keys, false).unwrap();
    let expected = [Some(202), Some(100), None, Some(100), Some(200)];
    assert_eq!(longs(&values), expected);
    let previous = table.column(0).chunk_by_keys(&keys, true).unwrap();
    assert_eq!(longs(&previous), expected);
    let same = table.column(0).prev_chunk(&table.prev_row_set()).unwrap();
    assert_eq!(longs(&same), [100, 101, 102, 200, 201, 202].map(Some));
}

#[test]
fn the_columns_do_not_hold_their_values() {
    let locations: Vec<Arc<dyn TableLocation>> = vec![Counted::new("a", 100, 3)];
    let table = stored_table(definition(), locations).unwrap();
    assert!(!holds_values(&table.column(0)));
    assert!(!holds_values(&table.column(1)));
}

#[test]
fn a_read_that_fails_is_the_failure_of_the_chunk() {
    let locations: Vec<Arc<dyn TableLocation>> = vec![
        Counted::new("a", 100, 3),
        Counted::faulty("b", 200, 3, Some(Fault::Fails)),
    ];
    let table = stored_table(definition(), locations).unwrap();
    let good = table.column(0).chunk(&RowSet::flat(3)).unwrap();
    assert_eq!(longs(&good), [100, 101, 102].map(Some));
    let error = table.column(0).chunk(&table.row_set()).unwrap_err();
    assert_eq!(error.message, "b is gone");
}

#[test]
fn an_answer_of_the_wrong_type_or_length_is_a_failure() {
    for fault in [Fault::WrongType, Fault::TooShort] {
        let locations: Vec<Arc<dyn TableLocation>> =
            vec![Counted::faulty("a", 100, 3, Some(fault))];
        let table = stored_table(definition(), locations).unwrap();
        let error = table.column(0).chunk(&table.row_set()).unwrap_err();
        assert!(
            error.message.starts_with("a answered "),
            "{}",
            error.message
        );
    }
}

#[test]
fn a_row_key_outside_every_location_is_a_failure() {
    let locations: Vec<Arc<dyn TableLocation>> = vec![Counted::new("a", 100, 3)];
    let table = stored_table(definition(), locations).unwrap();
    let source = table.column(0);
    let beyond_location = source.chunk(&RowSet::from_range(2, 3)).unwrap_err();
    assert!(beyond_location.message.contains("outside a"));
    let beyond_regions = source
        .chunk(&RowSet::from_range(REGION, REGION))
        .unwrap_err();
    assert!(beyond_regions.message.contains("in no location"));
}

#[test]
fn a_location_can_have_no_more_rows_than_a_region_has_keys() {
    let fits: Vec<Arc<dyn TableLocation>> = vec![
        Counted::new("full", 0, MAX_LOCATION_ROWS),
        Counted::new("next", 0, 1),
    ];
    let table = stored_table(definition(), fits).unwrap();
    assert_eq!(table.row_set().size(), MAX_LOCATION_ROWS + 1);
    // the two regions touch: one range of row keys, read as two ranges of rows
    assert_eq!(table.row_set().ranges().len(), 1);
    let across = RowSet::from_range(REGION - 1, REGION);
    let values = table.column(0).chunk(&across).unwrap();
    assert_eq!(longs(&values), [Some(REGION - 1), Some(0)]);

    let too_many: Vec<Arc<dyn TableLocation>> = vec![Counted::new("big", 0, MAX_LOCATION_ROWS + 1)];
    let error = stored_table(definition(), too_many).err().unwrap();
    assert!(error.message.starts_with("big has "), "{}", error.message);
}
