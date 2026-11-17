use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::table::holds_values;
use arrow::array::{Int32Array, Int64Array};
use oxhaven_contracts::rowset::NULL_ROW_KEY;
use oxhaven_contracts::schema::ColumnDefinition;
use oxhaven_contracts::table::Table;
use oxhaven_testkit::{ConsistencyChecker, Recorder};
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
    assert_eq!(table.row_set().range_count(), 1);
    let across = RowSet::from_range(REGION - 1, REGION);
    let values = table.column(0).chunk(&across).unwrap();
    assert_eq!(longs(&values), [Some(REGION - 1), Some(0)]);

    let too_many: Vec<Arc<dyn TableLocation>> = vec![Counted::new("big", 0, MAX_LOCATION_ROWS + 1)];
    let error = stored_table(definition(), too_many).err().unwrap();
    assert!(error.message.starts_with("big has "), "{}", error.message);
}

/// A provider that answers what a test hands it.
#[derive(Default)]
struct Handed(Mutex<Vec<Found>>);

/// What one look of a provider finds.
type Found = Result<Vec<Arc<dyn TableLocation>>, TableFailure>;

impl Handed {
    fn hand(&self, locations: Vec<Arc<dyn TableLocation>>) {
        self.0.lock().unwrap().push(Ok(locations));
    }
}

impl LocationProvider for Handed {
    fn poll(&self) -> Result<Vec<Arc<dyn TableLocation>>, TableFailure> {
        let mut handed = self.0.lock().unwrap();
        if handed.is_empty() {
            Ok(Vec::new())
        } else {
            handed.remove(0)
        }
    }
}

#[test]
fn a_following_table_gains_the_rows_of_the_locations_found_later() {
    let graph = SteppedUpdateGraph::new();
    let provider = Arc::new(Handed::default());
    let first: Vec<Arc<dyn TableLocation>> = vec![Counted::new("a", 100, 2)];
    let table = following_table(graph.clone(), definition(), first, provider.clone()).unwrap();
    let checker = ConsistencyChecker::attach(table.clone());
    let recorder = Recorder::attach(table.clone());
    // Java gives a source table that follows its locations this attribute alone
    assert!(table.attributes().append_only && !table.attributes().add_only);
    assert!(table.is_refreshing() && !table.is_flat());
    assert!(!holds_values(&table.column(0)));
    assert_eq!(*table.row_set(), RowSet::from_range(0, 1));

    // nothing found: no update
    graph.run_cycle();
    assert!(recorder.events().is_empty());

    // a location without rows gets no region, and the ones with rows come after the first
    provider.hand(vec![
        Counted::new("empty", 0, 0),
        Counted::new("b", 200, 3),
        Counted::new("c", 300, 1),
    ]);
    graph.run_cycle();
    let added = RowSet::from_ranges([
        RowRange::new(REGION, REGION + 2),
        RowRange::new(2 * REGION, 2 * REGION),
    ]);
    let update = TableUpdate {
        added,
        ..TableUpdate::empty()
    };
    assert_eq!(recorder.events(), [Ok(update)]);
    checker.assert_consistent();
    let all = table.column(0).chunk(&table.row_set()).unwrap();
    assert_eq!(longs(&all), [100, 101, 200, 201, 202, 300].map(Some));

    graph.run_cycle();
    assert_eq!(recorder.events().len(), 1);
    checker.assert_consistent();
}

#[test]
fn a_provider_that_fails_fails_the_table() {
    let graph = SteppedUpdateGraph::new();
    let provider = Arc::new(Handed::default());
    let table = following_table(graph.clone(), definition(), Vec::new(), provider.clone()).unwrap();
    let recorder = Recorder::attach(table.clone());
    assert!(table.row_set().is_empty());
    provider
        .0
        .lock()
        .unwrap()
        .push(Err(failure("the directory is gone")));
    graph.run_cycle();
    assert!(table.is_failed());
    assert_eq!(recorder.events(), [Err(failure("the directory is gone"))]);
    // and is not asked again
    provider.hand(vec![Counted::new("late", 0, 1)]);
    graph.run_cycle();
    assert_eq!(provider.0.lock().unwrap().len(), 1);

    // a location too large for a region fails it too
    let provider = Arc::new(Handed::default());
    let table = following_table(graph.clone(), definition(), Vec::new(), provider.clone()).unwrap();
    provider.hand(vec![Counted::new("big", 0, MAX_LOCATION_ROWS + 1)]);
    graph.run_cycle();
    assert!(table.is_failed());
}

/// Which results Java 42.9 gives a partitioning column, of a table read from partition
/// directories (`tasks/7.1b-parquet-read-more.md`).
#[test]
fn a_partitioning_column_stays_one_where_java_keeps_the_definition() {
    use crate::ops::merge::{flatten, merge};
    use crate::ops::select::drop_columns;
    use crate::ops::sort::{head, reverse, sort, tail, SortColumn};
    let definition = TableDefinition::new(vec![
        ColumnDefinition::partitioning("Row", ColumnType::Long),
        ColumnDefinition::new("Odd", ColumnType::Int),
    ])
    .unwrap();
    let locations: Vec<Arc<dyn TableLocation>> =
        vec![Counted::new("a", 100, 2), Counted::new("b", 200, 2)];
    let table: Arc<dyn Table> = stored_table(definition, locations).unwrap();
    let partitioning = |table: &Arc<dyn Table>| -> Vec<bool> {
        let columns = table.definition().columns().iter();
        columns.map(|column| column.partitioning).collect()
    };
    assert_eq!(partitioning(&table), [true, false]);
    // the definition of the table, or the source of the column, goes into the result
    assert_eq!(partitioning(&head(&table, 3).unwrap()), [true, false]);
    assert_eq!(partitioning(&tail(&table, 3).unwrap()), [true, false]);
    assert_eq!(partitioning(&reverse(&table).unwrap()), [true, false]);
    assert_eq!(
        partitioning(&drop_columns(&table, &["Odd"]).unwrap()),
        [true]
    );
    // a sort that moves no row answers the table itself
    let in_order = sort(&table, &[SortColumn::ascending("Row")]).unwrap();
    assert_eq!(partitioning(&in_order), [true, false]);
    // the result is made of new columns
    let sorted = sort(&table, &[SortColumn::descending("Row")]).unwrap();
    assert_eq!(partitioning(&sorted), [false, false]);
    assert_eq!(partitioning(&flatten(&table).unwrap()), [false, false]);
    let merged = merge(&[table.clone(), table.clone()]).unwrap();
    assert_eq!(partitioning(&merged), [false, false]);
}
