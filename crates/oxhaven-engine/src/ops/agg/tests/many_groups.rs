//! Aggregations and `select_distinct` over many groups that come, go and come back while
//! the parent ticks: the group key table grows across cycles, each layout of key columns
//! (one 64-bit column, one string column, several columns) is used, and keys change.

use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::ops::distinct::select_distinct;
use crate::table::keyed_input_table;
use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_testkit::{test_seeds, ConsistencyChecker};

const COLUMNS: [(&str, ColumnType); 4] = [
    ("Id", ColumnType::Long),
    ("K", ColumnType::Int),
    ("S", ColumnType::String),
    ("V", ColumnType::Double),
];

/// Rows with ids `ids` and the keys `key_of(id)`: `K` the key itself (a null for one
/// key in 97), `S` a string of it, and a value.
fn rows(ids: &[i64], key_of: impl Fn(i64) -> i64) -> RecordBatch {
    let keys: Vec<i64> = ids.iter().map(|&id| key_of(id)).collect();
    let fields: Vec<Field> = COLUMNS
        .iter()
        .map(|(name, column_type)| Field::new(*name, column_type.arrow_type(), true))
        .collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from(ids.to_vec())),
        Arc::new(Int32Array::from_iter(
            keys.iter().map(|&k| (k % 97 != 3).then_some(k as i32)),
        )),
        Arc::new(StringArray::from_iter(
            keys.iter()
                .map(|&k| (k % 89 != 5).then(|| format!("s{}", k % 5000))),
        )),
        Arc::new(Float64Array::from_iter(
            ids.iter()
                .map(|&id| (id % 13 != 0).then_some((id % 1000) as f64 / 8.0)),
        )),
    ];
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("a batch")
}

/// Ids as a batch of the key column, for a delete.
fn ids(ids: &[i64]) -> RecordBatch {
    let field = Field::new("Id", DataType::Int64, true);
    let column: ArrayRef = Arc::new(Int64Array::from(ids.to_vec()));
    RecordBatch::try_new(Arc::new(Schema::new(vec![field])), vec![column]).expect("a batch")
}

/// A small deterministic generator.
struct Random(u64);

impl Random {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

/// Checks `result` after every cycle against `make` of a static copy of `parent`.
fn checked(
    parent: &Arc<dyn Table>,
    result: Arc<dyn Table>,
    keys: usize,
    make: impl Fn(&Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync + 'static,
) -> Arc<ConsistencyChecker> {
    let (parent, like) = (parent.clone(), result.clone());
    ConsistencyChecker::attach_with_recompute(result, move || {
        in_order_of(&*make(&static_copy(&*parent)), &*like, keys)
    })
}

#[test]
fn many_groups_that_come_go_and_change_key_over_many_cycles() {
    for seed in test_seeds(0..10) {
        let mut random = Random(0x2545_f491_4f6c_dd1d + seed);
        let graph: Arc<dyn UpdateGraph> = SteppedUpdateGraph::new();
        let definition = oxhaven_testkit::definition(&COLUMNS);
        // Keys from a range that widens every cycle, so that the key table grows across
        // cycles, and rows that move between keys.
        let mut span = 50;
        let key_of = |id: i64, span: i64, salt: u64| (id * 7919 + salt as i64) % span;
        let initial: Vec<i64> = (0..200).collect();
        let parent: Arc<dyn Table> = keyed_input_table(
            graph.clone(),
            definition,
            &["Id"],
            Some(&rows(&initial, |id| key_of(id, span, 0))),
        )
        .expect("an input table");
        let input = parent.input_table().expect("an input table");
        let aggregations = || {
            vec![
                count("N"),
                agg(AggSpec::Sum, &["SV = V"]),
                agg(AggSpec::Last, &["LV = V", "LId = Id"]),
                agg(AggSpec::First, &["FV = V"]),
                agg(AggSpec::Min, &["MinV = V"]),
            ]
        };
        let mut checkers = Vec::new();
        for by in [&["K"][..], &["S"], &["K", "S"]] {
            let result = aggregate(&parent, &aggregations(), false, by).expect("an aggregation");
            checkers.push(checked(&parent, result, by.len(), move |parent| {
                aggregate(parent, &aggregations(), false, by).expect("an aggregation")
            }));
            let columns: Vec<String> = by.iter().map(|name| name.to_string()).collect();
            let distinct = select_distinct(&parent, &columns).expect("a select_distinct");
            checkers.push(checked(&parent, distinct, by.len(), move |parent| {
                select_distinct(parent, &columns).expect("a select_distinct")
            }));
        }
        let mut next_id = 200;
        for cycle in 0..12 {
            span += 400;
            let live: Vec<i64> = (0..next_id).collect();
            // Delete some rows, which empties groups.
            let gone: Vec<i64> = live
                .iter()
                .copied()
                .filter(|_| random.below(5) == 0)
                .collect();
            input.delete(ids(&gone)).expect("a delete");
            // Rows with a new key: those still present are modified, the others come back.
            let again: Vec<i64> = live
                .iter()
                .copied()
                .filter(|_| random.below(4) == 0)
                .collect();
            let salt = random.below(1000);
            input
                .add(rows(&again, |id| key_of(id, span, salt)))
                .expect("an add");
            // New rows, many with keys never seen.
            let added: Vec<i64> = (next_id..next_id + 300 + 100 * cycle).collect();
            next_id = *added.last().expect("rows") + 1;
            input
                .add(rows(&added, |id| key_of(id, span, 0)))
                .expect("an add");
            for checker in &checkers {
                checker.assert_consistent();
            }
        }
        assert!(parent.row_set().size() > 1000, "seed {seed}");
    }
}

/// A group that loses its rows leaves the result; when a row brings its key back, it
/// returns at its old row key, also after the key table has grown in between.
#[test]
fn a_group_that_empties_returns_to_its_row_after_the_table_grows() {
    let graph: Arc<dyn UpdateGraph> = SteppedUpdateGraph::new();
    let definition = oxhaven_testkit::definition(&COLUMNS);
    let parent: Arc<dyn Table> = keyed_input_table(
        graph,
        definition,
        &["Id"],
        Some(&rows(&[0, 1, 2], |id| 10 + id)),
    )
    .expect("an input table");
    let input = parent.input_table().expect("an input table");
    let sums = aggregate(&parent, &[agg(AggSpec::Sum, &["SV = V"])], false, &["K"])
        .expect("an aggregation");
    let distinct = select_distinct(&parent, &["S".to_string()]).expect("a select_distinct");
    let keys_of = |table: &Arc<dyn Table>, column: usize| -> Vec<String> {
        let values = table
            .column(column)
            .chunk(&table.row_set())
            .expect("a read");
        (0..values.len()).map(|row| cell(&values, row)).collect()
    };
    input.delete(ids(&[1])).expect("a delete");
    assert_eq!(*sums.row_set(), RowSet::from_keys([0, 2]));
    assert_eq!(*distinct.row_set(), RowSet::from_keys([0, 2]));
    // Thousands of new keys, so that the table grows several times.
    let many: Vec<i64> = (100..5100).collect();
    input.add(rows(&many, |id| 1000 + id)).expect("an add");
    // The keys of one row in 97 are null, which is one group.
    let grown = sums.row_set().size();
    assert_eq!(grown, 2 + 5000 - 5000 / 97 + 1);
    input.add(rows(&[7], |_| 11)).expect("an add");
    let back: RowKey = 1;
    assert!(sums.row_set().contains(back));
    assert!(distinct.row_set().contains(back));
    assert_eq!(keys_of(&sums, 0)[1], "11");
    assert_eq!(keys_of(&distinct, 0)[1], "s11");
    assert_eq!(sums.row_set().size(), grown + 1);
}
