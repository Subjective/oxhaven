//! Times as-of joins inside the engine, without a server: the benchmark's `aj` case
//! (`docs/BENCHMARK.md`: the source table joined with a table of a tenth of its rows on
//! `K10` and `Id >= Stamp`) and variations of it: the other rules, no exact-match column, a
//! string key, left stamps out of order, and refreshing tables that grow in cycles.
//!
//! ```text
//! cargo run --release -p oxhaven-engine --example asof-cases            # 1 million rows
//! cargo run --release -p oxhaven-engine --example asof-cases -- 10000000 3
//! ```
//!
//! The optional second argument is the number of timed repetitions (default 5); each case
//! prints its median, minimum and maximum in milliseconds. The optional third argument
//! runs only the cases whose names start with it.

use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{Field, Schema};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_engine::ops::asof::{aj, raj, AsOfMatch, AsOfRule, JoinAddition, JoinMatch};
use oxhaven_engine::table::{append_only_input_table, in_memory_table};
use oxhaven_testkit::definition;
use std::sync::Arc;
use std::time::Instant;

const LEFT: [(&str, ColumnType); 4] = [
    ("Id", ColumnType::Long),
    ("K10", ColumnType::Int),
    ("A", ColumnType::Int),
    ("S", ColumnType::String),
];

const RIGHT: [(&str, ColumnType); 6] = [
    ("K10", ColumnType::Int),
    ("Stamp", ColumnType::Long),
    ("R4", ColumnType::Long),
    ("StampA", ColumnType::Int),
    ("S", ColumnType::String),
    ("RS", ColumnType::Long),
];

fn batch(columns: &[(&str, ColumnType)], arrays: Vec<ArrayRef>) -> RecordBatch {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, column_type)| Field::new(*name, column_type.arrow_type(), true))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("the columns")
}

fn ints(ids: std::ops::Range<i64>, f: impl Fn(i64) -> i64) -> ArrayRef {
    Arc::new(Int32Array::from_iter_values(ids.map(|i| f(i) as i32)))
}

fn longs(ids: std::ops::Range<i64>, f: impl Fn(i64) -> i64) -> ArrayRef {
    Arc::new(Int64Array::from_iter_values(ids.map(f)))
}

/// Rows `ids` of the benchmark's source table: `Id = ii`, `K10 = ii % 10`, `A` (a value in
/// no order) and `S`.
fn left_batch(ids: std::ops::Range<i64>) -> RecordBatch {
    batch(
        &LEFT,
        vec![
            longs(ids.clone(), |i| i),
            ints(ids.clone(), |i| i % 10),
            ints(ids.clone(), |i| (i * 31_337) % 100_003),
            Arc::new(StringArray::from_iter_values(
                ids.map(|i| format!("s{}", i % 1000)),
            )),
        ],
    )
}

/// Rows `ids` of the benchmark's right table `RA`: `K10 = ii % 10`, `Stamp = ii * 10 + 3`,
/// `R4 = ii`, with an int stamp `StampA` in no order and a string key `S`.
fn right_batch(ids: std::ops::Range<i64>) -> RecordBatch {
    batch(
        &RIGHT,
        vec![
            ints(ids.clone(), |i| i % 10),
            longs(ids.clone(), |i| i * 10 + 3),
            longs(ids.clone(), |i| i),
            ints(ids.clone(), |i| (i * 7) % 100_003),
            Arc::new(StringArray::from_iter_values(
                ids.clone().map(|i| format!("s{}", i % 1000)),
            )),
            longs(ids, |i| i * 2),
        ],
    )
}

fn time(what: &str, reps: usize, mut run: impl FnMut()) {
    if !selected(what) {
        return;
    }
    run();
    let mut times: Vec<f64> = (0..reps)
        .map(|_| {
            let started = Instant::now();
            run();
            started.elapsed().as_secs_f64() * 1e3
        })
        .collect();
    times.sort_by(f64::total_cmp);
    println!(
        "{what:<28} {:>9.1} ms  (min {:.1}, max {:.1})",
        times[times.len() / 2],
        times[0],
        times[times.len() - 1]
    );
}

fn selected(name: &str) -> bool {
    std::env::args()
        .nth(3)
        .is_none_or(|prefix| name.starts_with(&prefix))
}

/// The sum of the long column `name` of `table`, read for every row.
fn sum(table: &Arc<dyn Table>, name: &str) -> i64 {
    let index = table.definition().index_of(name).expect("the column");
    let values = table
        .column(index)
        .chunk(&table.row_set())
        .expect("the column's values");
    let values = values.as_any().downcast_ref::<Int64Array>().expect("longs");
    values.iter().flatten().sum()
}

fn main() {
    let mut args = std::env::args().skip(1);
    let rows: i64 = args.next().map_or(1_000_000, |a| a.parse().expect("rows"));
    let reps: usize = args.next().map_or(5, |a| a.parse().expect("repetitions"));
    let left: Arc<dyn Table> =
        in_memory_table(definition(&LEFT), &[left_batch(0..rows)]).expect("a left table");
    let right: Arc<dyn Table> =
        in_memory_table(definition(&RIGHT), &[right_batch(0..rows / 10)]).expect("a right table");
    let k10 = [JoinMatch::same("K10")];
    let s = [JoinMatch::same("S")];
    let r4 = [JoinAddition::same("R4")];
    let stamp = |rule| AsOfMatch::new("Id", rule, "Stamp");
    println!("{rows} left rows, {} right rows", rows / 10);

    time("aj", reps, || {
        aj(&left, &right, &k10, &stamp(AsOfRule::GreaterThanEqual), &r4).expect("aj");
    });
    time("aj_read", reps, || {
        let joined =
            aj(&left, &right, &k10, &stamp(AsOfRule::GreaterThanEqual), &r4).expect("aj");
        sum(&joined, "R4");
    });
    time("aj_strict", reps, || {
        aj(&left, &right, &k10, &stamp(AsOfRule::GreaterThan), &r4).expect("aj");
    });
    time("raj", reps, || {
        raj(&left, &right, &k10, &stamp(AsOfRule::LessThanEqual), &r4).expect("raj");
    });
    time("aj_no_key", reps, || {
        aj(&left, &right, &[], &stamp(AsOfRule::GreaterThanEqual), &r4).expect("aj");
    });
    time("aj_string_key", reps, || {
        aj(&left, &right, &s, &stamp(AsOfRule::GreaterThanEqual), &r4).expect("aj");
    });
    time("aj_unordered_stamps", reps, || {
        let rule = AsOfMatch::new("A", AsOfRule::GreaterThanEqual, "StampA");
        aj(&left, &right, &k10, &rule, &r4).expect("aj");
    });

    let graph: Arc<dyn UpdateGraph> = SteppedUpdateGraph::new();
    if selected("refreshing_left") {
        let grown: Arc<dyn Table> = append_only_input_table(
            graph.clone(),
            definition(&LEFT),
            Some(&left_batch(0..rows)),
        )
        .expect("a left table");
        let started = Instant::now();
        let joined =
            aj(&grown, &right, &k10, &stamp(AsOfRule::GreaterThanEqual), &r4).expect("aj");
        let built = started.elapsed().as_secs_f64() * 1e3;
        let input = grown.input_table().expect("an input table");
        let started = Instant::now();
        for cycle in 0..20 {
            let first = rows + cycle * 1000;
            input
                .add(left_batch(first..first + 1000))
                .expect("an append");
        }
        println!(
            "{:<28} {built:>9.1} ms, then 20 cycles of 1,000 left rows {:.1} ms (sum {})",
            "refreshing_left",
            started.elapsed().as_secs_f64() * 1e3,
            sum(&joined, "R4")
        );
    }
    if selected("refreshing_right") {
        let initial = rows / 10 - 10_000;
        let grown: Arc<dyn Table> = append_only_input_table(
            graph,
            definition(&RIGHT),
            Some(&right_batch(0..initial)),
        )
        .expect("a right table");
        let started = Instant::now();
        let joined =
            aj(&left, &grown, &k10, &stamp(AsOfRule::GreaterThanEqual), &r4).expect("aj");
        let built = started.elapsed().as_secs_f64() * 1e3;
        let input = grown.input_table().expect("an input table");
        let started = Instant::now();
        for cycle in 0..10 {
            let first = initial + cycle * 1000;
            input
                .add(right_batch(first..first + 1000))
                .expect("an append");
        }
        println!(
            "{:<28} {built:>9.1} ms, then 10 cycles of 1,000 right rows {:.1} ms (sum {})",
            "refreshing_right",
            started.elapsed().as_secs_f64() * 1e3,
            sum(&joined, "R4")
        );
    }
}
