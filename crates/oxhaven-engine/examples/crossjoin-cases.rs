//! Times keyed cross joins (`join` in Deephaven) of the shapes the end-to-end benchmark's
//! `join_on_key` uses and a few around it: one or several right rows per key, a string key,
//! and refreshing tables that tick. Each case runs several times and prints its median and
//! minimum. `docs/BENCHMARK.md` records a run.
//!
//! ```text
//! cargo run --release -p oxhaven-engine --example crossjoin-cases             # 1 million left rows
//! cargo run --release -p oxhaven-engine --example crossjoin-cases -- 10000000 # or this many
//! ```

use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_engine::ops::crossjoin::cross_join;
use oxhaven_engine::ops::join::{JoinAddition, JoinMatch};
use oxhaven_engine::table::{append_only_input_table, in_memory_table};
use oxhaven_testkit::definition;
use std::sync::Arc;
use std::time::Instant;

const RUNS: usize = 7;

/// Runs `run` [`RUNS`] times and prints the median and the minimum in milliseconds.
fn timed(what: &str, mut run: impl FnMut()) {
    let mut times: Vec<f64> = (0..RUNS)
        .map(|_| {
            let started = Instant::now();
            run();
            started.elapsed().as_secs_f64() * 1e3
        })
        .collect();
    times.sort_by(f64::total_cmp);
    println!(
        "{what}: median {:.1} ms, min {:.1} ms",
        times[RUNS / 2],
        times[0]
    );
}

const LEFT: [(&str, ColumnType); 4] = [
    ("K10k", ColumnType::Int),
    ("K1M", ColumnType::Int),
    ("S", ColumnType::String),
    ("Id", ColumnType::Long),
];

/// Rows `ids` of the benchmark's source table: its two int keys, its string and its row
/// number.
fn left_batch(ids: std::ops::Range<i64>) -> RecordBatch {
    let ints = |f: fn(i64) -> i64| -> ArrayRef {
        Arc::new(Int32Array::from_iter_values(
            ids.clone().map(|i| f(i) as i32),
        ))
    };
    let columns: Vec<ArrayRef> = vec![
        ints(|i| (i * 7919) % 10_000),
        ints(|i| (i * 104_729) % 1_000_000),
        Arc::new(StringArray::from_iter_values(
            ids.clone().map(|i| format!("s{}", i % 1000)),
        )),
        Arc::new(Int64Array::from_iter_values(ids.clone())),
    ];
    let fields = [
        Field::new("K10k", DataType::Int32, true),
        Field::new("K1M", DataType::Int32, true),
        Field::new("S", DataType::Utf8, true),
        Field::new("Id", DataType::Int64, true),
    ];
    RecordBatch::try_new(Arc::new(Schema::new(fields.to_vec())), columns).expect("the left rows")
}

/// A right table of `keys` keys named `key`, each `copies` times, with a value `R`.
fn right_batch(key: &str, keys: i64, copies: i64) -> (Vec<(&str, ColumnType)>, RecordBatch) {
    let ids = 0..keys * copies;
    let (key_type, values): (ColumnType, ArrayRef) = if key == "S" {
        let texts = ids.clone().map(|i| format!("s{}", i % keys));
        (
            ColumnType::String,
            Arc::new(StringArray::from_iter_values(texts)),
        )
    } else {
        let ints = ids.clone().map(|i| (i % keys) as i32);
        (
            ColumnType::Int,
            Arc::new(Int32Array::from_iter_values(ints)),
        )
    };
    let fields = [
        Field::new(key, values.data_type().clone(), true),
        Field::new("R", DataType::Int64, true),
    ];
    let r: ArrayRef = Arc::new(Int64Array::from_iter_values(ids.map(|i| i * 3)));
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields.to_vec())), vec![values, r]);
    (
        vec![(key, key_type), ("R", ColumnType::Long)],
        batch.expect("the right rows"),
    )
}

fn static_right(key: &'static str, keys: i64, copies: i64) -> Arc<dyn Table> {
    let (columns, batch) = right_batch(key, keys, copies);
    in_memory_table(definition(&columns), &[batch]).expect("a right table")
}

/// Reads column `R` of `joined` for every row, and returns its sum.
fn read_r(joined: &Arc<dyn Table>) -> i64 {
    let index = joined.definition().index_of("R").expect("the column R");
    let values = joined
        .column(index)
        .chunk(&joined.row_set())
        .expect("the added column");
    let values = values.as_any().downcast_ref::<Int64Array>().expect("longs");
    values.iter().flatten().sum()
}

fn join(left: &Arc<dyn Table>, right: &Arc<dyn Table>, key: &str) -> Arc<dyn Table> {
    cross_join(
        left,
        right,
        &[JoinMatch::same(key)],
        &[JoinAddition::same("R")],
        10,
    )
    .expect("a join")
}

fn main() {
    let rows: i64 = match std::env::args().nth(1) {
        Some(text) => text.parse().expect("the number of rows"),
        None => 1_000_000,
    };
    let left: Arc<dyn Table> =
        in_memory_table(definition(&LEFT), &[left_batch(0..rows)]).expect("a left table");
    println!("{rows} left rows, {RUNS} runs of each case");

    let cases = [
        ("int key, 10,000 right rows, one per key", "K10k", 10_000, 1),
        (
            "int key, 1,000,000 right rows, one per key",
            "K1M",
            1_000_000,
            1,
        ),
        ("int key, 1,000 keys, 3 right rows per key", "K10k", 1000, 3),
        ("string key, 1,000 right rows, one per key", "S", 1000, 1),
    ];
    for (what, key, keys, copies) in cases {
        let right = static_right(key, keys, copies);
        let joined = join(&left, &right, key);
        println!("  {} result rows", joined.row_set().size());
        drop(joined);
        timed(&format!("cross join, {what}"), || {
            join(&left, &right, key);
        });
        let joined = join(&left, &right, key);
        timed("  read the added column of every row", || {
            read_r(&joined);
        });
    }

    // Refreshing tables: the join keeps what it needs to follow changes.
    let graph: Arc<dyn UpdateGraph> = SteppedUpdateGraph::new();
    let small = static_right("K10k", 10_000, 1);
    let ticking = |what: &str, appended: i64| {
        let ticking_left: Arc<dyn Table> =
            append_only_input_table(graph.clone(), definition(&LEFT), Some(&left_batch(0..rows)))
                .expect("a left table");
        timed(&format!("cross join, refreshing left, {what}"), || {
            join(&ticking_left, &small, "K10k");
        });
        let joined = join(&ticking_left, &small, "K10k");
        let input = ticking_left.input_table().expect("an input table");
        let mut first = rows + appended;
        timed("  20 cycles that append 1,000 left rows each", || {
            for _ in 0..20 {
                input
                    .add(left_batch(first..first + 1000))
                    .expect("an append");
                first += 1000;
            }
        });
        assert!(joined.row_set().size() as i64 >= rows);
    };
    ticking("static right, 10,000 right rows", 0);

    let (columns, batch) = right_batch("K10k", 10_000, 1);
    let ticking_right: Arc<dyn Table> = append_only_input_table(
        graph.clone(),
        definition(&columns),
        Some(&batch.slice(0, 9000)),
    )
    .expect("a right table");
    timed("cross join, static left, refreshing right", || {
        join(&left, &ticking_right, "K10k");
    });
    let joined = join(&left, &ticking_right, "K10k");
    let input = ticking_right.input_table().expect("an input table");
    input.add(batch.slice(9000, 1000)).expect("an append");
    println!("  sum of R: {}", read_r(&joined));
}
