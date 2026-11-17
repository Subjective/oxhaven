//! Times natural joins of the shapes the end-to-end benchmark uses (`docs/BENCHMARK.md`), and
//! the parts a join is made of: reading the key columns, the join itself, and reading an
//! added column back through the result's row redirection.
//!
//! ```text
//! cargo run --release -p oxhaven-engine --example join-phases             # 1 million left rows
//! cargo run --release -p oxhaven-engine --example join-phases -- 10000000 # or this many
//! ```

use arrow::array::{ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_engine::ops::join::{exact_join, natural_join, JoinMatch, NaturalJoinType};
use oxhaven_engine::table::{append_only_input_table, in_memory_table};
use oxhaven_testkit::definition;
use std::sync::Arc;
use std::time::Instant;

fn timed<T>(what: &str, run: impl FnOnce() -> T) -> T {
    let started = Instant::now();
    let result = run();
    println!("{what}: {:.1} ms", started.elapsed().as_secs_f64() * 1e3);
    result
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

/// A right table of `keys` unique keys named `key`, with a value `R`.
fn right_batch(key: &str, keys: i64) -> (Vec<(&str, ColumnType)>, RecordBatch) {
    let (key_type, values): (ColumnType, ArrayRef) = if key == "S" {
        let texts = (0..keys).map(|i| format!("s{i}"));
        (
            ColumnType::String,
            Arc::new(StringArray::from_iter_values(texts)),
        )
    } else {
        let ints = (0..keys).map(|i| i as i32);
        (
            ColumnType::Int,
            Arc::new(Int32Array::from_iter_values(ints)),
        )
    };
    let fields = [
        Field::new(key, values.data_type().clone(), true),
        Field::new("R", DataType::Int64, true),
    ];
    let r: ArrayRef = Arc::new(Int64Array::from_iter_values((0..keys).map(|i| i * 3)));
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields.to_vec())), vec![values, r]);
    (
        vec![(key, key_type), ("R", ColumnType::Long)],
        batch.expect("the right rows"),
    )
}

fn static_right(key: &'static str, keys: i64) -> Arc<dyn Table> {
    let (columns, batch) = right_batch(key, keys);
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

fn main() {
    let rows: i64 = match std::env::args().nth(1) {
        Some(text) => text.parse().expect("the number of rows"),
        None => 1_000_000,
    };
    let left: Arc<dyn Table> =
        in_memory_table(definition(&LEFT), &[left_batch(0..rows)]).expect("a left table");
    let on = |key: &str| vec![JoinMatch::same(key)];
    let error = NaturalJoinType::ErrorOnDuplicate;
    println!("{rows} left rows");

    timed("read the int key column of every left row", || {
        left.column(0).chunk(&left.row_set()).expect("a chunk")
    });
    timed("read the string key column of every left row", || {
        left.column(2).chunk(&left.row_set()).expect("a chunk")
    });

    let small = static_right("K10k", 10_000);
    let large = static_right("K1M", 1_000_000);
    let texts = static_right("S", 1000);
    let joined = timed("natural join, int key, 10,000 right rows", || {
        natural_join(&left, &small, &on("K10k"), &[], error).expect("a join")
    });
    let sum = timed("  read the added column of every row", || read_r(&joined));
    println!("  sum of R: {sum}");
    drop(joined);
    let joined = timed("natural join, int key, 1,000,000 right rows", || {
        natural_join(&left, &large, &on("K1M"), &[], error).expect("a join")
    });
    timed("  read the added column of every row", || read_r(&joined));
    drop(joined);
    timed("exact join, int key, 10,000 right rows", || {
        exact_join(&left, &small, &on("K10k"), &[]).expect("a join")
    });
    timed("natural join, string key, 1,000 right rows", || {
        natural_join(&left, &texts, &on("S"), &[], error).expect("a join")
    });
    // The right table of the two-key join: every pair of a small key and a string.
    let pairs: Arc<dyn Table> = {
        let (columns, batch) = right_batch("K10k", 10_000);
        let k = batch.column(0).clone();
        // Row `i` of the left table has K10k = 7919 i mod 10,000 and S = i mod 1000; the
        // inverse of 7919 modulo 10,000 is 7679, so the row with K10k = k has
        // S = 7679 k mod 1000.
        let s_of_k: ArrayRef = Arc::new(StringArray::from_iter_values(
            (0..10_000i64).map(|k| format!("s{}", (k * 7679 % 10_000) % 1000)),
        ));
        let fields = [
            Field::new("K10k", DataType::Int32, true),
            Field::new("S", DataType::Utf8, true),
            Field::new("R", DataType::Int64, true),
        ];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(fields.to_vec())),
            vec![k, s_of_k, batch.column(1).clone()],
        )
        .expect("the right rows");
        let mut definitions = columns;
        definitions.insert(1, ("S", ColumnType::String));
        in_memory_table(definition(&definitions), &[batch]).expect("a right table")
    };
    let joined = timed(
        "natural join, int and string keys, 10,000 right rows",
        || {
            let on = [JoinMatch::same("K10k"), JoinMatch::same("S")];
            natural_join(&left, &pairs, &on, &[], error).expect("a join")
        },
    );
    println!("  sum of R: {}", read_r(&joined));
    drop(joined);

    // Refreshing tables: the join keeps what it needs to follow changes.
    let graph: Arc<dyn UpdateGraph> = SteppedUpdateGraph::new();
    let ticking_left: Arc<dyn Table> =
        append_only_input_table(graph.clone(), definition(&LEFT), Some(&left_batch(0..rows)))
            .expect("a left table");
    let joined = timed("natural join, refreshing left, static right", || {
        natural_join(&ticking_left, &small, &on("K10k"), &[], error).expect("a join")
    });
    let input = ticking_left.input_table().expect("an input table");
    timed("  20 cycles that append 1,000 left rows each", || {
        for cycle in 0..20 {
            let first = rows + cycle * 1000;
            input
                .add(left_batch(first..first + 1000))
                .expect("an append");
        }
    });
    assert_eq!(joined.row_set().size() as i64, rows + 20_000);
    drop(joined);

    let (columns, batch) = right_batch("K10k", 10_000);
    let ticking_right: Arc<dyn Table> =
        append_only_input_table(graph, definition(&columns), Some(&batch.slice(0, 9000)))
            .expect("a right table");
    let joined = timed("natural join, static left, refreshing right", || {
        natural_join(&left, &ticking_right, &on("K10k"), &[], error).expect("a join")
    });
    let input = ticking_right.input_table().expect("an input table");
    timed("  10 cycles that append 100 right rows each", || {
        for cycle in 0..10 {
            input
                .add(batch.slice(9000 + cycle * 100, 100))
                .expect("an append");
        }
    });
    println!("  sum of R: {}", read_r(&joined));
}
