//! Times table operations over a table whose row set has about as many ranges as rows: an
//! append-only table of `rows` longs filtered to its even values, then filtered again,
//! sliced, sorted, and appended to. `docs/ROWSET.md` records a run.
//!
//! ```text
//! cargo run --release -p oxhaven-engine --example fragmented-rows            # 4 million rows
//! cargo run --release -p oxhaven-engine --example fragmented-rows -- 1000000 # or this many
//! ```

use arrow::array::{Int64Array, RecordBatch};
use arrow::datatypes::{DataType, Field, Schema};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_engine::ops::filter::unstructured_filter;
use oxhaven_engine::ops::sort::{slice, sort, SortColumn};
use oxhaven_engine::table::append_only_input_table;
use oxhaven_expr::Compiler;
use oxhaven_testkit::definition;
use std::sync::Arc;
use std::time::Instant;

fn timed<T>(what: &str, run: impl FnOnce() -> T) -> T {
    let started = Instant::now();
    let result = run();
    println!("{what}: {:.1} ms", started.elapsed().as_secs_f64() * 1e3);
    result
}

fn main() {
    let rows: i64 = match std::env::args().nth(1) {
        Some(text) => text.parse().expect("the number of rows"),
        None => 4_000_000,
    };
    let schema = Arc::new(Schema::new(vec![Field::new("X", DataType::Int64, true)]));
    let batch = |values: std::ops::Range<i64>| {
        let values = Arc::new(Int64Array::from_iter_values(values));
        RecordBatch::try_new(schema.clone(), vec![values]).expect("one long column")
    };
    let graph: Arc<dyn UpdateGraph> = SteppedUpdateGraph::new();
    let definition = definition(&[("X", ColumnType::Long)]);
    let parent: Arc<dyn Table> =
        append_only_input_table(graph, definition, Some(&batch(0..rows))).expect("a table");

    let even = timed("filter every row to the even ones", || {
        unstructured_filter(&parent, &["X % 2 == 0"], &Compiler).expect("a filter")
    });
    println!(
        "  {} rows in {} ranges",
        even.row_set().size(),
        even.row_set().ranges().count()
    );
    let sixth = timed("filter the even rows again", || {
        unstructured_filter(&even, &["X % 3 == 0"], &Compiler).expect("a filter")
    });
    let middle = even.row_set().size() as i64 / 2;
    timed("100 slices of 100 rows from the middle", || {
        for step in 0..100 {
            let first = middle + step * 1000;
            let window = slice(&even, first, first + 100).expect("a slice");
            assert_eq!(window.row_set().size(), 100);
        }
    });
    timed("sort the even rows, descending", || {
        sort(&even, &[SortColumn::descending("X")]).expect("a sort")
    });
    let input = parent.input_table().expect("an input table");
    timed("20 cycles that append 10 rows each", || {
        for cycle in 0..20 {
            let first = rows + cycle * 10;
            input.add(batch(first..first + 10)).expect("an append");
        }
    });
    assert_eq!(even.row_set().size() as i64, (rows + 201) / 2);
    assert_eq!(sixth.row_set().size() as i64, (rows + 205) / 6);
}
