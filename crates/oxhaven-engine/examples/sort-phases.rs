//! Times sorts of the shapes the end-to-end benchmark uses (`docs/BENCHMARK.md`), reading a
//! sorted column back through the result's row redirection, an incremental sort, and the
//! parts a sort by row-format keys with a box per row is made of (the method before task
//! 9.3), done with Arrow directly. `docs/BENCHMARK.md` records a run.
//!
//! ```text
//! cargo run --release -p oxhaven-engine --example sort-phases             # 1 million rows
//! cargo run --release -p oxhaven-engine --example sort-phases -- 10000000 # or this many
//! ```

use arrow::array::{Array, ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch};
use arrow::array::{StringArray, UInt64Array};
use arrow::compute::{take, SortOptions};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::row::{RowConverter, SortField};
use oxhaven_column::WritableRowRedirection;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_engine::ops::sort::{sort, SortColumn};
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

const COLUMNS: [(&str, ColumnType); 5] = [
    ("Id", ColumnType::Long),
    ("K10", ColumnType::Int),
    ("A", ColumnType::Int),
    ("B", ColumnType::Double),
    ("S", ColumnType::String),
];

/// Rows `ids` of the benchmark's source table, with the columns the sort cases use.
fn batch(ids: std::ops::Range<i64>) -> RecordBatch {
    let ints = |f: fn(i64) -> i64| -> ArrayRef {
        Arc::new(Int32Array::from_iter_values(
            ids.clone().map(|i| f(i) as i32),
        ))
    };
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(ids.clone())),
        ints(|i| i % 10),
        ints(|i| (i * 31337) % 100_003),
        Arc::new(Float64Array::from_iter_values(
            ids.clone()
                .map(|i| ((i * 15_485_863) % 1_000_003) as f64 / 7.0),
        )),
        Arc::new(StringArray::from_iter_values(
            ids.clone().map(|i| format!("s{}", i % 1000)),
        )),
    ];
    let fields = [
        Field::new("Id", DataType::Int64, true),
        Field::new("K10", DataType::Int32, true),
        Field::new("A", DataType::Int32, true),
        Field::new("B", DataType::Float64, true),
        Field::new("S", DataType::Utf8, true),
    ];
    RecordBatch::try_new(Arc::new(Schema::new(fields.to_vec())), columns).expect("the rows")
}

/// Reads column `Id` of `sorted` for every row, and returns a sum weighted by position, so
/// that the order counts.
fn read_id(sorted: &Arc<dyn Table>) -> i64 {
    let index = sorted.definition().index_of("Id").expect("the column Id");
    let values = sorted
        .column(index)
        .chunk(&sorted.row_set())
        .expect("the column");
    let values = values.as_any().downcast_ref::<Int64Array>().expect("longs");
    (0..)
        .zip(values.values())
        .map(|(p, v)| (p % 1009) * v)
        .sum()
}

/// The parts of a sort by `on` the way the sort before task 9.3 made it: the row format of
/// the key columns, one box per row, a stable sort of the boxes and one redirection write
/// per row.
fn boxed_phases(table: &Arc<dyn Table>, on: &[&str]) {
    let rows = table.row_set();
    let columns: Vec<ArrayRef> = timed("  read the key columns", || {
        let read = |name: &&str| {
            let index = table.definition().index_of(name).expect("a column");
            table.column(index).chunk(&rows).expect("a chunk")
        };
        on.iter().map(read).collect()
    });
    let fields = columns
        .iter()
        .map(|c| SortField::new_with_options(c.data_type().clone(), SortOptions::default()))
        .collect();
    let converter = RowConverter::new(fields).expect("a converter");
    let encoded = timed("  encode them as rows in one buffer", || {
        converter.convert_columns(&columns).expect("rows")
    });
    let mut entries: Vec<(RowKey, Box<[u8]>)> = timed("  copy each row into a box", || {
        (0..encoded.num_rows())
            .map(|row| (row as RowKey, encoded.row(row).as_ref().into()))
            .collect()
    });
    timed("  stable sort of the boxes", || {
        entries.sort_by(|a, b| a.1.cmp(&b.1))
    });
    let mut order: Vec<u32> = timed("  stable sort of positions over the one buffer", || {
        let mut order: Vec<u32> = (0..encoded.num_rows() as u32).collect();
        order.sort_by(|&a, &b| encoded.row(a as usize).cmp(&encoded.row(b as usize)));
        order
    });
    timed("  one redirection write per row", || {
        let redirection = WritableRowRedirection::new();
        for (position, (parent, _)) in entries.iter().enumerate() {
            redirection.put(position as RowKey, *parent);
        }
    });
    timed("  the redirection in one piece", || {
        let inner = entries.iter().map(|(parent, _)| *parent).collect();
        WritableRowRedirection::from_rows(&RowSet::flat(entries.len() as u64), inner)
    });
    let positions = UInt64Array::from_iter_values(order.drain(..).map(u64::from));
    timed("  gather a column by the order (arrow take)", || {
        take(columns[0].as_ref(), &positions, None).expect("a gather")
    });
}

fn main() {
    let rows: i64 = match std::env::args().nth(1) {
        Some(text) => text.parse().expect("the number of rows"),
        None => 1_000_000,
    };
    let table: Arc<dyn Table> =
        in_memory_table(definition(&COLUMNS), &[batch(0..rows)]).expect("a table");
    println!("{rows} rows");

    let shapes: [(&str, Vec<SortColumn>, Vec<&str>); 4] = [
        (
            "sort by an int",
            vec![SortColumn::ascending("A")],
            vec!["A"],
        ),
        (
            "sort by an int, descending",
            vec![SortColumn::descending("A")],
            vec!["A"],
        ),
        (
            "sort by an int and a double",
            vec![SortColumn::ascending("K10"), SortColumn::ascending("B")],
            vec!["K10", "B"],
        ),
        (
            "sort by a string",
            vec![SortColumn::ascending("S")],
            vec!["S"],
        ),
    ];
    for (what, columns, on) in shapes {
        let sorted = timed(what, || sort(&table, &columns).expect("a sort"));
        let sum = timed("  read a column of every row back", || read_id(&sorted));
        println!("  weighted sum of Id: {sum}");
        drop(sorted);
        println!("the parts of a sort by boxed row-format keys");
        boxed_phases(&table, &on);
    }

    // A refreshing table: the sort places each cycle's rows among the others.
    let graph: Arc<dyn UpdateGraph> = SteppedUpdateGraph::new();
    let ticking: Arc<dyn Table> =
        append_only_input_table(graph, definition(&COLUMNS), Some(&batch(0..rows)))
            .expect("a refreshing table");
    for (what, column) in [("an int", "A"), ("a string", "S")] {
        let sorted = timed(&format!("sort of a refreshing table by {what}"), || {
            sort(&ticking, &[SortColumn::ascending(column)]).expect("a sort")
        });
        let input = ticking.input_table().expect("an input table");
        let start = ticking.row_set().size() as i64;
        timed("  10 cycles that append 1,000 rows each", || {
            for cycle in 0..10 {
                let first = start + cycle * 1000;
                input.add(batch(first..first + 1000)).expect("an append");
            }
        });
        assert_eq!(sorted.row_set().size() as i64, start + 10_000);
        let sum = timed("  read a column of every row back", || read_id(&sorted));
        println!("  weighted sum of Id: {sum}");
    }
}
