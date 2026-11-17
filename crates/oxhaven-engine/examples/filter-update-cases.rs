//! Times the filter and formula cases of the end-to-end benchmark (`docs/BENCHMARK.md`)
//! inside the engine, without a server: `where_range`, `where_string`, `where_conj`,
//! `update_arith`, `update_cond`, `update_string` and `build`, over a static table of `rows`
//! rows built as the benchmark builds its source. Then the parts such an operation is made
//! of, each alone: a pass over the `int` column's memory, reading the column as a chunk,
//! evaluating the formula, and making a row set of the rows a mask keeps; and, for task
//! 9.7, joining the pieces of a formula's result and concatenating strings.
//!
//! ```text
//! cargo run --release -p oxhaven-engine --example filter-update-cases            # 1 million rows
//! cargo run --release -p oxhaven-engine --example filter-update-cases -- 10000000 5 1,2,4
//! ```
//!
//! The optional second argument is the number of timed repetitions (default 5), the third
//! the thread counts to run every case with (default: the engine's own default). Each case
//! prints its median, minimum and maximum in milliseconds and the median per row.

use arrow::array::{Array, ArrayRef, AsArray, Float64Array, Int32Array, Int64Array, RecordBatch};
use arrow::array::{BooleanArray, StringArray};
use arrow::datatypes::{Field, Int32Type, Schema};
use oxhaven_contracts::expr::FormulaCompiler;
use oxhaven_contracts::rowset::{RowRange, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_engine::ops::filter::unstructured_filter;
use oxhaven_engine::ops::select::update;
use oxhaven_engine::table::{empty_table, in_memory_table};
use oxhaven_expr::Compiler;
use oxhaven_rowset::{concat_ordered, RowSetBuilderSequential};
use oxhaven_testkit::definition;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Instant;

const COLUMNS: [(&str, ColumnType); 7] = [
    ("Id", ColumnType::Long),
    ("K10", ColumnType::Int),
    ("K10k", ColumnType::Int),
    ("K1M", ColumnType::Int),
    ("A", ColumnType::Int),
    ("B", ColumnType::Double),
    ("S", ColumnType::String),
];

/// The formulas of the benchmark's source table (`bench/bench.py`).
const SOURCE_FORMULAS: [&str; 7] = [
    "Id = ii",
    "K10 = (int)(ii % 10)",
    "K10k = (int)((ii * 7919) % 10000)",
    "K1M = (int)((ii * 104729) % 1000000)",
    "A = (int)((ii * 31337) % 100003)",
    "B = ((ii * 15485863) % 1000003) / 7.0",
    "S = `s` + (ii % 1000)",
];

/// The benchmark's source table of `rows` rows.
fn source(rows: i64) -> RecordBatch {
    let ids = 0..rows;
    let ints = |f: fn(i64) -> i64| -> ArrayRef {
        Arc::new(Int32Array::from_iter_values(
            ids.clone().map(|i| f(i) as i32),
        ))
    };
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(ids.clone())),
        ints(|i| i % 10),
        ints(|i| (i * 7919) % 10_000),
        ints(|i| (i * 104_729) % 1_000_000),
        ints(|i| (i * 31_337) % 100_003),
        Arc::new(Float64Array::from_iter_values(
            ids.clone()
                .map(|i| ((i * 15_485_863) % 1_000_003) as f64 / 7.0),
        )),
        Arc::new(StringArray::from_iter_values(
            ids.clone().map(|i| format!("s{}", i % 1000)),
        )),
    ];
    let fields: Vec<Field> = COLUMNS
        .iter()
        .map(|(name, column_type)| Field::new(*name, column_type.arrow_type(), true))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("the source columns")
}

fn time(what: &str, rows: i64, reps: usize, mut run: impl FnMut()) {
    run();
    let mut times: Vec<f64> = (0..reps)
        .map(|_| {
            let started = Instant::now();
            run();
            started.elapsed().as_secs_f64() * 1e3
        })
        .collect();
    times.sort_by(f64::total_cmp);
    let median = times[times.len() / 2];
    println!(
        "{what:<34} {median:>9.1} ms  (min {:.1}, max {:.1})  {:>6.1} ns/row",
        times[0],
        times[times.len() - 1],
        median * 1e6 / rows as f64
    );
}

fn load() -> String {
    std::fs::read_to_string("/proc/loadavg")
        .map(|text| text.split(' ').take(3).collect::<Vec<_>>().join(" "))
        .unwrap_or_else(|_| "unknown".into())
}

fn cases(table: &Arc<dyn Table>, rows: i64, reps: usize) {
    let filter = |name: &str, filters: &[&str]| {
        time(name, rows, reps, || {
            black_box(unstructured_filter(table, filters, &Compiler).expect("where"));
        });
    };
    let formulas = |name: &str, columns: &[&str]| {
        time(name, rows, reps, || {
            black_box(update(table, columns, &Compiler).expect("update"));
        });
    };
    filter("where_range", &["A >= 20000 && A < 60000"]);
    filter("where_range (two filters)", &["A >= 20000", "A < 60000"]);
    filter("where_string", &["S in `s1`, `s42`, `s999`"]);
    filter("where_conj", &["K10 = 3", "B > 50000.0", "A < 50000"]);
    formulas(
        "update_arith",
        &["C = A * 2 + 1", "D = B * 1.5 - A", "E = Id % 7"],
    );
    formulas("update_cond", &["F = A > 50000 ? B : -B"]);
    formulas("update_string", &["S2 = S + `_x`"]);
    let empty: Arc<dyn Table> = empty_table(rows as u64);
    time("build", rows, reps, || {
        black_box(update(&empty, &SOURCE_FORMULAS, &Compiler).expect("build"));
    });
    time("build (numbers only)", rows, reps, || {
        black_box(update(&empty, &SOURCE_FORMULAS[..6], &Compiler).expect("build"));
    });
}

/// The parts of a filter or a formula column, each alone and on one thread.
fn parts(table: &Arc<dyn Table>, rows: i64, reps: usize) {
    let all = table.row_set();
    let a = table.column(4);
    let values = a.chunk(&all).expect("A");
    let ints = values.as_primitive::<Int32Type>().clone();
    time("scan A (count in range)", rows, reps, || {
        let kept = ints
            .values()
            .iter()
            .filter(|v| (20_000..60_000).contains(*v))
            .count();
        black_box(kept);
    });
    time("read A as a chunk", rows, reps, || {
        black_box(a.chunk(&all).expect("A"));
    });
    let compile = |text: &str| {
        let expr = Compiler.parse(text).expect("a formula");
        Compiler
            .compile(&expr, table.definition())
            .expect("a formula of the table")
    };
    for text in [
        "A >= 20000 && A < 60000",
        "A * 2 + 1",
        "B * 1.5 - A",
        "Id % 7",
        "A > 50000 ? B : -B",
        "S + `_x`",
        "`s` + (Id % 1000)",
    ] {
        let formula = compile(text);
        time(&format!("evaluate {text}"), rows, reps, || {
            black_box(formula.evaluate(&**table, &all, false).expect("values"));
        });
    }
    let mask: BooleanArray = ints
        .values()
        .iter()
        .map(|v| Some((20_000..60_000).contains(v)))
        .collect();
    time("row set of the kept 40%", rows, reps, || {
        let kept = RowSet::from_keys(
            all.iter()
                .zip(mask.iter())
                .filter_map(|(key, keep)| (keep == Some(true)).then_some(key)),
        );
        black_box(kept);
    });
    let sparse = unstructured_filter(table, &["A >= 20000", "A < 60000"], &Compiler)
        .expect("where")
        .row_set();
    time("read A at the kept 40%", rows, reps, || {
        black_box(a.chunk(&sparse).expect("A"));
    });
    time("read A at the kept 40%, 4 threads", rows, reps, || {
        oxhaven_column::parallel::with_threads(4, || black_box(a.chunk(&sparse).expect("A")));
    });
    // The stages of `where_conj`, each on the result of the one before.
    let where_one = |from: &Arc<dyn Table>, filter: &str| {
        unstructured_filter(from, &[filter], &Compiler).expect("where")
    };
    let mut stage = table.clone();
    for filter in ["K10 = 3", "B > 50000.0", "A < 50000"] {
        let from = stage.clone();
        let what = format!("{filter}, of {} rows", from.row_set().size());
        time(&what, rows, reps, || {
            black_box(where_one(&from, filter));
        });
        stage = where_one(&from, filter);
    }
    // The row set of `where_range`, built by one sequential builder over all the kept
    // ranges, as a filter built it before task 9.6, and as it is built since: a row set
    // per piece of 65536 rows, the pieces then joined (here on one thread).
    let kept = mask.values();
    time("ranges of the kept 40%", rows, reps, || {
        black_box(kept.set_slices().collect::<Vec<_>>());
    });
    time("sequential builder over them", rows, reps, || {
        let mut builder = RowSetBuilderSequential::new();
        for (start, end) in kept.set_slices() {
            builder.append_range(start as i64, end as i64 - 1);
        }
        black_box(builder.build());
    });
    time("a row set per piece, joined", rows, reps, || {
        let pieces = oxhaven_column::parallel::pieces_of(kept.len(), 1 << 16);
        let parts = pieces.iter().map(|&(offset, length)| {
            let piece = kept.slice(offset, length);
            RowSet::from_ranges(piece.set_slices().map(|(start, end)| {
                RowRange::new((offset + start) as i64, (offset + end) as i64 - 1)
            }))
        });
        black_box(concat_ordered(parts));
    });
    black_box(values.len());
    joins(table, rows, reps);
}

/// What joining the pieces of a formula's result costs (task 9.7): the pieces of 65536
/// doubles, each in memory of its own as an evaluation leaves them, joined by Arrow's
/// `concat` and copied into one buffer made beforehand; and the strings of `S + "_x"`
/// made with a `String` per row and as offsets and bytes.
fn joins(table: &Arc<dyn Table>, rows: i64, reps: usize) {
    let all = table.row_set();
    let b = table.column(5).chunk(&all).expect("B");
    let doubles = b.as_primitive::<arrow::datatypes::Float64Type>();
    let pieces: Vec<Float64Array> = oxhaven_column::parallel::pieces_of(doubles.len(), 1 << 16)
        .into_iter()
        .map(|(offset, length)| {
            let values = &doubles.values()[offset..offset + length];
            Float64Array::from_iter_values(values.iter().map(|v| v * 1.5))
        })
        .collect();
    time("join pieces of doubles: concat", rows, reps, || {
        let pieces: Vec<&dyn Array> = pieces.iter().map(|piece| piece as &dyn Array).collect();
        black_box(arrow::compute::concat(&pieces).expect("a join"));
    });
    time("join pieces of doubles: one buffer", rows, reps, || {
        let mut joined = vec![0.0_f64; doubles.len()];
        for (piece, to) in pieces.iter().zip(joined.chunks_mut(1 << 16)) {
            to.copy_from_slice(piece.values());
        }
        black_box(Float64Array::from(joined));
    });
    time("evaluate B * 1.5 into one buffer", rows, reps, || {
        let mut joined = vec![0.0_f64; doubles.len()];
        for (from, to) in doubles.values().iter().zip(&mut joined) {
            *to = from * 1.5;
        }
        black_box(Float64Array::from(joined));
    });
    let s = table.column(6).chunk(&all).expect("S");
    let strings = s.as_string::<i32>();
    time("S + `_x`: a String per row", rows, reps, || {
        let joined = strings
            .iter()
            .map(|s| format!("{}{}", s.expect("a string"), "_x"));
        black_box(StringArray::from_iter_values(joined));
    });
    time("S + `_x`: offsets and bytes", rows, reps, || {
        let mut bytes = String::with_capacity(strings.value_data().len() + 2 * strings.len());
        let mut offsets = Vec::with_capacity(strings.len() + 1);
        offsets.push(0_i32);
        for s in strings.iter() {
            bytes.push_str(s.expect("a string"));
            bytes.push_str("_x");
            offsets.push(bytes.len() as i32);
        }
        let offsets = arrow::buffer::OffsetBuffer::new(offsets.into());
        black_box(StringArray::new(offsets, bytes.into_bytes().into(), None));
    });
}

fn main() {
    let mut args = std::env::args().skip(1);
    let rows: i64 = args.next().map_or(1_000_000, |a| a.parse().expect("rows"));
    let reps: usize = args.next().map_or(5, |a| a.parse().expect("repetitions"));
    let threads: Vec<usize> = args.next().map_or(Vec::new(), |a| {
        a.split(',').map(|n| n.parse().expect("threads")).collect()
    });
    let table: Arc<dyn Table> =
        in_memory_table(definition(&COLUMNS), &[source(rows)]).expect("a table");
    println!("{rows} rows, {reps} repetitions, load average {}", load());
    if threads.is_empty() {
        cases(&table, rows, reps);
    }
    for count in threads {
        println!("-- {count} thread(s), load average {}", load());
        oxhaven_column::parallel::with_threads(count, || cases(&table, rows, reps));
    }
    println!("-- parts, one thread, load average {}", load());
    oxhaven_column::parallel::with_threads(1, || parts(&table, rows, reps));
    println!("load average {}", load());
}
