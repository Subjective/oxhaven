//! Times the aggregation cases of the end-to-end benchmark (`docs/BENCHMARK.md`) inside the
//! engine, without a server: `select_distinct`, `last_by`, `sum_by`, `avg_by` and `count_by`
//! at three key cardinalities, a five-way `agg_by` and `group_by`, over a static table of
//! `rows` rows built as the benchmark builds its source. Then the same `select_distinct`,
//! `last_by` and `sum_by` kept up to date over an append-only table that grows in 20 cycles.
//!
//! ```text
//! cargo run --release -p oxhaven-engine --example aggregation-cases            # 1 million rows
//! cargo run --release -p oxhaven-engine --example aggregation-cases -- 10000000 5
//! ```
//!
//! The optional second argument is the number of timed repetitions (default 5); each case
//! prints its median, minimum and maximum in milliseconds. The optional third argument
//! runs only the cases whose names start with it (for a profiler), and skips the
//! append-only cycles unless it names them (`append_only_cycles`).

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, RecordBatch, StringArray};
use arrow::datatypes::{Field, Schema};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_engine::graph::SteppedUpdateGraph;
use oxhaven_engine::ops::agg::{
    aggregate, aggregate_all_with, group_by, AggSpec, Aggregation, Pair,
};
use oxhaven_engine::ops::distinct::select_distinct;
use oxhaven_engine::table::{append_only_input_table, in_memory_table};
use oxhaven_expr::Compiler;
use oxhaven_testkit::definition;
use std::sync::Arc;
use std::time::Instant;

const COLUMNS: [(&str, ColumnType); 8] = [
    ("Id", ColumnType::Long),
    ("K10", ColumnType::Int),
    ("K10k", ColumnType::Int),
    ("K1M", ColumnType::Int),
    ("A", ColumnType::Int),
    ("B", ColumnType::Double),
    ("S", ColumnType::String),
    ("K", ColumnType::Int),
];

/// Rows `first..first + rows` of the benchmark's source table, with `K` a copy of the key
/// column `key` (the benchmark's `view(["K = <key>", "A", "B"])`).
fn source(first: i64, rows: i64, key: &str) -> RecordBatch {
    let ids = first..first + rows;
    let ints = |f: fn(i64) -> i64| -> ArrayRef {
        Arc::new(Int32Array::from_iter_values(
            ids.clone().map(|i| f(i) as i32),
        ))
    };
    let k10 = ints(|i| i % 10);
    let k10k = ints(|i| (i * 7919) % 10_000);
    let k1m = ints(|i| (i * 104_729) % 1_000_000);
    let k = match key {
        "K10" => k10.clone(),
        "K10k" => k10k.clone(),
        _ => k1m.clone(),
    };
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(ids.clone())),
        k10,
        k10k,
        k1m,
        ints(|i| (i * 31_337) % 100_003),
        Arc::new(Float64Array::from_iter_values(
            ids.clone()
                .map(|i| ((i * 15_485_863) % 1_000_003) as f64 / 7.0),
        )),
        Arc::new(StringArray::from_iter_values(
            ids.clone().map(|i| format!("s{}", i % 1000)),
        )),
        k,
    ];
    let fields: Vec<Field> = COLUMNS
        .iter()
        .map(|(name, column_type)| Field::new(*name, column_type.arrow_type(), true))
        .collect();
    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("the source columns")
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

/// Whether the case `name` runs: every case, or those that the third argument starts.
fn selected(name: &str) -> bool {
    std::env::args()
        .nth(3)
        .is_none_or(|prefix| name.starts_with(&prefix))
}

fn main() {
    let mut args = std::env::args().skip(1);
    let rows: i64 = args.next().map_or(1_000_000, |a| a.parse().expect("rows"));
    let reps: usize = args.next().map_or(5, |a| a.parse().expect("repetitions"));
    let definition = definition(&COLUMNS);
    let table = |key: &str| -> Arc<dyn Table> {
        in_memory_table(definition.clone(), &[source(0, rows, key)]).expect("a table")
    };
    let all = table("K1M");
    let others = |except: &[&str]| -> Vec<Pair> {
        COLUMNS
            .iter()
            .map(|(name, _)| *name)
            .filter(|name| !except.contains(name) && *name != "K")
            .map(Pair::of)
            .collect()
    };

    time("select_distinct_one", reps, || {
        select_distinct(&all, &["K10k".into()]).expect("distinct");
    });
    time("select_distinct_two", reps, || {
        select_distinct(&all, &["K10".into(), "S".into()]).expect("distinct");
    });
    time("last_by", reps, || {
        aggregate_all_with(&all, AggSpec::Last, others(&["K10k"]), &["K10k"], &Compiler)
            .expect("last_by");
    });
    for (card, key) in [("low", "K10"), ("med", "K10k"), ("high", "K1M")] {
        let keyed = table(key);
        for (name, spec) in [("sum_by", AggSpec::Sum), ("avg_by", AggSpec::Avg)] {
            time(&format!("{name}_{card}"), reps, || {
                let pairs = [Pair::of("A"), Pair::of("B")];
                aggregate_all_with(&keyed, spec.clone(), pairs, &["K"], &Compiler).expect("agg");
            });
        }
        time(&format!("count_by_{card}"), reps, || {
            aggregate(&keyed, &[Aggregation::count("N")], false, &["K"]).expect("count");
        });
    }
    time("agg_by_multi", reps, || {
        let one = |spec: AggSpec, out: &str, input: &str| {
            Aggregation::columns(spec, [Pair::new(out, input)])
        };
        let aggs = [
            one(AggSpec::Sum, "SA", "A"),
            one(AggSpec::Avg, "AB", "B"),
            one(AggSpec::Min, "MinA", "A"),
            one(AggSpec::Max, "MaxB", "B"),
            Aggregation::count("N"),
        ];
        aggregate(&all, &aggs, false, &["K10k"]).expect("agg_by");
    });
    time("group_by", reps, || {
        let narrow: Arc<dyn Table> = in_memory_table(
            oxhaven_testkit::definition(&[("K10k", ColumnType::Int), ("A", ColumnType::Int)]),
            &[source(0, rows, "K10k")
                .project(&[2, 4])
                .expect("two columns")],
        )
        .expect("a table");
        group_by(&narrow, &["K10k"]).expect("group_by");
    });

    // The same operations kept up to date over an append-only table: 20 cycles, each
    // appending a twentieth of the rows.
    if !selected("append_only_cycles") {
        return;
    }
    let graph: Arc<dyn UpdateGraph> = SteppedUpdateGraph::new();
    let step = rows / 20;
    let grown: Arc<dyn Table> =
        append_only_input_table(graph, definition.clone(), Some(&source(0, step, "K1M")))
            .expect("an input table");
    let _distinct = select_distinct(&grown, &["K10k".into()]).expect("distinct");
    let _last = aggregate_all_with(
        &grown,
        AggSpec::Last,
        others(&["K10k"]),
        &["K10k"],
        &Compiler,
    )
    .expect("last_by");
    let _sum = aggregate_all_with(
        &grown,
        AggSpec::Sum,
        [Pair::of("A"), Pair::of("B")],
        &["K1M"],
        &Compiler,
    )
    .expect("sum_by");
    let input = grown.input_table().expect("an input table");
    let started = Instant::now();
    for cycle in 1..20 {
        input
            .add(source(cycle * step, step, "K1M"))
            .expect("an append");
    }
    println!(
        "{:<28} {:>9.1} ms  (19 cycles of {step} rows into select_distinct, last_by, sum_by)",
        "append_only_cycles",
        started.elapsed().as_secs_f64() * 1e3
    );
}
