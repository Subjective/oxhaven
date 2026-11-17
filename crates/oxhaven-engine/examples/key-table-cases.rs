//! Times the key table (`ops::key_table`) alone on the shapes of key the benchmark's joins
//! and aggregations have (`docs/BENCHMARK.md`): numbering the keys of `rows` rows in a new
//! table as the joins do, finding them all again, and numbering them as the aggregations
//! do, once in chunks of 4,096 rows (an aggregation's) and once in one chunk (a join's).
//!
//! ```text
//! cargo run --release -p oxhaven-engine --example key-table-cases            # 1 million rows
//! cargo run --release -p oxhaven-engine --example key-table-cases -- 10000000 3
//! ```
//!
//! The optional second argument is the number of timed repetitions (default 7); each
//! number is the median in milliseconds.

use arrow::array::{ArrayRef, Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::DataType;
use oxhaven_engine::ops::key_table::KeyTable;
use std::sync::Arc;
use std::time::Instant;

fn median(reps: usize, mut run: impl FnMut() -> u64) -> (f64, u64) {
    let mut check = run();
    let mut times: Vec<f64> = (0..reps)
        .map(|_| {
            let started = Instant::now();
            check = run();
            started.elapsed().as_secs_f64() * 1e3
        })
        .collect();
    times.sort_by(f64::total_cmp);
    (times[times.len() / 2], check)
}

fn main() {
    let mut args = std::env::args().skip(1);
    let rows: i64 = args.next().map_or(1_000_000, |a| a.parse().expect("rows"));
    let reps: usize = args.next().map_or(7, |a| a.parse().expect("repetitions"));
    let ids = 0..rows;
    let ints = |f: fn(i64) -> i64| -> ArrayRef {
        Arc::new(Int32Array::from_iter_values(
            ids.clone().map(|i| f(i) as i32),
        ))
    };
    let k10 = ints(|i| i % 10);
    let k10k = ints(|i| (i * 7919) % 10_000);
    let k1m = ints(|i| (i * 104_729) % 1_000_000);
    let id: ArrayRef = Arc::new(Int64Array::from_iter_values(ids.clone()));
    let b: ArrayRef = Arc::new(Float64Array::from_iter_values(
        ids.clone()
            .map(|i| ((i * 15_485_863) % 1_000_003) as f64 / 7.0),
    ));
    let s: ArrayRef = Arc::new(StringArray::from_iter_values(
        ids.clone().map(|i| format!("s{}", i % 1000)),
    ));
    let s1m: ArrayRef = Arc::new(StringArray::from_iter_values(
        ids.clone()
            .map(|i| format!("key{}", (i * 104_729) % 1_000_000)),
    ));
    let cases: Vec<(&str, Vec<ArrayRef>)> = vec![
        ("int 10", vec![k10.clone()]),
        ("int 10k", vec![k10k.clone()]),
        ("int 1M", vec![k1m.clone()]),
        ("long 1M (Id)", vec![id]),
        ("double 1M", vec![b]),
        ("string 1000", vec![s.clone()]),
        ("string 1M", vec![s1m]),
        ("int 10 + string 1000", vec![k10.clone(), s]),
        ("int 10 + int 10k", vec![k10, k10k]),
    ];
    println!("{rows} rows, median of {reps}, ms");
    for chunk in [4096usize, rows as usize] {
        println!("chunks of {chunk}");
        for (name, columns) in &cases {
            let types: Vec<DataType> = columns.iter().map(|c| c.data_type().clone()).collect();
            let pieces: Vec<Vec<ArrayRef>> = (0..rows as usize)
                .step_by(chunk)
                .map(|start| {
                    let len = chunk.min(rows as usize - start);
                    columns.iter().map(|c| c.slice(start, len)).collect()
                })
                .collect();
            let (insert, _) = median(reps, || {
                let mut table = KeyTable::new();
                let mut sum = 0u64;
                for piece in &pieces {
                    for slot in table.insert(piece, piece[0].len()) {
                        sum += u64::from(slot);
                    }
                }
                sum
            });
            let mut full = KeyTable::new();
            for piece in &pieces {
                full.insert(piece, piece[0].len());
            }
            let (find, _) = median(reps, || {
                let mut sum = 0u64;
                for piece in &pieces {
                    for slot in full.find(piece, piece[0].len()) {
                        sum += u64::from(slot);
                    }
                }
                sum
            });
            let (declared, _) = median(reps, || {
                let mut table = KeyTable::for_types(&types).expect("key types");
                let mut sum = 0u64;
                let (mut slots, mut created): (Vec<usize>, Vec<usize>) = (Vec::new(), Vec::new());
                for piece in &pieces {
                    slots.clear();
                    created.clear();
                    table
                        .find_or_create(piece, &mut slots, &mut created)
                        .expect("keys");
                    sum += slots.iter().map(|slot| *slot as u64).sum::<u64>();
                }
                sum
            });
            println!(
                "  {name:<22} insert {insert:>7.1}  find {find:>7.1}  find_or_create {declared:>7.1}"
            );
        }
    }
}
