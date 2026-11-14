//! Aggregations of the fixtures in `ops/agg/fixtures`, checked against the results recorded
//! there from the Java server (`fixtures/record.py` says how they are recorded and what a
//! fixture holds).
//!
//! These cover the aggregations the oracle's scenarios do not have yet. Every operation of
//! every fixture is run and compared at every recorded step, by the rules of `PARITY.md` as
//! `oracle/harness/rules.py` applies them (`corpus::assert_matches`); an operation Java refused
//! must be refused too.

use super::super::AggregateError;
use super::corpus::{assert_matches, inputs, json, list, rows_batch, run_step, text};
use super::*;
use crate::graph::SteppedUpdateGraph;
use arrow::datatypes::Schema;
use serde_json::Value;
use std::path::PathBuf;

/// The number of (table, step) results the fixtures must compare, and of operations Java
/// refused. An operation that silently stops being run shows as a different count.
const COMPARED: usize = 388;
const REFUSED: usize = 15;

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/ops/agg/fixtures")
}

fn flag(entry: &Value, name: &str) -> bool {
    entry[name]
        .as_bool()
        .unwrap_or_else(|| panic!("{name} is not a Boolean"))
}

/// The aggregation of one entry of an `agg` operation; see `fixtures/record.py`.
fn aggregation(entry: &Value) -> Aggregation {
    let kind = text(&entry["agg"], "agg");
    if kind == "count" {
        return count(text(&entry["column"], "column"));
    }
    let spec = match kind {
        "sum" => AggSpec::Sum,
        "abs_sum" => AggSpec::AbsSum,
        "avg" => AggSpec::Avg,
        "min" => AggSpec::Min,
        "max" => AggSpec::Max,
        "first" => AggSpec::First,
        "last" => AggSpec::Last,
        "var" => AggSpec::Var,
        "std" => AggSpec::Std,
        "median" => AggSpec::Median {
            average_evenly_divided: flag(entry, "average"),
        },
        "pct" => AggSpec::Percentile {
            percentile: entry["percentile"].as_f64().expect("a percentile"),
            average_evenly_divided: flag(entry, "average"),
        },
        "count_distinct" => AggSpec::CountDistinct {
            count_nulls: flag(entry, "count_nulls"),
        },
        "unique" => AggSpec::Unique {
            include_nulls: flag(entry, "include_nulls"),
        },
        "weighted_avg" => AggSpec::WeightedAvg {
            weight: text(&entry["weight"], "weight").to_string(),
        },
        "weighted_sum" => AggSpec::WeightedSum {
            weight: text(&entry["weight"], "weight").to_string(),
        },
        "sorted_first" => AggSpec::SortedFirst {
            columns: vec![text(&entry["order_by"], "order_by").to_string()],
        },
        "sorted_last" => AggSpec::SortedLast {
            columns: vec![text(&entry["order_by"], "order_by").to_string()],
        },
        other => panic!("no aggregation {other}"),
    };
    let pairs: Vec<&str> = list(&entry["cols"], "cols")
        .iter()
        .map(|pair| text(pair, "pair"))
        .collect();
    agg(spec, &pairs)
}

/// The output columns that PARITY.md A1 lets differ within a tolerance: the floating point
/// results of `sum`, `abs_sum`, `avg`, `var`, `std`, `weighted_avg` and `weighted_sum`. A
/// median or percentile is compared exactly, also where it averages two values.
fn computed_columns(aggregations: &[Aggregation]) -> Vec<String> {
    aggregations
        .iter()
        .filter_map(|aggregation| match aggregation {
            Aggregation::Columns {
                spec:
                    AggSpec::Sum
                    | AggSpec::AbsSum
                    | AggSpec::Avg
                    | AggSpec::Var
                    | AggSpec::Std
                    | AggSpec::WeightedAvg { .. }
                    | AggSpec::WeightedSum { .. },
                pairs,
            } => Some(pairs.iter().map(|pair| pair.output.clone())),
            _ => None,
        })
        .flatten()
        .collect()
}

/// The table Java returned at one step, as a batch with the `IsFlat` attribute in its
/// schema metadata, as `corpus::assert_matches` expects.
fn expected(recorded: &Value) -> RecordBatch {
    let columns: Vec<&Value> = list(&recorded["columns"], "columns").iter().collect();
    let (_, batch) = rows_batch(&columns, list(&recorded["rows"], "rows"));
    let flat = recorded["attributes"]["IsFlat"] == true;
    let metadata: HashMap<String, String> =
        [("deephaven:attribute.IsFlat".to_string(), flat.to_string())]
            .into_iter()
            .collect();
    let schema = Schema::new(batch.schema().fields().clone()).with_metadata(metadata);
    batch
        .with_schema(Arc::new(schema))
        .expect("the same fields")
}

#[derive(Default)]
struct Tally {
    compared: usize,
    refused: usize,
}

fn run_fixture(path: &PathBuf, tally: &mut Tally) {
    let fixture = json(path);
    let name = text(&fixture["name"], "name");
    let recorded_steps = list(&fixture["java"]["steps"], "java.steps");
    let graph = SteppedUpdateGraph::new();
    let inputs = inputs(&fixture, &graph);

    let mut tables: Vec<(&str, Arc<dyn Table>, Vec<String>)> = Vec::new();
    for operation in list(&fixture["operations"], "operations") {
        let table_name = text(&operation["name"], "name");
        let context = format!("{name}.{table_name}");
        assert_eq!(operation["op"], "agg", "{context}: only `agg` is run here");
        let input = &inputs[text(&operation["source"], "source")];
        let aggregations: Vec<Aggregation> = list(&operation["aggs"], "aggs")
            .iter()
            .map(aggregation)
            .collect();
        let by: Vec<&str> = list(&operation["by"], "by")
            .iter()
            .map(|column| text(column, "by"))
            .collect();
        let result = aggregate(&input.table, &aggregations, false, &by);
        let failure = &recorded_steps[0][table_name]["failure"];
        match result {
            Ok(table) if failure.is_null() => {
                tables.push((table_name, table, computed_columns(&aggregations)));
            }
            Err(error) if !failure.is_null() => {
                assert_eq!(failure, "INVALID_ARGUMENT", "{context}");
                assert!(
                    !matches!(error, AggregateError::Failed(_)),
                    "{context}: {error} is not a refusal of the request"
                );
                tally.refused += 1;
            }
            Ok(_) => panic!("{context}: Java refused with {failure}"),
            Err(error) => panic!("{context}: {error}, where Java gave a table"),
        }
    }

    let steps = fixture["steps"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        recorded_steps.len(),
        steps.len() + 1,
        "{name}: recorded steps"
    );
    for (step, recorded) in recorded_steps.iter().enumerate() {
        if step > 0 {
            run_step(&steps[step - 1], &inputs, &graph);
        }
        for (table_name, table, computed) in &tables {
            let context = format!("{name}.{table_name} at step {step}");
            let entry = &recorded[*table_name];
            assert!(entry["failure"].is_null(), "{context}: Java failed");
            assert!(!table.is_failed(), "{context}: failed");
            assert_matches(&context, &**table, &expected(entry), computed);
            tally.compared += 1;
        }
    }
}

#[test]
fn aggregations_match_the_recorded_java_fixtures() {
    let mut paths: Vec<PathBuf> = std::fs::read_dir(fixtures())
        .expect("the fixtures directory")
        .map(|entry| entry.expect("a directory entry").path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == "json")
        })
        .collect();
    paths.sort();
    assert!(!paths.is_empty());
    let mut tally = Tally::default();
    for path in &paths {
        run_fixture(path, &mut tally);
    }
    assert_eq!((tally.compared, tally.refused), (COMPARED, REFUSED));
}
