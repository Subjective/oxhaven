//! Aggregations of the fixtures in `ops/agg/fixtures`, checked against the results recorded
//! there from the Java server (`fixtures/record.py` says how they are recorded and what a
//! fixture holds).
//!
//! These cover the aggregations the oracle's scenarios do not have yet. Every operation of
//! every fixture is run and compared at every recorded step, by the rules of `PARITY.md` as
//! `oracle/harness/rules.py` applies them (`corpus::assert_matches`), and the column types
//! with it, a vector apart from an array; an operation Java refused must be refused too.
//!
//! For the tables a fixture names under `updates`, every update is compared as well: the
//! added, removed and modified rows by position and the modified columns, update by update.
//! PARITY.md A4 does not ask for that. It is compared because the fixtures can: the tables
//! are aggregations of input tables, which change in the same way on both sides.

use super::super::{aggregate_with, group_by, AggregateError};
use super::corpus::{apply_action, assert_matches, inputs, json, list, rows_batch, run_step, text};
use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::ops::join::{natural_join, JoinAddition, JoinMatch, NaturalJoinType};
use arrow::datatypes::Schema;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_testkit::Recorder;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;

/// The number of (table, step) results the fixtures must compare, and of operations Java
/// refused. An operation that silently stops being run shows as a different count.
const COMPARED: usize = 586;
const REFUSED: usize = 19;
/// The number of (table, step) lists of updates the fixtures must compare.
const UPDATES: usize = 119;

fn fixtures() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("src/ops/agg/fixtures")
}

fn flag(entry: &Value, name: &str) -> bool {
    entry[name]
        .as_bool()
        .unwrap_or_else(|| panic!("{name} is not a Boolean"))
}

/// The non-unique sentinel of a `unique` entry: absent or null for none, and otherwise
/// `{"type": value}` with the type a client sends it as; see `fixtures/record.py`.
fn sentinel(entry: &Value) -> Sentinel {
    let Some(given) = entry.get("sentinel").filter(|given| !given.is_null()) else {
        return Sentinel::Null;
    };
    let (kind, value) = given
        .as_object()
        .and_then(|object| object.iter().next())
        .expect("a sentinel of one type");
    let integer = || value.as_i64().expect("an integer sentinel");
    let float = || match value.as_str() {
        Some("NaN") => f64::NAN,
        Some("Infinity") => f64::INFINITY,
        Some("-Infinity") => f64::NEG_INFINITY,
        _ => value.as_f64().expect("a floating point sentinel"),
    };
    match kind.as_str() {
        "byte" => Sentinel::Byte(integer() as i8),
        "short" => Sentinel::Short(integer() as i16),
        "int" => Sentinel::Int(integer() as i32),
        "long" => Sentinel::Long(integer()),
        "char" => Sentinel::Char(integer() as u16),
        "float" => Sentinel::Float(float() as f32),
        "double" => Sentinel::Double(float()),
        "string" => Sentinel::String(value.as_str().expect("a string sentinel").to_string()),
        "bool" => Sentinel::Boolean(value.as_bool().expect("a Boolean sentinel")),
        other => panic!("no sentinel type {other}"),
    }
}

/// The aggregation of one entry of an `agg` operation; see `fixtures/record.py`.
fn aggregation(entry: &Value) -> Aggregation {
    let kind = text(&entry["agg"], "agg");
    if kind == "count" {
        return count(text(&entry["column"], "column"));
    }
    if kind == "count_where" {
        let filters = list(&entry["filters"], "filters")
            .iter()
            .map(|filter| text(filter, "filter"));
        return Aggregation::count_where(text(&entry["column"], "column"), filters);
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
            non_unique: sentinel(entry),
        },
        "distinct" => AggSpec::Distinct {
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
        "group" => AggSpec::Group,
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

/// The table Java returned at one step: its definition, and a batch with the `IsFlat`
/// attribute in its schema metadata, as `corpus::assert_matches` expects.
fn expected(recorded: &Value) -> (TableDefinition, RecordBatch) {
    let columns: Vec<&Value> = list(&recorded["columns"], "columns").iter().collect();
    let (definition, batch) = rows_batch(&columns, list(&recorded["rows"], "rows"));
    let flat = recorded["attributes"]["IsFlat"] == true;
    let metadata: HashMap<String, String> =
        [("deephaven:attribute.IsFlat".to_string(), flat.to_string())]
            .into_iter()
            .collect();
    let schema = Schema::new(batch.schema().fields().clone()).with_metadata(metadata);
    let batch = batch
        .with_schema(Arc::new(schema))
        .expect("the same fields");
    (definition, batch)
}

/// The updates a table of a fixture delivered, as the fixture records Java's.
struct Watched<'a> {
    name: &'a str,
    table: Arc<dyn Table>,
    recorder: Arc<Recorder>,
    /// The number of events of the recorder already compared.
    seen: usize,
    /// The table's rows after those events.
    rows: RowSet,
}

impl Watched<'_> {
    /// The updates delivered since the last call, in the form of `fixtures/record.py`:
    /// rows as positions, removed rows by their position before the update and the others
    /// after it, and the modified columns by name.
    fn drain(&mut self, context: &str) -> Value {
        let positions = |rows: &RowSet, within: &RowSet| -> Vec<i64> {
            let found = within.invert(rows).expect("rows of the table");
            found.iter().collect()
        };
        let definition = self.table.definition();
        let events = self.recorder.events().split_off(self.seen);
        self.seen += events.len();
        let mut updates = Vec::new();
        for event in events {
            let update: TableUpdate =
                event.unwrap_or_else(|failure| panic!("{context}: {}", failure.message));
            assert!(update.shifted.is_empty(), "{context}: a shift");
            let removed = positions(&update.removed, &self.rows);
            self.rows.remove(&update.removed);
            self.rows.insert(&update.added);
            let names = definition.columns().iter();
            let modified_columns: Vec<&str> = match &update.modified_columns {
                _ if update.modified.is_empty() => Vec::new(),
                ModifiedColumnSet::All => names.map(|column| column.name.as_str()).collect(),
                columns => names
                    .enumerate()
                    .filter(|(index, _)| columns.contains(*index))
                    .map(|(_, column)| column.name.as_str())
                    .collect(),
            };
            updates.push(serde_json::json!({
                "added": positions(&update.added, &self.rows),
                "removed": removed,
                "modified": positions(&update.modified, &self.rows),
                "modified_columns": modified_columns,
            }));
        }
        assert_eq!(self.rows, *self.table.row_set(), "{context}: the rows");
        Value::Array(updates)
    }
}

#[derive(Default)]
struct Tally {
    compared: usize,
    refused: usize,
    updates: usize,
}

/// The names of an operation's list `key`.
fn names<'a>(operation: &'a Value, key: &str) -> Vec<&'a str> {
    list(&operation[key], key)
        .iter()
        .map(|column| text(column, key))
        .collect()
}

fn run_fixture(path: &PathBuf, tally: &mut Tally) {
    let fixture = json(path);
    let name = text(&fixture["name"], "name");
    let recorded_steps = list(&fixture["java"]["steps"], "java.steps");
    let graph = SteppedUpdateGraph::new();
    let inputs = inputs(&fixture, &graph);

    // Every table by name: the inputs, and the results as they are made.
    let mut made: BTreeMap<&str, Arc<dyn Table>> = inputs
        .iter()
        .map(|(name, input)| (name.as_str(), input.table.clone()))
        .collect();
    let mut tables: Vec<(&str, Arc<dyn Table>, Vec<String>)> = Vec::new();
    for operation in list(&fixture["operations"], "operations") {
        let table_name = text(&operation["name"], "name");
        let context = format!("{name}.{table_name}");
        let source = |key: &str| &made[text(&operation[key], key)];
        let mut computed = Vec::new();
        let result = match text(&operation["op"], "op") {
            "agg" => {
                let aggregations: Vec<Aggregation> = list(&operation["aggs"], "aggs")
                    .iter()
                    .map(aggregation)
                    .collect();
                computed = computed_columns(&aggregations);
                aggregate_with(
                    source("source"),
                    &aggregations,
                    false,
                    &names(operation, "by"),
                    &oxhaven_expr::Compiler,
                )
            }
            "group_by" => group_by(source("source"), &names(operation, "by")),
            "natural_join" => {
                let on: Vec<JoinMatch> = names(operation, "on")
                    .into_iter()
                    .map(JoinMatch::same)
                    .collect();
                let add: Vec<JoinAddition> = names(operation, "joins")
                    .into_iter()
                    .map(JoinAddition::same)
                    .collect();
                let (left, right) = (source("left"), source("right"));
                let joined =
                    natural_join(left, right, &on, &add, NaturalJoinType::ErrorOnDuplicate);
                Ok(joined.unwrap_or_else(|error| panic!("{context}: {error:?}")))
            }
            other => panic!("{context}: no operation {other}"),
        };
        let failure = &recorded_steps[0][table_name]["failure"];
        match result {
            Ok(table) if failure.is_null() => {
                made.insert(table_name, table.clone());
                tables.push((table_name, table, computed));
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
    let watches = !fixture["updates"].is_null();
    let mut watched: Vec<Watched> = fixture["updates"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|table| {
            let name = text(table, "updates");
            let table = made[name].clone();
            Watched {
                name,
                recorder: Recorder::attach(table.clone()),
                seen: 0,
                rows: (*table.row_set()).clone(),
                table,
            }
        })
        .collect();

    let steps = fixture["steps"].as_array().cloned().unwrap_or_default();
    assert_eq!(
        recorded_steps.len(),
        steps.len() + 1,
        "{name}: recorded steps"
    );
    for (step, recorded) in recorded_steps.iter().enumerate() {
        if step > 0 && watches {
            // As `fixtures/record.py` does it: each action in a cycle of its own.
            for action in list(&steps[step - 1]["actions"], "actions") {
                apply_action(action, &inputs);
                graph.run_cycle();
            }
        } else if step > 0 {
            run_step(&steps[step - 1], &inputs, &graph);
        }
        for (table_name, table, computed) in &tables {
            let context = format!("{name}.{table_name} at step {step}");
            let entry = &recorded[*table_name];
            assert!(entry["failure"].is_null(), "{context}: Java failed");
            assert!(!table.is_failed(), "{context}: failed");
            let (definition, batch) = expected(entry);
            assert_eq!(
                table.definition().columns(),
                definition.columns(),
                "{context}: the columns and their types"
            );
            assert_matches(&context, &**table, &batch, computed);
            tally.compared += 1;
        }
        for watched in watched.iter_mut().filter(|_| step > 0) {
            let context = format!("{name}.{} during step {step}", watched.name);
            let delivered = watched.drain(&context);
            let recorded = &fixture["java"]["updates"][step - 1][watched.name];
            assert_eq!(&delivered, recorded, "{context}: the updates");
            tally.updates += 1;
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
    assert_eq!(
        (tally.compared, tally.refused, tally.updates),
        (COMPARED, REFUSED, UPDATES)
    );
}
