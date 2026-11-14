//! Aggregations of randomly changing tables, checked after every cycle against the same
//! aggregation of a static copy of the parent.

use super::*;
use crate::graph::SteppedUpdateGraph;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_testkit::{ConsistencyChecker, RandomUpdates, ScriptedTable};

/// The parent's columns: three to group by, and one of each type to aggregate.
fn parent_columns() -> Vec<(&'static str, ColumnType)> {
    vec![
        ("K", ColumnType::String),
        ("G", ColumnType::Int),
        ("X", ColumnType::Double),
        ("I", ColumnType::Int),
        ("L", ColumnType::Long),
        ("D", ColumnType::Double),
        ("F", ColumnType::Float),
        ("S", ColumnType::String),
        ("B", ColumnType::Boolean),
        ("T", ColumnType::Instant),
        ("C", ColumnType::Char),
        ("Y", ColumnType::Byte),
        ("H", ColumnType::Short),
    ]
}

/// Every aggregation Java has for each value column.
fn every_aggregation() -> Vec<Aggregation> {
    let mut aggregations = vec![count("N")];
    for (name, column_type) in &parent_columns()[3..] {
        let mut specs = vec![AggSpec::Min, AggSpec::Max, AggSpec::First, AggSpec::Last];
        if !matches!(column_type, ColumnType::String | ColumnType::Instant) {
            specs.push(AggSpec::Sum);
        }
        if !matches!(
            column_type,
            ColumnType::String | ColumnType::Instant | ColumnType::Boolean
        ) {
            specs.push(AggSpec::Avg);
        }
        for spec in specs {
            aggregations.push(agg(spec, &[&format!("{spec:?}{name} = {name}")]));
        }
    }
    aggregations
}

struct Run {
    seed: u64,
    by: &'static [&'static str],
    preserve_empty: bool,
    initial_rows: u64,
    target_size: u64,
    steps: usize,
    /// Which kinds of change to make: adds, removes, modifies, shifts.
    kinds: [bool; 4],
}

impl Run {
    fn new(seed: u64, by: &'static [&'static str]) -> Run {
        Run {
            seed,
            by,
            preserve_empty: false,
            initial_rows: 6,
            target_size: 5,
            steps: 40,
            kinds: [true; 4],
        }
    }

    /// Returns the number of updates the result delivered.
    fn run(&self) -> u64 {
        let graph = SteppedUpdateGraph::new();
        let mut random = RandomUpdates::new(self.seed);
        [random.adds, random.removes, random.modifies, random.shifts] = self.kinds;
        let columns = parent_columns();
        let values = columns
            .iter()
            .map(|(_, column_type)| random.values(column_type, self.initial_rows))
            .collect();
        let source = ScriptedTable::new(
            graph.clone(),
            oxhaven_testkit::definition(&columns),
            RowSet::flat(self.initial_rows),
            values,
        );
        let parent: Arc<dyn Table> = source.table();
        let aggregations = every_aggregation();
        let result = aggregate(&parent, &aggregations, self.preserve_empty, self.by)
            .expect("an aggregation");

        let (by, preserve_empty) = (self.by, self.preserve_empty);
        let recompute = {
            let (parent, result) = (parent.clone(), result.clone());
            move || {
                let from_scratch =
                    aggregate(&static_copy(&*parent), &aggregations, preserve_empty, by)
                        .expect("a static aggregation");
                in_order_of(&*from_scratch, &*result, by.len())
            }
        };
        let checker = ConsistencyChecker::attach_with_recompute(result.clone(), recompute);
        checker.assert_consistent();
        for _ in 0..self.steps {
            source.push(random.next_change(&*parent, self.target_size));
            graph.run_cycle();
            checker.assert_consistent();
            assert!(!result.is_failed(), "seed {}", self.seed);
        }
        checker.updates_seen()
    }
}

#[test]
fn random_updates_by_one_key() {
    let updates: u64 = (0..30).map(|seed| Run::new(seed, &["K"]).run()).sum();
    assert!(updates > 500, "only {updates} updates were checked");
}

#[test]
fn random_updates_by_two_keys() {
    for seed in 100..120 {
        Run::new(seed, &["K", "G"]).run();
    }
}

#[test]
fn random_updates_by_a_floating_point_key() {
    for seed in 200..220 {
        Run::new(seed, &["X"]).run();
    }
}

#[test]
fn random_updates_without_keys() {
    for seed in 300..320 {
        Run::new(seed, &[]).run();
        Run {
            preserve_empty: true,
            ..Run::new(seed, &[])
        }
        .run();
    }
}

#[test]
fn random_updates_of_each_kind_alone_and_in_pairs() {
    let kinds = [
        [true, false, false, false],
        [true, true, false, false],
        [true, false, true, false],
        [true, false, false, true],
        [false, true, true, true],
        [true, true, false, true],
    ];
    for (index, kinds) in kinds.into_iter().enumerate() {
        for seed in 0..4 {
            for by in [&["K"][..], &[]] {
                Run {
                    kinds,
                    initial_rows: 12,
                    ..Run::new(400 + 10 * index as u64 + seed, by)
                }
                .run();
            }
        }
    }
}

#[test]
fn random_updates_of_tables_that_span_chunks() {
    for (seed, by) in [(500, &["K"][..]), (501, &["G", "B"]), (502, &[])] {
        Run {
            initial_rows: 9_000,
            target_size: 700,
            steps: 3,
            ..Run::new(seed, by)
        }
        .run();
    }
}
