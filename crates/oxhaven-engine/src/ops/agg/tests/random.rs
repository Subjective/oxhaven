//! Aggregations of randomly changing tables, checked after every cycle against the same
//! aggregation of a static copy of the parent.

use super::*;
use crate::graph::SteppedUpdateGraph;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_testkit::{ConsistencyChecker, RandomUpdates, ScriptedTable};

/// The parent's columns: three to group by, and one of each type to aggregate. The last
/// two hold vectors and arrays, which only `group` aggregates.
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
        ("V", ColumnType::vector(ColumnType::Int)),
        ("A", ColumnType::array(ColumnType::String)),
    ]
}

/// Every aggregation Java has for each value column whose result does not depend on the
/// update history, and with `weighted_averages` the weighted averages too.
///
/// Two are left out because Java's own result depends on the history, so that the
/// aggregation of a static copy is not what Java gives. The fixtures compare both with Java
/// (`fixtures/agg_ticking.json`, groups `w` and `n`):
/// - The weighted average keeps a stale sum of weights after a modification that makes the
///   new sum of weights equal the old weighted sum
///   (`ChunkedWeightedAverageOperator#modifyChunk`). Without modifications it does not, so
///   `weighted_averages` is for runs without them.
/// - The weighted sum of an integer column by an integer weight adds each chunk's sum with
///   `NullSafeAddition#plusLong`, which drops a chunk sum that wrapped to exactly
///   `NULL_LONG` (`LongChunkedWeightedSumOperator#addChunk`); a null `int` value and weight
///   count as -2147483648 each, so two such rows do that.
fn every_aggregation(weighted_averages: bool) -> Vec<Aggregation> {
    let mut aggregations = vec![count("N")];
    for (name, column_type) in &parent_columns()[3..] {
        if column_type.list_element().is_some() {
            aggregations.push(agg(AggSpec::Group, &[&format!("G{name} = {name}")]));
            continue;
        }
        let mut specs = vec![
            AggSpec::Group,
            AggSpec::Min,
            AggSpec::Max,
            AggSpec::First,
            AggSpec::Last,
            AggSpec::Median {
                average_evenly_divided: false,
            },
            AggSpec::Percentile {
                percentile: 0.3,
                average_evenly_divided: false,
            },
            AggSpec::CountDistinct { count_nulls: false },
            AggSpec::CountDistinct { count_nulls: true },
            AggSpec::Unique {
                include_nulls: false,
            },
            AggSpec::Unique {
                include_nulls: true,
            },
            AggSpec::SortedFirst {
                columns: vec!["D".to_string()],
            },
            AggSpec::SortedLast {
                columns: vec!["D".to_string()],
            },
            AggSpec::SortedFirst {
                columns: vec!["S".to_string(), "I".to_string()],
            },
            AggSpec::SortedLast {
                columns: vec!["B".to_string(), "T".to_string(), "F".to_string()],
            },
        ];
        if !matches!(column_type, ColumnType::String | ColumnType::Instant) {
            specs.push(AggSpec::Sum);
        }
        if !matches!(
            column_type,
            ColumnType::String | ColumnType::Instant | ColumnType::Boolean
        ) {
            specs.extend([
                AggSpec::Avg,
                AggSpec::AbsSum,
                AggSpec::Var,
                AggSpec::Std,
                AggSpec::Median {
                    average_evenly_divided: true,
                },
                AggSpec::Percentile {
                    percentile: 0.7,
                    average_evenly_divided: true,
                },
            ]);
            let floating = matches!(column_type, ColumnType::Double | ColumnType::Float);
            for weight in ["I", "X", "C"] {
                if floating || weight == "X" {
                    specs.push(AggSpec::WeightedSum {
                        weight: weight.to_string(),
                    });
                }
                if weighted_averages {
                    specs.push(AggSpec::WeightedAvg {
                        weight: weight.to_string(),
                    });
                }
            }
        }
        for (index, spec) in specs.into_iter().enumerate() {
            aggregations.push(agg(spec, &[&format!("A{index}{name} = {name}")]));
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
    /// Whether to include the weighted averages; see [`every_aggregation`].
    weighted_averages: bool,
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
            weighted_averages: false,
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
        assert!(
            !(self.weighted_averages && self.kinds[2]),
            "weighted averages are compared only without modifications"
        );
        let aggregations = every_aggregation(self.weighted_averages);
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
fn random_updates_of_weighted_averages_without_modifications() {
    let mut updates = 0;
    for seed in 600..630 {
        for by in [&["K"][..], &["X"], &[]] {
            updates += Run {
                kinds: [true, true, false, true],
                weighted_averages: true,
                ..Run::new(seed, by)
            }
            .run();
        }
    }
    assert!(updates > 500, "only {updates} updates were checked");
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
