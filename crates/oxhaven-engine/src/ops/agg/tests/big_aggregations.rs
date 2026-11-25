//! Sums, averages, variances and orderings of `BigDecimal` and `BigInteger` columns.
//!
//! Every expected value was seen on Deephaven 42.9 embedded in Python (`BigDecimal#toString`
//! of the result cells; a client is sent a decimal without the zeros at the end of its
//! digits): static tables from `new_table` with `new BigDecimal(S)` and `new BigInteger(T)`,
//! and ticking ones as an input table keyed by a row number `R`, each addition or deletion
//! in a cycle of its own. The row number is the row key here.

use super::big_numbers::{columns, decimals, integers, texts};
use super::incremental::Fixture;
use super::*;
use crate::graph::SteppedUpdateGraph;
use arrow::array::Int32Array;
use crate::ops::select::update_view;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_testkit::{test_seeds, ConsistencyChecker, RandomUpdates, ScriptedTable, TableChange};

/// The aggregations the static tables were asked for, of column `input`.
fn every_aggregation_of(input: &str) -> Vec<Aggregation> {
    let of = |spec: AggSpec, output: &str| agg(spec, &[&format!("{output} = {input}")]);
    vec![
        of(AggSpec::Sum, "Sum"),
        of(AggSpec::AbsSum, "Abs"),
        of(AggSpec::Avg, "Avg"),
        of(AggSpec::Var, "Var"),
        of(AggSpec::Std, "Std"),
        of(AggSpec::Min, "Min"),
        of(AggSpec::Max, "Max"),
        of(
            AggSpec::Median {
                average_evenly_divided: false,
            },
            "Med",
        ),
        of(
            AggSpec::Percentile {
                percentile: 0.25,
                average_evenly_divided: false,
            },
            "P25",
        ),
        of(
            AggSpec::Percentile {
                percentile: 0.75,
                average_evenly_divided: true,
            },
            "P75",
        ),
        of(
            AggSpec::Median {
                average_evenly_divided: true,
            },
            "MedA",
        ),
        of(AggSpec::CountDistinct { count_nulls: false }, "CD"),
        of(AggSpec::CountDistinct { count_nulls: true }, "CDN"),
        of(
            AggSpec::Distinct {
                include_nulls: false,
            },
            "Dist",
        ),
        of(
            AggSpec::Distinct {
                include_nulls: true,
            },
            "DistN",
        ),
        of(
            AggSpec::Unique {
                include_nulls: false,
                non_unique: Sentinel::Null,
            },
            "U",
        ),
    ]
}

fn handover_table() -> Arc<dyn Table> {
    static_table(&columns(
        &[0, 0, 0, 1, 1, 1],
        &[0; 6],
        &[
            Some("1.0"),
            Some("1.00"),
            Some("2.5"),
            Some("3"),
            None,
            Some("3.000"),
        ],
        &[Some(1), Some(1), Some(2), Some(3), None, Some(3)],
    ))
}

/// Seen: the groups `[1.0, 1.00, 2.5]` and `[3, null, 3.000]`, and the same as
/// `BigInteger`s (`tasks/11.5-agg-refusals.md`, Handover, A20). The sum keeps the largest
/// scale and the average the sum's; the variance divides at the scale of the squares; the
/// standard deviation has ten places, or is `0`. The orderings take `compareTo`, under
/// which `1.0` and `1.00` are equal, and the first of equal values, but count, list and
/// tell apart the values by `equals`. A percentile of big numbers is never an average.
#[test]
fn every_aggregation_of_a_static_table() {
    let decimal = agg_by(&handover_table(), &every_aggregation_of("B"), &["K"]);
    assert_eq!(
        texts(&*decimal),
        [
            "0|4.50|4.50|1.50|0.7500|0.8660254038|1.0|2.5|1.00|1.00|2.5|1.00|3|3\
             |[1.0, 1.00, 2.5]|[1.0, 1.00, 2.5]|null",
            "1|6.000|6.000|3.000|0.000000|0|3|3|3.000|3|3.000|3.000|2|3\
             |[3, 3.000]|[null, 3, 3.000]|null",
        ]
    );
    let types: Vec<ColumnType> = schema(&*decimal)[1..].iter().map(|c| c.1.clone()).collect();
    let mut expected = vec![ColumnType::BigDecimal; 11];
    expected.extend([ColumnType::Long, ColumnType::Long]);
    expected.extend(vec![ColumnType::vector(ColumnType::BigDecimal); 2]);
    expected.push(ColumnType::BigDecimal);
    assert_eq!(types, expected);

    let integer = agg_by(&handover_table(), &every_aggregation_of("G"), &["K"]);
    assert_eq!(
        texts(&*integer),
        [
            "0|4|4|1|1|1.0000000000|1|2|1|1|2|1|2|2|[1, 2]|[1, 2]|null",
            "1|6|6|3|0|0|3|3|3|3|3|3|1|2|[3]|[null, 3]|3",
        ]
    );
    let types: Vec<ColumnType> = schema(&*integer)[1..].iter().map(|c| c.1.clone()).collect();
    let mut expected = vec![ColumnType::BigInteger; 2];
    expected.extend(vec![ColumnType::BigDecimal; 3]);
    expected.extend(vec![ColumnType::BigInteger; 6]);
    expected.extend([ColumnType::Long, ColumnType::Long]);
    expected.extend(vec![ColumnType::vector(ColumnType::BigInteger); 2]);
    expected.push(ColumnType::BigInteger);
    assert_eq!(types, expected);
}

/// `K`, `O`, `B` and `G` of one row each, `B` as `text`.
fn rows_of(keys: &[i32], big: &[Option<&str>]) -> Vec<ArrayRef> {
    columns(keys, &vec![0; keys.len()], big, &vec![None; keys.len()])
        .into_iter()
        .map(|(_, _, array)| array)
        .collect()
}

/// Seen on a ticking table (`K` 1 unless said; `R` is the row key):
/// add `(0, 1.00)`, `(1, 2.0)`; delete 0; set 1 to `5`; add `(2, 1)`; set 1 to null; delete 2;
/// add `(3, 7)`; add `(4, -7.000)` and `(5, 0.5)` of `K` 2; move 4 to `K` 2; add `(6, 0)`
/// and `(7, -0.0)`.
///
/// A running sum keeps the largest scale of the values it was given until its group has
/// no value that is not null, so it differs from the sum of the same rows on a static
/// table (`2.00` here where that would be `2.0`). The sum takes a modification as the
/// difference of the new and old values, the average and the variance as a removal and an
/// addition, so after `2.0` becomes `5` the sum is `5.00` and the average `5`.
#[test]
fn a_running_sum_keeps_the_largest_scale_it_was_given() {
    let aggregations = vec![
        agg(AggSpec::Sum, &["Sum = B"]),
        agg(AggSpec::AbsSum, &["Abs = B"]),
        agg(AggSpec::Avg, &["Avg = B"]),
        agg(AggSpec::Var, &["Var = B"]),
        agg(AggSpec::Std, &["Std = B"]),
    ];
    let initial = columns(&[1, 1], &[0, 0], &[Some("1.00"), Some("2.0")], &[None, None]);
    let fixture = Fixture::with(&initial, &[0, 1], aggregations, &["K"], false, false);
    let shown = || texts(&*fixture.result);
    assert_eq!(shown(), ["1|3.00|3.00|1.50|0.5000|0.7071067812"]);
    let steps: Vec<(TableChange, Vec<&str>)> = vec![
        (
            TableChange::new().remove([0]),
            vec!["1|2.00|2.00|2.00|null|null"],
        ),
        (
            TableChange::new().modify([1], [(2, decimals(&[Some("5")]))]),
            vec!["1|5.00|5.00|5|null|null"],
        ),
        (
            TableChange::new().add([2], rows_of(&[1], &[Some("1")])),
            vec!["1|6.00|6.00|3|8|2.8284271247"],
        ),
        (
            TableChange::new().modify([1], [(2, decimals(&[None]))]),
            vec!["1|1.00|1.00|1|null|null"],
        ),
        (
            TableChange::new().remove([2]),
            vec!["1|null|null|null|null|null"],
        ),
        (
            TableChange::new().add([3], rows_of(&[1], &[Some("7")])),
            vec!["1|7|7|7|null|null"],
        ),
        (
            TableChange::new().add([4, 5], rows_of(&[1, 2], &[Some("-7.000"), Some("0.5")])),
            vec![
                "1|0.000|14.000|0.000|98.000000|9.8994949366",
                "2|0.5|0.5|0.5|null|null",
            ],
        ),
        (
            TableChange::new().modify([4], [(0, Arc::new(Int32Array::from(vec![2])) as ArrayRef)]),
            vec![
                "1|7.000|7.000|7.000|null|null",
                "2|-6.500|7.500|-3.250|28.125000|5.3033008589",
            ],
        ),
        (
            TableChange::new().add([6, 7], rows_of(&[1, 1], &[Some("0"), Some("-0.0")])),
            vec![
                "1|7.000|7.000|2.333|16.333334|4.0414519668",
                "2|-6.500|7.500|-3.250|28.125000|5.3033008589",
            ],
        ),
    ];
    for (step, (change, expected)) in steps.into_iter().enumerate() {
        fixture.step(change);
        assert_eq!(shown(), expected, "step {}", step + 1);
    }
}

/// The last event of `fixture` after `change`: the failure of its result.
fn failure_after(fixture: &Fixture, change: TableChange) -> String {
    fixture.source.push(change);
    fixture.graph.run_cycle();
    assert!(fixture.result.is_failed(), "the result did not fail");
    match fixture.recorder.events().last() {
        Some(Err(failure)) => failure.message.clone(),
        other => panic!("unexpected last event {other:?}"),
    }
}

/// Seen on Deephaven 42.9: a ticking group of `1.0`, `1.00` and `2.5` is answered as a
/// static one is, but deleting the row of `1.00` leaves the minimum `1.00`, the median
/// `2.5` and the distinct values `[1.00, 2.5]`: Java's sorted set removed the `1.0`. The
/// answer depends on where its searches land, so the result fails instead; and a minimum of
/// such a group fails as soon as it holds both.
#[test]
fn a_ticking_group_of_one_value_at_two_scales_fails() {
    let sets = vec![
        agg(
            AggSpec::Median {
                average_evenly_divided: false,
            },
            &["Med = B"],
        ),
        agg(AggSpec::CountDistinct { count_nulls: false }, &["CD = B"]),
        agg(
            AggSpec::Distinct {
                include_nulls: false,
            },
            &["Dist = B"],
        ),
    ];
    let initial = columns(
        &[1, 1, 1],
        &[0; 3],
        &[Some("1.0"), Some("1.00"), Some("2.5")],
        &[None; 3],
    );
    let fixture = Fixture::with(&initial, &[0, 1, 2], sets.clone(), &["K"], false, true);
    assert_eq!(texts(&*fixture.result), ["1|1.00|3|[1.0, 1.00, 2.5]"]);
    let message = failure_after(&fixture, TableChange::new().remove([1]));
    assert!(
        message.starts_with(
            "unported: io.deephaven.engine.table.impl.ssms.ObjectSegmentedSortedMultiset"
        ) && message.contains("(1.0 and 1.00)"),
        "{message}"
    );

    // A group that comes to hold a value at a second scale fails too.
    let initial = columns(&[1, 1], &[0; 2], &[Some("1.0"), Some("2.5")], &[None; 2]);
    let fixture = Fixture::with(&initial, &[0, 1], sets, &["K"], false, true);
    fixture.step(TableChange::new().add([5], rows_of(&[1], &[Some("2.5")])));
    assert_eq!(texts(&*fixture.result), ["1|2.5|2|[1.0, 2.5]"]);
    let message = failure_after(
        &fixture,
        TableChange::new().modify([5], [(2, decimals(&[Some("1.000")]))]),
    );
    assert!(message.contains("(1.0 and 1.000)"), "{message}");

    // The minimum of a ticking table keeps a sorted set too.
    let fixture = Fixture::with(
        &initial,
        &[0, 1],
        vec![agg(AggSpec::Min, &["Min = B"]), agg(AggSpec::Max, &["Max = B"])],
        &["K"],
        false,
        true,
    );
    assert_eq!(texts(&*fixture.result), ["1|1.0|2.5"]);
    fixture.step(TableChange::new().modify([0], [(2, decimals(&[Some("1.00")]))]));
    assert_eq!(texts(&*fixture.result), ["1|1.00|2.5"]);
    let message = failure_after(
        &fixture,
        TableChange::new().add([3], rows_of(&[1], &[Some("2.50")])),
    );
    assert!(
        message.starts_with(
            "unported: io.deephaven.engine.table.impl.by.ssmminmax.SsmChunkedMinMaxOperator"
        ) && message.contains("(2.5 and 2.50)"),
        "{message}"
    );

    // So does a sorted first or last by such a column.
    let fixture = Fixture::with(
        &initial,
        &[0, 1],
        vec![agg(
            AggSpec::SortedFirst {
                columns: vec!["B".into()],
            },
            &["F = O"],
        )],
        &["K"],
        false,
        true,
    );
    let message = failure_after(
        &fixture,
        TableChange::new().add([3], rows_of(&[1], &[Some("1.00")])),
    );
    assert!(
        message.starts_with("unported: io.deephaven.engine.table.impl.ssa.ObjectSegmentedSortedArray")
            && message.contains("([1.0] and [1.00])"),
        "{message}"
    );
}

/// A static group's values at several scales are answered whatever the aggregation, and
/// so are a sorted first and last by them (sorted first keeps the first row of equal keys).
#[test]
fn static_orderings_of_one_value_at_two_scales_are_answered() {
    let table = static_table(&columns(
        &[0, 0, 0],
        &[1, 2, 3],
        &[Some("2.0"), Some("1.00"), Some("1.0")],
        &[None; 3],
    ));
    let result = agg_by(
        &table,
        &[
            agg(AggSpec::Min, &["Min = B"]),
            agg(
                AggSpec::SortedFirst {
                    columns: vec!["B".into()],
                },
                &["F = O"],
            ),
            agg(
                AggSpec::SortedLast {
                    columns: vec!["B".into()],
                },
                &["L = O"],
            ),
        ],
        &["K"],
    );
    assert_eq!(texts(&*result), ["0|1.00|2|1"]);
}

/// The sums, averages, variances and orderings of a ticking table of big numbers whose
/// decimals all have one scale, which makes them independent of the history, checked
/// after every cycle against the same aggregation of a static copy.
#[test]
fn random_updates_of_big_numbers_of_one_scale() {
    let parent_columns = [
        ("K", ColumnType::String),
        ("L", ColumnType::Long),
        ("I", ColumnType::Int),
    ];
    let mut aggregations = Vec::new();
    for input in ["B", "G", "W"] {
        for (index, spec) in [
            AggSpec::Sum,
            AggSpec::AbsSum,
            AggSpec::Avg,
            AggSpec::Var,
            AggSpec::Std,
            AggSpec::Min,
            AggSpec::Max,
            AggSpec::Median {
                average_evenly_divided: true,
            },
            AggSpec::Percentile {
                percentile: 0.3,
                average_evenly_divided: false,
            },
            AggSpec::CountDistinct { count_nulls: true },
            AggSpec::Distinct {
                include_nulls: false,
            },
            AggSpec::Unique {
                include_nulls: false,
                non_unique: Sentinel::Null,
            },
            AggSpec::First,
            AggSpec::Last,
        ]
        .into_iter()
        .enumerate()
        {
            aggregations.push(agg(spec, &[&format!("A{index}{input} = {input}")]));
        }
        aggregations.push(agg(
            AggSpec::SortedFirst {
                columns: vec![input.to_string()],
            },
            &[&format!("F{input} = I")],
        ));
        aggregations.push(agg(
            AggSpec::SortedLast {
                columns: vec![input.to_string(), "I".to_string()],
            },
            &[&format!("L{input} = L")],
        ));
    }
    for seed in test_seeds(0..30) {
        let graph = SteppedUpdateGraph::new();
        let mut random = RandomUpdates::new(seed);
        let values = parent_columns
            .iter()
            .map(|(_, column_type)| random.values(column_type, 6))
            .collect();
        let source = ScriptedTable::new(
            graph.clone(),
            oxhaven_testkit::definition(&parent_columns),
            RowSet::flat(6),
            values,
        );
        let scripted: Arc<dyn Table> = source.table();
        // Decimals of scale 2, integers, and decimals of scale -3; nulls where `L` and `I`
        // are. Those of scale 0 and less are positive: Java's sum of a group whose only value goes
        // from null to a zero of such a scale stays null (`a_sum_that_a_zero_does_not_change`).
        let parent = update_view(
            &scripted,
            &[
                "B = isNull(L) ? null : java.math.BigDecimal.valueOf(L, 2)",
                "G = isNull(L) ? null : java.math.BigInteger.valueOf(abs(L) + 1).multiply(java.math.BigInteger.TEN.pow(20))",
                "W = isNull(I) ? null : java.math.BigDecimal.valueOf(abs(I) + 1, -3)",
            ],
            &oxhaven_expr::Compiler,
        )
        .expect("the big number columns");
        let result = agg_by(&parent, &aggregations, &["K"]);
        let recompute = {
            let (parent, result, aggregations) = (parent.clone(), result.clone(), aggregations.clone());
            move || {
                let from_scratch = agg_by(&static_copy(&*parent), &aggregations, &["K"]);
                in_order_of(&*from_scratch, &*result, 1)
            }
        };
        let checker = ConsistencyChecker::attach_with_recompute(result.clone(), recompute);
        checker.assert_consistent();
        for _ in 0..30 {
            source.push(random.next_change(&*scripted, 6));
            graph.run_cycle();
            checker.assert_consistent();
            assert!(!result.is_failed(), "seed {seed}");
        }
        assert!(checker.updates_seen() > 0, "seed {seed}");
    }
}

/// Integers beyond the range of a `long` are summed, averaged and squared exactly.
#[test]
fn integers_beyond_a_long_are_summed_exactly() {
    let table = static_table(&columns(
        &[0, 0],
        &[0, 0],
        &[None, None],
        &[Some(i128::MAX), Some(i128::MAX)],
    ));
    let result = agg_by(
        &table,
        &[
            agg(AggSpec::Sum, &["S = G"]),
            agg(AggSpec::Avg, &["A = G"]),
            agg(AggSpec::Var, &["V = G"]),
        ],
        &["K"],
    );
    assert_eq!(
        texts(&*result),
        [
            "0|340282366920938463463374607431768211454|170141183460469231731687303715884105727|0"
        ]
    );
}

/// Seen on a ticking table of one group: add `(0, null)`; set 0 to `0`; add `(1, 5)`; set
/// 0 to null; delete 1; set 0 to `0.0` (`0` for the integers). Java's sum takes a
/// modification as the difference of the new and old sums and ignores one that `equals`
/// `BigDecimal.ZERO`, so a group whose only value goes from null to `0` keeps a null sum,
/// where a static table's is `0`; `0.0` is not `ZERO`.
#[test]
fn a_sum_that_a_zero_does_not_change() {
    let aggregations = vec![
        agg(AggSpec::Sum, &["SB = B", "SG = G"]),
        agg(AggSpec::AbsSum, &["AB = B", "AG = G"]),
    ];
    let initial = columns(&[1], &[0], &[None], &[None]);
    let fixture = Fixture::with(&initial, &[0], aggregations, &["K"], false, false);
    let shown = || texts(&*fixture.result);
    assert_eq!(shown(), ["1|null|null|null|null"]);
    let set = |big: Option<&str>, integer: Option<i128>| {
        TableChange::new().modify([0], [(2, decimals(&[big])), (3, integers(&[integer]))])
    };
    let added = columns(&[1], &[0], &[Some("5")], &[Some(5)]);
    let steps = [
        (set(Some("0"), Some(0)), "1|null|null|null|null"),
        (
            TableChange::new().add([1], added.into_iter().map(|(_, _, a)| a).collect()),
            "1|5|5|5|5",
        ),
        (set(None, None), "1|5|5|5|5"),
        (TableChange::new().remove([1]), "1|null|null|null|null"),
        (set(Some("0.0"), Some(0)), "1|0.0|null|0.0|null"),
    ];
    for (step, (change, expected)) in steps.into_iter().enumerate() {
        fixture.step(change);
        assert_eq!(shown(), [expected], "step {}", step + 1);
    }
}
