//! Aggregation values of the `ZonedDateTime` and `Period` columns (task 4.22). The expected
//! values are those the Java 42.9 reference gave for the same table and aggregations.

use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::ops::select::update;
use arrow::array::{Int32Array, StructArray};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_testkit::{at_least_for, test_seeds, ConsistencyChecker, RandomUpdates, ScriptedTable};

const HOUR: i64 = 3_600_000_000_000;
/// 2024-03-10T00:00:00Z.
const BASE: i64 = 1_710_028_800_000_000_000;
const NY: &str = "America/New_York";

/// The table of the probes: `I` is the row, `K` its parity, and `Z` the instant
/// `BASE + (5 - I % 6) * 7` hours (null in row 2) in New York in the even rows, in UTC in the
/// odd ones and in Tokyo in row 6, so rows 0 and 6 share an instant. `P` is a period of `I`
/// days, null where `I` is a multiple of 3.
fn table() -> Arc<dyn Table> {
    let zoned: Vec<Option<(i64, &str)>> = (0..8)
        .map(|i| {
            (i != 2).then(|| {
                let zone = match i {
                    6 => "Asia/Tokyo",
                    _ if i % 2 == 0 => NY,
                    _ => "UTC",
                };
                (BASE + (5 - i % 6) * 7 * HOUR, zone)
            })
        })
        .collect();
    let DataType::Struct(fields) = ColumnType::Period.arrow_type() else {
        unreachable!()
    };
    let days: Int32Array = (0..8).collect();
    let period = StructArray::new(
        fields,
        vec![
            Arc::new(Int32Array::from(vec![0; 8])),
            Arc::new(Int32Array::from(vec![0; 8])),
            Arc::new(days),
        ],
        Some((0..8).map(|i| i % 3 != 0).collect::<Vec<bool>>().into()),
    );
    static_table(&[
        (
            "I",
            ColumnType::Int,
            Arc::new(Int32Array::from_iter_values(0..8)),
        ),
        (
            "K",
            ColumnType::Int,
            Arc::new(Int32Array::from_iter_values((0..8).map(|i| i % 2))),
        ),
        (
            "Z",
            ColumnType::ZonedDateTime,
            crate::ops::zoned::tests::zoned(&zoned),
        ),
        ("P", ColumnType::Period, Arc::new(period)),
    ])
}

/// The cells of the `ZonedDateTime` column `name`, as hours after `BASE` and zone ids.
fn zoned_column(table: &dyn Table, name: &str) -> Vec<Option<(i64, String)>> {
    let index = table.definition().index_of(name).expect("the column");
    let array = &read(table)[index];
    let (cells, instants, zones) = crate::ops::zoned::parts(array).expect("zoned cells");
    (0..cells.len())
        .map(|row| {
            cells
                .is_valid(row)
                .then(|| ((instants[row] - BASE) / HOUR, zones.value(row).to_string()))
        })
        .collect()
}

/// The rows of the columns `names` of `table` as text, as [`rows`] gives them.
fn rows_of(table: &dyn Table, names: &[&str]) -> Vec<String> {
    let columns = read(table);
    let indexes: Vec<usize> = names
        .iter()
        .map(|name| table.definition().index_of(name).expect("the column"))
        .collect();
    (0..table.row_set().size() as usize)
        .map(|row| {
            indexes
                .iter()
                .map(|&index| cell(&columns[index], row))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

fn at(hours: i64, zone: &str) -> Option<(i64, String)> {
    Some((hours, zone.to_string()))
}

fn by_k(aggregations: &[Aggregation]) -> Arc<dyn Table> {
    agg_by(&table(), aggregations, &["K"])
}

fn sorted_first(by: &str) -> AggSpec {
    AggSpec::SortedFirst {
        columns: vec![by.to_string()],
    }
}

fn sorted_last(by: &str) -> AggSpec {
    AggSpec::SortedLast {
        columns: vec![by.to_string()],
    }
}

/// Seen on Java 42.9: the minimum is the earliest instant; of one instant the maximum is
/// the zone with the greater offset (Tokyo before New York); first and last keep the zone.
#[test]
fn min_max_first_and_last_of_a_zoned_date_time() {
    let result = by_k(&[
        agg(AggSpec::Min, &["Min = Z"]),
        agg(AggSpec::Max, &["Max = Z"]),
        agg(AggSpec::First, &["First = Z"]),
        agg(AggSpec::Last, &["Last = Z"]),
    ]);
    assert_eq!(
        schema(&*result)[1..],
        ["Min", "Max", "First", "Last"].map(|n| (n.to_string(), ColumnType::ZonedDateTime))
    );
    assert_eq!(zoned_column(&*result, "Min"), [at(7, NY), at(0, "UTC")]);
    assert_eq!(
        zoned_column(&*result, "Max"),
        [at(35, "Asia/Tokyo"), at(28, "UTC")]
    );
    assert_eq!(zoned_column(&*result, "First"), [at(35, NY), at(28, "UTC")]);
    assert_eq!(
        zoned_column(&*result, "Last"),
        [at(35, "Asia/Tokyo"), at(28, "UTC")]
    );
    let whole = agg_by(
        &table(),
        &[
            agg(AggSpec::Min, &["Min = Z"]),
            agg(AggSpec::Max, &["Max = Z"]),
        ],
        &[],
    );
    assert_eq!(zoned_column(&*whole, "Min"), [at(0, "UTC")]);
    assert_eq!(zoned_column(&*whole, "Max"), [at(35, "Asia/Tokyo")]);
}

/// Seen on Java 42.9: one instant in two zones is two values.
#[test]
fn count_distinct_and_unique_of_a_zoned_date_time_tell_zones_apart() {
    let result = by_k(&[
        agg(AggSpec::CountDistinct { count_nulls: false }, &["C = Z"]),
        agg(AggSpec::CountDistinct { count_nulls: true }, &["N = Z"]),
        agg(
            AggSpec::Unique {
                include_nulls: false,
                non_unique: Sentinel::Null,
            },
            &["U = Z"],
        ),
    ]);
    assert_eq!(rows_of(&*result, &["K", "C", "N"]), ["0|3|4", "1|3|3"]);
    assert_eq!(zoned_column(&*result, "U"), [None, None]);
    let unique_of = |keys: &[i64]| {
        let filter = format!(
            "I in {}",
            keys.iter()
                .map(|r| r.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        let parent =
            crate::ops::filter::unstructured_filter(&table(), &[filter], &oxhaven_expr::Compiler)
                .expect("a filter");
        let result = agg_by(
            &parent,
            &[
                agg(
                    AggSpec::Unique {
                        include_nulls: false,
                        non_unique: Sentinel::Null,
                    },
                    &["U = Z"],
                ),
                agg(AggSpec::CountDistinct { count_nulls: false }, &["C = Z"]),
            ],
            &["K"],
        );
        (zoned_column(&*result, "U"), rows_of(&*result, &["C"]))
    };
    assert_eq!(unique_of(&[1, 7]), (vec![at(28, "UTC")], vec!["1".into()]));
    assert_eq!(unique_of(&[0, 2]), (vec![at(35, NY)], vec!["1".into()]));
    assert_eq!(unique_of(&[0, 6]), (vec![None], vec!["2".into()]));
}

/// Seen on Java 42.9: a median and a percentile select one value, in the order of
/// `compareTo`.
#[test]
fn median_and_percentile_of_a_zoned_date_time() {
    let result = by_k(&[
        agg(
            AggSpec::Median {
                average_evenly_divided: true,
            },
            &["M = Z"],
        ),
        agg(
            AggSpec::Percentile {
                percentile: 0.25,
                average_evenly_divided: false,
            },
            &["Q = Z"],
        ),
    ]);
    assert_eq!(zoned_column(&*result, "M"), [at(35, NY), at(28, "UTC")]);
    assert_eq!(zoned_column(&*result, "Q"), [at(35, NY), at(14, "UTC")]);
}

/// Seen on Java 42.9: sorted by a zoned date time a null is first, and of one instant
/// Tokyo is after New York; the zoned date time of the row sorted first by another column
/// keeps its zone.
#[test]
fn sorted_first_and_last_by_and_of_a_zoned_date_time() {
    let result = by_k(&[
        agg(sorted_first("Z"), &["F = I"]),
        agg(sorted_last("Z"), &["L = I"]),
        agg(sorted_first("I"), &["FZ = Z"]),
        agg(sorted_last("I"), &["LZ = Z"]),
    ]);
    assert_eq!(rows_of(&*result, &["K", "F", "L"]), ["0|2|6", "1|5|7"]);
    assert_eq!(zoned_column(&*result, "FZ"), [at(35, NY), at(28, "UTC")]);
    assert_eq!(
        zoned_column(&*result, "LZ"),
        [at(35, "Asia/Tokyo"), at(28, "UTC")]
    );
}

/// Behavior: java.time.ZonedDateTime#equals and java.time.Period#equals as group keys: one
/// instant in two zones is two groups, and every null is one group whatever the fields
/// under it hold (the nulls of `P` hold different days).
#[test]
fn zoned_date_times_and_periods_group_by_every_field() {
    let by_z = by_k_of(&["Z"]);
    assert_eq!(rows_of(&*by_z, &["N"]), ["1", "2", "1", "1", "1", "1", "1"]);
    assert_eq!(
        zoned_column(&*by_z, "Z"),
        [
            at(35, NY),
            at(28, "UTC"),
            None,
            at(14, "UTC"),
            at(7, NY),
            at(0, "UTC"),
            at(35, "Asia/Tokyo")
        ]
    );
    let by_p = by_k_of(&["P"]);
    assert_eq!(rows_of(&*by_p, &["N"]), ["3", "1", "1", "1", "1", "1"]);
}

fn by_k_of(keys: &[&str]) -> Arc<dyn Table> {
    agg_by(&table(), &[count("N")], keys)
}

/// Seen on Java 42.9: the first and last of a `Period` column are its values, nulls
/// included.
#[test]
fn first_and_last_of_a_period() {
    let result = by_k(&[
        agg(AggSpec::First, &["F = P"]),
        agg(AggSpec::Last, &["L = P"]),
    ]);
    assert_eq!(schema(&*result)[1].1, ColumnType::Period);
    let cells = read(&*result);
    let days = |column: &ArrayRef, row: usize| {
        let cells = column.as_struct();
        cells.is_valid(row).then(|| {
            cells
                .column(2)
                .as_primitive::<arrow::datatypes::Int32Type>()
                .value(row)
        })
    };
    assert_eq!([days(&cells[1], 0), days(&cells[2], 0)], [None, None]);
    assert_eq!([days(&cells[1], 1), days(&cells[2], 1)], [Some(1), Some(7)]);
}

/// Seen on Java 42.9: a static parent's sorted first or last by a `Period` compares each
/// row with the best so far, in row order, and fails only when both are periods (not
/// `Comparable`). Sorted first by `P` keeps a null as soon as it meets one; sorted last
/// moves past it to the next period.
#[test]
fn sorted_first_and_last_by_a_period_fail_when_two_periods_meet() {
    let outcome = |rows: &[i64], first: bool, by: &[&str]| {
        let filter = format!(
            "I in {}",
            rows.iter()
                .map(|r| r.to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        let parent =
            crate::ops::filter::unstructured_filter(&table(), &[filter], &oxhaven_expr::Compiler)
                .expect("a filter");
        let spec = if first {
            sorted_first("P")
        } else {
            sorted_last("P")
        };
        aggregate(&parent, &[agg(spec, &["J = I"])], false, by).map(|result| super::rows(&*result))
    };
    let all: Vec<i64> = (0..8).collect();
    assert_eq!(outcome(&all, true, &["K"]).unwrap(), ["0|0", "1|3"]);
    assert!(outcome(&all, false, &["K"]).is_err());
    assert!(outcome(&[1, 5], true, &["K"]).is_err());
    assert_eq!(outcome(&[1, 3, 5], true, &[]).unwrap(), ["3"]);
    assert_eq!(outcome(&[0, 3, 6], false, &["K"]).unwrap(), ["0|6", "1|3"]);
    assert_eq!(outcome(&[1, 3], false, &["K"]).unwrap(), ["1|1"]);
    assert!(outcome(&[1, 3, 5], false, &["K"]).is_err());
}

/// The aggregations of a zoned date time made by a formula from a randomly changing
/// table, checked after every cycle against the same aggregation of a static copy.
#[test]
fn random_updates_of_zoned_date_times() {
    let mut updates = 0;
    for seed in test_seeds(0..20) {
        let graph = SteppedUpdateGraph::new();
        let mut random = RandomUpdates::new(seed);
        let columns = vec![
            ("K", ColumnType::Int),
            ("T", ColumnType::Instant),
            ("G", ColumnType::Int),
        ];
        let values = columns
            .iter()
            .map(|(_, column_type)| random.values(column_type, 8))
            .collect();
        let source = ScriptedTable::new(
            graph.clone(),
            oxhaven_testkit::definition(&columns),
            RowSet::flat(8),
            values,
        );
        let base: Arc<dyn Table> = source.table();
        // Zones of three offsets, two of them equal, so that equal instants differ.
        let parent = update(
            &base,
            &["Z = toZonedDateTime(T, timeZone(G % 3 == 0 ? `UTC` : G % 3 == 1 ? `Asia/Tokyo` : `Europe/London`))"],
            &oxhaven_expr::Compiler,
        )
        .expect("an update");
        let specs = [
            AggSpec::Min,
            AggSpec::Max,
            AggSpec::First,
            AggSpec::Last,
            AggSpec::CountDistinct { count_nulls: true },
            AggSpec::Unique {
                include_nulls: false,
                non_unique: Sentinel::Null,
            },
            AggSpec::Median {
                average_evenly_divided: false,
            },
            sorted_first("G"),
            sorted_last("Z"),
        ];
        let mut aggregations: Vec<Aggregation> = specs
            .into_iter()
            .enumerate()
            .map(|(index, spec)| agg(spec, &[&format!("A{index} = Z")]))
            .collect();
        aggregations.push(agg(sorted_first("Z"), &["B = G"]));
        let result = agg_by(&parent, &aggregations, &["K"]);
        let recompute = {
            let (parent, result, aggregations) = (parent.clone(), result.clone(), aggregations);
            move || {
                let from_scratch = agg_by(&static_copy(&*parent), &aggregations, &["K"]);
                in_order_of(&*from_scratch, &*result, 1)
            }
        };
        let checker = ConsistencyChecker::attach_with_recompute(result.clone(), recompute);
        checker.assert_consistent();
        for _ in 0..30 {
            source.push(random.next_change(&*base, 6));
            graph.run_cycle();
            checker.assert_consistent();
            assert!(!result.is_failed(), "seed {seed}");
        }
        updates += checker.updates_seen();
    }
    assert!(
        updates > at_least_for(200, 0..20),
        "only {updates} updates were checked"
    );
}

/// The ordering aggregations of a `Period` that Java can answer, with their result names.
fn period_specs() -> Vec<(AggSpec, &'static str)> {
    vec![
        (AggSpec::Min, "Mn"),
        (AggSpec::Max, "Mx"),
        (
            AggSpec::Median {
                average_evenly_divided: false,
            },
            "Md",
        ),
        (
            AggSpec::Percentile {
                percentile: 0.25,
                average_evenly_divided: false,
            },
            "Pc",
        ),
        (AggSpec::CountDistinct { count_nulls: false }, "Cd"),
        (AggSpec::CountDistinct { count_nulls: true }, "Cn"),
        (
            AggSpec::Unique {
                include_nulls: false,
                non_unique: Sentinel::Null,
            },
            "U",
        ),
        (
            AggSpec::Distinct {
                include_nulls: false,
            },
            "D",
        ),
        (
            AggSpec::Distinct {
                include_nulls: true,
            },
            "Dn",
        ),
    ]
}

fn period_orderings(input: &str) -> Vec<Aggregation> {
    period_specs()
        .into_iter()
        .map(|(spec, out)| agg(spec, &[&format!("{out} = {input}")]))
        .collect()
}

/// The rows `rows` of the probe table.
fn rows_in(rows: &[i64]) -> Arc<dyn Table> {
    let filter = format!(
        "I in {}",
        rows.iter()
            .map(|r| r.to_string())
            .collect::<Vec<_>>()
            .join(", ")
    );
    crate::ops::filter::unstructured_filter(&table(), &[filter], &oxhaven_expr::Compiler)
        .expect("a filter")
}

/// Seen on Java 42.9: a group of nulls and one period is answered by every ordering
/// aggregation (K = 1 holds P1D and a null, K = 0 holds P2D); a group of two periods that
/// differ fails each of them with a `ClassCastException`.
#[test]
fn orderings_of_a_period_answer_a_group_of_one_period() {
    let result = aggregate(&rows_in(&[1, 2, 3]), &period_orderings("P"), false, &["K"])
        .expect("an aggregation");
    let one = |days: i32| format!("{{years: 0, months: 0, days: {days}}}");
    let (p1, p2) = (one(1), one(2));
    assert_eq!(
        super::rows(&*result),
        [
            format!("1|{p1}|{p1}|{p1}|{p1}|1|2|{p1}|[{p1}]|[null, {p1}]"),
            format!("0|{p2}|{p2}|{p2}|{p2}|1|1|{p2}|[{p2}]|[{p2}]"),
        ]
    );
    for aggregation in period_orderings("P") {
        let Err(error) = aggregate(
            &rows_in(&[1, 5]),
            std::slice::from_ref(&aggregation),
            false,
            &[],
        ) else {
            panic!("{aggregation:?} of two periods was answered");
        };
        assert!(error.to_string().contains("ClassCastException"), "{error}");
    }
}

/// Seen on Java 42.9 with `Q = I == 2 ? null : 'P1D'`, whose periods are one object: the
/// minimum and maximum of a static parent are answered, and the aggregations that sort
/// the rows of a group fail, as they do for one object given twice at once.
#[test]
fn one_period_twice_is_answered_only_by_the_minimum_and_maximum() {
    let parent = update(
        &rows_in(&[0, 1, 2]),
        &["Q = I == 2 ? null : 'P1D'"],
        &oxhaven_expr::Compiler,
    )
    .expect("an update");
    for (index, aggregation) in period_orderings("Q").into_iter().enumerate() {
        let outcome = aggregate(&parent, &[aggregation], false, &[]);
        match index {
            0 | 1 => assert_eq!(
                super::rows(&*outcome.expect("the minimum or maximum")),
                ["{years: 0, months: 0, days: 1}"]
            ),
            _ => assert!(outcome.is_err(), "aggregation {index} was answered"),
        }
    }
}
