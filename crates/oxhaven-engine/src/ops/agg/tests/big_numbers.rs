//! First, last, sorted first and sorted last of `BigDecimal` and `BigInteger` columns, and
//! the aggregations of them that are not ported. Every expected value was seen on
//! Deephaven 42.9 (`BigDecimal#toString` of the result cells).

use super::incremental::Fixture;
use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::table::{tracked_source, QueryTable, RootSource, TableSpec};
use arrow::array::{BinaryArray, Int32Array};
use oxhaven_column::java_values::{
    big_decimal_cell, big_decimal_parts, big_decimal_text, big_integer_cell, big_integer_text,
};
use oxhaven_column::WritableSource;
use oxhaven_contracts::graph::{UpdateGraph, UpdateSource};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::table::{ColumnSource, TableAttributes};
use oxhaven_contracts::update::TableUpdate;
use oxhaven_testkit::{ConsistencyChecker, TableChange};
use std::sync::{Mutex, Weak};

/// The cell of `text`, a decimal as Java's `new BigDecimal(String)` reads it: digits with
/// an optional point and an optional exponent (`1.20`, `2E+3`).
fn decimal(text: &str) -> Vec<u8> {
    let (mantissa, exponent) = match text.split_once('E') {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().expect("an exponent")),
        None => (text, 0),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let unscaled: i128 = format!("{whole}{fraction}").parse().expect("digits");
    big_decimal_cell(
        fraction.len() as i32 - exponent,
        &unscaled.to_be_bytes()[..],
    )
}

fn decimals(values: &[Option<&str>]) -> ArrayRef {
    let cells: Vec<Option<Vec<u8>>> = values.iter().map(|v| v.map(decimal)).collect();
    Arc::new(cells.iter().map(|c| c.as_deref()).collect::<BinaryArray>())
}

fn integers(values: &[Option<i128>]) -> ArrayRef {
    let cells: Vec<Option<Vec<u8>>> = values.iter().map(|v| v.map(big_integer_cell)).collect();
    Arc::new(cells.iter().map(|c| c.as_deref()).collect::<BinaryArray>())
}

/// The rows of `table` as text, a big number as Java's `toString` writes it.
fn texts(table: &dyn Table) -> Vec<String> {
    let columns = read(table);
    let types: Vec<ColumnType> = table
        .definition()
        .columns()
        .iter()
        .map(|column| column.column_type.clone())
        .collect();
    let size = columns.first().map_or(0, |column| column.len());
    (0..size)
        .map(|row| {
            columns
                .iter()
                .zip(&types)
                .map(|(column, column_type)| {
                    if column.is_null(row) {
                        return "null".to_string();
                    }
                    let bytes = || column.as_binary::<i32>().value(row);
                    match column_type {
                        ColumnType::BigDecimal => {
                            let (scale, unscaled) = big_decimal_parts(bytes()).expect("a cell");
                            big_decimal_text(scale, unscaled)
                        }
                        ColumnType::BigInteger => big_integer_text(bytes()),
                        _ => cell(column, row),
                    }
                })
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

/// `K`, `O`, `B` and `G`: a key, an order, a decimal and an integer.
fn columns(
    keys: &[i32],
    order: &[i32],
    big: &[Option<&str>],
    integer: &[Option<i128>],
) -> Vec<(&'static str, ColumnType, ArrayRef)> {
    vec![
        (
            "K",
            ColumnType::Int,
            Arc::new(Int32Array::from(keys.to_vec())),
        ),
        (
            "O",
            ColumnType::Int,
            Arc::new(Int32Array::from(order.to_vec())),
        ),
        ("B", ColumnType::BigDecimal, decimals(big)),
        ("G", ColumnType::BigInteger, integers(integer)),
    ]
}

/// Seen: `empty_table(6)` with `K = i % 2`, `B = new BigDecimal("" + (i * 1.5))` but null
/// at `i == 3`, `G = BigInteger.valueOf(i * 7 - 10)`; and sorted by `O` the table of
/// `1.0`, `1.00`, `2.5` and `3`, null, `3.000`.
#[test]
fn first_last_and_sorted_first_last_of_a_static_table_are_the_rows_values() {
    let table = static_table(&columns(
        &[0, 1, 0, 1, 0, 1],
        &[0, 0, 0, 0, 0, 0],
        &[
            Some("0.0"),
            Some("1.5"),
            Some("3.0"),
            None,
            Some("6.0"),
            Some("7.5"),
        ],
        &[Some(-10), Some(-3), Some(4), Some(11), Some(18), Some(25)],
    ));
    let result = agg_by(
        &table,
        &[
            agg(AggSpec::First, &["BF = B", "GF = G"]),
            agg(AggSpec::Last, &["BL = B", "GL = G"]),
        ],
        &["K"],
    );
    assert_eq!(texts(&*result), ["0|0.0|-10|6.0|18", "1|1.5|-3|7.5|25"]);
    assert_eq!(
        schema(&*result)[1..],
        [
            ("BF".to_string(), ColumnType::BigDecimal),
            ("GF".to_string(), ColumnType::BigInteger),
            ("BL".to_string(), ColumnType::BigDecimal),
            ("GL".to_string(), ColumnType::BigInteger),
        ]
    );

    let table = static_table(&columns(
        &[0, 0, 0, 1, 1, 1],
        &[3, 1, 2, 5, 6, 4],
        &[
            Some("1.0"),
            Some("1.00"),
            Some("2.5"),
            Some("3"),
            None,
            Some("3.000"),
        ],
        &[Some(1), Some(1), Some(2), Some(3), None, Some(3)],
    ));
    let order = || vec!["O".to_string()];
    let result = agg_by(
        &table,
        &[
            agg(
                AggSpec::SortedFirst { columns: order() },
                &["BF = B", "GF = G"],
            ),
            agg(
                AggSpec::SortedLast { columns: order() },
                &["BL = B", "GL = G"],
            ),
        ],
        &["K"],
    );
    assert_eq!(texts(&*result), ["0|1.00|1|1.0|1", "1|3.000|3|null|null"]);
}

/// A blink table of `K`, `O`, `B` and `G`: each cycle its rows are replaced by the next
/// queued rows.
struct BigBlink {
    table: Weak<QueryTable>,
    columns: Vec<Arc<WritableSource>>,
    queued: Mutex<Vec<Vec<ArrayRef>>>,
    size: Mutex<u64>,
}

impl UpdateSource for BigBlink {
    fn refresh(&self) {
        let rows = {
            let mut queued = self.queued.lock().expect("the queue");
            (!queued.is_empty()).then(|| queued.remove(0))
        };
        let mut size = self.size.lock().expect("the size");
        let removed = RowSet::flat(*size);
        let added = RowSet::flat(rows.as_ref().map_or(0, |rows| rows[0].len() as u64));
        if removed.is_empty() && added.is_empty() {
            return;
        }
        for (index, column) in self.columns.iter().enumerate() {
            column.remove(&removed);
            if let Some(rows) = &rows {
                column.fill(&added, &rows[index]).expect("a write");
            }
        }
        *size = added.size();
        if let Some(table) = self.table.upgrade() {
            table.notify(TableUpdate {
                added,
                removed,
                ..TableUpdate::empty()
            });
        }
    }
}

fn big_blink(graph: &Arc<SteppedUpdateGraph>) -> (Arc<dyn Table>, Arc<BigBlink>) {
    let dyn_graph: Arc<dyn UpdateGraph> = graph.clone();
    let names = columns(&[], &[], &[], &[]);
    let sources: Vec<Arc<WritableSource>> = names
        .iter()
        .map(|(_, column_type, _)| tracked_source(&dyn_graph, column_type.clone()))
        .collect();
    let definition = oxhaven_testkit::definition(
        &names
            .iter()
            .map(|(name, column_type, _)| (*name, column_type.clone()))
            .collect::<Vec<_>>(),
    );
    let mut attributes = TableAttributes::default();
    attributes.blink = true;
    let readers: Vec<Arc<dyn ColumnSource>> = sources
        .iter()
        .map(|column| column.clone() as Arc<dyn ColumnSource>)
        .collect();
    let spec = TableSpec::new(definition, RowSet::empty(), readers).with_attributes(attributes);
    let mut made = None;
    let table = QueryTable::root(dyn_graph, spec, |table| {
        let source = Arc::new(BigBlink {
            table,
            columns: sources,
            queued: Mutex::default(),
            size: Mutex::new(0),
        });
        made = Some(source.clone());
        RootSource {
            source,
            input: None,
        }
    });
    (table, made.expect("the source"))
}

/// Seen with a table publisher: `last_by("K")` and `first_by("K")` of three cycles,
/// `(0, 1.00, 0) (1, 1.10, 1) (0, 1.20, 2)`, then `(1, null, -5)`, then `(2, 2E+3, null)`
/// and `(3, 2E+3, null)`; and `sorted_first`/`sorted_last` by `O` of two cycles, `(0, 5,
/// 1.10) (0, 2, 2.200) (1, 7, null)` then `(0, 2, 9) (1, 9, 4E+1) (1, 1, 0.5)`. The
/// decimals keep their scale; the change-data-capture table of a Kafka consumer is this
/// `last_by`.
#[test]
fn first_last_and_sorted_first_last_of_a_blink_table_keep_the_rows_values() {
    let graph = SteppedUpdateGraph::new();
    let (parent, source) = big_blink(&graph);
    let last = agg_by(&parent, &[agg(AggSpec::Last, &["B", "G"])], &["K"]);
    let first = agg_by(&parent, &[agg(AggSpec::First, &["B", "G"])], &["K"]);
    let checkers = [&last, &first].map(|t| ConsistencyChecker::attach(t.clone()));
    let cycle = |keys: &[i32], order: &[i32], big: &[Option<&str>], integer: &[Option<i128>]| {
        let rows = columns(keys, order, big, integer);
        let arrays = rows.into_iter().map(|(_, _, array)| array).collect();
        source.queued.lock().expect("the queue").push(arrays);
        graph.run_cycle();
        for checker in &checkers {
            checker.assert_consistent();
        }
    };
    cycle(
        &[0, 1, 0],
        &[0; 3],
        &[Some("1.00"), Some("1.10"), Some("1.20")],
        &[Some(0), Some(1), Some(2)],
    );
    cycle(&[1], &[0], &[None], &[Some(-5)]);
    cycle(
        &[2, 3],
        &[0, 0],
        &[Some("2E+3"), Some("2E+3")],
        &[None, None],
    );
    assert_eq!(
        texts(&*last),
        ["0|1.20|2", "1|null|-5", "2|2E+3|null", "3|2E+3|null"]
    );
    assert_eq!(
        texts(&*first),
        ["0|1.00|0", "1|1.10|1", "2|2E+3|null", "3|2E+3|null"]
    );

    let graph = SteppedUpdateGraph::new();
    let (parent, source) = big_blink(&graph);
    let order = || vec!["O".to_string()];
    let sorted = agg_by(
        &parent,
        &[
            agg(AggSpec::SortedFirst { columns: order() }, &["BF = B"]),
            agg(AggSpec::SortedLast { columns: order() }, &["BL = B"]),
        ],
        &["K"],
    );
    let checker = ConsistencyChecker::attach(sorted.clone());
    for (keys, order, big) in [
        ([0, 0, 1], [5, 2, 7], [Some("1.10"), Some("2.200"), None]),
        ([0, 1, 1], [2, 9, 1], [Some("9"), Some("4E+1"), Some("0.5")]),
    ] {
        let rows = columns(&keys, &order, &big, &[None, None, None]);
        let arrays = rows.into_iter().map(|(_, _, array)| array).collect();
        source.queued.lock().expect("the queue").push(arrays);
        graph.run_cycle();
        checker.assert_consistent();
    }
    assert_eq!(texts(&*sorted), ["0|2.200|1.10", "1|0.5|4E+1"]);
}

/// The first and last rows of a ticking table's groups, as rows come, go and change. A
/// change of scale alone (`1.0` to `1.00`) is a change of value.
#[test]
fn first_and_last_of_a_ticking_table_follow_its_rows() {
    let fixture = Fixture::new(
        &columns(
            &[0, 0, 1],
            &[0, 0, 0],
            &[Some("1.0"), Some("2.5"), None],
            &[Some(1), Some(2), Some(3)],
        ),
        &[0, 1, 2],
        vec![
            agg(AggSpec::First, &["BF = B", "GF = G"]),
            agg(AggSpec::Last, &["BL = B", "GL = G"]),
        ],
        &["K"],
    );
    let shown = || texts(&*fixture.result);
    assert_eq!(shown(), ["0|1.0|1|2.5|2", "1|null|3|null|3"]);
    let change = TableChange::new().modify([0], [(2, decimals(&[Some("1.00")]))]);
    fixture.step(change);
    assert_eq!(shown(), ["0|1.00|1|2.5|2", "1|null|3|null|3"]);
    let added = columns(
        &[1],
        &[0],
        &[Some("-7.25")],
        &[Some(-170_141_183_460_469_231_731_687_303_715_884_105_728)],
    );
    let change = TableChange::new().add([5], added.into_iter().map(|(_, _, a)| a).collect());
    fixture.step(change);
    assert_eq!(
        shown(),
        [
            "0|1.00|1|2.5|2",
            "1|null|3|-7.25|-170141183460469231731687303715884105728"
        ]
    );
    fixture.step(TableChange::new().remove([0, 2]));
    assert_eq!(
        shown(),
        [
            "0|2.5|2|2.5|2",
            "1|-7.25|-170141183460469231731687303715884105728|-7.25|-170141183460469231731687303715884105728"
        ]
    );
}

/// Java serves these, with arbitrary precision arithmetic and orderings that are not
/// ported (seen: the sum of `1.0`, `1.00` and `2.5` is `4.50`; the minimum of `1.0` and
/// `1.00` is the first).
#[test]
fn computed_and_ordered_aggregations_of_big_numbers_are_unported() {
    let table = || {
        static_table(&columns(
            &[0, 0],
            &[1, 2],
            &[Some("1.0"), Some("1.00")],
            &[Some(1), Some(1)],
        ))
    };
    let order = |column: &str| vec![column.to_string()];
    for (spec, input) in [
        (AggSpec::Sum, "B"),
        (AggSpec::AbsSum, "G"),
        (AggSpec::Avg, "B"),
        (AggSpec::Std, "G"),
        (AggSpec::Min, "B"),
        (AggSpec::Max, "G"),
        (
            AggSpec::Median {
                average_evenly_divided: false,
            },
            "B",
        ),
        (AggSpec::CountDistinct { count_nulls: false }, "B"),
        (
            AggSpec::Distinct {
                include_nulls: false,
            },
            "G",
        ),
        (
            AggSpec::SortedFirst {
                columns: order("B"),
            },
            "O",
        ),
        (
            AggSpec::SortedLast {
                columns: order("G"),
            },
            "O",
        ),
    ] {
        let described = format!("{spec:?} of {input}");
        let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let aggregation = agg(spec, &[&format!("R = {input}")]);
            let _ = aggregate(&table(), &[aggregation], false, &["K"]);
        }));
        let message = refused
            .err()
            .and_then(|panic| panic.downcast::<String>().ok())
            .unwrap_or_else(|| panic!("{described} was not refused"));
        assert!(
            message.starts_with("unported: io.deephaven.engine.table.impl.by.AggregationProcessor"),
            "{described}: {message}"
        );
    }
    // Java refuses the weighted ones too: an `UnsupportedOperationException`.
    let weighted = aggregate(
        &table(),
        &[agg(AggSpec::WeightedSum { weight: "O".into() }, &["R = B"])],
        false,
        &["K"],
    );
    assert!(matches!(
        weighted,
        Err(AggregateError::UnsupportedType { .. })
    ));
}
