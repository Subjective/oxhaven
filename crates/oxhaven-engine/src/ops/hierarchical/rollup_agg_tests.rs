//! Count where, group and formula aggregations in a rollup, and rollups and trees of blink
//! tables (task 10.4). The rows of the scripted cases, and the refusals, are what Java
//! 42.9 was observed to answer for the same requests.

use super::tests::{cell, text, EXPAND_ALL};
use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::ops::agg::{AggSpec, Aggregation, Pair, Sentinel};
use crate::ops::sort::testing::{blink_table, frozen};
use arrow::array::UInt64Array;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_expr::Compiler;
use oxhaven_testkit::{
    at_least_for, definition, doubles, ints, strings, test_seeds, ConsistencyChecker,
    RandomUpdates, ReferenceTable, ScriptedTable, TableSnapshot,
};
use std::collections::HashMap;

/// `empty_table(8).update(["K = `k` + (i % 2)", "J = i % 3", "V = i", "D = i * 0.5"])`
fn source() -> Arc<dyn Table> {
    ReferenceTable::new_static(
        definition(&[
            ("K", ColumnType::String),
            ("J", ColumnType::Int),
            ("V", ColumnType::Int),
            ("D", ColumnType::Double),
        ]),
        RowSet::flat(8),
        vec![
            strings((0..8).map(|i| if i % 2 == 0 { "k0" } else { "k1" })),
            ints((0..8).map(|i| i % 3)),
            ints(0..8),
            doubles((0..8).map(|i| f64::from(i) * 0.5)),
        ],
    )
}

fn rolled(aggregations: &[Aggregation], by: &[&str]) -> Result<RollupTable, HierarchicalError> {
    rollup(&source(), aggregations, false, by, Some(&Compiler))
}

/// Every row of `rollup` with everything expanded, as `text` shows them.
fn expanded(rollup: RollupTable) -> Vec<String> {
    let all = ReferenceTable::new_static(
        definition(&[
            ("__DEPTH__", ColumnType::Int),
            ("K", ColumnType::String),
            ("J", ColumnType::Int),
            ("Action", ColumnType::Byte),
        ]),
        RowSet::flat(1),
        vec![
            ints([0]),
            arrow::array::new_null_array(&arrow::datatypes::DataType::Utf8, 1),
            arrow::array::new_null_array(&arrow::datatypes::DataType::Int32, 1),
            Arc::new(arrow::array::Int8Array::from(vec![EXPAND_ALL])),
        ],
    );
    let view = View::new(Arc::new(rollup), Some(all), Some("Action".to_string()));
    text(&view.snapshot(None, 0, 100).unwrap())
}

fn formulas(formulas: &[&str]) -> Vec<Aggregation> {
    formulas
        .iter()
        .map(|formula| {
            let (column, expression) = formula.split_once(" = ").unwrap();
            Aggregation::formula(column, expression)
        })
        .collect()
}

/// Observed on Java 42.9: the count of every level is the count of the source's rows of
/// its group, as Java's sum of the counts of the level below gives it.
#[test]
fn a_count_where_counts_the_rows_of_every_level() {
    let aggregations = [
        Aggregation::count_where("C", ["V > 2", "D < 3"]),
        Aggregation::columns(AggSpec::Sum, [Pair::of("V")]),
    ];
    assert_eq!(
        expanded(rolled(&aggregations, &["K", "J"]).unwrap()),
        [
            "1|true|-|-|3|28",
            "2|true|k0|-|1|12",
            "3|-|k0|0|0|6",
            "3|-|k0|2|0|2",
            "3|-|k0|1|1|4",
            "2|true|k1|-|2|16",
            "3|-|k1|1|0|8",
            "3|-|k1|0|1|3",
            "3|-|k1|2|1|5",
        ]
    );
}

/// Observed on Java 42.9: a group of every level has the source's values of its rows, in
/// the order of the source.
#[test]
fn a_group_has_the_rows_of_every_level() {
    let aggregations = [Aggregation::columns(
        AggSpec::Group,
        [Pair::new("G", "V"), Pair::of("D")],
    )];
    assert_eq!(
        expanded(rolled(&aggregations, &["K", "J"]).unwrap()),
        [
            "1|true|-|-|[0, 1, 2, 3, 4, 5, 6, 7]|[0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5]",
            "2|true|k0|-|[0, 2, 4, 6]|[0.0, 1.0, 2.0, 3.0]",
            "3|-|k0|0|[0, 6]|[0.0, 3.0]",
            "3|-|k0|2|[2]|[1.0]",
            "3|-|k0|1|[4]|[2.0]",
            "2|true|k1|-|[1, 3, 5, 7]|[0.5, 1.5, 2.5, 3.5]",
            "3|-|k1|1|[1, 7]|[0.5, 3.5]",
            "3|-|k1|0|[3]|[1.5]",
            "3|-|k1|2|[5]|[2.5]",
        ]
    );
}

/// Observed on Java 42.9: a formula is evaluated over the source's rows of the group at
/// every level, and may read the level's number of group-by columns and their names.
#[test]
fn a_formula_is_evaluated_over_the_rows_of_every_level() {
    let first = formulas(&[
        "S = sum(V) + 1",
        "Dep = __FORMULA_DEPTH__",
        "Ks = __FORMULA_KEYS__",
        "T = max(V) - min(V)",
    ]);
    assert_eq!(
        expanded(rolled(&first, &["K", "J"]).unwrap()),
        [
            "1|true|-|-|29|0|[]|7",
            "2|true|k0|-|13|1|[K]|6",
            "3|-|k0|0|7|2|[K, J]|6",
            "3|-|k0|2|3|2|[K, J]|0",
            "3|-|k0|1|5|2|[K, J]|0",
            "2|true|k1|-|17|1|[K]|6",
            "3|-|k1|1|9|2|[K, J]|6",
            "3|-|k1|0|4|2|[K, J]|0",
            "3|-|k1|2|6|2|[K, J]|0",
        ]
    );
    let second = formulas(&["S = sum(V) * count(D)", "T = max(V) + min(D)", "U = 1"]);
    assert_eq!(
        expanded(rolled(&second, &["K", "J"]).unwrap()),
        [
            "1|true|-|-|224|7.0|1",
            "2|true|k0|-|48|6.0|1",
            "3|-|k0|0|12|6.0|1",
            "3|-|k0|2|2|3.0|1",
            "3|-|k0|1|4|6.0|1",
            "2|true|k1|-|64|7.5|1",
            "3|-|k1|1|16|7.5|1",
            "3|-|k1|0|3|4.5|1",
            "3|-|k1|2|5|7.5|1",
        ]
    );
    // a result named like its input; Java also shows `N = `` + __FORMULA_KEYS__`, whose
    // concatenation of a vector the expression crate does not have
    let named = formulas(&["V = sum(V)"]);
    assert_eq!(
        expanded(rolled(&named, &["K", "J"]).unwrap()),
        [
            "1|true|-|-|28",
            "2|true|k0|-|12",
            "3|-|k0|0|6",
            "3|-|k0|2|2",
            "3|-|k0|1|4",
            "2|true|k1|-|16",
            "3|-|k1|1|8",
            "3|-|k1|0|3",
            "3|-|k1|2|5",
        ]
    );
    // a later formula may read what the first one reads
    let subset = formulas(&["S = sum(V) + sum(D)", "T = max(D)"]);
    let rows = expanded(rolled(&subset, &["K", "J"]).unwrap());
    assert_eq!(rows[0], "1|true|-|-|42.0|3.5");
    assert_eq!(rows[6], "3|-|k1|1|12.0|3.5");
    // without group-by columns there is one level, so any formulas
    let mut total = formulas(&["S = sum(V)", "T = sum(D)"]);
    total.push(Aggregation::count_where("C", ["V > 2"]));
    assert_eq!(expanded(rolled(&total, &[]).unwrap()), ["1|-|28|14.0|5"]);
}

/// Observed on Java 42.9: the rows of the source under a finest group are null in the
/// formula's column.
#[test]
fn a_formula_with_constituents() {
    let rollup = rollup(
        &source(),
        &formulas(&["S = sum(V)"]),
        true,
        &["K", "J"],
        Some(&Compiler),
    )
    .unwrap();
    let rows = expanded(rollup);
    assert_eq!(rows.len(), 17);
    assert_eq!(
        rows[..4],
        [
            "1|true|-|-|28|-|-|-|-",
            "2|true|k0|-|12|-|-|-|-",
            "3|true|k0|0|6|-|-|-|-",
            "4|-|-|-|-|k0|0|0|0.0",
        ]
    );
}

/// Observed on Java 42.9: a formula that reads a group-by column fails at the level that
/// stops grouping by it, when its result there has another type.
#[test]
fn a_formula_whose_type_changes_with_the_level_is_refused() {
    let refused = |formula: &str| rolled(&formulas(&[formula]), &["K", "J"]).err();
    assert_eq!(
        refused("L = J"),
        Some(HierarchicalError::IllegalArgument(
            "Inconsistent return type in rollup for Formula column 'L': previous level was \
             int, but level 1 is interface io.deephaven.vector.IntVector"
                .to_string()
        ))
    );
    assert_eq!(
        refused("L = K"),
        Some(HierarchicalError::IllegalArgument(
            "Inconsistent return type in rollup for Formula column 'L': previous level was \
             class java.lang.String, but level 0 is interface io.deephaven.vector.ObjectVector"
                .to_string()
        ))
    );
    // the finest level fails first, the formulas in their order
    let both = formulas(&["L = V", "M = J"]);
    assert_eq!(
        rolled(&both, &["K", "J"]).err(),
        Some(HierarchicalError::IllegalArgument(
            "Inconsistent return type in rollup for Formula column 'M': previous level was \
             int, but level 1 is interface io.deephaven.vector.IntVector"
                .to_string()
        ))
    );
    // Java's walk reads `__FORMULA_DEPTH__` only in a rollup
    assert!(aggregate_with_check(&["S = __FORMULA_DEPTH__"]));
}

fn aggregate_with_check(formula: &[&str]) -> bool {
    crate::ops::agg::aggregate_with(&source(), &formulas(formula), false, &["K"], &Compiler)
        .is_err()
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.by.GroupByReaggregateOperator#GroupByReaggregateOperator"
)]
fn a_formula_that_reads_a_group_by_column_is_not_ported() {
    let _ = rolled(&formulas(&["L = J == null"]), &["K", "J"]);
}

/// Observed on Java 42.9: fails with a `NoSuchColumnException` for `D_GRP___ROLLUP__`.
#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.by.GroupByReaggregateOperator#GroupByReaggregateOperator"
)]
fn a_later_formula_that_reads_another_column_is_not_ported() {
    let _ = rolled(
        &formulas(&["S = sum(V)", "T = max(V) + sum(D)"]),
        &["K", "J"],
    );
}

/// Observed on Java 42.9: fails with a `NoSuchColumnException` for `V_GRP___ROLLUP__`.
#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.by.GroupByReaggregateOperator#GroupByReaggregateOperator"
)]
fn a_formula_after_one_that_reads_nothing_is_not_ported() {
    let _ = rolled(&formulas(&["U = 1", "S = sum(V)"]), &["K"]);
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.by.AggregationProcessor.Converter#ensureGroupByOperator"
)]
fn a_group_and_a_formula_are_not_ported() {
    let mut aggregations = formulas(&["S = sum(V)"]);
    aggregations.push(Aggregation::columns(AggSpec::Group, [Pair::of("V")]));
    let _ = rolled(&aggregations, &["K", "J"]);
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.by.AggregationProcessor.Converter#ensureGroupByOperator"
)]
fn two_groups_are_not_ported() {
    let aggregations = [
        Aggregation::columns(AggSpec::Group, [Pair::new("G", "D")]),
        Aggregation::columns(AggSpec::Group, [Pair::new("H", "V")]),
    ];
    let _ = rolled(&aggregations, &["K", "J"]);
}

/// Observed on Java 42.9: the null column of a group-by column at a coarser level and a
/// result of its name are two outputs of one name.
#[test]
fn a_count_where_or_formula_named_like_a_group_by_column_is_refused() {
    let duplicate = |name: &str| {
        Some(HierarchicalError::IllegalArgument(format!(
            "Duplicate output columns found: {name} used 2 times"
        )))
    };
    let count = [Aggregation::count_where("K", ["V > 2"])];
    assert_eq!(rolled(&count, &["K", "J"]).err(), duplicate("K"));
    assert_eq!(
        rolled(&formulas(&["J = sum(V)"]), &["K", "J"]).err(),
        duplicate("J")
    );
}

const BLINK_COLUMNS: [(&str, ColumnType); 4] = [
    ("K", ColumnType::String),
    ("J", ColumnType::Int),
    ("V", ColumnType::Int),
    ("D", ColumnType::Double),
];

fn blink_rows(rows: &[(&str, i32, i32, f64)]) -> Vec<ArrayRef> {
    vec![
        strings(rows.iter().map(|row| row.0)),
        ints(rows.iter().map(|row| row.1)),
        ints(rows.iter().map(|row| row.2)),
        doubles(rows.iter().map(|row| row.3)),
    ]
}

/// Observed on Java 42.9 with a table publisher's blink table.
#[test]
fn java_refuses_some_rollups_of_a_blink_table() {
    let graph: Arc<dyn UpdateGraph> = SteppedUpdateGraph::new();
    let (blink, _) = blink_table(&graph, &BLINK_COLUMNS);
    let refused = |aggregations: &[Aggregation], constituents: bool| {
        rollup(&blink, aggregations, constituents, &["K"], Some(&Compiler))
            .err()
            .map(|error| error.to_string())
    };
    let blink_text = |what: &str| {
        Some(format!(
            "Blink tables do not support {what}; use BlinkTableTools.blinkToAppendOnly to \
             accumulate full history"
        ))
    };
    let of = |spec: AggSpec| Aggregation::columns(spec, [Pair::new("F", "D")]);
    let redirected = "with exposed row redirections (e.g. for rollup(), AggFirstRowKey, or \
                      AggLastRowKey)";
    assert_eq!(
        refused(&[of(AggSpec::First)], false),
        blink_text(&format!("AggFirst {redirected}"))
    );
    assert_eq!(
        refused(&[of(AggSpec::Last)], false),
        blink_text(&format!("AggLast {redirected}"))
    );
    let sorted = vec!["V".to_string()];
    assert_eq!(
        refused(
            &[of(AggSpec::SortedFirst {
                columns: sorted.clone()
            })],
            false
        ),
        blink_text("AggSortedFirst with exposed row redirections (e.g. for rollup())")
    );
    assert_eq!(
        refused(&[of(AggSpec::SortedLast { columns: sorted })], false),
        blink_text("AggSortedLast with exposed row redirections (e.g. for rollup())")
    );
    let group = of(AggSpec::Group);
    assert_eq!(
        refused(&[group.clone()], false),
        blink_text("AggGroup for rollup")
    );
    assert_eq!(
        refused(&formulas(&["G = sum(V)"]), false),
        blink_text("AggFormula for rollup")
    );
    let sum = Aggregation::columns(AggSpec::Sum, [Pair::of("V")]);
    assert_eq!(
        refused(&[sum.clone()], true),
        blink_text("rollup with included constituents")
    );
    // in the order of the aggregations, a partition first
    let median = of(AggSpec::Median {
        average_evenly_divided: true,
    });
    assert_eq!(
        refused(&[group.clone(), median.clone()], false),
        blink_text("AggGroup for rollup")
    );
    assert_eq!(
        refused(&[median.clone(), group], false),
        Some("AggMedian is not supported for rollup()".to_string())
    );
    let partition = Aggregation::Partition {
        column: "P".to_string(),
        include_group_by_columns: true,
    };
    assert_eq!(
        refused(&[median, partition], false),
        Some("AggPartition is not supported for rollup()".to_string())
    );
    assert_eq!(refused(&[sum], false), None);
}

/// The rows of a table as text, in row order.
fn rows_of(table: &dyn Table) -> Vec<String> {
    let snapshot = TableSnapshot::current(table).unwrap();
    (0..snapshot.rows.size() as usize)
        .map(|row| {
            snapshot
                .columns
                .iter()
                .map(|column| cell(column, row))
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

/// Observed on Java 42.9 (a table publisher's blink table): every level keeps the rows of
/// every cycle, as the blink operators of Java's finest level do.
#[test]
fn a_rollup_of_a_blink_table_keeps_every_row_it_had() {
    let stepped = SteppedUpdateGraph::new();
    let graph: Arc<dyn UpdateGraph> = stepped.clone();
    let (blink, publisher) = blink_table(&graph, &BLINK_COLUMNS);
    let of = |spec: AggSpec, pair: &str| {
        let (output, input) = pair.split_once(" = ").unwrap();
        Aggregation::columns(spec, [Pair::new(output, input)])
    };
    let aggregations = vec![
        of(AggSpec::Sum, "S = V"),
        Aggregation::count("N"),
        of(AggSpec::Avg, "A = D"),
        of(AggSpec::Min, "Mn = V"),
        of(AggSpec::Max, "Mx = V"),
        of(AggSpec::Var, "Va = D"),
        of(AggSpec::CountDistinct { count_nulls: false }, "CD = V"),
        of(
            AggSpec::Distinct {
                include_nulls: false,
            },
            "Di = V",
        ),
        Aggregation::columns(
            AggSpec::Unique {
                include_nulls: false,
                non_unique: Sentinel::Null,
            },
            [Pair::new("U", "J")],
        ),
        Aggregation::columns(
            AggSpec::WeightedAvg {
                weight: "V".to_string(),
            },
            [Pair::new("W", "D")],
        ),
        Aggregation::count_where("C", ["V > 2"]),
        of(AggSpec::AbsSum, "AS = D"),
    ];
    let rollup = rollup(&blink, &aggregations, false, &["K", "J"], Some(&Compiler)).unwrap();
    let levels: Vec<Arc<dyn Table>> = (0..3)
        .map(|depth| rollup.level(depth).unwrap().clone())
        .collect();
    let checkers: Vec<_> = levels
        .iter()
        .map(|level| ConsistencyChecker::attach(level.clone()))
        .collect();
    let cycle = |rows: &[(&str, i32, i32, f64)]| {
        if !rows.is_empty() {
            publisher.push(blink_rows(rows));
        }
        stepped.run_cycle();
        for checker in &checkers {
            checker.assert_consistent();
        }
    };
    cycle(&[("a", 1, 7, 4.0), ("c", 1, 3, -1.0)]);
    assert_eq!(
        rows_of(&*levels[0]),
        ["10|2|1.5|3|7|12.5|2|[3, 7]|1|2.5|2|5.0"]
    );
    assert_eq!(
        rows_of(&*levels[1]),
        [
            "a|7|1|4.0|7|7|NaN|1|[7]|1|4.0|1|4.0",
            "c|3|1|-1.0|3|3|NaN|1|[3]|1|-1.0|1|1.0"
        ]
    );
    cycle(&[]);
    assert_eq!(blink.row_set().size(), 0);
    assert_eq!(
        rows_of(&*levels[0]),
        ["10|2|1.5|3|7|12.5|2|[3, 7]|1|2.5|2|5.0"]
    );
    cycle(&[("a", 2, 1, 1.0)]);
    assert_eq!(
        rows_of(&*levels[0]),
        ["11|3|1.3333333333333333|1|7|6.333333333333334|3|[1, 3, 7]|-|2.3636363636363638|2|6.0"]
    );
    assert_eq!(
        rows_of(&*levels[1]),
        [
            "a|8|2|2.5|1|7|4.5|2|[1, 7]|-|3.625|1|5.0",
            "c|3|1|-1.0|3|3|NaN|1|[3]|1|-1.0|1|1.0"
        ]
    );
    assert_eq!(rows_of(&*levels[2]).len(), 3);
}

/// Observed on Java 42.9: `QueryTable#tree` refuses a blink table before it looks at the
/// columns.
#[test]
fn java_refuses_a_tree_of_a_blink_table() {
    let graph: Arc<dyn UpdateGraph> = SteppedUpdateGraph::new();
    let (blink, _) = blink_table(&graph, &[("Id", ColumnType::Int), ("Par", ColumnType::Int)]);
    let text = "Blink tables do not support tree; use BlinkTableTools.blinkToAppendOnly to \
                accumulate full history";
    for (id, parent) in [("Id", "Par"), ("Nope", "Par")] {
        assert_eq!(
            tree(&blink, id, parent)
                .err()
                .map(|error| error.to_string()),
            Some(text.to_string())
        );
    }
    // the gRPC request with orphans promoted joins the table with itself first
    let promoted = promote_orphans(&blink, "Id", "Par", &Compiler).unwrap();
    assert_eq!(
        tree(&promoted, "Id", "Par")
            .err()
            .map(|error| error.to_string()),
        Some(text.to_string())
    );
}

/// Observed on Java 42.9: the text names each column's type as `Class#toString` does,
/// and the component type of an array or a vector.
#[test]
fn a_tree_of_columns_of_two_types_is_refused_with_both() {
    let columns = [
        ("Id", ColumnType::Int),
        ("Ld", ColumnType::LocalDate),
        ("Bd", ColumnType::BigDecimal),
        ("Arr", ColumnType::Array(Box::new(ColumnType::Long))),
        ("Vec", ColumnType::vector(ColumnType::Int)),
        ("Obj", ColumnType::Object(None)),
        (
            "Bools",
            ColumnType::Array(Box::new(ColumnType::PrimitiveBoolean)),
        ),
    ];
    let source = ReferenceTable::new_static(
        definition(&columns),
        RowSet::empty(),
        columns
            .iter()
            .map(|(_, column_type)| arrow::array::new_null_array(&column_type.arrow_type(), 0))
            .collect(),
    );
    let refusal = |id: &str, parent: &str| {
        let error = tree(&(source.clone() as Arc<dyn Table>), id, parent)
            .err()
            .unwrap()
            .to_string();
        let prefix = "tree parent and identifier columns must have the same data type, but ";
        error.strip_prefix(prefix).unwrap().to_string()
    };
    assert_eq!(
        refusal("Id", "Ld"),
        "parent is [Ld, class java.time.LocalDate] and identifier is [Id, int]"
    );
    assert_eq!(
        refusal("Bd", "Id"),
        "parent is [Id, int] and identifier is [Bd, class java.math.BigDecimal]"
    );
    assert_eq!(
        refusal("Arr", "Id"),
        "parent is [Id, int] and identifier is [Arr, class [J, long]"
    );
    assert_eq!(
        refusal("Id", "Vec"),
        "parent is [Vec, interface io.deephaven.vector.IntVector, int] and identifier is [Id, int]"
    );
    assert_eq!(
        refusal("Obj", "Id"),
        "parent is [Id, int] and identifier is [Obj, class java.lang.Object]"
    );
    assert_eq!(
        refusal("Bools", "Id"),
        "parent is [Id, int] and identifier is [Bools, class [Z, boolean]"
    );
}

/// `expected` with its rows in the order `like` has their first `keys` columns, then the
/// rows `like` does not have, as a flat static table. A level kept up to date has its
/// groups in the order they were first seen, one made now in the order of the rows.
fn in_order_of(expected: &dyn Table, like: &dyn Table, keys: usize) -> Arc<dyn Table> {
    let columns = |table: &dyn Table| TableSnapshot::current(table).unwrap().columns;
    let (expected_columns, like_columns) = (columns(expected), columns(like));
    let key = |columns: &[ArrayRef], row: usize| {
        columns[..keys]
            .iter()
            .map(|column| cell(column, row))
            .collect::<Vec<_>>()
            .join("|")
    };
    let size = |columns: &[ArrayRef]| columns.first().map_or(0, |column| column.len());
    let mut row_of: HashMap<String, u64> = (0..size(&expected_columns))
        .map(|row| (key(&expected_columns, row), row as u64))
        .collect();
    let mut order: Vec<u64> = (0..size(&like_columns))
        .filter_map(|row| row_of.remove(&key(&like_columns, row)))
        .collect();
    let mut rest: Vec<u64> = row_of.into_values().collect();
    rest.sort_unstable();
    order.extend(rest);
    let order = UInt64Array::from(order);
    ReferenceTable::new_static(
        expected.definition().clone(),
        RowSet::flat(order.len() as u64),
        expected_columns
            .iter()
            .map(|column| arrow::compute::take(column, &order, None).unwrap())
            .collect(),
    )
}

/// Random changes of the source: every level of a rollup with count where and group, and
/// of one with formulas, stays what the rollup of the source as it is now gives.
fn random_levels(seed: u64) -> u64 {
    let graph = SteppedUpdateGraph::new();
    let mut random = RandomUpdates::new(seed);
    let size = 16;
    let source = ScriptedTable::new(
        graph.clone(),
        definition(&BLINK_COLUMNS),
        RowSet::flat(size),
        BLINK_COLUMNS
            .iter()
            .map(|(_, column_type)| random.values(column_type, size))
            .collect(),
    );
    let parent: Arc<dyn Table> = source.table();
    let by: &[&str] = if seed % 2 == 0 { &["K", "J"] } else { &["J"] };
    let mut grouped = vec![
        Aggregation::count_where("C", ["V > 0", "D < 0.5"]),
        Aggregation::columns(AggSpec::Group, [Pair::new("G", "V"), Pair::of("D")]),
    ];
    let mut computed = formulas(&[
        "S = sum(V) * 2 + count(D) + __FORMULA_DEPTH__",
        "T = max(D) - min(V)",
        "Ks = __FORMULA_KEYS__",
    ]);
    computed.push(Aggregation::count_where("C", ["V > 0"]));
    if seed % 3 == 0 {
        grouped.reverse();
    }
    let mut checkers = Vec::new();
    for aggregations in [grouped, computed] {
        let made = rollup(&parent, &aggregations, false, by, Some(&Compiler)).unwrap();
        for depth in 0..=by.len() {
            let level = made.level(depth).unwrap().clone();
            let (parent, aggregations) = (parent.clone(), aggregations.clone());
            let by: Vec<&'static str> = by.to_vec();
            let like = level.clone();
            checkers.push(ConsistencyChecker::attach_with_recompute(
                level,
                move || {
                    let again = rollup(
                        &frozen(&*parent),
                        &aggregations,
                        false,
                        &by,
                        Some(&Compiler),
                    )
                    .unwrap();
                    in_order_of(&**again.level(depth).unwrap(), &*like, depth)
                },
            ));
        }
    }
    let mut rows = 0;
    for _ in 0..25 {
        source.push(random.next_change(&*parent, size));
        graph.run_cycle();
        for checker in &checkers {
            checker.assert_consistent();
        }
        rows += parent.row_set().size();
    }
    rows
}

#[test]
fn random_changes_keep_count_where_group_and_formula_levels_right() {
    let mut rows = 0;
    for seed in test_seeds(0..30) {
        rows += random_levels(seed);
    }
    assert!(rows >= at_least_for(4000, 0..30), "only {rows} rows");
}
