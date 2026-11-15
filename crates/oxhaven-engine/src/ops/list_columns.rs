//! Vector and array columns (`ColumnType::Vector`, `ColumnType::Array`) through the
//! operations that exist: an operation that does not look into a column carries its list
//! cells like any others, and an operation that would have to compare, order or compute
//! with list cells refuses to, by `unported!`, until that is ported.

use crate::ops::agg::{aggregate, AggSpec, Aggregation, Pair, Sentinel};
use crate::ops::distinct::select_distinct;
use crate::ops::filter::unstructured_filter;
use crate::ops::join::{natural_join, JoinAddition, JoinMatch, NaturalJoinType};
use crate::ops::merge::{flatten, merge};
use crate::ops::select::{drop_columns, update};
use crate::ops::sort::testing::{frozen, graph};
use crate::ops::sort::{head, reverse, sort, SortColumn};
use crate::ops::ungroup::ungroup;
use crate::ops::update_by::{update_by, UpdateByOperation, UpdateBySpec};
use crate::table::{in_memory_table, keyed_input_table};
use arrow::array::{Array, ArrayRef, RecordBatch};
use arrow::datatypes::{Field, Schema};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_expr::Compiler;
use oxhaven_testkit::{
    definition, ints, list_of, strings, ConsistencyChecker, RandomUpdates, ScriptedTable,
    TableSnapshot,
};
use std::sync::Arc;

fn columns() -> Vec<(&'static str, ColumnType)> {
    vec![
        ("K", ColumnType::Int),
        ("G", ColumnType::String),
        ("V", ColumnType::vector(ColumnType::Int)),
        ("A", ColumnType::array(ColumnType::Double)),
        (
            "N",
            ColumnType::vector(ColumnType::array(ColumnType::String)),
        ),
    ]
}

type Operation = Box<dyn Fn(&Arc<dyn Table>, &Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync>;

/// Operations of two tables with the columns of [`columns`] that leave the list columns
/// as they are.
fn carrying() -> Vec<(&'static str, Operation)> {
    fn operation(
        f: impl Fn(&Arc<dyn Table>, &Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync + 'static,
    ) -> Operation {
        Box::new(f)
    }
    vec![
        (
            "sort",
            operation(|t, _| sort(t, &[SortColumn::descending("K")]).unwrap()),
        ),
        (
            "sort and head",
            operation(|t, _| {
                let sorted = sort(t, &[SortColumn::ascending("G")]).unwrap();
                head(&sorted, 3).unwrap()
            }),
        ),
        ("reverse", operation(|t, _| reverse(t).unwrap())),
        ("flatten", operation(|t, _| flatten(t).unwrap())),
        (
            "merge",
            operation(|t, other| merge(&[t.clone(), other.clone()]).unwrap()),
        ),
        (
            "where",
            operation(|t, _| unstructured_filter(t, &["K > 0"], &Compiler).unwrap()),
        ),
        (
            "update",
            operation(|t, _| update(t, &["X = K + 1"], &Compiler).unwrap()),
        ),
        (
            "drop_columns",
            operation(|t, _| drop_columns(t, &["K", "A"]).unwrap()),
        ),
        (
            // The list cells come from the right table, and are null without a match.
            "natural_join",
            operation(|t, other| {
                let on = [JoinMatch::same("K")];
                let add = [
                    JoinAddition::renamed("RV", "V"),
                    JoinAddition::renamed("RN", "N"),
                ];
                natural_join(t, other, &on, &add, NaturalJoinType::FirstMatch).unwrap()
            }),
        ),
    ]
}

#[test]
fn operations_that_do_not_read_a_list_column_carry_its_cells() {
    let mut compared = 0;
    for seed in 0..12 {
        let graph = graph();
        let mut random = RandomUpdates::new(seed);
        let mut source = |size: u64| {
            let values = columns()
                .iter()
                .map(|(_, column_type)| random.values(column_type, size))
                .collect();
            ScriptedTable::new(
                graph.clone(),
                definition(&columns()),
                RowSet::from_keys((0..size as RowKey).map(|k| 3 * k + 1)),
                values,
            )
        };
        let scripts = [source(8), source(5)];
        let tables: [Arc<dyn Table>; 2] = [scripts[0].table(), scripts[1].table()];
        let cases: Vec<(&str, Arc<dyn Table>, Arc<ConsistencyChecker>)> = carrying()
            .into_iter()
            .map(|(name, operation)| {
                let result = operation(&tables[0], &tables[1]);
                let tables = tables.clone();
                let recompute = move || operation(&frozen(&*tables[0]), &frozen(&*tables[1]));
                let checker = ConsistencyChecker::attach_with_recompute(result.clone(), recompute);
                (name, result, checker)
            })
            .collect();
        for step in 0..15 {
            for script in &scripts {
                script.push(random.next_change(&*script.table(), 10));
            }
            graph.run_cycle();
            for (name, result, checker) in &cases {
                if let Err(error) = checker.verify() {
                    panic!("{name}, seed {seed}, step {step}: {error}");
                }
                // The result still has the list columns, with their types.
                for column in result.definition().columns() {
                    let source = columns().into_iter().find(|(n, _)| {
                        *n == column.name || column.name.strip_prefix('R') == Some(n)
                    });
                    if let Some((_, column_type)) = source {
                        assert_eq!(column.column_type, column_type, "{name}: {}", column.name);
                    }
                }
                compared += 1;
            }
        }
    }
    assert_eq!(compared, 12 * 15 * carrying().len());
}

/// A static table of three rows: `K` 1, 2, 3, `G` a, b, a, and a vector column `V` of
/// `[1, 2]`, null, `[]`.
fn listed() -> Arc<dyn Table> {
    let vector = ColumnType::vector(ColumnType::Int);
    let columns = [
        ("K", ColumnType::Int, ints([1, 2, 3])),
        ("G", ColumnType::String, strings(["a", "b", "a"])),
        (
            "V",
            vector.clone(),
            list_of(&vector, [Some(2), None, Some(0)], ints([1, 2])),
        ),
    ];
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, column_type, _)| Field::new(*name, column_type.arrow_type(), true))
        .collect();
    let arrays: Vec<ArrayRef> = columns.iter().map(|(_, _, cells)| cells.clone()).collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    let names: Vec<(&str, ColumnType)> = columns
        .iter()
        .map(|(name, column_type, _)| (*name, column_type.clone()))
        .collect();
    in_memory_table(definition(&names), &[batch]).unwrap()
}

#[test]
fn an_in_memory_table_holds_list_cells_and_a_sort_reorders_them_whole() {
    let table = listed();
    let sorted = sort(&table, &[SortColumn::descending("K")]).unwrap();
    let cells = TableSnapshot::current(&*sorted).unwrap().columns[2].clone();
    let vector = ColumnType::vector(ColumnType::Int);
    let expected = list_of(&vector, [Some(0), None, Some(2)], ints([1, 2]));
    assert_eq!(cells.to_data(), expected.to_data());
    assert_eq!(cells.null_count(), 1);
}

#[test]
#[should_panic(expected = "unported: io.deephaven.engine.table.impl.SortOperation#SortOperation")]
fn sort_by_a_list_column_is_not_ported() {
    let _ = sort(&listed(), &[SortColumn::ascending("V")]);
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.by.typed.TypedHasherFactory#make: \
                a group key column of type Vector(Int)"
)]
fn aggregation_by_a_list_key_is_not_ported() {
    let sum = Aggregation::columns(AggSpec::Sum, [Pair::of("K")]);
    let _ = aggregate(&listed(), &[sum], false, &["V"]);
}

#[test]
fn first_and_last_of_a_list_column_are_whole_cells() {
    let aggregations = [
        Aggregation::columns(AggSpec::First, [Pair::new("F", "V")]),
        Aggregation::columns(AggSpec::Last, [Pair::new("L", "V")]),
    ];
    let result = aggregate(&listed(), &aggregations, false, &["G"]).unwrap();
    let vector = ColumnType::vector(ColumnType::Int);
    assert_eq!(result.definition().columns()[1].column_type, vector);
    let columns = TableSnapshot::current(&*result).unwrap().columns;
    // Group a has the rows `[1, 2]` and `[]`, group b the null.
    let first = list_of(&vector, [Some(2), None], ints([1, 2]));
    let last = list_of(&vector, [Some(0), None], ints([1, 2]));
    assert_eq!(columns[1].to_data(), first.to_data());
    assert_eq!(columns[2].to_data(), last.to_data());
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.by.AggregationProcessor: \
                makeMinOrMaxOperator for a column of type Vector(Int)"
)]
fn min_of_a_list_column_is_not_ported() {
    let min = Aggregation::columns(AggSpec::Min, [Pair::of("V")]);
    let _ = aggregate(&listed(), &[min], false, &["G"]);
}

/// The message of the panic `f` ends in.
fn panic_of(f: impl FnOnce() + std::panic::UnwindSafe) -> String {
    let payload = std::panic::catch_unwind(f).expect_err("a panic");
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(_) => panic!("a panic without a message"),
    }
}

#[test]
fn aggregations_that_compare_or_store_list_cells_are_not_ported() {
    let cases = [
        (
            AggSpec::Median {
                average_evenly_divided: true,
            },
            "visit(AggSpecPercentile)",
        ),
        (
            AggSpec::Percentile {
                percentile: 0.25,
                average_evenly_divided: false,
            },
            "visit(AggSpecPercentile)",
        ),
        (
            AggSpec::CountDistinct { count_nulls: false },
            "makeCountDistinctOperator",
        ),
        (
            AggSpec::Unique {
                include_nulls: false,
                non_unique: Sentinel::Null,
            },
            "makeUniqueOperator",
        ),
        (
            AggSpec::Distinct {
                include_nulls: false,
            },
            "makeDistinctOperator",
        ),
        (
            AggSpec::SortedFirst {
                columns: vec!["K".to_string()],
            },
            "addSortedFirstOrLastOperator",
        ),
        (
            AggSpec::SortedLast {
                columns: vec!["K".to_string()],
            },
            "addSortedFirstOrLastOperator",
        ),
    ];
    for (spec, java_method) in cases {
        let aggregation = Aggregation::columns(spec.clone(), [Pair::of("V")]);
        let message = panic_of(move || {
            let _ = aggregate(&listed(), &[aggregation], false, &["G"]);
        });
        assert_eq!(
            message,
            format!(
                "unported: io.deephaven.engine.table.impl.by.AggregationProcessor: \
                 {java_method} for a column of type Vector(Int)"
            ),
            "{spec:?}"
        );
    }
    // a scalar column picked by the order of a list column
    let by_list = AggSpec::SortedFirst {
        columns: vec!["V".to_string()],
    };
    let aggregation = Aggregation::columns(by_list, [Pair::of("K")]);
    let message = panic_of(move || {
        let _ = aggregate(&listed(), &[aggregation], false, &["G"]);
    });
    assert!(
        message.ends_with("addSortedFirstOrLastOperator for a column of type Vector(Int)"),
        "{message}"
    );
}

#[test]
fn numeric_aggregations_of_a_list_column_are_refused_as_those_of_a_string() {
    let weight = || "K".to_string();
    for spec in [
        AggSpec::AbsSum,
        AggSpec::Avg,
        AggSpec::Var,
        AggSpec::Std,
        AggSpec::WeightedAvg { weight: weight() },
        AggSpec::WeightedSum { weight: weight() },
    ] {
        let message = |column: &str| {
            let aggregation = Aggregation::columns(spec.clone(), [Pair::of(column)]);
            let refused = aggregate(&listed(), &[aggregation], false, &["K"]);
            refused.err().map(|error| error.to_string())
        };
        let of_list = message("V").unwrap_or_else(|| panic!("{spec:?} of a list is accepted"));
        let of_string = message("G").unwrap_or_else(|| panic!("{spec:?} of a string is accepted"));
        assert_eq!(
            of_list,
            of_string
                .replace("column G", "column V")
                .replace("type String", "type Vector(Int)"),
            "{spec:?}"
        );
    }
}

#[test]
fn ungroup_recognizes_a_list_column_and_cannot_read_it_yet() {
    let expected = "unported: \
        io.deephaven.engine.table.impl.sources.UngroupedColumnSource#getColumnSource: \
        column V of type Vector(Int)";
    // every array column, which is V,
    let message = panic_of(|| {
        let _ = ungroup(&listed(), false, &[]);
    });
    assert_eq!(message, expected);
    // and V by name
    let message = panic_of(|| {
        let _ = ungroup(&listed(), true, &["V".to_string()]);
    });
    assert_eq!(message, expected);
    // A column that is not one is still refused as Java refuses it.
    let refused = ungroup(&listed(), false, &["K".to_string()]);
    let message = refused.err().map(|error| error.to_string());
    assert_eq!(
        message.as_deref(),
        Some("Column K is not an array or Vector")
    );
}

#[test]
fn sum_of_a_list_column_is_refused_as_java_refuses_a_type_without_a_sum() {
    let sum = Aggregation::columns(AggSpec::Sum, [Pair::of("V")]);
    let refused = aggregate(&listed(), &[sum], false, &["G"]);
    let message = refused.err().map(|error| error.to_string());
    assert_eq!(
        message.as_deref(),
        Some("Sum is not supported for column V of type Vector(Int)")
    );
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.by.typed.TypedHasherFactory#make: \
                a key column stored as List"
)]
fn select_distinct_of_a_list_column_is_not_ported() {
    let _ = select_distinct(&listed(), &["V".to_string()]);
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.by.typed.TypedHasherFactory#makeNaturalJoin"
)]
fn a_join_on_a_list_column_is_not_ported() {
    let on = [JoinMatch::same("V")];
    let add = [JoinAddition::renamed("RK", "K")];
    let _ = natural_join(&listed(), &listed(), &on, &add, NaturalJoinType::FirstMatch);
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.updateby.UpdateBy#updateBy: \
                a bucket or input column of type Vector(Int)"
)]
fn update_by_with_a_list_bucket_column_is_not_ported() {
    let _ = update_by(
        &listed(),
        &[UpdateByOperation::new(UpdateBySpec::CumSum, &["K"])],
        &["V"],
    );
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.util.KeyedArrayBackedInputTable#makeKeySource: \
                vector or array key column V"
)]
fn an_input_table_keyed_by_a_list_column_is_not_ported() {
    let names = [("V", ColumnType::array(ColumnType::Long))];
    let _ = keyed_input_table(graph(), definition(&names), &["V"], None);
}

#[test]
fn a_list_column_is_copied_by_name_and_a_formula_that_reads_one_is_refused() {
    let table = listed();
    // `X = V` is a copy of the column, not a formula.
    let copied = update(&table, &["X = V"], &Compiler).unwrap();
    let vector = ColumnType::vector(ColumnType::Int);
    assert_eq!(copied.definition().columns()[3].column_type, vector);
    let cells = TableSnapshot::current(&*copied).unwrap().columns;
    assert_eq!(cells[3].to_data(), cells[2].to_data());
    assert_eq!(cells[3].null_count(), 1);

    let refused = update(&table, &["X = isNull(V)"], &Compiler);
    let message = refused.err().map(|error| error.to_string());
    assert!(
        message
            .as_deref()
            .is_some_and(|message| message.contains("column V of type Vector(Int)")),
        "{message:?}"
    );
    assert!(unstructured_filter(&table, &["isNull(V)"], &Compiler).is_err());
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.select.MatchFilter.ColumnTypeConvertorFactory#getConvertor: \
                column type Vector(Int)"
)]
fn a_match_filter_of_a_list_column_is_not_ported() {
    let _ = unstructured_filter(&listed(), &["V == null"], &Compiler);
}
