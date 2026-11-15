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
use crate::ops::sort::testing::{cells, frozen, graph};
use crate::ops::sort::{head, reverse, sort, SortColumn};
use crate::ops::ungroup::ungroup;
use crate::ops::update_by::{update_by, UpdateByOperation, UpdateBySpec};
use crate::table::{in_memory_table, keyed_input_table};
use arrow::array::{Array, ArrayRef, Int32Array, RecordBatch};
use arrow::datatypes::{Field, Schema};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_expr::Compiler;
use oxhaven_testkit::{
    definition, ints, list_of, strings, test_seeds, ConsistencyChecker, RandomUpdates,
    ScriptedTable, TableSnapshot,
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
    for seed in test_seeds(0..12) {
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
    assert_eq!(compared, test_seeds(0..12).count() * 15 * carrying().len());
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
    let last = list_of(&vector, [Some(0), None], ints([]));
    assert_eq!(columns[1].to_data(), first.to_data());
    assert_eq!(columns[2].to_data(), last.to_data());
}

/// A static table of a key `G`, a vector column `V` and its row number `S`, one row for
/// each of `rows`. A `None` among the elements is a null element.
fn vectors(rows: &[(&str, &[Option<i32>])]) -> Arc<dyn Table> {
    let vector = ColumnType::vector(ColumnType::Int);
    let elements: Int32Array = rows.iter().flat_map(|(_, cells)| cells.iter()).collect();
    let columns = [
        (
            "G",
            ColumnType::String,
            strings(rows.iter().map(|(key, _)| *key)),
        ),
        (
            "V",
            vector.clone(),
            list_of(
                &vector,
                rows.iter().map(|(_, cells)| Some(cells.len())),
                Arc::new(elements),
            ),
        ),
        ("S", ColumnType::Int, ints(0..rows.len() as i32)),
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

/// The cells of the column `A` that `spec` makes of `input`, by `G` or of the whole table.
fn aggregated(table: &Arc<dyn Table>, spec: AggSpec, input: &str, by: &[&str]) -> Vec<String> {
    let aggregation = Aggregation::columns(spec, [Pair::new("A", input)]);
    let result = aggregate(table, &[aggregation], false, by).unwrap();
    let column = result.definition().index_of("A").unwrap();
    cells(&*result, column)
}

fn median() -> AggSpec {
    AggSpec::Median {
        average_evenly_divided: true,
    }
}

fn percentile(percentile: f64) -> AggSpec {
    AggSpec::Percentile {
        percentile,
        average_evenly_divided: false,
    }
}

fn unique() -> AggSpec {
    AggSpec::Unique {
        include_nulls: false,
        non_unique: Sentinel::Null,
    }
}

/// A vector is a `Comparable` in Java, so the aggregations that compare objects take a
/// vector column: a vector is less than another if its first element that differs is, or
/// if it is the start of the other. The expected cells are what the reference answers for
/// the same vectors made by `group_by` (`tasks/4.17-fuzz-findings.md`, A2).
///
/// Behavior: `makeMinOrMaxOperator`, `visit(AggSpecPercentile)`,
/// `makeCountDistinctOperator`, `makeUniqueOperator` and `addSortedFirstOrLastOperator` of
/// io.deephaven.engine.table.impl.by.AggregationProcessor over
/// io.deephaven.vector.IntVector#compareTo
#[test]
fn aggregations_that_compare_take_a_vector_column_in_the_order_of_its_elements() {
    let some = |cells: &[i32]| cells.iter().copied().map(Some).collect::<Vec<_>>();
    let (long, four, seven) = (some(&[5, 3]), some(&[4]), some(&[7]));
    let table = vectors(&[("a", &long), ("a", &four), ("b", &seven)]);
    let by_key = |spec: AggSpec| aggregated(&table, spec, "V", &["G"]);
    let of_all = |spec: AggSpec| aggregated(&table, spec, "V", &[]);
    assert_eq!(by_key(AggSpec::Min), ["[4]", "[7]"]);
    assert_eq!(by_key(AggSpec::Max), ["[5, 3]", "[7]"]);
    assert_eq!(of_all(AggSpec::Min), ["[4]"]);
    assert_eq!(of_all(AggSpec::Max), ["[7]"]);
    // Of two values the median is the greater: the mean of two vectors is not taken.
    assert_eq!(by_key(median()), ["[5, 3]", "[7]"]);
    assert_eq!(of_all(median()), ["[5, 3]"]);
    assert_eq!(by_key(percentile(0.25)), ["[4]", "[7]"]);
    assert_eq!(of_all(percentile(0.25)), ["[5, 3]"]);
    let count = AggSpec::CountDistinct { count_nulls: false };
    assert_eq!(by_key(count.clone()), ["2", "1"]);
    assert_eq!(of_all(count.clone()), ["3"]);
    assert_eq!(by_key(unique()), ["null", "[7]"]);
    assert_eq!(of_all(unique()), ["null"]);
    // A scalar picked by the order of the vectors, and a vector by the order of a scalar.
    let by_vector = |first: bool| {
        let columns = vec!["V".to_string()];
        let spec = match first {
            true => AggSpec::SortedFirst { columns },
            false => AggSpec::SortedLast { columns },
        };
        aggregated(&table, spec, "S", &["G"])
    };
    assert_eq!(by_vector(true), ["1", "2"]);
    assert_eq!(by_vector(false), ["0", "2"]);
    let by_scalar = AggSpec::SortedLast {
        columns: vec!["S".to_string()],
    };
    assert_eq!(by_key(by_scalar), ["[4]", "[7]"]);

    // Equal vectors are one value.
    let (five, six) = (some(&[5]), some(&[6]));
    let table = vectors(&[("a", &five), ("a", &five), ("a", &six), ("b", &seven)]);
    let by_key = |spec: AggSpec| aggregated(&table, spec, "V", &["G"]);
    assert_eq!(by_key(count), ["2", "1"]);
    assert_eq!(by_key(unique()), ["null", "[7]"]);
    assert_eq!(by_key(median()), ["[5]", "[7]"]);
    assert_eq!(aggregated(&table, median(), "V", &[]), ["[6]"]);
    let table = vectors(&[("a", &five), ("a", &five), ("b", &seven)]);
    assert_eq!(aggregated(&table, unique(), "V", &["G"]), ["[5]", "[7]"]);

    // A null element is less than any other, and the start of a vector less than it.
    let rows: [(&str, &[Option<i32>]); 5] = [
        ("a", &[None, Some(1)]),
        ("a", &[Some(i32::MIN + 1)]),
        ("a", &[None]),
        ("a", &[None, Some(1), Some(0)]),
        ("a", &[]),
    ];
    let table = vectors(&rows);
    let of_all = |spec: AggSpec| aggregated(&table, spec, "V", &[]);
    assert_eq!(of_all(AggSpec::Min), ["[]"]);
    assert_eq!(of_all(AggSpec::Max), ["[-2147483647]"]);
    assert_eq!(of_all(percentile(0.25)), ["[null]"]);
    assert_eq!(of_all(median()), ["[null, 1]"]);
    assert_eq!(of_all(percentile(0.75)), ["[null, 1, 0]"]);

    // The null cell and the empty cell of `listed` are a null and a value.
    let by_key = |spec: AggSpec| aggregated(&listed(), spec, "V", &["G"]);
    assert_eq!(by_key(AggSpec::Min), ["[]", "null"]);
    assert_eq!(by_key(AggSpec::Max), ["[1, 2]", "null"]);
    let count_nulls = AggSpec::CountDistinct { count_nulls: true };
    assert_eq!(by_key(count_nulls), ["2", "1"]);
    let result = aggregate(
        &listed(),
        &[Aggregation::columns(AggSpec::Min, [Pair::of("V")])],
        false,
        &["G"],
    )
    .unwrap();
    let vector = ColumnType::vector(ColumnType::Int);
    assert_eq!(result.definition().columns()[1].column_type, vector);
    let column = TableSnapshot::current(&*result).unwrap().columns[1].clone();
    let expected = list_of(&vector, [Some(0), None], ints([]));
    assert_eq!(column.to_data(), expected.to_data());
}

/// The message of the panic `f` ends in.
fn panic_of(f: impl FnOnce() + std::panic::UnwindSafe) -> String {
    let payload = std::panic::catch_unwind(f).expect_err("a panic");
    match payload.downcast::<String>() {
        Ok(message) => *message,
        Err(_) => panic!("a panic without a message"),
    }
}

/// The distinct values of a vector column would be a vector of vectors, and an array is
/// not a `Comparable` in Java, which fails when it first compares two of them.
#[test]
fn aggregations_of_a_list_column_that_are_not_ported() {
    let distinct = AggSpec::Distinct {
        include_nulls: false,
    };
    let aggregation = Aggregation::columns(distinct, [Pair::of("V")]);
    let message = panic_of(move || {
        let _ = aggregate(&listed(), &[aggregation], false, &["G"]);
    });
    assert_eq!(
        message,
        "unported: io.deephaven.engine.table.impl.by.AggregationProcessor: \
         makeDistinctOperator for a column of type Vector(Int)"
    );

    let array = ColumnType::array(ColumnType::Int);
    let nested = ColumnType::vector(ColumnType::vector(ColumnType::Int));
    for (column_type, name) in [(array, "Array(Int)"), (nested, "Vector(Vector(Int))")] {
        let cases = [
            (AggSpec::Min, "makeMinOrMaxOperator"),
            (AggSpec::Max, "makeMinOrMaxOperator"),
            (median(), "visit(AggSpecPercentile)"),
            (percentile(0.25), "visit(AggSpecPercentile)"),
            (
                AggSpec::CountDistinct { count_nulls: false },
                "makeCountDistinctOperator",
            ),
            (unique(), "makeUniqueOperator"),
            (
                AggSpec::SortedFirst {
                    columns: vec!["K".to_string()],
                },
                "addSortedFirstOrLastOperator",
            ),
        ];
        let names = [("K", ColumnType::Int), ("L", column_type)];
        let table = || -> Arc<dyn Table> { in_memory_table(definition(&names), &[]).unwrap() };
        for (spec, java_method) in cases {
            let aggregation = Aggregation::columns(spec.clone(), [Pair::of("L")]);
            let message = panic_of(move || {
                let _ = aggregate(&table(), &[aggregation], false, &["K"]);
            });
            assert_eq!(
                message,
                format!(
                    "unported: io.deephaven.engine.table.impl.by.AggregationProcessor: \
                     {java_method} for a column of type {name}"
                ),
                "{spec:?}"
            );
        }
        // a scalar column picked by the order of a list column
        let by_list = AggSpec::SortedFirst {
            columns: vec!["L".to_string()],
        };
        let aggregation = Aggregation::columns(by_list, [Pair::of("K")]);
        let message = panic_of(move || {
            let _ = aggregate(&table(), &[aggregation], false, &[]);
        });
        assert!(
            message.ends_with(&format!(
                "addSortedFirstOrLastOperator for a column of type {name}"
            )),
            "{message}"
        );
    }
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
fn ungroup_replaces_a_list_column_by_its_elements() {
    // every array column, which is V, and V by name
    for columns in [vec![], vec!["V".to_string()]] {
        let ungrouped = ungroup(&listed(), false, &columns).unwrap();
        let types: Vec<&ColumnType> = ungrouped
            .definition()
            .columns()
            .iter()
            .map(|c| &c.column_type)
            .collect();
        assert_eq!(
            types,
            [&ColumnType::Int, &ColumnType::String, &ColumnType::Int]
        );
        // The null cell and the empty cell have no rows.
        let snapshot = TableSnapshot::current(&*ungrouped).unwrap();
        assert_eq!(snapshot.columns[0].to_data(), ints([1, 1]).to_data());
        assert_eq!(snapshot.columns[2].to_data(), ints([1, 2]).to_data());
    }
    // A column that is not one is refused as Java refuses it.
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
fn a_list_column_is_copied_by_name_and_read_by_a_formula() {
    let table = listed();
    // `X = V` is a copy of the column, not a formula.
    let copied = update(&table, &["X = V"], &Compiler).unwrap();
    let vector = ColumnType::vector(ColumnType::Int);
    assert_eq!(copied.definition().columns()[3].column_type, vector);
    let cells = TableSnapshot::current(&*copied).unwrap().columns;
    assert_eq!(cells[3].to_data(), cells[2].to_data());
    assert_eq!(cells[3].null_count(), 1);

    // The cells are [1, 2], null and [].
    let computed = update(&table, &["X = isNull(V)", "S = sum(V)"], &Compiler).unwrap();
    let cells = TableSnapshot::current(&*computed).unwrap().columns;
    let flags: ArrayRef = Arc::new(arrow::array::BooleanArray::from(vec![false, true, false]));
    let sums: ArrayRef = Arc::new(arrow::array::Int64Array::from(vec![Some(3), None, None]));
    assert_eq!(cells[3].to_data(), flags.to_data());
    assert_eq!(cells[4].to_data(), sums.to_data());
    let kept = unstructured_filter(&table, &["isNull(V)"], &Compiler).unwrap();
    let keys = TableSnapshot::current(&*kept).unwrap().columns[0].clone();
    assert_eq!(keys.to_data(), ints([2]).to_data());

    // A vector of vectors is not read yet.
    let nested = ColumnType::vector(vector.clone());
    let cells = list_of(&nested, [Some(1)], list_of(&vector, [Some(1)], ints([1])));
    let fields = vec![Field::new("N", nested.arrow_type(), true)];
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), vec![cells]).unwrap();
    let table: Arc<dyn Table> = in_memory_table(definition(&[("N", nested)]), &[batch]).unwrap();
    let refused = update(&table, &["X = len(N)"], &Compiler);
    let message = refused.err().map(|error| error.to_string());
    assert!(
        message
            .as_deref()
            .is_some_and(|message| message.contains("column N of type Vector(Vector(Int))")),
        "{message:?}"
    );
}

/// Behavior: io.deephaven.engine.table.impl.select.MatchFilter.ColumnTypeConvertorFactory#getConvertor,
/// which has no value but null for a vector.
#[test]
fn a_match_filter_of_a_list_column_takes_null_only() {
    let size = |filter: &str| {
        unstructured_filter(&listed(), &[filter], &Compiler)
            .unwrap()
            .row_set()
            .size()
    };
    assert_eq!(size("V == null"), 1);
    assert_eq!(size("V != null"), 2);
    assert!(unstructured_filter(&listed(), &["V == 1"], &Compiler).is_err());
}

#[test]
#[should_panic(expected = "unported: io.deephaven.engine.table.impl.select.RangeFilter#init")]
fn a_range_filter_of_a_list_column_is_not_ported() {
    let _ = unstructured_filter(&listed(), &["V > null"], &Compiler);
}

/// Java gives the minimum, maximum, median, percentile and unique value of a vector of
/// Booleans or Instants as a column without a component type, which a client receives as
/// lists of strings. That is not reproduced. The aggregations that count, or that read
/// the input column, are answered.
#[test]
fn a_stored_vector_of_booleans_or_instants_is_not_ported() {
    for element in [ColumnType::Boolean, ColumnType::Instant] {
        let names = [("K", ColumnType::Int), ("L", ColumnType::vector(element))];
        let table = || -> Arc<dyn Table> { in_memory_table(definition(&names), &[]).unwrap() };
        for spec in [
            AggSpec::Min,
            AggSpec::Max,
            median(),
            percentile(0.5),
            unique(),
        ] {
            let aggregation = Aggregation::columns(spec.clone(), [Pair::of("L")]);
            let message = panic_of(move || {
                let _ = aggregate(&table(), &[aggregation], false, &["K"]);
            });
            assert!(
                message.ends_with("whose result Java gives without its component type"),
                "{spec:?}: {message}"
            );
        }
        let sorted = AggSpec::SortedLast {
            columns: vec!["K".to_string()],
        };
        let count = AggSpec::CountDistinct { count_nulls: false };
        for spec in [AggSpec::First, AggSpec::Last, sorted, count] {
            let aggregation = Aggregation::columns(spec, [Pair::of("L")]);
            aggregate(&table(), &[aggregation], false, &["K"]).unwrap();
        }
    }
}
