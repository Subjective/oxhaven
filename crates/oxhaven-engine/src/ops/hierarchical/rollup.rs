//! A rollup: the aggregations of a table at every level of a key hierarchy.

use super::keys::{parts, KeyIndex};
use super::node_ops::{node_table, NodeColumns, NodeOperations};
use super::{
    merged_sorts, Expandable, HierarchicalError, Hierarchy, Located, NodeId, NodeKey, Parent, Part,
    DEPTH_COLUMN_NAME, EXPANDED_COLUMN_NAME,
};
use crate::ops::agg::{
    aggregate_with_constants, java_class, AggSpec, AggregateError, Aggregation, FormulaConstant,
};
use crate::ops::filter::{CheckedFilters, FilterError};
use crate::ops::partitioned::{partition_by, tables_of, PartitionedTable};
use crate::ops::select::{drop_columns, ArrayFormula};
use crate::ops::sort::{check_sort_columns, sorted_rows, SortColumn, SortError};
use crate::table::sortable_columns;
use arrow::array::{ArrayRef, AsArray, Int32Array, ListArray, StringArray};
use arrow::buffer::OffsetBuffer;
use arrow::datatypes::DataType;
use arrow::datatypes::Int32Type;
use oxhaven_contracts::expr::FormulaCompiler;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::Table;
use std::sync::Arc;

/// The children of the rows of one level: the next level partitioned by this level's
/// keys, and where each key's row is in the partition's underlying table.
struct ChildParts {
    partitioned: PartitionedTable,
    index: KeyIndex,
}

/// The levels of a rollup and how a node is found in them. Rollups that differ only in
/// how their nodes are sorted share them.
struct Levels {
    /// One per level, 0 to the number of group-by columns.
    tables: Vec<Arc<dyn Table>>,
    /// Per level, the row of each key.
    indexes: Vec<KeyIndex>,
    /// `children[d]` for `d >= 1` and below the last level: the rows of level `d + 1` by
    /// the keys of level `d`. Level 0 has one row, whose children are level 1. With
    /// constituents and group-by columns, the last one is the rows of the source by all
    /// the keys.
    children: Vec<Option<ChildParts>>,
    /// In a rollup with constituents, what the levels aggregate: without group-by columns
    /// its rows are the children of the grand total.
    constituents: Option<Arc<dyn Table>>,
}

/// The aggregations of a table by every prefix of its group-by columns.
///
/// Level `d` is the source aggregated by the first `d` group-by columns: level 0 is the
/// one row of the grand total. The children of a row of level `d` are the rows of level
/// `d + 1` with the same first `d` keys.
///
/// Behavior: io.deephaven.engine.table.impl.hierarchical.RollupTableImpl. Java makes the
/// coarser levels by aggregating the finer ones again; here every level aggregates the
/// source (`docs/HIERARCHICAL.md`, "Decision").
#[derive(Clone)]
pub struct RollupTable {
    source: Arc<dyn Table>,
    /// What the levels aggregate: the source, or the rows of it that the filters on
    /// group-by columns accept.
    base: Arc<dyn Table>,
    aggregations: Vec<Aggregation>,
    group_by: Vec<String>,
    include_constituents: bool,
    levels: Arc<Levels>,
    /// The depth, whether the row is expanded, and the columns of the aggregated nodes.
    aggregated: TableDefinition,
    /// The columns of a snapshot: those of `aggregated`, then with constituents the
    /// columns of the source.
    columns: Vec<ColumnDefinition>,
    /// What the rows of every aggregated node are sorted by, the column that decides
    /// first.
    sorts: Vec<SortColumn>,
    /// What the rows of the source under a finest group are sorted by.
    constituent_sorts: Vec<SortColumn>,
    /// The columns a client may sort by, when the source's sorting was restricted.
    sortable: Option<Vec<String>>,
    /// The columns of the constituent nodes, in a rollup that has them: the source's, then
    /// those that node operations added.
    constituent: Option<TableDefinition>,
    /// The update views and formats of the aggregated nodes, whose columns are the last
    /// of `aggregated`.
    aggregated_columns: NodeColumns,
    /// Those of the constituent nodes, whose columns are the last of `constituent`.
    constituent_columns: NodeColumns,
}

/// The name Java refuses `aggregation` in a rollup by, if it does.
///
/// Behavior: io.deephaven.engine.table.impl.by.AggregationProcessor.UnsupportedRollupAggregations,
/// and RollupBaseConverter#visit(Partition) for a rollup without constituents
fn refused_by_java(aggregation: &Aggregation) -> Option<&'static str> {
    match aggregation {
        Aggregation::Columns { spec, .. } => match spec {
            AggSpec::Median { .. } => Some("Median"),
            AggSpec::Percentile { .. } => Some("Percentile"),
            AggSpec::Formula { .. } => Some("Formula"),
            _ => None,
        },
        Aggregation::Partition { .. } => Some("Partition"),
        _ => None,
    }
}

/// What Java's way of making the coarser levels, aggregating the finer ones again, refuses
/// or answers differently from aggregating the source, which is how the levels are made
/// here.
///
/// A coarser level has a null column for every group-by column it does not group by, and
/// Java counts those among the outputs: an aggregation's output of such a name is a
/// duplicate. And Java sorts the rows of a finer level for a sorted first or last by that
/// level's columns of the sort columns' names: it fails when there is none
/// (`NoSuchColumnException`), and where there is one that is not the sort column's own
/// values the result is another. Only a sort column that the aggregation itself carries
/// under its own name gives what the source gives; the rest is refused until it is ruled
/// whether Java's answers are to be reproduced.
///
/// Behavior: the constructor of io.deephaven.engine.table.impl.by.AggregationProcessor
/// (`RollupAggregationOutputs`), and
/// AggregationProcessor.RollupReaggregatedConverter#reaggregateSortedFirstOrLastOperator
fn check_reaggregation(
    aggregations: &[Aggregation],
    group_by: &[&str],
) -> Result<(), HierarchicalError> {
    let mut duplicates = Vec::new();
    for (index, key) in group_by.iter().enumerate() {
        if group_by[..index].contains(key) {
            continue;
        }
        let outputs = aggregations.iter().map(|aggregation| match aggregation {
            Aggregation::Columns { pairs, .. } => {
                pairs.iter().filter(|pair| pair.output == *key).count()
            }
            Aggregation::Count { column }
            | Aggregation::CountWhere { column, .. }
            | Aggregation::Formula { column, .. } => usize::from(column == key),
            _ => 0,
        });
        let used = outputs.sum::<usize>() + group_by.iter().filter(|other| *other == key).count();
        if used > 1 {
            duplicates.push(format!("{key} used {used} times"));
        }
    }
    if !duplicates.is_empty() {
        return Err(HierarchicalError::IllegalArgument(format!(
            "Duplicate output columns found: {}",
            duplicates.join(", ")
        )));
    }
    for aggregation in aggregations {
        let Aggregation::Columns { spec, pairs } = aggregation else {
            continue;
        };
        let (AggSpec::SortedFirst { columns } | AggSpec::SortedLast { columns }) = spec else {
            continue;
        };
        let carried = |column: &String| {
            pairs
                .iter()
                .any(|pair| pair.output == *column && pair.input == *column)
        };
        if !columns.iter().all(carried) {
            oxhaven_contracts::unported!(
                "io.deephaven.engine.table.impl.by.AggregationProcessor.RollupReaggregatedConverter#reaggregateSortedFirstOrLastOperator",
                "a sorted first or last in a rollup with group-by columns, sorted by a column that is not one of its own results"
            );
        }
    }
    Ok(())
}

/// Refuses as not ported a rollup of a refreshing source, with group-by columns, that
/// computes or orders `BigDecimal` or `BigInteger` values: anything but a first, last,
/// group or formula of them.
///
/// Java makes each coarser level from the level below it: it adds up the hidden running
/// sums of that level for an average or a variance, and merges the sorted sets of that
/// level for a minimum, maximum or distinct value. The running scale of a sum and the
/// sorted sets of big numbers depend on the history of a ticking table (task 15.3), and
/// here every level is computed from the source. Whether the two agree after updates is
/// not checked, so the case is refused.
///
/// Behavior: io.deephaven.engine.table.impl.by.AggregationProcessor.RollupReaggregatedConverter
/// (#reaggregateAsSum, #reaggregateAvgOperator, #reaggregateStdOrVarOperators,
/// #reaggregateMinOrMaxOperators, #reaggregateSsmBackedOperator,
/// #reaggregateSortedFirstOrLastOperator)
fn check_big_numbers(source: &dyn Table, aggregations: &[Aggregation]) {
    let definition = source.definition();
    let big = |name: &str| {
        definition
            .index_of(name)
            .map(|index| &definition.columns()[index].column_type)
            .filter(|column_type| {
                matches!(column_type, ColumnType::BigDecimal | ColumnType::BigInteger)
            })
    };
    for aggregation in aggregations {
        let Aggregation::Columns { spec, pairs } = aggregation else {
            continue;
        };
        let column = match spec {
            AggSpec::First | AggSpec::Last | AggSpec::Group | AggSpec::Formula { .. } => None,
            AggSpec::SortedFirst { columns } | AggSpec::SortedLast { columns } => columns
                .iter()
                .find_map(|name| big(name).map(|ty| (name, ty))),
            _ => pairs
                .iter()
                .find_map(|pair| big(&pair.input).map(|ty| (&pair.input, ty))),
        };
        if let Some((name, column_type)) = column {
            oxhaven_contracts::unported!(
                "io.deephaven.engine.table.impl.by.AggregationProcessor.RollupReaggregatedConverter",
                "a rollup of a refreshing table that reaggregates {name} of type {column_type:?} ({spec:?})"
            );
        }
    }
}

/// Java's text for what a blink table does not support.
///
/// Behavior: io.deephaven.engine.table.impl.by.AggregationProcessor.Converter#unsupportedForBlinkTables
fn unsupported_for_blink(what: &str) -> HierarchicalError {
    HierarchicalError::Unsupported(format!(
        "Blink tables do not support {what}; use BlinkTableTools.blinkToAppendOnly to \
         accumulate full history"
    ))
}

/// What Java refuses of `aggregation` in a rollup of a blink table, if it does: what
/// needs the rows of a group again at a coarser level (a group, a formula) or the row a
/// first or last came from.
///
/// Behavior: AggregationProcessor.RollupBaseConverter#visit(AggSpecGroup), #visit(Formula),
/// and Converter#addFirstOrLastOperators and #addSortedFirstOrLastOperator with a
/// redirection to expose
fn refused_for_blink(aggregation: &Aggregation) -> Option<&'static str> {
    match aggregation {
        Aggregation::Columns { spec, .. } => match spec {
            AggSpec::First => Some(
                "AggFirst with exposed row redirections (e.g. for rollup(), AggFirstRowKey, or \
                 AggLastRowKey)",
            ),
            AggSpec::Last => Some(
                "AggLast with exposed row redirections (e.g. for rollup(), AggFirstRowKey, or \
                 AggLastRowKey)",
            ),
            AggSpec::SortedFirst { .. } => {
                Some("AggSortedFirst with exposed row redirections (e.g. for rollup())")
            }
            AggSpec::SortedLast { .. } => {
                Some("AggSortedLast with exposed row redirections (e.g. for rollup())")
            }
            AggSpec::Group => Some("AggGroup for rollup"),
            _ => None,
        },
        Aggregation::Formula { .. } => Some("AggFormula for rollup"),
        _ => None,
    }
}

/// What Java refuses of a rollup before it aggregates, in the order it finds it: a blink
/// table with constituents, a partition aggregation, then per aggregation in their order
/// one that has no rollup, or none of a blink table.
///
/// Java finds a refusal of its walk among the other errors of that aggregation's place:
/// an unknown column, or a type without the aggregation, in an earlier aggregation comes
/// first there and after the refusal here. Only the text differs (PARITY A5).
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#rollup,
/// AggregationProcessor#forRollupBase and the walk of RollupBaseConverter
fn check_refusals(
    source: &Arc<dyn Table>,
    aggregations: &[Aggregation],
    include_constituents: bool,
) -> Result<(), HierarchicalError> {
    let blink = source.attributes().blink;
    if blink && include_constituents {
        return Err(unsupported_for_blink("rollup with included constituents"));
    }
    if aggregations
        .iter()
        .any(|aggregation| matches!(aggregation, Aggregation::Partition { .. }))
    {
        return Err(HierarchicalError::Unsupported(
            "AggPartition is not supported for rollup()".to_string(),
        ));
    }
    for aggregation in aggregations {
        if let Some(name) = refused_by_java(aggregation) {
            return Err(HierarchicalError::Unsupported(format!(
                "Agg{name} is not supported for rollup()"
            )));
        }
        if let Some(what) = refused_for_blink(aggregation).filter(|_| blink) {
            return Err(unsupported_for_blink(what));
        }
    }
    Ok(())
}

/// Refuses as not ported a rollup with two group aggregations, or with a group aggregation
/// and a formula aggregation: Java shares one group operator between them, at every level,
/// and whether it answers depends on their order and names. Observed on Java 42.9, by `K`
/// and `J`: `group("V"), group("D")` and `group("G = V"), group("V")` serve,
/// `group("G = D"), group("H = V")` fails with a `NullPointerException`,
/// `formula("S = sum(V)"), group("V")` serves and `group("G = V"), formula("S = sum(V)")`
/// fails with a `NoSuchColumnException` for `V_GRP___ROLLUP__`.
///
/// Behavior: AggregationProcessor.Converter#ensureGroupByOperator and
/// #ensureGroupByReaggregateOperator
fn check_group_users(aggregations: &[Aggregation]) {
    let groups = aggregations
        .iter()
        .filter(|aggregation| {
            matches!(
                aggregation,
                Aggregation::Columns {
                    spec: AggSpec::Group,
                    ..
                }
            )
        })
        .count();
    let formulas = aggregations
        .iter()
        .any(|aggregation| matches!(aggregation, Aggregation::Formula { .. }));
    if groups > 1 {
        oxhaven_contracts::unported!(
            "io.deephaven.engine.table.impl.by.AggregationProcessor.Converter#ensureGroupByOperator",
            "two group aggregations in one rollup, which Java serves or fails depending on their names"
        );
    }
    if groups == 1 && formulas {
        oxhaven_contracts::unported!(
            "io.deephaven.engine.table.impl.by.AggregationProcessor.Converter#ensureGroupByOperator",
            "a group and a formula aggregation in one rollup, which Java serves or fails depending on their order"
        );
    }
}

/// The name of the number of group-by columns of a level, which a formula aggregation in
/// a rollup may read.
///
/// Behavior: AggregationProcessor#ROLLUP_FORMULA_DEPTH
const FORMULA_DEPTH: &str = "__FORMULA_DEPTH__";

/// The name of the names of the group-by columns of a level, which a formula aggregation
/// in a rollup may read.
///
/// Behavior: AggregationProcessor#ROLLUP_FORMULA_KEYS
const FORMULA_KEYS: &str = "__FORMULA_KEYS__";

/// What a formula aggregation of the level grouped by `keys` reads besides the source's
/// columns: an `int` and a vector of strings, which take the place of a source column of
/// their name.
///
/// Behavior: AggregationProcessor#EXTRA_ROLLUP_FORMULA_DEFINITIONS, and the depth and key
/// name sources that RollupBaseConverter#visit(Formula) and
/// RollupReaggregatedConverter#visit(Formula) give FormulaMultiColumnChunkedOperator
fn formula_constants(keys: &[&str]) -> Vec<FormulaConstant> {
    let names = ColumnType::vector(ColumnType::String);
    let DataType::List(field) = names.arrow_type() else {
        unreachable!("a vector is an Arrow list");
    };
    let values = StringArray::from_iter_values(keys.iter().copied());
    let list = ListArray::new(
        field,
        OffsetBuffer::from_lengths([keys.len()]),
        Arc::new(values),
        None,
    );
    vec![
        FormulaConstant {
            name: FORMULA_DEPTH.to_string(),
            column_type: ColumnType::Int,
            value: Arc::new(Int32Array::from(vec![keys.len() as i32])),
        },
        FormulaConstant {
            name: FORMULA_KEYS.to_string(),
            column_type: names,
            value: Arc::new(list),
        },
    ]
}

/// The columns a formula aggregation of the level grouped by `keys` is compiled against:
/// the source's, as a vector where it is not grouped by, and the constants of the level.
///
/// Behavior: AggregationProcessor#maybeInitializeVectorColumns
fn formula_columns(source: &TableDefinition, keys: &[&str]) -> Vec<(String, ColumnType)> {
    let mut columns: Vec<(String, ColumnType)> = source
        .columns()
        .iter()
        .filter(|column| column.name != FORMULA_DEPTH && column.name != FORMULA_KEYS)
        .map(|column| {
            let seen = if keys.contains(&column.name.as_str()) {
                column.column_type.clone()
            } else {
                ColumnType::vector(column.column_type.clone())
            };
            (column.name.clone(), seen)
        })
        .collect();
    for constant in formula_constants(keys) {
        columns.push((constant.name, constant.column_type));
    }
    columns
}

/// What Java refuses of the formula aggregations of a rollup with group-by columns.
///
/// Java makes a coarser level from the rows of the level below it, which keeps, of the
/// columns the formulas read, those that the first formula reads and that are not grouped
/// by there. A formula at a coarser level that reads another column fails there: every
/// formula that reads a group-by column, at the level that stops grouping by it, and
/// every formula after the first that reads a column the first does not. Java checks
/// first that the formula's result has the type it has one level below, which fails only
/// for such a formula; then it fails looking for the column in the level below by an
/// internal name (`NoSuchColumnException` for `J_GRP___ROLLUP__`), which is refused here
/// as not ported. The levels are made from the finest up, the formulas in their order.
///
/// Behavior: AggregationProcessor.RollupReaggregatedConverter#visit(Formula), and
/// io.deephaven.engine.table.impl.by.GroupByReaggregateOperator#GroupByReaggregateOperator
fn check_formulas(
    source: &Arc<dyn Table>,
    aggregations: &[Aggregation],
    group_by: &[&str],
    compiler: Option<&dyn FormulaCompiler>,
) -> Result<(), HierarchicalError> {
    let formulas: Vec<(&str, &str)> = aggregations
        .iter()
        .filter_map(|aggregation| match aggregation {
            Aggregation::Formula { column, expression } => {
                Some((column.as_str(), expression.as_str()))
            }
            _ => None,
        })
        .collect();
    let (Some(compiler), Some(first)) = (compiler, formulas.first()) else {
        return Ok(());
    };
    let definition = source.definition();
    let compiled = |expression: &str, keys: &[&str]| {
        let columns = formula_columns(definition, keys);
        let columns: Vec<(&str, ColumnType)> = columns
            .iter()
            .map(|(name, column_type)| (name.as_str(), column_type.clone()))
            .collect();
        ArrayFormula::compile(compiler, expression, &columns).map_err(|error| {
            HierarchicalError::Aggregate(AggregateError::InvalidFormula(error.to_string()))
        })
    };
    // The source columns a formula reads at the finest level. One that does not compile
    // there fails when the levels are made, as in Java, which makes the finest first.
    let read = |expression: &str| -> Option<Vec<String>> {
        let formula = compiled(expression, group_by).ok()?;
        Some(
            formula
                .inputs_read()
                .iter()
                .filter(|name| name.as_str() != FORMULA_DEPTH && name.as_str() != FORMULA_KEYS)
                .cloned()
                .collect(),
        )
    };
    let Some(first_read) = read(first.1) else {
        return Ok(());
    };
    let mut reads = Vec::with_capacity(formulas.len());
    for &(_, expression) in &formulas {
        let Some(inputs) = read(expression) else {
            return Ok(());
        };
        reads.push(inputs);
    }
    for level in (0..group_by.len()).rev() {
        let keys = &group_by[..level];
        let below = &group_by[..=level];
        for (&(name, expression), inputs) in formulas.iter().zip(&reads) {
            let kept = |input: &String| {
                keys.contains(&input.as_str())
                    || (first_read.contains(input) && !below.contains(&input.as_str()))
            };
            if inputs.iter().all(kept) {
                continue;
            }
            let here = compiled(expression, keys)?.result_type().clone();
            let before = compiled(expression, below)?.result_type().clone();
            if here != before {
                let (was, is) = (java_class(&before), java_class(&here));
                let component = |column_type: &ColumnType| {
                    column_type
                        .list_element()
                        .map_or("null".to_string(), java_class)
                };
                return Err(HierarchicalError::IllegalArgument(if was != is {
                    format!(
                        "Inconsistent return type in rollup for Formula column '{name}': \
                         previous level was {was}, but level {level} is {is}"
                    )
                } else {
                    format!(
                        "Inconsistent return component type in rollup for Formula column \
                         '{name}': previous level was {}, but level {level} is {}",
                        component(&before),
                        component(&here)
                    )
                }));
            }
            oxhaven_contracts::unported!(
                "io.deephaven.engine.table.impl.by.GroupByReaggregateOperator#GroupByReaggregateOperator",
                "a formula aggregation in a rollup that reads a group-by column, or a column that \
                 the first formula does not read, which Java fails with a NoSuchColumnException \
                 for an internal column"
            );
        }
    }
    Ok(())
}

/// `source` rolled up by `group_by` with `aggregations`. `compiler` compiles what the
/// aggregations hold of formulas.
///
/// Behavior: io.deephaven.engine.table.impl.QueryTable#rollup,
/// RollupTableImpl#makeRollup
pub fn rollup(
    source: &Arc<dyn Table>,
    aggregations: &[Aggregation],
    include_constituents: bool,
    group_by: &[&str],
    compiler: Option<&dyn FormulaCompiler>,
) -> Result<RollupTable, HierarchicalError> {
    check_refusals(source, aggregations, include_constituents)?;
    check_group_users(aggregations);
    if !group_by.is_empty() {
        check_reaggregation(aggregations, group_by)?;
        if source.is_refreshing() {
            check_big_numbers(source.as_ref(), aggregations);
        }
        check_formulas(source, aggregations, group_by, compiler)?;
    }
    let finest = build_levels(
        source,
        aggregations,
        group_by,
        include_constituents,
        compiler,
    )?;
    // Behavior: RollupTableImpl#makeRollup, which copies the restriction of the source's
    // sorting for the columns the rollup has
    let sortable = sortable_columns(source.as_ref()).map(|names| {
        names
            .into_iter()
            .filter(|name| finest.1.index_of(name).is_some())
            .collect()
    });
    Ok(RollupTable {
        source: source.clone(),
        base: source.clone(),
        aggregations: aggregations.to_vec(),
        group_by: group_by.iter().map(|name| name.to_string()).collect(),
        include_constituents,
        levels: Arc::new(finest.0),
        columns: snapshot_columns(&finest.1, include_constituents.then(|| source.definition())),
        aggregated: finest.1,
        sorts: Vec::new(),
        constituent_sorts: Vec::new(),
        sortable,
        constituent: include_constituents.then(|| source.definition().clone()),
        aggregated_columns: NodeColumns::default(),
        constituent_columns: NodeColumns::default(),
    })
}

/// The columns of a snapshot: the structural columns and those of the aggregated nodes,
/// then those of the constituents.
///
/// Behavior: RollupTableImpl#computeAvailableColumnDefinitions
fn snapshot_columns(
    aggregated: &TableDefinition,
    constituents: Option<&TableDefinition>,
) -> Vec<ColumnDefinition> {
    let mut columns = aggregated.columns().to_vec();
    if let Some(constituents) = constituents {
        columns.extend(constituents.columns().iter().cloned());
    }
    columns
}

/// The levels of `base` rolled up by `group_by`, and the structural columns with the
/// columns of the aggregated nodes. With `include_constituents` the rows of `base` are
/// the children of the rows of the last level.
///
/// Behavior: RollupTableImpl#makeRollupInternal; for the constituents,
/// AggregationProcessor#forRollupBase, which adds a partition of the source by the
/// group-by columns to the aggregations of the last level
fn build_levels(
    base: &Arc<dyn Table>,
    aggregations: &[Aggregation],
    group_by: &[&str],
    include_constituents: bool,
    compiler: Option<&dyn FormulaCompiler>,
) -> Result<(Levels, TableDefinition), HierarchicalError> {
    let source = base;
    // A rollup without aggregations has the group-by columns alone: its levels are counted,
    // which an aggregation without aggregations cannot be, and the count is left out.
    let mut hidden = "__ROLLUP_ROWS__".to_string();
    while source.definition().index_of(&hidden).is_some() {
        hidden.push('_');
    }
    let counted = [Aggregation::count(hidden.clone())];
    let (aggregated, hidden) = if aggregations.is_empty() {
        (&counted[..], Some(hidden))
    } else {
        (aggregations, None)
    };
    let formulas = aggregations
        .iter()
        .any(|aggregation| matches!(aggregation, Aggregation::Formula { .. }));
    let level = |keys: &[&str]| -> Result<Arc<dyn Table>, HierarchicalError> {
        let constants = if formulas {
            formula_constants(keys)
        } else {
            Vec::new()
        };
        let level = aggregate_with_constants(source, aggregated, keys, compiler, &constants)?;
        match &hidden {
            Some(hidden) => drop_columns(&level, &[hidden])
                .map_err(|error| HierarchicalError::IllegalArgument(error.to_string())),
            None => Ok(level),
        }
    };
    let levels = (0..=group_by.len())
        .map(|depth| level(&group_by[..depth]))
        .collect::<Result<Vec<_>, _>>()?;
    let indexes = levels
        .iter()
        .enumerate()
        .map(|(depth, level)| KeyIndex::new(level.clone(), (0..depth).collect()))
        .collect();
    let mut children = Vec::with_capacity(levels.len());
    for depth in 0..levels.len() {
        let last = depth + 1 == levels.len();
        children.push(if depth == 0 || (last && !include_constituents) {
            None
        } else {
            let below = if last { source } else { &levels[depth + 1] };
            let partitioned = partition_by(below, &group_by[..depth], false)?;
            let index = KeyIndex::new(partitioned.table().clone(), (0..depth).collect());
            Some(ChildParts { partitioned, index })
        });
    }
    // The node columns are those of the finest level: the group-by columns, then the
    // results. Behavior: RollupTableImpl#computeAvailableColumnDefinitions
    let finest = levels[group_by.len()].definition();
    let mut columns = vec![
        ColumnDefinition::new(DEPTH_COLUMN_NAME, ColumnType::Int),
        ColumnDefinition::new(EXPANDED_COLUMN_NAME, ColumnType::Boolean),
    ];
    columns.extend(finest.columns().iter().cloned());
    let columns = TableDefinition::new(columns).map_err(|duplicate| {
        HierarchicalError::IllegalArgument(format!("Duplicate column name {}", duplicate.0))
    })?;
    Ok((
        Levels {
            tables: levels,
            indexes,
            children,
            constituents: include_constituents.then(|| source.clone()),
        },
        columns,
    ))
}

impl RollupTable {
    /// Behavior: RollupTable#getAggregations
    pub fn aggregations(&self) -> &[Aggregation] {
        &self.aggregations
    }

    /// Behavior: RollupTable#getGroupByColumns
    pub fn group_by(&self) -> &[String] {
        &self.group_by
    }

    /// Behavior: RollupTable#includesConstituents
    pub fn includes_constituents(&self) -> bool {
        self.include_constituents
    }

    /// The depth, whether a row is expanded, and the columns of the aggregated nodes:
    /// the group-by columns, then the results.
    ///
    /// Behavior: RollupTable#getNodeDefinition(Aggregated), after the structural columns
    pub fn aggregated_definition(&self) -> &TableDefinition {
        &self.aggregated
    }

    /// The columns of the constituent nodes, which are the source's, in a rollup that has
    /// them.
    ///
    /// Behavior: RollupTable#getNodeDefinition(Constituent)
    pub fn constituent_definition(&self) -> Option<&TableDefinition> {
        self.constituent.as_ref()
    }

    /// The columns of the nodes of one type, without the structural ones: of the
    /// constituent nodes with `constituents`, which the rollup must have, of the
    /// aggregated ones without.
    fn node_columns(&self, constituents: bool) -> Result<&[ColumnDefinition], HierarchicalError> {
        if constituents {
            self.assert_includes_constituents()?;
            Ok(self
                .constituent
                .as_ref()
                .map_or(&[], |definition| definition.columns()))
        } else {
            Ok(&self.aggregated.columns()[2..])
        }
    }

    /// A recorder of operations on the nodes of one type, with nothing recorded.
    ///
    /// Behavior: RollupTableImpl#makeNodeOperationsRecorder
    pub fn node_operations(&self, constituents: bool) -> Result<NodeOperations, HierarchicalError> {
        Ok(NodeOperations::new(
            self.node_columns(constituents)?.to_vec(),
        ))
    }

    /// Whether the nodes that `operations` was recorded for can be sorted by `sorts`, as
    /// [`RollupTable::check_sorts`] says it of the nodes of this rollup: a column an
    /// update view of `operations` adds can be sorted by, a formatting column cannot.
    ///
    /// Behavior: BaseNodeOperationsRecorder.SortRecordingTableAdapter#sort, on the
    /// recorder's result definition
    pub fn check_recorded_sorts(
        &self,
        constituents: bool,
        operations: &NodeOperations,
        sorts: &[SortColumn],
    ) -> Result<(), HierarchicalError> {
        let existing = if constituents {
            &self.constituent_columns
        } else {
            &self.aggregated_columns
        };
        if !operations.adds_columns() && existing.is_empty() {
            return if constituents {
                self.check_constituent_sorts(sorts)
            } else {
                self.check_sorts(sorts)
            };
        }
        let visible = operations.visible_names();
        let missing: Vec<&str> = sorts
            .iter()
            .map(|sort| sort.column.as_str())
            .filter(|name| !visible.contains(name))
            .collect();
        if !missing.is_empty() {
            return Err(HierarchicalError::IllegalArgument(format!(
                "Unknown column names [{}], available column names are [{}]",
                missing.join(", "),
                visible.join(", ")
            )));
        }
        let base = if constituents {
            self.assert_includes_constituents()?;
            &self.base
        } else {
            &self.levels.tables[self.group_by.len()]
        };
        let columns = existing.with(operations);
        let table = node_table(base, &[], &columns)?;
        check_sort_columns(table.as_ref(), sorts).map_err(|error| match error {
            SortError::Table(failure) => HierarchicalError::Table(failure),
            other => HierarchicalError::IllegalArgument(other.to_string()),
        })
    }

    /// This rollup with what `operations` recorded for the nodes of one type: its update
    /// views and formats after those the nodes have, and its sorts in front of theirs.
    /// The rollup itself when nothing was recorded.
    ///
    /// Java keeps one recorder per node type and merges a second into it only when both
    /// give the nodes the same columns. So operations that add a column are refused for
    /// nodes that already have operations of any kind, sorts included, and accepted for
    /// nodes that have none; operations that add none are always accepted.
    ///
    /// Behavior: RollupTableImpl#withNodeOperations and #accumulateOperations, which is
    /// RollupNodeOperationsRecorder#withOperations
    pub fn with_node_operations(
        &self,
        constituents: bool,
        operations: &NodeOperations,
    ) -> Result<RollupTable, HierarchicalError> {
        if operations.is_empty() {
            return Ok(self.clone());
        }
        let current = self.node_columns(constituents)?;
        let (sorts, columns) = if constituents {
            (&self.constituent_sorts, &self.constituent_columns)
        } else {
            (&self.sorts, &self.aggregated_columns)
        };
        let operated = !sorts.is_empty() || !columns.is_empty();
        if operated && operations.definition() != current {
            return Err(HierarchicalError::IllegalArgument(
                "Incompatible operation recorders; compatible recorders must be created from \
                 the same table, with the same node type"
                    .to_string(),
            ));
        }
        let duplicate = |error| HierarchicalError::IllegalArgument(format!("{error:?}"));
        let mut result = self.clone();
        let merged = merged_sorts(operations.sorts(), sorts);
        let columns = columns.with(operations);
        if constituents {
            if !operated {
                result.constituent = Some(
                    TableDefinition::new(operations.definition().to_vec()).map_err(duplicate)?,
                );
            }
            result.constituent_sorts = merged;
            result.constituent_columns = columns;
        } else {
            if !operated {
                let mut all = self.aggregated.columns()[..2].to_vec();
                all.extend(operations.definition().iter().cloned());
                result.aggregated = TableDefinition::new(all).map_err(duplicate)?;
            }
            result.sorts = merged;
            result.aggregated_columns = columns;
        }
        result.columns = snapshot_columns(&result.aggregated, result.constituent.as_ref());
        Ok(result)
    }

    /// What the rows of every aggregated node are sorted by, the column that decides
    /// first.
    pub fn sorts(&self) -> &[SortColumn] {
        &self.sorts
    }

    /// What the rows of the source under a finest group are sorted by.
    pub fn constituent_sorts(&self) -> &[SortColumn] {
        &self.constituent_sorts
    }

    /// The columns a client may sort by, or `None` when it may sort by any: the columns
    /// the source's sorting was restricted to, of those the rollup has.
    ///
    /// Behavior: io.deephaven.engine.table.impl.BaseGridAttributes#getSortableColumns
    pub fn sortable_columns(&self) -> Option<&[String]> {
        self.sortable.as_deref()
    }

    /// Whether the nodes of this rollup can be sorted by `sorts`: its nodes have the
    /// columns, of types that sort, and the absolute ones of types that have an absolute
    /// value.
    ///
    /// # Panics
    /// With `unported!` for a column of a type the engine's sort does not have, such as
    /// the vectors of a distinct aggregation.
    ///
    /// Behavior: io.deephaven.engine.table.impl.hierarchical.BaseNodeOperationsRecorder.SortRecordingTableAdapter#sort
    /// and #absoluteSelectColumns
    pub fn check_sorts(&self, sorts: &[SortColumn]) -> Result<(), HierarchicalError> {
        if !self.aggregated_columns.is_empty() {
            return self.check_recorded_sorts(false, &self.node_operations(false)?, sorts);
        }
        let finest = &self.levels.tables[self.group_by.len()];
        check_sort_columns(finest.as_ref(), sorts).map_err(|error| match error {
            SortError::NoSuchColumn(missing) => {
                let available: Vec<&str> = self.aggregated.columns()[2..]
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect();
                HierarchicalError::IllegalArgument(format!(
                    "Unknown column names [{}], available column names are [{}]",
                    missing.join(", "),
                    available.join(", ")
                ))
            }
            SortError::Table(failure) => HierarchicalError::Table(failure),
            other => HierarchicalError::IllegalArgument(other.to_string()),
        })
    }

    /// This rollup with the rows of every node sorted by `sorts`, and where those leave
    /// rows equal by what they were sorted by before. A column named again, absolute or
    /// not as before, is taken where it is first named. The sorts are not checked:
    /// [`RollupTable::check_sorts`] does that, for the rollup they were recorded for.
    ///
    /// Behavior: RollupTableImpl#withNodeOperations with the sorts of a recorder for
    /// aggregated nodes, and BaseNodeOperationsRecorder#mergeSortColumns
    pub fn with_sorts(&self, sorts: &[SortColumn]) -> RollupTable {
        RollupTable {
            sorts: merged_sorts(sorts, &self.sorts),
            ..self.clone()
        }
    }

    /// Whether the rows of the source under a finest group can be sorted by `sorts`, as
    /// [`RollupTable::check_sorts`] says it of the aggregated nodes: the rollup has
    /// constituents, and the source has the columns, of types that sort.
    ///
    /// Behavior: RollupTableImpl#makeNodeOperationsRecorder(Constituent), which is
    /// #assertIncludesConstituents, and
    /// BaseNodeOperationsRecorder.SortRecordingTableAdapter#sort
    pub fn check_constituent_sorts(&self, sorts: &[SortColumn]) -> Result<(), HierarchicalError> {
        self.assert_includes_constituents()?;
        if !self.constituent_columns.is_empty() {
            return self.check_recorded_sorts(true, &self.node_operations(true)?, sorts);
        }
        check_sort_columns(self.base.as_ref(), sorts).map_err(|error| match error {
            SortError::NoSuchColumn(missing) => {
                let definition = self.base.definition();
                let available: Vec<&str> = definition
                    .columns()
                    .iter()
                    .map(|column| column.name.as_str())
                    .collect();
                HierarchicalError::IllegalArgument(format!(
                    "Unknown column names [{}], available column names are [{}]",
                    missing.join(", "),
                    available.join(", ")
                ))
            }
            SortError::Table(failure) => HierarchicalError::Table(failure),
            other => HierarchicalError::IllegalArgument(other.to_string()),
        })
    }

    /// Behavior: RollupTableImpl#assertIncludesConstituents
    pub fn assert_includes_constituents(&self) -> Result<(), HierarchicalError> {
        if self.include_constituents {
            Ok(())
        } else {
            Err(HierarchicalError::IllegalArgument(
                "Rollup does not have constituent nodes".to_string(),
            ))
        }
    }

    /// This rollup with the rows of the source under every finest group sorted by
    /// `sorts`, and where those leave rows equal by what they were sorted by before, as
    /// [`RollupTable::with_sorts`] sorts the aggregated nodes. The sorts are not checked:
    /// [`RollupTable::check_constituent_sorts`] does that.
    ///
    /// Behavior: RollupTableImpl#withNodeOperations with the sorts of a recorder for
    /// constituent nodes
    pub fn with_constituent_sorts(&self, sorts: &[SortColumn]) -> RollupTable {
        RollupTable {
            constituent_sorts: merged_sorts(sorts, &self.constituent_sorts),
            ..self.clone()
        }
    }

    /// What sorts the constituent nodes as `sorts` sort the aggregated ones: each result
    /// of an aggregation becomes the column it was aggregated from. A group-by column is
    /// left out, since the rows under one group are equal by it, and so is a result that
    /// has no input, a count. An absolute value stays one where the input is a number.
    ///
    /// Behavior: RollupTableImpl#translateAggregatedNodeOperationsForConstituentNodes,
    /// which is #translateSorts
    pub fn constituent_sorts_of(&self, sorts: &[SortColumn]) -> Vec<SortColumn> {
        let definition = self.base.definition();
        sorts
            .iter()
            .filter(|sort| !self.group_by.contains(&sort.column))
            .filter_map(|sort| {
                // no two results of a rollup have one name
                let input = self.aggregations.iter().find_map(|aggregation| {
                    let Aggregation::Columns { pairs, .. } = aggregation else {
                        return None;
                    };
                    let pair = pairs.iter().find(|pair| pair.output == sort.column)?;
                    Some(pair.input.as_str())
                })?;
                let number = definition.index_of(input).is_some_and(|index| {
                    matches!(
                        definition.columns()[index].column_type,
                        ColumnType::Byte
                            | ColumnType::Short
                            | ColumnType::Int
                            | ColumnType::Long
                            | ColumnType::Float
                            | ColumnType::Double
                    )
                });
                Some(SortColumn {
                    column: input.to_string(),
                    absolute: sort.absolute && number,
                    ..sort.clone()
                })
            })
            .collect()
    }

    /// Why `filters`, made for the columns of the source, may not filter this rollup, if
    /// they may not: a filter reads a column that an aggregation reads, or a column at
    /// other rows.
    ///
    /// Behavior: RollupTableImpl#initializeAndValidateFilters
    pub fn check_filters(&self, filters: &CheckedFilters) -> Result<(), String> {
        let aggregated = |name: &str| {
            self.aggregations
                .iter()
                .any(|aggregation| match aggregation {
                    Aggregation::Columns { pairs, .. } => {
                        pairs.iter().any(|pair| pair.input == name)
                    }
                    _ => false,
                })
        };
        let valid: Vec<&str> = self
            .source
            .definition()
            .columns()
            .iter()
            .map(|column| column.name.as_str())
            .filter(|name| !aggregated(name))
            .collect();
        for filter in filters.reads() {
            let invalid: Vec<&str> = filter
                .columns
                .iter()
                .map(String::as_str)
                .filter(|name| !valid.contains(name))
                .collect();
            if !invalid.is_empty() {
                return Err(format!(
                    "Invalid filter found: {} may only use non-aggregation columns, which are \
                     [{}], but has used [{}]",
                    filter.text,
                    valid.join(", "),
                    invalid.join(", ")
                ));
            }
            if filter.arrays {
                return Err(format!(
                    "Invalid filter found: {} may not use column arrays",
                    filter.text
                ));
            }
        }
        Ok(())
    }

    /// This rollup of the rows of the source that every filter accepts. The filters are
    /// made for the columns of the source ([`CheckedFilters`]) and may read every column
    /// that no aggregation reads. With filters that read group-by columns only the source
    /// stays the source; otherwise the filtered table becomes it. The sorts are kept.
    ///
    /// Java filters the finest level when the filters read group-by columns only, and
    /// aggregates the coarser levels from that; a group is wholly kept or wholly left
    /// out, so aggregating the filtered source gives the same levels in the same order.
    ///
    /// # Panics
    /// With `unported!` for a filter that uses `i`, `ii` or `k`: Java accepts one as a
    /// filter of group-by columns, and it then counts the rows of the finest level.
    ///
    /// Behavior: RollupTableImpl#withFilter and #rebase
    pub fn with_filters(
        &self,
        filters: &CheckedFilters,
        compiler: &dyn FormulaCompiler,
    ) -> Result<RollupTable, HierarchicalError> {
        self.check_filters(filters)
            .map_err(HierarchicalError::IllegalArgument)?;
        if filters.reads().iter().any(|filter| filter.row_variables) {
            oxhaven_contracts::unported!(
                "io.deephaven.engine.table.impl.hierarchical.RollupTableImpl#withFilter",
                "a filter of a rollup that uses i, ii or k, which Java evaluates on the rows of the finest level"
            );
        }
        let base = filters
            .apply(&self.base, compiler)
            .map_err(|error| match error {
                FilterError::Invalid(message) => HierarchicalError::IllegalArgument(message),
                FilterError::Failed(failure) => HierarchicalError::Table(failure),
            })?;
        let only_group_by = filters.reads().iter().all(|filter| {
            filter
                .columns
                .iter()
                .all(|name| self.group_by.contains(name))
        });
        let group_by: Vec<&str> = self.group_by.iter().map(String::as_str).collect();
        let (levels, aggregated) = build_levels(
            &base,
            &self.aggregations,
            &group_by,
            self.include_constituents,
            Some(compiler),
        )?;
        // the columns node operations added stay where they are, after those of the levels
        let mut all = aggregated.columns().to_vec();
        all.extend(self.aggregated.columns()[all.len()..].iter().cloned());
        let aggregated = TableDefinition::new(all)
            .map_err(|error| HierarchicalError::IllegalArgument(format!("{error:?}")))?;
        Ok(RollupTable {
            source: if only_group_by {
                self.source.clone()
            } else {
                base.clone()
            },
            base,
            levels: Arc::new(levels),
            aggregated,
            ..self.clone()
        })
    }

    /// The source aggregated by the first `depth` group-by columns.
    pub fn level(&self, depth: usize) -> Option<&Arc<dyn Table>> {
        self.levels.tables.get(depth)
    }

    /// The table of the child rows of `node` as the levels have it.
    ///
    /// Behavior: RollupTableImpl#nodeIdToNodeBaseTable
    fn base_children(&self, node: NodeId) -> Result<Option<Arc<dyn Table>>, HierarchicalError> {
        let depth = node.depth as usize;
        if depth == 1 && self.group_by.is_empty() {
            // the grand total is the last level: its children are the rows of the source
            return Ok(self.levels.constituents.clone());
        }
        if depth <= 1 {
            // the root's child is the grand total, whose children are all of level 1
            return Ok(self.levels.tables.get(depth).cloned());
        }
        let Some(Some(parts)) = self.levels.children.get(depth - 1) else {
            return Ok(None);
        };
        let key = self.key_of(depth - 1, node.row)?;
        let Some(row) = parts.index.find(&key)? else {
            return Ok(None);
        };
        let underlying = parts.partitioned.table();
        let cell = underlying
            .definition()
            .index_of(parts.partitioned.constituent_column())
            .ok_or_else(|| super::failure("a partition without its constituent column"))?;
        let handles = underlying
            .column(cell)
            .chunk(&RowSet::from_range(row, row))?;
        Ok(tables_of(&handles)?.pop().flatten())
    }

    /// The key values of row `row` of level `width`, which has `width` key columns.
    fn key_of(&self, width: usize, row: i64) -> Result<Vec<Part>, HierarchicalError> {
        let rows = RowSet::from_range(row, row);
        let level = &self.levels.tables[width];
        (0..width)
            .map(|column| {
                let mut values = parts(&level.column(column).chunk(&rows)?)?;
                values
                    .pop()
                    .ok_or_else(|| super::failure("a rollup level has no such row").into())
            })
            .collect()
    }
}

impl Hierarchy for RollupTable {
    fn source(&self) -> &Arc<dyn Table> {
        &self.source
    }

    fn columns(&self) -> &[ColumnDefinition] {
        &self.columns
    }

    /// Behavior: RollupTableImpl#getDefaultExpansionNodeKeys
    fn default_expansions(&self) -> Vec<NodeKey> {
        if self.group_by.is_empty() && !self.include_constituents {
            vec![NodeKey::Root]
        } else {
            vec![NodeKey::Root, NodeKey::Path(Vec::new())]
        }
    }

    /// Behavior: io.deephaven.engine.table.impl.hierarchical.RollupNodeKeySource#fillFromGroupByValues,
    /// with the columns RollupTableImpl#makeNodeKeySource takes from the key table
    fn node_keys(&self, key_table: &dyn Table) -> Result<Vec<NodeKey>, HierarchicalError> {
        let definition = key_table.definition();
        let column = |name: &str, column_type: &ColumnType| {
            definition
                .index_of(name)
                .filter(|&index| &definition.columns()[index].column_type == column_type)
                .ok_or_else(|| {
                    HierarchicalError::IllegalArgument(format!(
                        "The key table has no column {name} of the type the rollup has"
                    ))
                })
        };
        let rows = key_table.row_set();
        let depth_column = column(DEPTH_COLUMN_NAME, &ColumnType::Int)?;
        // Java asks the key table for every group-by column, of the rollup's type.
        // Behavior: RollupTableImpl#makeNodeKeySource
        let key_columns = self
            .group_by
            .iter()
            .enumerate()
            .map(|(index, name)| column(name, &self.aggregated.columns()[2 + index].column_type))
            .collect::<Result<Vec<_>, _>>()?;
        let depths = key_table.column(depth_column).chunk(&rows)?;
        let depths = depths
            .as_primitive_opt::<Int32Type>()
            .ok_or_else(|| super::failure("the depth column of a key table is not int"))?;
        let max_depth = self.group_by.len() as i64 + 1;
        // Java reads the key columns of the widest key only.
        let widest = depths
            .iter()
            .map(|depth| i64::from(depth.unwrap_or(i32::MIN)) - 1)
            .max()
            .unwrap_or(0)
            .clamp(0, self.group_by.len() as i64) as usize;
        let mut values: Vec<Vec<Part>> = Vec::with_capacity(widest);
        for &key_column in key_columns.iter().take(widest) {
            values.push(parts(&key_table.column(key_column).chunk(&rows)?)?);
        }
        depths
            .iter()
            .enumerate()
            .map(|(row, depth)| {
                // Java's null int is a depth like any other here, and not a valid one
                let depth = i64::from(depth.unwrap_or(i32::MIN));
                if depth < 0 || depth > max_depth {
                    return Err(HierarchicalError::IllegalArgument(format!(
                        "Invalid depth {depth}, maximum for this rollup is {max_depth}"
                    )));
                }
                Ok(if depth == 0 {
                    NodeKey::Root
                } else {
                    NodeKey::Path(
                        values[..depth as usize - 1]
                            .iter()
                            .map(|column| column[row].clone())
                            .collect(),
                    )
                })
            })
            .collect()
    }

    /// Behavior: RollupTableImpl#nodeKeyToNodeId and #findRowKeyInParentUnsorted
    fn locate(&self, key: &NodeKey) -> Result<Option<Located>, HierarchicalError> {
        let NodeKey::Path(key) = key else {
            return Ok(Some(Located {
                node: self.root(),
                row_in_parent: None,
            }));
        };
        // a row of the last level has no children but the constituents
        let deepest = self.group_by.len() + usize::from(self.include_constituents);
        if key.len() >= deepest {
            return Ok(None);
        }
        // A key whose group has no rows now keeps its node, as in Java, whose lookup is
        // the aggregation's and forgets no key.
        Ok(self.levels.indexes[key.len()]
            .known(key)?
            .map(|(row, _)| Located {
                node: NodeId {
                    depth: key.len() as u32 + 1,
                    row,
                },
                row_in_parent: Some(row),
            }))
    }

    /// Behavior: RollupTableImpl#findParentNodeKey
    fn parent(&self, key: &NodeKey) -> Result<Parent, HierarchicalError> {
        Ok(match key {
            NodeKey::Root => Parent::None,
            NodeKey::Path(key) => Parent::Key(match key.split_last() {
                None => NodeKey::Root,
                Some((_, parent)) => NodeKey::Path(parent.to_vec()),
            }),
        })
    }

    fn root(&self) -> NodeId {
        NodeId { depth: 0, row: 0 }
    }

    /// Behavior: RollupTableImpl#nodeIdToNodeBaseTable, and for nodes with update views
    /// or formats #applyNodeFormatsAndFilters
    fn children(&self, node: NodeId) -> Result<Option<Arc<dyn Table>>, HierarchicalError> {
        let Some(base) = self.base_children(node)? else {
            return Ok(None);
        };
        // the children of `node` are at the depth below it
        if node.depth as usize + 1 == self.group_by.len() + 2 {
            if self.constituent_columns.is_empty() {
                return Ok(Some(base));
            }
            return node_table(&base, &[], &self.constituent_columns).map(Some);
        }
        if self.aggregated_columns.is_empty() {
            return Ok(Some(base));
        }
        // a level has null in the group-by columns it does not group by
        let keys = &self.aggregated.columns()[2..2 + self.group_by.len()];
        node_table(&base, keys, &self.aggregated_columns).map(Some)
    }

    /// A coarser level does not have the group-by columns below its own; Java has them
    /// there as columns of nulls, by which every row is equal, so a sort by one is left
    /// out.
    ///
    /// Behavior: RollupTableImpl#applyNodeSorts, which is
    /// BaseNodeOperationsRecorder#applySorts
    fn order(
        &self,
        depth: u32,
        table: &dyn Table,
    ) -> Result<Option<Vec<RowKey>>, HierarchicalError> {
        let sorted = |sorts: &[SortColumn]| {
            sorted_rows(table, &table.row_set(), sorts)
                .map(Some)
                .map_err(|error| match error {
                    SortError::Table(failure) => HierarchicalError::Table(failure),
                    other => HierarchicalError::IllegalArgument(other.to_string()),
                })
        };
        if depth as usize == self.group_by.len() + 2 {
            // rows of the source
            if self.constituent_sorts.is_empty() {
                return Ok(None);
            }
            return sorted(&self.constituent_sorts);
        }
        // the rows at `depth` are rows of level `depth - 1`, which has that many keys
        let keys = (depth as usize).saturating_sub(1);
        let sorts: Vec<SortColumn> = self
            .sorts
            .iter()
            .filter(|sort| {
                self.group_by
                    .iter()
                    .position(|key| *key == sort.column)
                    .is_none_or(|key| key < keys)
            })
            .cloned()
            .collect();
        if sorts.is_empty() {
            // Java sorts by the columns of nulls all the same: the order of the table,
            // and a node that is sorted.
            return Ok((!self.sorts.is_empty()).then(|| table.row_set().iter().collect()));
        }
        sorted(&sorts)
    }

    /// Behavior: RollupTableImpl#levelExpandable
    fn rows_expandable(&self, depth: u32) -> Expandable {
        let deepest = self.group_by.len() + usize::from(self.include_constituents);
        if (depth as usize) <= deepest {
            Expandable::All
        } else {
            Expandable::None
        }
    }

    /// Behavior: RollupTableImpl#makeOrFillChunkSourceArray, with its null sources for the
    /// columns of the other node type, and the null columns of
    /// io.deephaven.engine.table.impl.by.rollup.NullColumns for the keys a level does not
    /// group by
    fn fill(
        &self,
        depth: u32,
        table: &dyn Table,
        rows: &RowSet,
        column: usize,
    ) -> Result<ArrayRef, HierarchicalError> {
        let all_keys = self.group_by.len();
        let null = || {
            let column_type = self.columns[column].column_type.arrow_type();
            Ok(arrow::array::new_null_array(
                &column_type,
                rows.size() as usize,
            ))
        };
        // Rows of the source have its columns and null in those of the aggregated nodes;
        // the others the reverse.
        let first_constituent = self.aggregated.columns().len();
        let constituent_rows = depth as usize == all_keys + 2;
        if (column >= first_constituent) != constituent_rows {
            return null();
        }
        let added = if constituent_rows {
            &self.constituent_columns
        } else {
            &self.aggregated_columns
        };
        if !added.is_empty() {
            // `table` is the node's table with the added columns, each under its name
            let name = &self.columns[column].name;
            let index = table.definition().index_of(name).ok_or_else(|| {
                super::failure(format!("a node of a rollup has no column {name}"))
            })?;
            return Ok(table.column(index).chunk(rows)?);
        }
        if constituent_rows {
            return Ok(table.column(column - first_constituent).chunk(rows)?);
        }
        // the rows at `depth` are rows of level `depth - 1`, which has that many keys
        let keys = depth as usize - 1;
        let node_column = column - 2;
        if node_column < all_keys && node_column >= keys {
            return null();
        }
        let in_level = if node_column < all_keys {
            node_column
        } else {
            node_column - all_keys + keys
        };
        Ok(table.column(in_level).chunk(rows)?)
    }
}

#[cfg(test)]
mod big_number_tests {
    use super::*;
    use crate::graph::SteppedUpdateGraph;
    use crate::ops::agg::Pair;
    use arrow::array::BinaryArray;
    use oxhaven_column::decimal::{self, Decimal};
    use oxhaven_contracts::graph::UpdateGraph;
    use oxhaven_testkit::{definition, ints, strings, ReferenceTable};
    use std::panic::{catch_unwind, AssertUnwindSafe};

    /// Two groups of a `BigDecimal` column `B`, a `BigInteger` column `G` and an int `O`.
    fn source(refreshing: bool) -> Arc<dyn Table> {
        let texts = ["1.0", "2.50", "3", "1.00"];
        let decimals: Vec<Vec<u8>> = texts
            .iter()
            .map(|text| Decimal::parse(text).expect("a decimal").cell())
            .collect();
        let integers: Vec<Vec<u8>> = [1i64, 2, 3, 1]
            .iter()
            .map(|value| decimal::integer_bytes(&(*value).into()))
            .collect();
        let binary = |cells: &[Vec<u8>]| -> ArrayRef {
            Arc::new(
                cells
                    .iter()
                    .map(|cell| Some(cell.as_slice()))
                    .collect::<BinaryArray>(),
            )
        };
        let definition = definition(&[
            ("K", ColumnType::String),
            ("B", ColumnType::BigDecimal),
            ("G", ColumnType::BigInteger),
            ("O", ColumnType::Int),
        ]);
        let columns = vec![
            strings(["k0", "k1", "k0", "k1"]),
            binary(&decimals),
            binary(&integers),
            ints([4, 3, 2, 1]),
        ];
        if refreshing {
            let graph: Arc<dyn UpdateGraph> = SteppedUpdateGraph::new();
            ReferenceTable::new_refreshing(graph, definition, RowSet::flat(4), columns)
        } else {
            ReferenceTable::new_static(definition, RowSet::flat(4), columns)
        }
    }

    /// The panic of the rollup of `source` by `by` with `aggregation`, or `None` when it is
    /// made.
    fn refusal(source: &Arc<dyn Table>, aggregation: Aggregation, by: &[&str]) -> Option<String> {
        let made = catch_unwind(AssertUnwindSafe(|| {
            rollup(source, &[aggregation], false, by, None).is_ok()
        }));
        match made {
            Ok(made) => {
                assert!(made, "the rollup failed with an error");
                None
            }
            Err(panic) => Some(
                panic
                    .downcast::<String>()
                    .map(|text| *text)
                    .unwrap_or_default(),
            ),
        }
    }

    #[test]
    fn a_reaggregation_of_big_numbers_of_a_refreshing_table_is_unported() {
        let refreshing = source(true);
        let computed = [
            AggSpec::Sum,
            AggSpec::AbsSum,
            AggSpec::Avg,
            AggSpec::Var,
            AggSpec::Std,
            AggSpec::Min,
            AggSpec::Max,
            AggSpec::CountDistinct { count_nulls: false },
            AggSpec::Distinct {
                include_nulls: false,
            },
            AggSpec::Unique {
                include_nulls: false,
                non_unique: crate::ops::agg::Sentinel::Null,
            },
        ];
        for spec in computed {
            for input in ["B", "G"] {
                let aggregation = Aggregation::columns(spec.clone(), [Pair::new("R", input)]);
                let message = refusal(&refreshing, aggregation, &["K"])
                    .unwrap_or_else(|| panic!("{spec:?} of {input} was not refused"));
                assert!(
                    message.starts_with(
                        "unported: io.deephaven.engine.table.impl.by.AggregationProcessor.RollupReaggregatedConverter"
                    ),
                    "{spec:?} of {input}: {message}"
                );
            }
        }
        // The sort column is carried, which the check of reaggregation asks of a sorted
        // first or last with group-by columns.
        for column in ["B", "G"] {
            let sorted = || vec![column.to_string()];
            for spec in [
                AggSpec::SortedFirst { columns: sorted() },
                AggSpec::SortedLast { columns: sorted() },
            ] {
                let pairs = [Pair::of("O"), Pair::of(column)];
                let aggregation = Aggregation::columns(spec.clone(), pairs);
                let message = refusal(&refreshing, aggregation, &["K"])
                    .unwrap_or_else(|| panic!("{spec:?} was not refused"));
                assert!(
                    message.starts_with(
                        "unported: io.deephaven.engine.table.impl.by.AggregationProcessor.RollupReaggregatedConverter: "
                    ),
                    "{spec:?}: {message}"
                );
            }
        }
        // What is not reaggregated from the big numbers, or not reaggregated at all, or
        // static, is made.
        let served = [
            (
                Aggregation::columns(AggSpec::First, [Pair::new("R", "B")]),
                true,
                &["K"][..],
            ),
            (
                Aggregation::columns(AggSpec::Last, [Pair::new("R", "G")]),
                true,
                &["K"],
            ),
            (
                Aggregation::columns(AggSpec::Group, [Pair::new("R", "B")]),
                true,
                &["K"],
            ),
            (
                Aggregation::columns(AggSpec::Sum, [Pair::new("R", "O")]),
                true,
                &["K"],
            ),
            (
                Aggregation::columns(AggSpec::Sum, [Pair::new("R", "B")]),
                true,
                &[],
            ),
            (
                Aggregation::columns(AggSpec::Min, [Pair::new("R", "B")]),
                false,
                &["K"],
            ),
        ];
        for (aggregation, refreshing, by) in served {
            let refused = refusal(&source(refreshing), aggregation.clone(), by);
            assert_eq!(refused, None, "{aggregation:?} by {by:?}");
        }
    }
}
