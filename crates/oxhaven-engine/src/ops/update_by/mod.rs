//! `update_by`: columns computed from each row and the rows around it in its bucket.
//!
//! [`update_by`] takes the parent table, a list of [`UpdateByOperation`]s and the names of
//! the bucket columns. The result has the parent's rows, in the parent's order and at the
//! parent's row keys, and the parent's columns followed by one column per operation and
//! input column; a result column named like a parent column replaces it where it stands.
//! With no bucket columns the whole table is one bucket; otherwise the rows with equal
//! values in the bucket columns form one, and every operator runs over the rows of a bucket
//! in table order.
//!
//! | Operation | Ported |
//! | --- | --- |
//! | cumulative sum, product, minimum, maximum; forward fill; delta | yes |
//! | rolling sum, average, minimum, maximum, count, product, over rows or time | yes |
//! | rolling standard deviation and weighted average, over rows or time | yes |
//! | rolling group, over rows or time | yes |
//! | rolling count-where, over rows or time | yes, with a formula compiler |
//! | rolling formula, over rows or time | yes, with a formula compiler |
//! | exponential moving average, sum, minimum, maximum, decaying by ticks or time | yes |
//! | exponential moving standard deviation, decaying by ticks or time | yes |
//! | cumulative count-where | yes, with a formula compiler |
//!
//! One `update_by` takes at most one timestamp column, for all operations in time. Within
//! a bucket its values must not decrease, nulls aside.
//!
//! Java's `UpdateByControl` has no option that changes the result for the column types of
//! this engine (its `mathContext` applies to `BigDecimal` only), so there is no counterpart.
//!
//! # Updates
//!
//! When the parent changes, the result recomputes, per bucket and per window, the rows Java
//! recomputes: for the cumulative operators every row from the first one a change can
//! reach to the end of the bucket; for a rolling window the rows whose window holds an
//! added, removed or modified row. Those rows are reported as modified in the result
//! columns. A row whose bucket columns changed leaves its bucket and joins another, as a
//! removed and an added row.

mod count_where;
mod cumulative;
mod em;
mod formula;
mod group;
mod rolling;
mod values;
mod window;

#[cfg(test)]
mod corpus;
#[cfg(test)]
mod oracle_corpus;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod vector_tests;

use crate::table::{tracked_source, QueryTable, TableSpec};
use crate::update::{modified_pre_shift, ModifiedColumnTransformer};
use arrow::array::UInt32Array;
use arrow::compute::take;
use count_where::CountWhere;
use cumulative::Cumulative;
use em::{Em, EmKind, StdState};
use formula::RollingFormula;
use oxhaven_column::WritableSource;
use oxhaven_contracts::expr::FormulaCompiler;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use rolling::{Rolling, Stamped, Ticks, Time};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex, PoisonError};
use values::Values;

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

/// What Java throws when a minimum or maximum of a Boolean column is given rows.
const BOOLEAN_AS_OBJECTS: &str = "class io.deephaven.chunk.ResettableWritableByteChunk cannot be \
    cast to class io.deephaven.chunk.ObjectChunk";

/// What `delta` gives for a value whose predecessor is null or missing.
///
/// Behavior: io.deephaven.api.updateby.DeltaControl and its `NullBehavior`
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum DeltaControl {
    /// Null. The default.
    #[default]
    NullDominates,
    /// The value itself.
    ValueDominates,
    /// Zero.
    ZeroDominates,
}

/// The length of one side of a window or of an exponential decay.
///
/// Behavior: io.deephaven.api.updateby.spec.WindowScale
#[derive(Clone, PartialEq, Debug)]
pub enum WindowScale {
    /// A number of rows. A rolling window uses the whole part.
    Ticks(f64),
    /// A span of time, measured on a timestamp column.
    Time { column: String, nanos: i64 },
}

/// A rolling window: `rev` reaches back from the current row and includes it, `fwd`
/// reaches forward from the row after it.
///
/// Behavior: io.deephaven.api.updateby.spec.RollingOpSpec
#[derive(Clone, PartialEq, Debug)]
pub struct RollingWindow {
    pub rev: WindowScale,
    pub fwd: WindowScale,
}

impl RollingWindow {
    /// A window of `rev` rows back, the current row included, and `fwd` rows forward.
    pub fn ticks(rev: i64, fwd: i64) -> Self {
        RollingWindow {
            rev: WindowScale::Ticks(rev as f64),
            fwd: WindowScale::Ticks(fwd as f64),
        }
    }
}

/// What an exponential moving operator does with a null or NaN value.
///
/// Behavior: io.deephaven.api.updateby.BadDataBehavior
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum BadDataBehavior {
    Reset,
    Skip,
    Throw,
    Poison,
}

/// The parameters of an exponential moving operator.
///
/// Behavior: io.deephaven.api.updateby.spec.EmaSpec with io.deephaven.api.updateby.OperationControl
#[derive(Clone, PartialEq, Debug)]
pub struct EmSpec {
    pub window: WindowScale,
    pub on_null: Option<BadDataBehavior>,
    pub on_nan: Option<BadDataBehavior>,
}

/// One kind of `update_by` operation, as the gRPC `UpdateBySpec` names them.
///
/// Behavior: io.deephaven.api.updateby.spec.UpdateBySpec and its implementations
#[derive(Clone, PartialEq, Debug)]
pub enum UpdateBySpec {
    CumSum,
    CumMin,
    CumMax,
    CumProd,
    ForwardFill,
    Delta(DeltaControl),
    RollingSum(RollingWindow),
    RollingAvg(RollingWindow),
    RollingMin(RollingWindow),
    RollingMax(RollingWindow),
    RollingCount(RollingWindow),
    RollingProduct(RollingWindow),
    RollingStd(RollingWindow),
    RollingWAvg {
        window: RollingWindow,
        weight_column: String,
    },
    RollingGroup(RollingWindow),
    RollingFormula {
        window: RollingWindow,
        formula: String,
        param_token: Option<String>,
    },
    RollingCountWhere {
        window: RollingWindow,
        result_column: String,
        filters: Vec<String>,
    },
    CumCountWhere {
        result_column: String,
        filters: Vec<String>,
    },
    Ema(EmSpec),
    Ems(EmSpec),
    EmMin(EmSpec),
    EmMax(EmSpec),
    EmStd(EmSpec),
}

/// An operation and the columns it applies to: each is `Result = Input`, or a name alone
/// for a result that replaces its input. With no columns the operation applies to every
/// column it can, except the bucket columns.
///
/// Behavior: io.deephaven.api.updateby.ColumnUpdateOperation
#[derive(Clone, PartialEq, Debug)]
pub struct UpdateByOperation {
    pub spec: UpdateBySpec,
    pub columns: Vec<String>,
}

impl UpdateByOperation {
    pub fn new<S: AsRef<str>>(spec: UpdateBySpec, columns: &[S]) -> Self {
        UpdateByOperation {
            spec,
            columns: columns.iter().map(|c| c.as_ref().to_owned()).collect(),
        }
    }
}

/// Why an `update_by` was refused.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum UpdateByError {
    /// The operations, their columns or the bucket columns do not fit the table: no
    /// operations, an unknown column, a column type the operation has no operator for, two
    /// results with one name, a window of negative size.
    InvalidArgument(String),
    /// The result could not be computed, or the parent table has failed.
    Failed(TableFailure),
}

impl fmt::Display for UpdateByError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UpdateByError::InvalidArgument(message) => f.write_str(message),
            UpdateByError::Failed(failure) => write!(f, "{failure}"),
        }
    }
}

impl std::error::Error for UpdateByError {}

impl From<TableFailure> for UpdateByError {
    fn from(failure: TableFailure) -> Self {
        UpdateByError::Failed(failure)
    }
}

fn invalid(message: impl Into<String>) -> UpdateByError {
    UpdateByError::InvalidArgument(message.into())
}

/// The parent's columns and the results of `operations`, computed per bucket of rows with
/// equal values in the `by` columns.
///
/// The count-where operators and the rolling formula need a formula compiler and are
/// refused here; [`update_by_with`] takes one.
///
/// # Panics
/// With `unported!` for the count-where filters and the rolling formulas that
/// `count_where::filters` and the `formula` module leave unported.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.UpdateBy#updateBy
pub fn update_by<S: AsRef<str>>(
    parent: &Arc<dyn Table>,
    operations: &[UpdateByOperation],
    by: &[S],
) -> Result<Arc<dyn Table>, UpdateByError> {
    update_by_inner(parent, operations, by, None)
}

/// [`update_by`], with the compiler of the formulas among the filters of the count-where
/// operators and of the rolling formulas.
pub fn update_by_with<S: AsRef<str>>(
    parent: &Arc<dyn Table>,
    operations: &[UpdateByOperation],
    by: &[S],
    compiler: &dyn FormulaCompiler,
) -> Result<Arc<dyn Table>, UpdateByError> {
    update_by_inner(parent, operations, by, Some(compiler))
}

fn update_by_inner<S: AsRef<str>>(
    parent: &Arc<dyn Table>,
    operations: &[UpdateByOperation],
    by: &[S],
    compiler: Option<&dyn FormulaCompiler>,
) -> Result<Arc<dyn Table>, UpdateByError> {
    let by: Vec<&str> = by.iter().map(AsRef::as_ref).collect();
    let plan = Arc::new(Plan::new(parent, operations, &by, compiler)?);
    let table = QueryTable::derive(parent, || {
        let rows = parent.row_set();
        let state = plan.initialize(&rows)?;
        let columns = plan.result_columns();
        let spec = TableSpec::new(plan.definition.clone(), (*rows).clone(), columns)
            .keeping_partitioning();
        let plan = plan.clone();
        let state = Mutex::new(state);
        Ok((spec, move |_result: &QueryTable, upstream: &TableUpdate| {
            let mut state = state.lock().unwrap_or_else(PoisonError::into_inner);
            plan.on_update(&mut state, upstream)
        }))
    })?;
    Ok(table)
}

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum Window {
    Cumulative,
    Ticks(Ticks),
    Time(Time),
}

#[derive(Clone, Copy)]
enum Kernel {
    Cumulative(Cumulative),
    Rolling(Rolling),
    /// The rolling group: a vector of the input values over the window.
    Group,
    /// The count of the rows, up to the current one or in its window, that pass the
    /// column's filters.
    CountWhere,
    /// The rolling formula: the column's formula over the vectors of the window.
    Formula,
    Em(Em),
}

/// One result column.
struct Column {
    kernel: Kernel,
    /// Index of the input column in the parent. A count-where has none: it reads the
    /// columns of its filters.
    input: usize,
    input_type: ColumnType,
    /// The parent columns whose modification changes the result, the timestamp column
    /// aside.
    reads: Vec<usize>,
    /// The filters of a count-where.
    filters: Option<CountWhere>,
    /// The formula of a rolling formula.
    formula: Option<RollingFormula>,
    output: Arc<WritableSource>,
    /// Index of the result column in the result.
    result: usize,
    /// Whether the operation is in time, so that the timestamp column affects it too.
    timed: bool,
    /// Index and type of the weight column in the parent, for a weighted average.
    weight: Option<(usize, ColumnType)>,
    /// The average and the variance after each row, for an exponential moving standard
    /// deviation: what a run that starts inside a bucket is seeded with.
    std_state: Option<[WritableSource; 2]>,
}

struct Plan {
    parent: Arc<dyn Table>,
    definition: TableDefinition,
    columns: Vec<Column>,
    /// The result columns that share a window, in the order the windows first appear.
    windows: Vec<(Window, Vec<usize>)>,
    /// Indices of the bucket columns in the parent.
    by: Vec<usize>,
    /// Index of the timestamp column in the parent, if an operation is in time.
    timestamp: Option<usize>,
    /// Parent columns to the result columns that pass them through.
    transformer: ModifiedColumnTransformer,
}

/// The rows of each bucket. Without bucket columns there is one bucket and no index.
struct State {
    buckets: Vec<RowSet>,
    index: HashMap<Vec<u8>, usize>,
}

/// One bucket's share of an update, as Java's bucket gets it from `partitionBy`.
#[derive(Default)]
struct Change {
    added: Vec<RowKey>,
    /// In the previous key space.
    removed: Vec<RowKey>,
    modified: Vec<RowKey>,
    /// The rows of `modified`, in the previous key space.
    modified_before: Vec<RowKey>,
}

/// Behavior: io.deephaven.api.Pair#parse
fn parse_pair(text: &str) -> Result<(String, String), UpdateByError> {
    let (output, input) = match text.split_once('=') {
        Some((output, input)) => (output.trim(), input.trim()),
        None => (text.trim(), text.trim()),
    };
    for name in [output, input] {
        let mut chars = name.chars();
        let valid = chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == '$')
            && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$');
        if !valid {
            return Err(invalid(format!("Invalid column name \"{name}\" in {text}")));
        }
    }
    Ok((output.to_owned(), input.to_owned()))
}

/// The length of one side of a window, in rows or nanoseconds; a fraction of a row is cut.
///
/// Behavior: io.deephaven.api.updateby.spec.WindowScale#getTimeScaleUnits
fn units(scale: &WindowScale) -> i64 {
    match scale {
        WindowScale::Ticks(ticks) => *ticks as i64,
        WindowScale::Time { nanos, .. } => *nanos,
    }
}

/// The window of a rolling operation, and its timestamp column if it is in time. Whether
/// it is, and the column, are those of the reverse side.
///
/// Behavior: io.deephaven.api.updateby.spec.RollingOpSpec#checkWindowSizes and
/// #checkCompatibleTimeColumns, and the operators of
/// io.deephaven.engine.table.impl.updateby.UpdateByOperatorFactory.OperationVisitor, which
/// take their timestamp column from `revWindowScale` and their units from
/// WindowScale#getTimeScaleUnits
fn rolling_window(window: &RollingWindow) -> Result<(Window, Option<String>), UpdateByError> {
    let fraction = |scale: &WindowScale| match scale {
        WindowScale::Ticks(ticks) => *ticks,
        WindowScale::Time { nanos, .. } => *nanos as f64,
    };
    let size = fraction(&window.rev) + fraction(&window.fwd);
    if size < 0.0 {
        return Err(invalid("UpdateBy rolling window size must be non-negative"));
    }
    if let (WindowScale::Time { column: a, .. }, WindowScale::Time { column: b, .. }) =
        (&window.rev, &window.fwd)
    {
        if a != b {
            return Err(invalid(
                "UpdateBy rolling window forward and reverse timestamp columns must match",
            ));
        }
    }
    let (rev, fwd) = (units(&window.rev), units(&window.fwd));
    match &window.rev {
        WindowScale::Ticks(_) => {
            // Behavior: io.deephaven.util.type.ArrayTypeUtils#MAX_ARRAY_SIZE
            if size > (i32::MAX - 8) as f64 {
                return Err(invalid(
                    "UpdateBy rolling window size may not exceed MAX_ARRAY_SIZE",
                ));
            }
            Ok((Window::Ticks(Ticks { rev, fwd }), None))
        }
        WindowScale::Time { column, .. } => {
            Ok((Window::Time(Time { rev, fwd }), Some(column.clone())))
        }
    }
}

/// An exponential moving operator of `kind` with the parameters of `spec`, and its
/// timestamp column if it decays in time.
///
/// Behavior: io.deephaven.api.updateby.spec.WindowScale#getFractionalTimeScaleUnits and
/// io.deephaven.api.updateby.OperationControl#onNullValueOrDefault and
/// #onNanValueOrDefault, whose default is `SKIP`
fn em_of(kind: EmKind, spec: &EmSpec) -> (Kernel, Window, Option<String>) {
    let (units, column) = match &spec.window {
        WindowScale::Ticks(ticks) => (*ticks, None),
        WindowScale::Time { column, nanos } => (*nanos as f64, Some(column.clone())),
    };
    let em = Em {
        kind,
        units,
        timed: column.is_some(),
        on_null: spec.on_null.unwrap_or(BadDataBehavior::Skip),
        on_nan: spec.on_nan.unwrap_or(BadDataBehavior::Skip),
    };
    (Kernel::Em(em), Window::Cumulative, column)
}

/// The kernel and window of an operation, and the timestamp column it needs.
fn kernel_of(spec: &UpdateBySpec) -> Result<(Kernel, Window, Option<String>), UpdateByError> {
    let cumulative = |kernel| Ok((Kernel::Cumulative(kernel), Window::Cumulative, None));
    let rolling = |kernel, window: &RollingWindow| {
        let (window, column) = rolling_window(window)?;
        Ok((Kernel::Rolling(kernel), window, column))
    };
    match spec {
        UpdateBySpec::CumSum => cumulative(Cumulative::Sum),
        UpdateBySpec::CumMin => cumulative(Cumulative::Min),
        UpdateBySpec::CumMax => cumulative(Cumulative::Max),
        UpdateBySpec::CumProd => cumulative(Cumulative::Product),
        UpdateBySpec::ForwardFill => cumulative(Cumulative::ForwardFill),
        UpdateBySpec::Delta(control) => cumulative(Cumulative::Delta(*control)),
        UpdateBySpec::RollingSum(window) => rolling(Rolling::Sum, window),
        UpdateBySpec::RollingAvg(window) => rolling(Rolling::Avg, window),
        UpdateBySpec::RollingMin(window) => rolling(Rolling::Min, window),
        UpdateBySpec::RollingMax(window) => rolling(Rolling::Max, window),
        UpdateBySpec::RollingCount(window) => rolling(Rolling::Count, window),
        UpdateBySpec::RollingProduct(window) => rolling(Rolling::Product, window),
        UpdateBySpec::RollingStd(window) => rolling(Rolling::Std, window),
        UpdateBySpec::RollingWAvg { window, .. } => rolling(Rolling::WAvg, window),
        // Behavior: UpdateByOperatorFactory.OperationVisitor#makeRollingGroupOperator
        UpdateBySpec::RollingGroup(window) => {
            let (window, column) = rolling_window(window)?;
            Ok((Kernel::Group, window, column))
        }
        // Behavior: UpdateByOperatorFactory.OperationVisitor#visit(RollingFormulaSpec)
        UpdateBySpec::RollingFormula { window, .. } => {
            let (window, column) = rolling_window(window)?;
            Ok((Kernel::Formula, window, column))
        }
        // Behavior: UpdateByOperatorFactory.OperationVisitor#makeCountWhereOperator
        UpdateBySpec::RollingCountWhere { window, .. } => {
            let (window, column) = rolling_window(window)?;
            Ok((Kernel::CountWhere, window, column))
        }
        UpdateBySpec::CumCountWhere { .. } => Ok((Kernel::CountWhere, Window::Cumulative, None)),
        UpdateBySpec::Ema(spec) => Ok(em_of(EmKind::Average, spec)),
        UpdateBySpec::Ems(spec) => Ok(em_of(EmKind::Sum, spec)),
        UpdateBySpec::EmMin(spec) => Ok(em_of(EmKind::Min, spec)),
        UpdateBySpec::EmMax(spec) => Ok(em_of(EmKind::Max, spec)),
        UpdateBySpec::EmStd(spec) => Ok(em_of(EmKind::Std, spec)),
    }
}

/// Whether an operation given no columns applies to a column of type `input`.
///
/// Behavior: io.deephaven.api.updateby.spec.UpdateBySpec#applicableTo as implemented by
/// `CumSumSpec`, `CumProdSpec`, `CumMinMaxSpec`, `FillBySpec`, `DeltaSpec`, `RollingSumSpec`,
/// `RollingAvgSpec`, `RollingMinMaxSpec`, `RollingCountSpec`, `RollingProductSpec`,
/// `RollingStdSpec`, `RollingWAvgSpec`, `RollingGroupSpec`, `EmaSpec`, `EmsSpec`,
/// `EmMinMaxSpec` and `EmStdSpec`
/// Whether the minimum and maximum of a vector of `element`s are ported: vectors of the
/// primitive types, of `String`, `Boolean`, `Instant`, `LocalDate`, `LocalTime`,
/// `LocalDateTime`, `Duration`, `DayOfWeek` and `ZonedDateTime`. Java makes the result of a
/// vector of objects a column of `ObjectVector` of `Object`, which a client receives as
/// strings; Oxhaven keeps the element type, allowed difference A27 of `PARITY.md` (owner
/// item 3.3; Java's answers are in `oracle/corpus/agg_typed_vectors` and
/// `oracle/corpus/agg_object_vectors`). Vectors of other objects are not ported: Java fails
/// to order two vectors of `ZoneId`s or `Period`s, which are not `Comparable`, unless the
/// elements it compares are the same objects, and two vectors of arrays (owner item 2.2),
/// and the order of `BigDecimal` and `BigInteger` is not ported.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.internal.BaseObjectUpdateByOperator,
/// whose result column has the input's class and the component type `Object`; observed on
/// Deephaven 42.9 for vectors of `String`, `Instant`, `Boolean`, `BigDecimal`, `LocalDate`
/// and `ZonedDateTime`
fn orderable_vector(element: &ColumnType) -> bool {
    use ColumnType::*;
    matches!(
        element,
        Byte | Char
            | Short
            | Int
            | Long
            | Float
            | Double
            | String
            | Boolean
            | Instant
            | LocalDate
            | LocalTime
            | LocalDateTime
            | Duration
            | DayOfWeek
            | ZonedDateTime
    )
}

fn applicable(kernel: Kernel, input: &ColumnType) -> bool {
    use ColumnType::*;
    let numeric = matches!(input, Byte | Short | Int | Long | Float | Double);
    let character = *input == Char;
    let boolean = *input == Boolean;
    // `String`, `Instant` and every vector are `Comparable`; so is `Boolean`, which Java
    // excludes. An array is not.
    let comparable = matches!(input, String | Instant | Vector(_));
    match kernel {
        Kernel::Cumulative(Cumulative::Sum)
        | Kernel::Rolling(Rolling::Sum | Rolling::Avg | Rolling::Product) => {
            numeric || character || boolean
        }
        Kernel::Cumulative(Cumulative::Product) => numeric,
        Kernel::Cumulative(Cumulative::Min | Cumulative::Max)
        | Kernel::Rolling(Rolling::Min | Rolling::Max) => numeric || character || comparable,
        Kernel::Cumulative(Cumulative::ForwardFill)
        | Kernel::Rolling(Rolling::Count)
        | Kernel::Group => true,
        Kernel::Cumulative(Cumulative::Delta(_))
        | Kernel::Rolling(Rolling::Std | Rolling::WAvg)
        | Kernel::Em(_) => numeric || character,
        // Behavior: `CumCountWhereSpec#applicableTo` and `RollingCountWhereSpec#applicableTo`;
        // the operator takes no input columns.
        Kernel::CountWhere => true,
        // Behavior: `RollingFormulaSpec#applicableTo`
        Kernel::Formula => true,
    }
}

impl Plan {
    /// Behavior: io.deephaven.engine.table.impl.updateby.UpdateBy.UpdateByOperatorCollection#from,
    /// with the column lists of io.deephaven.engine.table.impl.updateby.UpdateByOperatorFactory
    /// #createColumnsToAddIfMissing and its `OutputColumnVisitor`
    fn new(
        parent: &Arc<dyn Table>,
        operations: &[UpdateByOperation],
        by: &[&str],
        compiler: Option<&dyn FormulaCompiler>,
    ) -> Result<Plan, UpdateByError> {
        let declared = parent.definition().columns();
        if operations.is_empty() {
            return Err(invalid("At least one operator must be specified"));
        }
        let graph = parent.update_graph().filter(|_| parent.is_refreshing());
        let mut result: Vec<ColumnDefinition> = declared.to_vec();
        let mut replaced = BTreeSet::new();
        let mut names = BTreeSet::new();
        let mut duplicates = Vec::new();
        let mut columns = Vec::new();
        let mut windows: Vec<(Window, Vec<usize>)> = Vec::new();
        let mut timestamp: Option<String> = None;
        for operation in operations {
            let (kernel, window, timestamp_column) = kernel_of(&operation.spec)?;
            // Behavior: the check of the timestamp columns in
            // io.deephaven.engine.table.impl.updateby.UpdateBy.UpdateByOperatorCollection#from
            if let Some(column) = &timestamp_column {
                match &timestamp {
                    None => timestamp = Some(column.clone()),
                    Some(first) if first != column => {
                        return Err(invalid(format!(
                            "Cannot reference more than one timestamp source on a single UpdateBy call {{{first}, {column}}}"
                        )))
                    }
                    Some(_) => {}
                }
            }
            // Behavior: UpdateByOperatorFactory.OperationVisitor#visit(CumCountWhereSpec) and
            // #visit(RollingCountWhereSpec), which make one operator, whatever the column
            // pairs of the operation, and UpdateByOperatorFactory.OutputColumnVisitor
            if let UpdateBySpec::CumCountWhere {
                result_column,
                filters,
            }
            | UpdateBySpec::RollingCountWhere {
                result_column,
                filters,
                ..
            } = &operation.spec
            {
                let filters = count_where::filters(&**parent, filters, compiler)?;
                let (output, position) = register(
                    &mut result,
                    &mut replaced,
                    &mut names,
                    &mut duplicates,
                    &mut windows,
                    columns.len(),
                    result_column,
                    ColumnType::Long,
                    window,
                    &graph,
                );
                columns.push(Column {
                    kernel,
                    input: 0,
                    input_type: ColumnType::Long,
                    reads: filters.columns().to_vec(),
                    filters: Some(filters),
                    formula: None,
                    output,
                    result: position,
                    timed: timestamp_column.is_some(),
                    weight: None,
                    std_state: None,
                });
                continue;
            }
            // Behavior: UpdateByOperatorFactory.OperationVisitor#visit(RollingFormulaSpec):
            // without a token the operation is one operator, whatever its column pairs,
            // and UpdateByOperatorFactory.OutputColumnVisitor names its result
            if let UpdateBySpec::RollingFormula {
                formula,
                param_token: None,
                ..
            } = &operation.spec
            {
                let (name, formula) =
                    formula::of_columns(compiler, parent.definition(), formula, by)?;
                let (output, position) = register(
                    &mut result,
                    &mut replaced,
                    &mut names,
                    &mut duplicates,
                    &mut windows,
                    columns.len(),
                    &name,
                    formula.result_type().clone(),
                    window,
                    &graph,
                );
                columns.push(Column {
                    kernel,
                    input: 0,
                    input_type: ColumnType::Long,
                    reads: formula.reads(),
                    filters: None,
                    formula: Some(formula),
                    output,
                    result: position,
                    timed: timestamp_column.is_some(),
                    weight: None,
                    std_state: None,
                });
                continue;
            }
            let pairs = if operation.columns.is_empty() {
                declared
                    .iter()
                    .filter(|c| {
                        !by.contains(&c.name.as_str()) && applicable(kernel, &c.column_type)
                    })
                    .map(|c| (c.name.clone(), c.name.clone()))
                    .collect()
            } else {
                operation
                    .columns
                    .iter()
                    .map(|text| parse_pair(text))
                    .collect::<Result<Vec<_>, _>>()?
            };
            // Behavior: the visitors of io.deephaven.engine.table.impl.updateby
            // .UpdateByOperatorFactory.OperationVisitor for operations in time make no
            // operator for the timestamp column itself, and UpdateBy#updateBy then fails to
            // find the result column it was promised. The visitor of the rolling group
            // takes every pair, so the timestamps can be grouped.
            let grouped = matches!(kernel, Kernel::Group);
            if let Some((output, _)) = pairs
                .iter()
                .find(|(_, input)| !grouped && timestamp_column.as_ref() == Some(input))
            {
                return Err(invalid(format!(
                    "Requested output column '{output}' was not found in operator output"
                )));
            }
            for (output_name, input_name) in pairs {
                let input = parent
                    .definition()
                    .index_of(&input_name)
                    .ok_or_else(|| invalid(format!("Column {input_name} does not exist")))?;
                let input_type = declared[input].column_type.clone();
                // Behavior: CumMinMaxSpec#applicableTo and RollingMinMaxSpec#applicableTo: a
                // vector is `Comparable`, so Java orders vectors with `compareTo`
                // (io.deephaven.vector.IntVector#compareTo and its siblings), which is ported
                // for the vectors `orderable_vector` takes.
                if let (
                    Kernel::Cumulative(Cumulative::Min | Cumulative::Max)
                    | Kernel::Rolling(Rolling::Min | Rolling::Max),
                    ColumnType::Vector(element),
                ) = (kernel, &input_type)
                {
                    if !orderable_vector(element) {
                        oxhaven_contracts::unported!(
                            "io.deephaven.engine.table.impl.updateby.minmax.ComparableCumMinMaxOperator",
                            "minimum or maximum of vector column {input_name} of type \
                             {input_type:?}: Java gives the result column the element type \
                             Object (a client receives strings), or fails when it compares \
                             two arrays"
                        );
                    }
                    // A rolling minimum or maximum of vectors of floats or doubles keeps the
                    // earliest of equal vectors in the window (`rolling::extremes`), where
                    // Java keeps the one its ring buffer's layout favours: allowed
                    // difference A20 of PARITY.md (owner item 7.10).
                }
                // Behavior: UpdateByOperatorFactory.OperationVisitor#makeRollingWAvgOperator,
                // which looks at the weight column before the input column
                let weight = match &operation.spec {
                    UpdateBySpec::RollingWAvg { weight_column, .. } => {
                        let index =
                            parent.definition().index_of(weight_column).ok_or_else(|| {
                                invalid(format!("Column {weight_column} does not exist"))
                            })?;
                        let weight_type = declared[index].column_type.clone();
                        use ColumnType::*;
                        if !matches!(
                            weight_type,
                            Byte | Char | Short | Int | Long | Float | Double
                        ) {
                            return Err(invalid(format!(
                                "Can not perform RollingWAvg on weight column type {weight_type:?}"
                            )));
                        }
                        Some((index, weight_type))
                    }
                    _ => None,
                };
                let formula = match &operation.spec {
                    UpdateBySpec::RollingFormula {
                        formula,
                        param_token: Some(token),
                        ..
                    } => Some(formula::of_token(
                        compiler,
                        parent.definition(),
                        formula,
                        token,
                        input,
                    )?),
                    _ => None,
                };
                let output_type = match kernel {
                    Kernel::Formula => formula
                        .as_ref()
                        .map(|formula| formula.result_type().clone()),
                    Kernel::Cumulative(kernel) => kernel.output_type(&input_type),
                    Kernel::Rolling(kernel) => kernel.output_type(&input_type),
                    Kernel::Group => Some(group::output_type(&input_type)),
                    Kernel::Em(_) => Em::output_type(&input_type),
                    Kernel::CountWhere => Some(ColumnType::Long),
                }
                .ok_or_else(|| {
                    invalid(format!(
                        "Can not perform {:?} on type {input_type:?}",
                        operation.spec
                    ))
                })?;
                let (output, position) = register(
                    &mut result,
                    &mut replaced,
                    &mut names,
                    &mut duplicates,
                    &mut windows,
                    columns.len(),
                    &output_name,
                    output_type,
                    window,
                    &graph,
                );
                let std_state = matches!(kernel, Kernel::Em(em) if em.kind == EmKind::Std)
                    .then(|| [(); 2].map(|()| WritableSource::new(ColumnType::Double)));
                columns.push(Column {
                    kernel,
                    input,
                    input_type,
                    reads: std::iter::once(input)
                        .chain(weight.as_ref().map(|(index, _)| *index))
                        .collect(),
                    filters: None,
                    formula,
                    output,
                    result: position,
                    timed: timestamp_column.is_some(),
                    weight,
                    std_state,
                });
            }
        }
        if columns.is_empty() {
            return Err(invalid("At least one operator must be specified"));
        }
        if !duplicates.is_empty() {
            return Err(invalid(format!(
                "resulting column names must be unique {{{}}}",
                duplicates.join(", ")
            )));
        }
        let missing: Vec<&str> = by
            .iter()
            .copied()
            .filter(|name| parent.definition().index_of(name).is_none())
            .collect();
        if !missing.is_empty() {
            return Err(invalid(format!(
                "Missing byColumns in parent table {{{}}}",
                missing.join(", ")
            )));
        }
        let by: Vec<usize> = by
            .iter()
            .filter_map(|name| parent.definition().index_of(name))
            .collect();
        // Behavior: the check of the timestamp column's type in
        // io.deephaven.engine.table.impl.updateby.UpdateBy#updateBy: a long, or a type
        // ReinterpretUtils#maybeConvertToPrimitive makes a long of
        let timestamp = match timestamp {
            None => None,
            Some(name) => {
                let index = parent
                    .definition()
                    .index_of(&name)
                    .ok_or_else(|| invalid(format!("Column {name} does not exist")))?;
                match &declared[index].column_type {
                    ColumnType::Long | ColumnType::Instant => Some(index),
                    other => {
                        return Err(invalid(format!(
                            "Unsupported timestamp column type {other:?} for column '{name}'"
                        )))
                    }
                }
            }
        };
        // Behavior: io.deephaven.engine.table.impl.by.ChunkedOperatorAggregationHelper
        // #aggregation, which refuses an array key column when
        // io.deephaven.engine.table.impl.updateby.BucketedPartitionedUpdateByManager buckets
        // by `partitionedAggBy`
        for &index in &by {
            let column = &declared[index];
            if let ColumnType::Array(element) = &column.column_type {
                return Err(invalid(format!(
                    "Cannot aggregate using an array column: {}, column type is an array of {}",
                    column.name,
                    crate::ops::agg::java_class(element)
                )));
            }
        }
        for &index in &by {
            let column = &declared[index];
            if column.column_type.list_element().is_some()
                && !values::bucketable_vector(&column.column_type)
            {
                oxhaven_contracts::unported!(
                    "io.deephaven.engine.table.impl.updateby.UpdateBy#updateBy",
                    "bucket column {} of type {:?}: Java's buckets of vectors of arrays \
                     depend on the identity of the arrays, and those of vectors of vectors of \
                     float or double elements on the class of the vector objects (-0.0 and 0.0)",
                    column.name,
                    column.column_type
                );
            }
        }
        let definition = match TableDefinition::new(result) {
            Ok(definition) => definition,
            Err(e) => panic!("replacing columns by name keeps names distinct: {e}"),
        };
        // Behavior: the `preservedColumnNames` of UpdateByOperatorCollection#from, mapped by
        // io.deephaven.engine.table.impl.QueryTable#newModifiedColumnSetTransformer.
        let transformer = ModifiedColumnTransformer::new(
            (0..declared.len())
                .filter(|index| !replaced.contains(index))
                .map(|index| (index, [index])),
        );
        Ok(Plan {
            parent: parent.clone(),
            definition,
            columns,
            windows,
            by,
            timestamp,
            transformer,
        })
    }

    /// The filters of a count-where column.
    fn filters<'a>(&self, column: &'a Column) -> Result<&'a CountWhere, TableFailure> {
        column
            .filters
            .as_ref()
            .ok_or_else(|| failure("update_by: a count-where without its filters"))
    }

    /// The formula of a rolling formula column.
    fn formula<'a>(&self, column: &'a Column) -> Result<&'a RollingFormula, TableFailure> {
        column
            .formula
            .as_ref()
            .ok_or_else(|| failure("update_by: a rolling formula without its formula"))
    }

    fn result_columns(&self) -> Vec<Arc<dyn ColumnSource>> {
        let mut sources: Vec<Arc<dyn ColumnSource>> = (0..self.parent.definition().columns().len())
            .map(|index| self.parent.column(index))
            .collect();
        for column in &self.columns {
            let source: Arc<dyn ColumnSource> = column.output.clone();
            if column.result < sources.len() {
                sources[column.result] = source;
            } else {
                sources.push(source);
            }
        }
        sources
    }

    /// The bucket key of each of `rows`, in order: equal for rows of one bucket.
    ///
    /// Behavior: the key equality of io.deephaven.engine.table.impl.QueryTable#partitionedAggBy,
    /// which io.deephaven.engine.table.impl.updateby.BucketedPartitionedUpdateByManager buckets
    /// with: null is a key, every NaN is the same key, and `-0.0` and `0.0` are the same key
    /// (io.deephaven.util.compare.DoubleComparisons#eq)
    fn keys(&self, rows: &RowSet, previous: bool) -> Result<Vec<Vec<u8>>, TableFailure> {
        let mut keys = vec![Vec::new(); rows.size() as usize];
        for &index in &self.by {
            let source = self.parent.column(index);
            let chunk = if previous {
                source.prev_chunk(rows)?
            } else {
                source.chunk(rows)?
            };
            let column_type = &self.parent.definition().columns()[index].column_type;
            match Values::read(&chunk, column_type)? {
                Values::Int(values) => {
                    for (key, value) in keys.iter_mut().zip(values) {
                        key.push(u8::from(value.is_some()));
                        key.extend(value.unwrap_or(0).to_le_bytes());
                    }
                }
                Values::Float(values) => {
                    for (key, value) in keys.iter_mut().zip(values) {
                        let bits = match value {
                            Some(v) if v.is_nan() => f64::NAN.to_bits(),
                            Some(0.0) => 0,
                            Some(v) => v.to_bits(),
                            None => 0,
                        };
                        key.push(u8::from(value.is_some()));
                        key.extend(bits.to_le_bytes());
                    }
                }
                Values::Text(values) => {
                    for (key, value) in keys.iter_mut().zip(values) {
                        key.push(u8::from(value.is_some()));
                        let text = value.unwrap_or_default();
                        key.extend((text.len() as u64).to_le_bytes());
                        key.extend(text.as_bytes());
                    }
                }
                // `Plan::new` lets only the vectors whose equality is well defined through.
                Values::Lists(cells) => values::push_list_keys(&mut keys, &cells, column_type)?,
                Values::Ordered(_) => return Err(failure("a bucket key read to be ordered")),
            }
        }
        Ok(keys)
    }

    /// The timestamps of `rows`, now or before the current cycle.
    fn timestamps(&self, rows: &RowSet, previous: bool) -> Result<Vec<Option<i64>>, TableFailure> {
        let Some(index) = self.timestamp else {
            return Err(failure("update_by: no timestamp column"));
        };
        let source = self.parent.column(index);
        let chunk = if previous {
            source.prev_chunk(rows)?
        } else {
            source.chunk(rows)?
        };
        match Values::read(
            &chunk,
            &self.parent.definition().columns()[index].column_type,
        )? {
            Values::Int(values) => Ok(values),
            _ => Err(failure("update_by: a timestamp column that holds no longs")),
        }
    }

    /// The rows of a bucket that have a timestamp. Fails if the timestamps decrease.
    fn stamped(&self, rows: &RowSet) -> Result<Stamped, TableFailure> {
        Stamped::new(rows, &self.timestamps(rows, false)?)
    }

    /// The timestamp Java seeds the exponential moving operators on `input` with, for a run
    /// that follows the row at `position` of the bucket `rows`: that row's, unless it has
    /// none or its value does not count, then the one of the nearest row before it that has
    /// both, or, if there is none, of the bucket's first row. Which values count is decided
    /// by the first operator in time on the column.
    ///
    /// Behavior: io.deephaven.engine.table.impl.updateby.UpdateByWindowCumulative#processWindowBucketOperatorSet
    fn seed_timestamp(
        &self,
        members: &[usize],
        input: usize,
        rows: &RowSet,
        position: u64,
    ) -> Result<Option<i64>, TableFailure> {
        let first = members.iter().find_map(|&member| {
            let column = &self.columns[member];
            match column.kernel {
                Kernel::Em(em) if column.timed && column.input == input => Some((em, column)),
                _ => None,
            }
        });
        let Some((em, column)) = first else {
            return Ok(None);
        };
        // The rows from the bucket's first to the one before the run, read once.
        let (Some(first_key), Some(key_before)) = (rows.first_row_key(), rows.get(position)) else {
            return Err(failure(
                "update_by: the row before a run is not in its bucket",
            ));
        };
        let before = rows.sub_set_by_key_range(first_key, key_before);
        let stamps = self.timestamps(&before, false)?;
        let values = match Values::read(
            &self.parent.column(input).chunk(&before)?,
            &column.input_type,
        )? {
            Values::Int(v) => v.into_iter().map(|v| v.map(|v| v as f64)).collect(),
            Values::Float(v) => v,
            Values::Text(_) | Values::Lists(_) | Values::Ordered(_) => {
                return Err(failure("update_by: an exponential operator on objects"))
            }
        };
        let counts = |i: usize| stamps[i].is_some() && em.valid(values[i]);
        let last = position as usize;
        if counts(last) {
            return Ok(stamps[last]);
        }
        // Without such a row, the walk ends on the bucket's first row.
        Ok(stamps[(0..last).rev().find(|&i| counts(i)).unwrap_or(0)])
    }

    /// Buckets `rows` and computes every result column for all of them.
    ///
    /// Behavior: the initial step of io.deephaven.engine.table.impl.updateby.UpdateBy.PhasedUpdateProcessor
    fn initialize(&self, rows: &RowSet) -> Result<State, TableFailure> {
        let mut state = State {
            buckets: Vec::new(),
            index: HashMap::new(),
        };
        if self.by.is_empty() {
            state.buckets.push(rows.clone());
        } else {
            let mut members: Vec<Vec<RowKey>> = Vec::new();
            for (row, key) in rows.iter().zip(self.keys(rows, false)?) {
                let bucket = *state.index.entry(key).or_insert_with(|| {
                    members.push(Vec::new());
                    members.len() - 1
                });
                members[bucket].push(row);
            }
            state.buckets = members.into_iter().map(RowSet::from_keys).collect();
        }
        for bucket in &state.buckets {
            if bucket.is_empty() {
                continue;
            }
            if self.timestamp.is_some() {
                self.stamped(bucket)?;
            }
            for (window, members) in &self.windows {
                self.compute(*window, members, bucket, bucket)?;
            }
        }
        Ok(state)
    }

    /// Computes the result columns `members` of one window for the rows `affected` of the
    /// bucket `rows`, and stores them.
    ///
    /// Behavior: io.deephaven.engine.table.impl.updateby.UpdateByWindowCumulative#processWindowBucketOperatorSet
    /// and io.deephaven.engine.table.impl.updateby.UpdateByWindowRollingBase#processWindowBucketOperatorSet
    fn compute(
        &self,
        window: Window,
        members: &[usize],
        rows: &RowSet,
        affected: &RowSet,
    ) -> Result<(), TableFailure> {
        let internal = |what: &str| failure(format!("update_by: {what}"));
        match window {
            Window::Cumulative => {
                let first = affected
                    .first_row_key()
                    .ok_or_else(|| internal("no affected rows"))?;
                let position = rows
                    .find(first)
                    .ok_or_else(|| internal("an affected row is not in its bucket"))?;
                let before = match position {
                    0 => None,
                    _ => rows.get(position - 1).map(|key| RowSet::from_keys([key])),
                };
                let timed = members.iter().any(|&member| self.columns[member].timed);
                let stamps = if timed {
                    self.timestamps(affected, false)?
                } else {
                    Vec::new()
                };
                for &member in members {
                    let column = &self.columns[member];
                    if let Kernel::CountWhere = column.kernel {
                        let seed = match &before {
                            None => 0,
                            Some(before) => {
                                match Values::read(
                                    &column.output.chunk(before)?,
                                    &ColumnType::Long,
                                )? {
                                    Values::Int(v) => v[0].ok_or_else(|| {
                                        internal("a cumulative count-where that is null")
                                    })?,
                                    _ => return Err(internal("a count of another type")),
                                }
                            }
                        };
                        let counts =
                            self.filters(column)?
                                .cumulative(&*self.parent, affected, seed)?;
                        column.output.fill(affected, &counts)?;
                        continue;
                    }
                    let input = self.parent.column(column.input);
                    let values = input.chunk(affected)?;
                    let results = match column.kernel {
                        Kernel::Cumulative(kernel) => {
                            let seed = match &before {
                                None => None,
                                Some(before) if kernel.seeded_by_input() => {
                                    Some(input.chunk(before)?)
                                }
                                Some(before) => Some(column.output.chunk(before)?),
                            };
                            kernel.accumulate(&column.input_type, &values, seed.as_ref())?
                        }
                        Kernel::Em(em) if em.kind == EmKind::Std => {
                            let [averages, variances] =
                                column.std_state.as_ref().ok_or_else(|| {
                                    internal("a standard deviation without its state")
                                })?;
                            let seed = match &before {
                                None => None,
                                Some(before) => {
                                    let read = |source: &WritableSource| match Values::read(
                                        &source.chunk(before)?,
                                        &ColumnType::Double,
                                    )? {
                                        Values::Float(v) => Ok(v[0]),
                                        _ => Err(internal("an exponential result of another type")),
                                    };
                                    let state = StdState {
                                        value: read(&column.output)?,
                                        average: read(averages)?,
                                        variance: read(variances)?,
                                    };
                                    let stamp = self.seed_timestamp(
                                        members,
                                        column.input,
                                        rows,
                                        position - 1,
                                    )?;
                                    Some((state, stamp))
                                }
                            };
                            let results =
                                em.accumulate_std(&column.input_type, &values, &stamps, seed)?;
                            averages.fill(affected, &results.averages)?;
                            variances.fill(affected, &results.variances)?;
                            results.values
                        }
                        Kernel::Em(em) => {
                            let seed = match &before {
                                None => None,
                                Some(before) => {
                                    let value = match Values::read(
                                        &column.output.chunk(before)?,
                                        &ColumnType::Double,
                                    )? {
                                        Values::Float(v) => v[0],
                                        _ => {
                                            return Err(internal(
                                                "an exponential result of another type",
                                            ))
                                        }
                                    };
                                    let stamp = self.seed_timestamp(
                                        members,
                                        column.input,
                                        rows,
                                        position - 1,
                                    )?;
                                    Some((value, stamp))
                                }
                            };
                            em.accumulate(&column.input_type, &values, &stamps, seed)?
                        }
                        Kernel::Rolling(_)
                        | Kernel::Group
                        | Kernel::CountWhere
                        | Kernel::Formula => {
                            return Err(internal("a rolling operator in the cumulative window"))
                        }
                    };
                    column.output.fill(affected, &results)?;
                }
            }
            Window::Ticks(ticks) => {
                let size = rows.size();
                let positions = rows
                    .invert(affected)
                    .map_err(|_| internal("an affected row is not in its bucket"))?;
                let influencer_positions = ticks.influencers(size, &positions);
                let influencers = rows.sub_set_for_positions(&influencer_positions, false);
                let slides = ticks.slides(size, &positions, &influencer_positions);
                for &member in members {
                    let column = &self.columns[member];
                    let kernel = match column.kernel {
                        Kernel::Rolling(kernel) => kernel,
                        Kernel::Group => {
                            let slices = positions.iter().map(|at| ticks.slice(rows, at));
                            let results =
                                group::vectors(&*self.parent.column(column.input), slices)?;
                            column.output.fill(affected, &results)?;
                            continue;
                        }
                        Kernel::CountWhere => {
                            let entering: Vec<usize> =
                                slides.iter().map(|slide| slide.push).collect();
                            self.filters(column)?.check_chunks(&entering)?;
                            let results = self.filters(column)?.rolling(
                                &*self.parent,
                                &influencers,
                                &slides,
                            )?;
                            column.output.fill(affected, &results)?;
                            continue;
                        }
                        Kernel::Formula => {
                            let results = self.formula(column)?.evaluate(
                                &*self.parent,
                                affected,
                                &influencers,
                                &slides,
                            )?;
                            column.output.fill(affected, &results)?;
                            continue;
                        }
                        _ => return Err(internal("a cumulative operator in a rolling window")),
                    };
                    let values = self.parent.column(column.input).chunk(&influencers)?;
                    let weights = match &column.weight {
                        Some((index, _)) => Some(self.parent.column(*index).chunk(&influencers)?),
                        None => None,
                    };
                    let weights = column.weight.as_ref().map(|(_, t)| t).zip(weights.as_ref());
                    let results = kernel.evaluate(&column.input_type, &values, weights, &slides)?;
                    column.output.fill(affected, &results)?;
                }
            }
            Window::Time(time) => {
                // Behavior: io.deephaven.engine.table.impl.updateby.UpdateByWindowRollingTime
                // #computeAffectedRowsAndOperators for the influencers and #computeWindows; a
                // row without a timestamp gets null from every operator
                // (io.deephaven.engine.table.impl.updateby.internal.BaseLongUpdateByOperator.Context#accumulateRolling)
                let stamped = self.stamped(rows)?;
                let stamps = self.timestamps(affected, false)?;
                let grouped =
                    |member: &usize| matches!(self.columns[*member].kernel, Kernel::Group);
                if members.iter().any(grouped) {
                    stamped.positions_exist(affected, &stamps)?;
                }
                let windowed = stamped.rows().intersect(affected);
                let influencers = stamped.reach(stamps.iter().copied(), time.rev, time.fwd);
                let slides = time.slides(
                    &stamped.stamps_of(&windowed),
                    &stamped.stamps_of(&influencers),
                );
                let mut next = 0;
                let indices: UInt32Array = stamps
                    .iter()
                    .map(|stamp| {
                        stamp.map(|_| {
                            next += 1;
                            next - 1
                        })
                    })
                    .collect();
                for &member in members {
                    let column = &self.columns[member];
                    let results = match column.kernel {
                        Kernel::Rolling(kernel) => {
                            let values = self.parent.column(column.input).chunk(&influencers)?;
                            let weights = match &column.weight {
                                Some((index, _)) => {
                                    Some(self.parent.column(*index).chunk(&influencers)?)
                                }
                                None => None,
                            };
                            let weights =
                                column.weight.as_ref().map(|(_, t)| t).zip(weights.as_ref());
                            kernel.evaluate(&column.input_type, &values, weights, &slides)?
                        }
                        // A row without a timestamp gets a null here too, not an empty
                        // vector: RollingGroupOperator.Context#accumulateRolling writes a
                        // null start and a null end for it.
                        Kernel::Group => {
                            let values = self.parent.column(column.input).chunk(&influencers)?;
                            group::windows(&column.input_type, &values, &slides)?
                        }
                        // A row without a timestamp gets a null count.
                        Kernel::CountWhere => {
                            let mut pushes = slides.iter().map(|slide| slide.push);
                            let entering: Vec<usize> = stamps
                                .iter()
                                .map(|stamp| stamp.and_then(|_| pushes.next()).unwrap_or(0))
                                .collect();
                            self.filters(column)?.check_chunks(&entering)?;
                            self.filters(column)?
                                .rolling(&*self.parent, &influencers, &slides)?
                        }
                        // A row without a timestamp gets a null, and no formula.
                        Kernel::Formula => self.formula(column)?.evaluate(
                            &*self.parent,
                            &windowed,
                            &influencers,
                            &slides,
                        )?,
                        _ => return Err(internal("a cumulative operator in a rolling window")),
                    };
                    let results = take(results.as_ref(), &indices, None)
                        .map_err(|e| internal(&e.to_string()))?;
                    column.output.fill(affected, &results)?;
                }
            }
        }
        Ok(())
    }

    /// Each bucket's share of `upstream`, by bucket index. New buckets are created empty.
    ///
    /// Behavior: what the constituents of io.deephaven.engine.table.impl.QueryTable#partitionedAggBy
    /// deliver to io.deephaven.engine.table.impl.updateby.UpdateByBucketHelper.UpdateByBucketHelperListener#onUpdate:
    /// a modified row whose key changed is removed from one bucket and added to another
    fn split(
        &self,
        state: &mut State,
        upstream: &TableUpdate,
    ) -> Result<BTreeMap<usize, Change>, TableFailure> {
        let mut changes: BTreeMap<usize, Change> = BTreeMap::new();
        let before =
            modified_pre_shift(upstream).map_err(|e| failure(format!("update_by: {e}")))?;
        if self.by.is_empty() {
            changes.insert(
                0,
                Change {
                    added: upstream.added.iter().collect(),
                    removed: upstream.removed.iter().collect(),
                    modified: upstream.modified.iter().collect(),
                    modified_before: before.iter().collect(),
                },
            );
            return Ok(changes);
        }
        let known = |state: &State, key: &Vec<u8>| {
            state
                .index
                .get(key)
                .copied()
                .ok_or_else(|| failure("update_by: a row left a bucket that does not exist"))
        };
        let assign = |state: &mut State, key: Vec<u8>| {
            let next = state.buckets.len();
            let bucket = *state.index.entry(key).or_insert(next);
            if bucket == next {
                state.buckets.push(RowSet::empty());
            }
            bucket
        };
        for (row, key) in upstream
            .removed
            .iter()
            .zip(self.keys(&upstream.removed, true)?)
        {
            changes
                .entry(known(state, &key)?)
                .or_default()
                .removed
                .push(row);
        }
        if !upstream.modified.is_empty() {
            let keys = self.keys(&upstream.modified, false)?;
            if upstream.modified_columns.contains_any(&self.by) {
                let old_keys = self.keys(&before, true)?;
                let rows = before.iter().zip(upstream.modified.iter());
                for ((old_row, row), (old_key, key)) in rows.zip(old_keys.into_iter().zip(keys)) {
                    if old_key == key {
                        let bucket = known(state, &key)?;
                        let change = changes.entry(bucket).or_default();
                        change.modified.push(row);
                        change.modified_before.push(old_row);
                    } else {
                        let old_bucket = known(state, &old_key)?;
                        changes.entry(old_bucket).or_default().removed.push(old_row);
                        let bucket = assign(state, key);
                        changes.entry(bucket).or_default().added.push(row);
                    }
                }
            } else {
                let rows = before.iter().zip(upstream.modified.iter());
                for ((old_row, row), key) in rows.zip(keys) {
                    let bucket = known(state, &key)?;
                    let change = changes.entry(bucket).or_default();
                    change.modified.push(row);
                    change.modified_before.push(old_row);
                }
            }
        }
        for (row, key) in upstream
            .added
            .iter()
            .zip(self.keys(&upstream.added, false)?)
        {
            let bucket = assign(state, key);
            changes.entry(bucket).or_default().added.push(row);
        }
        Ok(changes)
    }

    /// The rows of a bucket a rolling window in time recomputes: those whose window holds a
    /// row that was added, removed, or modified in an input column (`changed`), with its
    /// timestamp before and after if that was modified, and the rows added or given a new
    /// timestamp themselves. `changed_before` holds the rows of `changed` before the cycle.
    ///
    /// Behavior: io.deephaven.engine.table.impl.updateby.UpdateByWindowRollingTime#computeAffectedRowsAndOperators
    /// and #computeAffectedRowsTime
    #[allow(clippy::too_many_arguments)]
    fn time_affected(
        &self,
        time: Time,
        rows: &RowSet,
        added: &RowSet,
        removed: &RowSet,
        changed: &RowSet,
        timestamps_modified: bool,
        changed_before: &RowSet,
    ) -> Result<RowSet, TableFailure> {
        let stamped = self.stamped(rows)?;
        // The rows whose window holds a row stamped `stamps`: the window turned around.
        let affected_by =
            |stamps: Vec<Option<i64>>, rev: i64, fwd: i64| stamped.reach(stamps, fwd, rev);
        let mut affected = RowSet::empty();
        if !changed.is_empty() {
            affected.insert(&affected_by(
                self.timestamps(changed, false)?,
                time.rev,
                time.fwd,
            ));
            if timestamps_modified {
                affected.insert(&affected_by(
                    self.timestamps(changed_before, true)?,
                    time.rev,
                    time.fwd,
                ));
                affected.insert(changed);
            }
        }
        let (rev, fwd) = (time.rev.max(0), time.fwd.max(0));
        if !added.is_empty() {
            affected.insert(&affected_by(self.timestamps(added, false)?, rev, fwd));
            affected.insert(added);
        }
        if !removed.is_empty() {
            affected.insert(&affected_by(self.timestamps(removed, true)?, rev, fwd));
        }
        Ok(affected)
    }

    /// Behavior: io.deephaven.engine.table.impl.updateby.UpdateBy.PhasedUpdateProcessor, from
    /// the shift of the result columns to #computeDownstreamUpdate
    fn on_update(
        &self,
        state: &mut State,
        upstream: &TableUpdate,
    ) -> Result<TableUpdate, TableFailure> {
        let shift_error = |e| failure(format!("update_by: {e}"));
        let survivors = self.parent.prev_row_set().minus(&upstream.removed);
        for column in &self.columns {
            let state = column.std_state.iter().flatten();
            for source in std::iter::once(&*column.output).chain(state) {
                source.remove(&upstream.removed);
                if !upstream.shifted.is_empty() {
                    source.apply_shifts(&upstream.shifted, &survivors)?;
                }
            }
        }

        let changes = self.split(state, upstream)?;
        let touched: Vec<usize> = if upstream.shifted.is_empty() {
            changes.keys().copied().collect()
        } else {
            (0..state.buckets.len()).collect()
        };
        let mut modified = upstream.modified.clone();
        let mut dirty_results = BTreeSet::new();
        let unchanged = Change::default();
        let nothing = RowSet::empty();
        for bucket in touched {
            let change = changes.get(&bucket).unwrap_or(&unchanged);
            let added = RowSet::from_keys(change.added.iter().copied());
            let removed = RowSet::from_keys(change.removed.iter().copied());
            let changed = RowSet::from_keys(change.modified.iter().copied());
            let previous = std::mem::replace(&mut state.buckets[bucket], RowSet::empty());
            let mut rows = previous.minus(&removed);
            if !upstream.shifted.is_empty() {
                rows = upstream.shifted.apply(&rows).map_err(shift_error)?;
            }
            rows.insert(&added);
            state.buckets[bucket] = rows;
            let rows = &state.buckets[bucket];
            if rows.is_empty() {
                continue;
            }
            if self.timestamp.is_some() {
                self.stamped(rows)?;
            }
            // Behavior: io.deephaven.engine.table.impl.updateby.UpdateByWindow#processUpdateForContext,
            // where an operation in time is affected by its timestamp column too
            // (io.deephaven.engine.table.impl.updateby.UpdateByOperator#getAffectingColumnNames)
            let adds_or_removes = !added.is_empty() || !removed.is_empty();
            let columns_modified = !changed.is_empty() && !upstream.modified_columns.is_empty();
            let timestamps_modified = columns_modified
                && self
                    .timestamp
                    .is_some_and(|index| upstream.modified_columns.contains(index));
            let input_modified = |member: &usize| {
                let column = &self.columns[*member];
                columns_modified
                    && (upstream.modified_columns.contains_any(&column.reads)
                        || (column.timed && timestamps_modified))
            };
            for (window, members) in &self.windows {
                let dirty: Vec<usize> = if adds_or_removes {
                    members.clone()
                } else {
                    members.iter().copied().filter(input_modified).collect()
                };
                if dirty.is_empty() {
                    continue;
                }
                // Behavior: UpdateByWindowCumulative#computeAffectedRowsAndOperators counts a
                // modified timestamp when any operation of the window is in time.
                let inputs_changed = members.iter().any(input_modified)
                    || (timestamps_modified
                        && members.iter().any(|&member| self.columns[member].timed));
                let affected = match window {
                    Window::Cumulative => cumulative_affected(
                        &previous,
                        rows,
                        upstream,
                        &added,
                        &removed,
                        if inputs_changed { &changed } else { &nothing },
                    ),
                    Window::Ticks(ticks) => ticks_affected(
                        *ticks,
                        &previous,
                        rows,
                        upstream,
                        &added,
                        &removed,
                        if inputs_changed { &changed } else { &nothing },
                    )?,
                    Window::Time(time) => self.time_affected(
                        *time,
                        rows,
                        &added,
                        &removed,
                        if inputs_changed { &changed } else { &nothing },
                        timestamps_modified,
                        &RowSet::from_keys(change.modified_before.iter().copied()),
                    )?,
                };
                if affected.is_empty() {
                    continue;
                }
                self.compute(*window, &dirty, rows, &affected)?;
                modified.insert(&affected);
                dirty_results.extend(dirty.iter().map(|member| self.columns[*member].result));
            }
        }

        modified.remove(&upstream.added);
        let mut modified_columns = if upstream.modified.is_empty() {
            ModifiedColumnSet::empty()
        } else {
            self.transformer.transform(&upstream.modified_columns)
        };
        modified_columns = modified_columns.union(&ModifiedColumnSet::of(dirty_results));
        Ok(TableUpdate {
            added: upstream.added.clone(),
            removed: upstream.removed.clone(),
            modified,
            shifted: upstream.shifted.clone(),
            modified_columns,
        })
    }
}

/// Adds a result column named `name` of type `output_type` to the result definition, in
/// the place of a column of that name if there is one, and to the columns of its window;
/// `index` is the index the column will have among the result columns. Returns its source
/// and its place in the result.
#[allow(clippy::too_many_arguments)]
fn register(
    result: &mut Vec<ColumnDefinition>,
    replaced: &mut BTreeSet<usize>,
    names: &mut BTreeSet<String>,
    duplicates: &mut Vec<String>,
    windows: &mut Vec<(Window, Vec<usize>)>,
    index: usize,
    name: &str,
    output_type: ColumnType,
    window: Window,
    graph: &Option<Arc<dyn oxhaven_contracts::graph::UpdateGraph>>,
) -> (Arc<WritableSource>, usize) {
    if !names.insert(name.to_owned()) {
        duplicates.push(name.to_owned());
    }
    let definition = ColumnDefinition::new(name, output_type.clone());
    let position = match result.iter().position(|c| c.name == name) {
        Some(position) => {
            result[position] = definition;
            replaced.insert(position);
            position
        }
        None => {
            result.push(definition);
            result.len() - 1
        }
    };
    let output = match graph {
        Some(graph) => tracked_source(graph, output_type),
        None => Arc::new(WritableSource::new(output_type)),
    };
    match windows.iter_mut().find(|(w, _)| *w == window) {
        Some((_, members)) => members.push(index),
        None => windows.push((window, vec![index])),
    }
    (output, position)
}

/// The rows of a bucket the cumulative operators recompute: all from the first row that
/// was added, that was modified in an input column (`changed`), or that followed a removed
/// row.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.UpdateByWindowCumulative#computeAffectedRowsAndOperators
/// and #smallestAffectedKey
fn cumulative_affected(
    previous: &RowSet,
    rows: &RowSet,
    upstream: &TableUpdate,
    added: &RowSet,
    removed: &RowSet,
    changed: &RowSet,
) -> RowSet {
    let mut smallest: Option<RowKey> = None;
    if let Some(first_removed) = removed.first_row_key() {
        smallest = match previous.find(first_removed) {
            Some(0) | None => rows.first_row_key(),
            Some(position) => previous
                .get(position - 1)
                .map(|before| upstream.shifted.apply_to_key(before))
                .and_then(|before| rows.find(before))
                .and_then(|position| rows.get(position + 1)),
        };
    }
    for first in [added.first_row_key(), changed.first_row_key()] {
        smallest = match (smallest, first) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        };
    }
    match (smallest, rows.last_row_key()) {
        (Some(first), Some(last)) => rows.sub_set_by_key_range(first, last),
        _ => RowSet::empty(),
    }
}

/// The rows of a bucket a rolling window in rows recomputes: those whose window holds a
/// row that was added, modified in an input column (`changed`) or removed.
///
/// Behavior: io.deephaven.engine.table.impl.updateby.UpdateByWindowRollingTicks#computeAffectedRowsAndOperators
fn ticks_affected(
    ticks: Ticks,
    previous: &RowSet,
    rows: &RowSet,
    upstream: &TableUpdate,
    added: &RowSet,
    removed: &RowSet,
    changed: &RowSet,
) -> Result<RowSet, TableFailure> {
    let positions = |of: &RowSet, within: &RowSet| {
        within
            .invert(of)
            .map_err(|_| failure("update_by: a changed row is not in its bucket"))
    };
    let mut affected = RowSet::empty();
    if !changed.is_empty() {
        let reach = ticks.affected(rows.size(), &positions(changed, rows)?);
        affected.insert(&rows.sub_set_for_positions(&reach, false));
    }
    if !added.is_empty() {
        let reach = ticks
            .at_least(1)
            .affected(rows.size(), &positions(added, rows)?);
        affected.insert(&rows.sub_set_for_positions(&reach, false));
    }
    if !removed.is_empty() {
        let reach = ticks
            .at_least(0)
            .affected(previous.size(), &positions(removed, previous)?);
        // A removed row is itself in reach; it has no current key.
        let reached = previous.sub_set_for_positions(&reach, false).minus(removed);
        let moved = upstream
            .shifted
            .apply(&reached)
            .map_err(|e| failure(format!("update_by: {e}")))?;
        affected.insert(&moved.intersect(rows));
    }
    Ok(affected)
}
