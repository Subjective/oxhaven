//! The operations of a `PartitionedTableProxy`: a table operation applied to every
//! constituent of a partitioned table, as a transform of it.
//!
//! The Python class runs the `Table` method once, on a table without rows that has the
//! columns of a constituent, and records the operations the method sends to the engine
//! ([`Recorded`]). The transform's function is those operations run again on a
//! constituent: Rust, with no Python in the update cycle beyond what a formula calls.
//!
//! Java's proxy runs its function in an execution context that has a query scope, and
//! was observed to bind the variables of a formula once, when the proxy operation is
//! made: a constituent that appears in a later cycle is given the values of then
//! (`docs/PARTITIONED.md`, "The proxy"). So the recording holds a copy of the caller's
//! variables, and every run of it compiles its formulas with them.

use std::sync::Arc;

use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_engine::ops::filter::CheckedFilters;
use oxhaven_engine::ops::partitioned::{require_matching_keys, PartitionedTable, Transformer};
use oxhaven_proto::grpc::batch_table_request::operation::Op;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyDict;
use serde_json::Value;

use super::ops::{failure, guarded, local_operation, LocalOperation};
use super::partitioned::PartitionedHandle;
use super::{
    error_text, parse_operation, runtime, udf, CallerScope, Runtime, TableHandle, NO_QUERY_SCOPE,
};
use crate::session::ExportError;
use crate::table::run_local;

/// A table of a recording: an argument of the transform's function, a table the
/// operations read besides, or what an earlier step made.
#[derive(Clone, Copy)]
enum Source {
    Input(usize),
    External(usize),
    Step(usize),
}

enum Work {
    /// An operation of the table service.
    Service(Op),
    /// A local operation (`console/python/ops.rs`) and its arguments.
    Local(LocalOperation, Value),
}

struct Step {
    work: Work,
    sources: Vec<Source>,
}

/// The operations a `Table` method sent to the engine, to run again on other tables.
struct Recorded {
    runtime: Arc<Runtime>,
    steps: Vec<Step>,
    externals: Vec<Arc<dyn Table>>,
    result: Source,
    /// The variables of the code that called the proxy, as they were then.
    scope: Py<PyDict>,
}

type Reference = (String, usize);
/// A step as Python hands it over: `"table"` or `"local"`, the name of a local
/// operation, the operation or its arguments as JSON, and the sources.
type RecordedStep = (String, String, String, Vec<Reference>);

impl Recorded {
    fn read(
        steps: Vec<RecordedStep>,
        result: Reference,
        externals: Vec<PyRef<'_, TableHandle>>,
        inputs: usize,
        scope: Py<PyDict>,
    ) -> PyResult<Recorded> {
        let externals: Vec<Arc<dyn Table>> = externals
            .iter()
            .map(|handle| handle.table.clone())
            .collect();
        let mut read: Vec<Step> = Vec::with_capacity(steps.len());
        let source = |(kind, index): &Reference, made: usize| {
            let (source, known) = match kind.as_str() {
                "input" => (Source::Input(*index), inputs),
                "external" => (Source::External(*index), externals.len()),
                "step" => (Source::Step(*index), made),
                _ => return Err(PyValueError::new_err(format!("not a source: {kind}"))),
            };
            match *index < known {
                true => Ok(source),
                false => Err(PyValueError::new_err(format!("no {kind} {index}"))),
            }
        };
        for (kind, name, json, sources) in &steps {
            let work = match kind.as_str() {
                "table" => Work::Service(parse_operation(json).map_err(PyValueError::new_err)?),
                "local" => {
                    let operation = local_operation(name).ok_or_else(|| {
                        PyValueError::new_err(format!("not a local operation: {name}"))
                    })?;
                    let args = serde_json::from_str(json).map_err(|error| {
                        PyValueError::new_err(format!("not the arguments of {name}: {error}"))
                    })?;
                    Work::Local(operation, args)
                }
                _ => return Err(PyValueError::new_err(format!("not a step: {kind}"))),
            };
            let sources = sources
                .iter()
                .map(|reference| source(reference, read.len()))
                .collect::<PyResult<Vec<_>>>()?;
            read.push(Step { work, sources });
        }
        let result = source(&result, read.len())?;
        Ok(Recorded {
            runtime: runtime()?,
            steps: read,
            externals,
            result,
            scope,
        })
    }

    /// What the recorded operations make of `inputs`.
    fn run(&self, inputs: &[Arc<dyn Table>]) -> Result<Arc<dyn Table>, ExportError> {
        let context = &self.runtime.context;
        let mut made: Vec<Arc<dyn Table>> = Vec::with_capacity(self.steps.len());
        let table = |source: Source, made: &[Arc<dyn Table>]| match source {
            Source::Input(index) => inputs[index].clone(),
            Source::External(index) => self.externals[index].clone(),
            Source::Step(index) => made[index].clone(),
        };
        for step in &self.steps {
            let sources: Vec<Arc<dyn Table>> = step
                .sources
                .iter()
                .map(|&source| table(source, &made))
                .collect();
            made.push(match &step.work {
                Work::Service(op) => run_local(context, op.clone(), &sources)?,
                Work::Local(operation, args) => {
                    guarded(&sources, || operation(context, args, &sources))?
                }
            });
        }
        Ok(table(self.result, &made))
    }

    /// Runs `work` as a table operation that the code of the recording's scope called:
    /// its formulas may name the variables of that scope.
    fn scoped<T: Send>(&self, work: impl FnOnce() -> T + Send) -> T {
        Python::attach(|py| {
            let _scope = CallerScope::enter(Some(self.scope.clone_ref(py)));
            let _calls = udf::Operation::enter(py);
            // a proxy has a query scope also where a transform's function has none
            let before = NO_QUERY_SCOPE.replace(false);
            let made = py.detach(work);
            NO_QUERY_SCOPE.set(before);
            made
        })
    }

    /// The recording as the function of a transform. It is called under the update
    /// graph's lock or in its cycle, and takes the interpreter lock for the scope only:
    /// the order is graph, interpreter (`docs/PYTHON_UDF.md`, "Locks").
    ///
    /// Behavior: the `transformer` of
    /// `io.deephaven.engine.table.impl.partitioned.PartitionedTableProxyImpl#basicTransform`
    /// and `#complexTransform`, applied in the execution context of
    /// `#getOrCreateExecutionContext`
    fn transformer(self: &Arc<Self>) -> Transformer {
        let recorded = self.clone();
        Arc::new(move |tables: &[Arc<dyn Table>]| {
            let made = recorded.scoped(|| recorded.run(tables));
            made.map(Some).map_err(|error| TableFailure {
                message: error_text(error),
            })
        })
    }
}

/// The partitioned table of a proxy operation on `target`: its constituents are what
/// the recorded operations make of each constituent, or, with `other`, of each pair of
/// constituents of `target` and `other` that have the same key.
///
/// - `externals` are the tables the operations read besides; a refreshing one is a
///   dependency of the transform.
/// - `require_matching_keys` and `join_keys`, the columns a join matches exactly in the
///   constituents of `target` and of `other`, are the checks of an operation with
///   `other`: they fail the call, or in a cycle the result.
/// - `consistent` are the key columns that the operation leaves as they are.
///
/// Behavior: `PartitionedTableProxyImpl#basicTransform` and `#complexTransform`
#[pyfunction]
#[pyo3(signature = (target, other, steps, result, externals, scope, consistent, require_matching_keys, join_keys))]
#[allow(clippy::too_many_arguments)]
fn proxy_transform(
    py: Python<'_>,
    target: &PartitionedHandle,
    other: Option<PyRef<'_, PartitionedHandle>>,
    steps: Vec<RecordedStep>,
    result: Reference,
    externals: Vec<PyRef<'_, TableHandle>>,
    scope: Py<PyDict>,
    consistent: Vec<String>,
    require_matching_keys: bool,
    join_keys: Option<(Vec<String>, Vec<String>)>,
) -> PyResult<PartitionedHandle> {
    let inputs = 1 + usize::from(other.is_some());
    let recorded = Arc::new(Recorded::read(steps, result, externals, inputs, scope)?);
    let dependencies: Vec<Arc<dyn Table>> = recorded
        .externals
        .iter()
        .filter(|table| table.is_refreshing())
        .cloned()
        .collect();
    let target = &target.partitioned;
    let other = other.as_ref().map(|other| other.partitioned.clone());
    let function = recorded.transformer();
    let made = py.detach(|| {
        let mut locked = vec![target.table().clone()];
        locked.extend(other.iter().map(|other| other.table().clone()));
        locked.extend(dependencies.iter().cloned());
        guarded(&locked, || {
            let made = match &other {
                None => target.transform(function, &dependencies)?,
                Some(other) => {
                    let (mut lhs, mut rhs): (PartitionedTable, PartitionedTable) =
                        ((**target).clone(), (**other).clone());
                    if require_matching_keys {
                        (lhs, rhs) = require_matching_keys_of(&lhs, &rhs)?;
                    }
                    if let Some((left, right)) = &join_keys {
                        lhs = lhs.require_distinct_join_keys(left)?;
                        rhs = rhs.require_distinct_join_keys(right)?;
                    }
                    lhs.partitioned_transform(&rhs, function, &dependencies)?
                }
            };
            Ok(made.with_consistent_key_columns(&consistent))
        })
    });
    made.map(|partitioned| PartitionedHandle {
        partitioned: Arc::new(partitioned),
    })
    .map_err(failure)
}

fn require_matching_keys_of(
    lhs: &PartitionedTable,
    rhs: &PartitionedTable,
) -> Result<(PartitionedTable, PartitionedTable), ExportError> {
    Ok(require_matching_keys(lhs, rhs)?)
}

/// What each of `filters`, filter strings of a `where`, reads of a constituent of
/// `target`: the columns it reads at the current row, and whether it reads a column at
/// other rows. `scope` is as in [`proxy_transform`].
///
/// Behavior: the `WhereFilter#init`, `#getColumns` and `#getColumnArrays` of
/// `PartitionedTableProxyImpl#where` and `#isValidAgainstKeyColumns`
#[pyfunction]
fn proxy_filter_reads(
    target: &PartitionedHandle,
    filters: Vec<String>,
    scope: Py<PyDict>,
) -> PyResult<Vec<(Vec<String>, bool)>> {
    let recorded = Recorded {
        runtime: runtime()?,
        steps: Vec::new(),
        externals: Vec::new(),
        result: Source::Input(0),
        scope,
    };
    let definition = target.partitioned.constituent_definition();
    let compiler = recorded.runtime.context.compiler.clone();
    let checked = recorded.scoped(|| CheckedFilters::parse(&filters, definition, &*compiler));
    let checked = checked.map_err(|error| failure(ExportError::Internal(error.to_string())))?;
    Ok(checked
        .reads()
        .iter()
        .map(|reads| (reads.columns.clone(), reads.arrays))
        .collect())
}

/// Adds the functions of this module to the native module `_oxhaven`.
pub(super) fn register(native: &Bound<'_, PyModule>) -> PyResult<()> {
    native.add_function(wrap_pyfunction!(proxy_transform, native)?)?;
    native.add_function(wrap_pyfunction!(proxy_filter_reads, native)?)?;
    Ok(())
}
