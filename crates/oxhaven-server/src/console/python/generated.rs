//! Function generated tables: what `deephaven.table_factory.function_generated_table` is
//! built on.
//!
//! The table's rows are the rows of the table a Python function returns. The function is
//! called once when the table is made and then by the update cycle: after the source
//! tables have changed, or while the graph's sources refresh once the interval has passed
//! (`oxhaven_engine::table::generated_from_sources` and `generated_on_interval`).
//!
//! # The lock rule
//!
//! A generated table is the third thing, after a formula and a listener, that runs Python
//! inside a cycle, and it keeps the rule of `listener.rs` the same way: update graph lock
//! first, interpreter lock second.
//!
//! - **In a cycle** the function is called on the cycle thread, which holds the graph's
//!   exclusive lock and takes the interpreter lock for the call. Table operations in the
//!   function take no lock there; `shared_lock`, `await_update` and a blocking write to an
//!   input table are refused, as in a listener.
//! - **Making the table** releases the interpreter lock first. The engine then takes the
//!   graph's shared lock where it needs it (to subscribe to the source tables, and to read
//!   a refreshing table the function returned) and the first call of the function takes
//!   the interpreter lock under it.
//!
//! The test `tests::a_generated_table_and_a_command_make_progress_together` runs a command
//! that makes such tables and writes to their source against the cycles that call their
//! functions.

use std::sync::Arc;

use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_engine::table::{generated_from_sources, generated_on_interval, TableGenerator};
use pyo3::prelude::*;

use super::ops::{failure, guarded};
use super::{runtime, TableHandle};
use crate::session::ExportError;

/// The table generator of a Python function: `function` takes no arguments and returns
/// the handle of a table and `None`, or `None` and the text of what it raised.
///
/// Behavior: the `Supplier` that `deephaven.table_factory.function_generated_table` makes
/// of its `table_generator_function`
fn generator(function: Py<PyAny>) -> TableGenerator {
    Arc::new(move || {
        // graph lock first: whoever calls this holds it, or needs none
        Python::attach(|py| {
            let raised = |error: PyErr| TableFailure {
                message: format!("Error in Python interpreter:\n{error}"),
            };
            let returned = function.bind(py).call0().map_err(raised)?;
            let (handle, text): (Option<PyRef<'_, TableHandle>>, Option<String>) =
                returned.extract().map_err(raised)?;
            match (handle, text) {
                (Some(handle), _) => Ok(handle.table.clone()),
                (None, text) => Err(TableFailure {
                    message: text.unwrap_or_else(|| "the function returned no table".to_owned()),
                }),
            }
        })
    })
}

/// Makes the table that `function` generates: again whenever one of `sources` changes,
/// or every `interval_millis` milliseconds of the server's clock when there are no
/// sources. `function` is as [`generator`] takes it.
///
/// Behavior: `io.deephaven.engine.table.impl.util.FunctionGeneratedTableFactory#create(Supplier, Table...)`
/// and `#create(Supplier, int)`
#[pyfunction]
fn function_generated_table(
    py: Python<'_>,
    function: Py<PyAny>,
    sources: Option<Vec<PyRef<'_, TableHandle>>>,
    interval_millis: Option<i32>,
) -> PyResult<TableHandle> {
    let runtime = runtime()?;
    let generate = generator(function);
    let sources: Option<Vec<Arc<dyn Table>>> =
        sources.map(|handles| handles.iter().map(|handle| handle.table.clone()).collect());
    let context = &runtime.context;
    // the rule: wait for the graph lock without the interpreter lock
    let made = py.detach(|| {
        guarded(&[], || {
            let table = match (&sources, interval_millis) {
                (Some(sources), _) => generated_from_sources(generate, sources)?,
                (None, Some(interval)) => generated_on_interval(
                    generate,
                    context.graph.clone(),
                    context.clock.clone(),
                    interval,
                )?,
                (None, None) => {
                    return Err(ExportError::Internal(
                        "neither source tables nor an interval".to_owned(),
                    ))
                }
            };
            Ok(table as Arc<dyn Table>)
        })
    });
    made.map(|table| TableHandle { table }).map_err(failure)
}

pub(super) fn register(native: &Bound<'_, PyModule>) -> PyResult<()> {
    native.add_function(wrap_pyfunction!(function_generated_table, native)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
