//! `deephaven.stream`: `blink_to_append_only` and the table publisher of
//! `deephaven.stream.table_publisher`, over the engine's stream tables
//! (`oxhaven_engine::table::stream_table`).
//!
//! The Python callbacks keep the lock rule of [`super::listener`]: the flush callback runs
//! on the cycle thread, which holds the graph's lock, and takes the interpreter lock for
//! the call. The shutdown callback runs on the thread that ends the stream: the cycle
//! thread that delivers a failure, or the thread that drops the table, which may hold the
//! interpreter lock already. Reading a table to publish it releases the interpreter lock
//! before it waits for the graph's.

use std::sync::Arc;

use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::{ColumnDefinition, TableDefinition};
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_engine::table::{
    blink_to_append_only as append_only, stream_table, StreamCallbacks, StreamPublisher,
};
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use super::{runtime, TableHandle};
use crate::schema::column_type_for_java_names;

#[cfg(test)]
mod tests;

/// The write side of a table publisher.
///
/// Behavior: io.deephaven.stream.TablePublisher
#[pyclass(frozen, module = "_oxhaven")]
struct PublisherHandle {
    publisher: StreamPublisher,
}

#[pymethods]
impl PublisherHandle {
    /// Behavior: io.deephaven.stream.TablePublisher#isAlive
    fn is_alive(&self) -> bool {
        self.publisher.is_alive()
    }

    /// Behavior: io.deephaven.stream.TablePublisher#add
    fn add(&self, py: Python<'_>, table: PyRef<'_, TableHandle>) -> PyResult<()> {
        let table: Arc<dyn Table> = table.table.clone();
        py.detach(|| self.publisher.add_table(&table))
            .map_err(|failure| PyRuntimeError::new_err(failure.message))
    }

    /// Behavior: io.deephaven.stream.TablePublisher#publishFailure, which the reference
    /// package calls with a `java.lang.RuntimeException` of the text of the failure
    fn publish_failure(&self, message: String) {
        self.publisher.fail(TableFailure {
            message: format!("java.lang.RuntimeException: {message}"),
        });
    }
}

/// A blink table with the columns `(name, Java class name of the type)` and its publisher.
/// `on_flush` is called without arguments at the start of every update cycle; an exception
/// in it fails the table. `on_shutdown` is called once, when the table is released or has
/// failed.
///
/// Behavior: io.deephaven.stream.TablePublisher#of, and the `chunkSize` check of
/// io.deephaven.stream.TableStreamPublisherImpl, whose chunk size has no other effect here
#[pyfunction]
fn table_publisher(
    columns: Vec<(String, String)>,
    on_flush: Option<Py<PyAny>>,
    on_shutdown: Option<Py<PyAny>>,
    chunk_size: i64,
) -> PyResult<(TableHandle, PublisherHandle)> {
    if chunk_size <= 0 {
        return Err(PyRuntimeError::new_err(
            "java.lang.IllegalArgumentException: chunkSize must be positive",
        ));
    }
    let mut definitions = Vec::with_capacity(columns.len());
    for (name, type_name) in columns {
        let column_type = column_type_for_java_names(&type_name, None).ok_or_else(|| {
            PyValueError::new_err(format!("the engine has no column type for {type_name}"))
        })?;
        definitions.push(ColumnDefinition::new(name, column_type));
    }
    let definition =
        TableDefinition::new(definitions).map_err(|e| PyValueError::new_err(e.to_string()))?;
    let callbacks = StreamCallbacks {
        on_flush: on_flush.map(|callback| {
            Box::new(move || {
                // Behavior: StreamToBlinkTableAdapter#run, which fails the table with what
                // the flush throws
                Python::attach(|py| callback.call0(py).map(drop)).map_err(|error| TableFailure {
                    message: format!("java.lang.RuntimeException: {error}"),
                })
            }) as Box<dyn Fn() -> Result<(), TableFailure> + Send + Sync>
        }),
        on_shutdown: on_shutdown.map(|callback| {
            Box::new(move || {
                Python::attach(|py| {
                    if let Err(error) = callback.call0(py) {
                        // Java throws this out of the update source, and its update graph
                        // then shuts the server down (observed on 42.9). Here it is logged.
                        tracing::error!(
                            "the on_shutdown callback of a table publisher raised: {error}; \
                             Java's PeriodicUpdateGraph shuts the server down here"
                        );
                    }
                })
            }) as Box<dyn FnOnce() + Send>
        }),
    };
    let graph: Arc<dyn UpdateGraph> = runtime()?.context.graph.clone();
    let (table, publisher) = stream_table(graph, definition, callbacks);
    Ok((TableHandle { table }, PublisherHandle { publisher }))
}

/// Behavior: io.deephaven.engine.table.impl.BlinkTableTools#blinkToAppendOnly(Table)
#[pyfunction]
fn blink_to_append_only(py: Python<'_>, table: PyRef<'_, TableHandle>) -> PyResult<TableHandle> {
    let table: Arc<dyn Table> = table.table.clone();
    py.detach(|| append_only(&table, u64::MAX))
        .map(|table| TableHandle { table })
        .map_err(|failure| PyRuntimeError::new_err(failure.message))
}

pub(super) fn register(native: &Bound<'_, PyModule>) -> PyResult<()> {
    native.add_class::<PublisherHandle>()?;
    native.add_function(wrap_pyfunction!(table_publisher, native)?)?;
    native.add_function(wrap_pyfunction!(blink_to_append_only, native)?)?;
    Ok(())
}
