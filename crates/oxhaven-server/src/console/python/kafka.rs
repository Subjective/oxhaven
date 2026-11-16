//! `deephaven.stream.kafka.consumer` reaches the Kafka consumer here.
//!
//! Java has no gRPC call that consumes a topic, so this is not a table service operation:
//! the Python module calls [`kafka_consume`] with the arguments of
//! `deephaven.stream.kafka.consumer.consume`, and the work is `oxhaven_kafka::consume`
//! (`docs/KAFKA.md`).

use std::sync::Arc;

use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_kafka::{ConsumeOptions, InitialOffset, KafkaError, Offsets, Spec, TableType};
use pyo3::exceptions::{PyNotImplementedError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use serde_json::Value;

use super::TableHandle;
use crate::schema::column_type_for_java_names;

fn exception(error: KafkaError) -> PyErr {
    let text = error.to_string();
    match error {
        KafkaError::NotPorted { .. } => PyNotImplementedError::new_err(text),
        _ => PyRuntimeError::new_err(text),
    }
}

fn column_type(name: &str) -> PyResult<ColumnType> {
    column_type_for_java_names(name, None)
        .ok_or_else(|| PyValueError::new_err(format!("the engine has no column type for {name}")))
}

/// A key or value spec, as `deephaven.stream.kafka.consumer.KeyValueSpec` writes it:
/// `{"kind": "ignore"}`, `{"kind": "simple", "column": name or null, "type": Java class
/// name or null}` or `{"kind": "json", "columns": [[name, Java class name]], "mapping":
/// [[field, column]] or null}`.
fn spec(json: &str) -> PyResult<Spec> {
    let bad = || PyValueError::new_err(format!("not a key or value spec: {json}"));
    let value: Value = serde_json::from_str(json).map_err(|_| bad())?;
    let text = |value: &Value| value.as_str().map(str::to_owned).ok_or_else(bad);
    let pairs = |value: &Value| -> PyResult<Vec<(String, String)>> {
        let pair = |pair: &Value| Ok((text(&pair[0])?, text(&pair[1])?));
        value.as_array().ok_or_else(bad)?.iter().map(pair).collect()
    };
    match value["kind"].as_str() {
        Some("ignore") => Ok(Spec::Ignore),
        Some("simple") => Ok(Spec::Simple {
            column: value["column"].as_str().map(str::to_owned),
            data_type: value["type"].as_str().map(column_type).transpose()?,
        }),
        Some("json") => {
            let mut columns = Vec::new();
            for (name, type_name) in pairs(&value["columns"])? {
                columns.push((name, column_type(&type_name)?));
            }
            let mapping = match &value["mapping"] {
                Value::Null => None,
                mapping => Some(pairs(mapping)?),
            };
            Ok(Spec::Json { columns, mapping })
        }
        _ => Err(bad()),
    }
}

/// Consumes `topic` into a table. `all_offsets` is where to start in every partition, as
/// Java's `SEEK_TO_BEGINNING`, `DONT_SEEK` or `SEEK_TO_END`; without it `offsets` says so
/// per partition. `table_type` is `blink`, `append` or `ring`, the last with `capacity`.
#[pyfunction]
#[allow(clippy::too_many_arguments)]
pub(super) fn kafka_consume(
    py: Python<'_>,
    config: Vec<(String, String)>,
    topic: &str,
    partitions: Option<Vec<i32>>,
    all_offsets: Option<i64>,
    offsets: Vec<(i32, i64)>,
    key_spec: &str,
    value_spec: &str,
    table_type: &str,
    capacity: i32,
) -> PyResult<TableHandle> {
    let options = ConsumeOptions {
        config,
        topic: topic.to_owned(),
        partitions,
        offsets: match all_offsets {
            Some(offset) => Offsets::Everywhere(InitialOffset::from_java(offset)),
            None => Offsets::ByPartition(offsets),
        },
        key: spec(key_spec)?,
        value: spec(value_spec)?,
        table_type: match table_type {
            "blink" => TableType::Blink,
            "append" => TableType::Append,
            "ring" => TableType::Ring(capacity),
            other => return Err(PyValueError::new_err(format!("no table type {other}"))),
        },
    };
    let graph: Arc<dyn UpdateGraph> = super::runtime()?.context.graph.clone();
    let table = py
        .detach(|| oxhaven_kafka::consume(graph, &options))
        .map_err(exception)?;
    Ok(TableHandle { table })
}
