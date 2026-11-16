//! Figures for the `deephaven` package: the handle its shown `Figure` wraps, and the
//! function that makes one.
//!
//! `deephaven.plot.figure` keeps what a script plots and, when the figure is shown,
//! describes it as the protobuf JSON of a `FigureDescriptor`. The description crosses to
//! Rust as that text, as a table operation does (`table_op`), and is read with the
//! descriptors the gRPC stubs were generated from.
//!
//! Behavior: `io.deephaven.plot.FigureImpl#show`, whose result is a `FigureWidget`

use std::sync::{Arc, OnceLock};

use oxhaven_proto::script::FigureDescriptor;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, SerializeOptions};
use pyo3::exceptions::PyRuntimeError;
use pyo3::prelude::*;

use super::partitioned::PartitionedHandle;
use super::TableHandle;
use crate::figure::{Figure, FigureWidget};

/// A reference to a figure that was shown, as Python holds it.
#[pyclass(frozen, module = "_oxhaven")]
pub(super) struct FigureHandle {
    pub(super) figure: Figure,
}

fn descriptor() -> &'static MessageDescriptor {
    static DESCRIPTOR: OnceLock<MessageDescriptor> = OnceLock::new();
    DESCRIPTOR.get_or_init(|| {
        DescriptorPool::decode(oxhaven_proto::FILE_DESCRIPTOR_SET)
            .expect("the descriptors the stubs were generated from decode")
            .get_message_by_name("io.deephaven.proto.backplane.script.grpc.FigureDescriptor")
            .expect("the console protocol has figures")
    })
}

/// Reads a `FigureDescriptor` from its protobuf JSON form.
fn parse(json: &str) -> Result<FigureDescriptor, String> {
    let failed = |error: &dyn std::fmt::Display| format!("not a figure description: {error}");
    let mut deserializer = serde_json::Deserializer::from_str(json);
    let message = DynamicMessage::deserialize(descriptor().clone(), &mut deserializer)
        .map_err(|error| failed(&error))?;
    deserializer.end().map_err(|error| failed(&error))?;
    message.transcode_to().map_err(|error| failed(&error))
}

#[pymethods]
impl FigureHandle {
    /// The description of the figure, as protobuf JSON with the field names of the
    /// protocol and every field that is set.
    fn description(&self) -> PyResult<String> {
        let mut message = DynamicMessage::new(descriptor().clone());
        message
            .transcode_from(self.figure.descriptor())
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        let options = SerializeOptions::new().use_proto_field_name(true);
        let mut text = Vec::new();
        let mut serializer = serde_json::Serializer::new(&mut text);
        message
            .serialize_with_options(&mut serializer, &options)
            .map_err(|error| PyRuntimeError::new_err(error.to_string()))?;
        String::from_utf8(text).map_err(|error| PyRuntimeError::new_err(error.to_string()))
    }

    /// The tables the figure draws from, in the order its sources name them.
    fn tables(&self) -> Vec<TableHandle> {
        let tables = self.figure.tables().iter().cloned();
        tables.map(|table| TableHandle { table }).collect()
    }

    /// The partitioned tables the figure draws from, in the order its sources name them.
    fn partitioned(&self) -> Vec<PartitionedHandle> {
        let partitioned = self.figure.partitioned().iter().cloned();
        partitioned
            .map(|partitioned| PartitionedHandle { partitioned })
            .collect()
    }
}

/// Makes the figure that `description` describes, the protobuf JSON of a
/// `FigureDescriptor` whose sources name `tables` and `partitioned` by their places.
#[pyfunction]
fn figure(
    description: &str,
    tables: Vec<PyRef<'_, TableHandle>>,
    partitioned: Vec<PyRef<'_, PartitionedHandle>>,
) -> PyResult<FigureHandle> {
    let descriptor = parse(description).map_err(PyRuntimeError::new_err)?;
    let tables = tables.iter().map(|handle| handle.table.clone()).collect();
    let partitioned = partitioned
        .iter()
        .map(|handle| handle.partitioned.clone())
        .collect();
    let widget =
        FigureWidget::new(descriptor, tables, partitioned).map_err(PyRuntimeError::new_err)?;
    Ok(FigureHandle {
        figure: Arc::new(widget),
    })
}

pub(super) fn register(native: &Bound<'_, PyModule>) -> PyResult<()> {
    native.add_class::<FigureHandle>()?;
    native.add_function(wrap_pyfunction!(figure, native)?)?;
    Ok(())
}
