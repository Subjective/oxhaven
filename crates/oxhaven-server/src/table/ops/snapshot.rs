//! `Snapshot` and `SnapshotWhen`: a static copy of a table, and a copy that is renewed when
//! a trigger table changes.

use super::prelude::*;
use oxhaven_engine::ops::snapshot::{
    snapshot, snapshot_when, SnapshotError, SnapshotWhenOptions, StampColumn,
};
use oxhaven_proto::grpc::{SnapshotTableRequest, SnapshotWhenTableRequest};

/// Java's `UnsupportedOperationException` from the options is answered `UNIMPLEMENTED`
/// (`SnapshotWhenTableGrpcImpl#validateRequest`); the handler finds it before the engine
/// does. Everything else the engine refuses is an exception out of the engine, which the
/// recordings show as `INVALID_ARGUMENT` for a stamp column that the trigger lacks or that
/// clashes with a base column.
///
/// Behavior: `io.deephaven.server.session.SessionService.ObfuscatingErrorTransformer#transform`
impl From<SnapshotError> for ExportError {
    fn from(error: SnapshotError) -> Self {
        match error {
            SnapshotError::Unsupported(message) => {
                ExportError::Status(Status::unimplemented(message))
            }
            other => ExportError::Internal(other.to_string()),
        }
    }
}

/// Behavior: `io.deephaven.server.table.ops.SnapshotTableGrpcImpl`
pub struct Snapshot;

impl TableOp for Snapshot {
    type Request = SnapshotTableRequest;

    fn result_id(request: &SnapshotTableRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    fn sources(request: &SnapshotTableRequest) -> Vec<TableReference> {
        vec![source(&request.source_id)]
    }

    /// Behavior: `SnapshotTableGrpcImpl#validateRequest`
    fn validate(request: &SnapshotTableRequest) -> Result<(), Status> {
        let Some(source) = &request.source_id else {
            return Err(missing_field("SnapshotTableRequest", "source_id", 2));
        };
        validate_reference(source)
    }

    /// Behavior: `SnapshotTableGrpcImpl#create`
    fn create(
        _context: &OpContext,
        _request: &SnapshotTableRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        Ok(snapshot(&sources[0])?)
    }
}

/// A stamp column as the request gives it, `Name` or `Name=Source`, split at the first `=`
/// with each side trimmed and checked as a column name. A name that is not one is an
/// exception, not a status, in Java.
///
/// Behavior: `io.deephaven.api.JoinAddition#parse`, `io.deephaven.api.ColumnName#parse`
fn stamp_column(text: &str) -> Result<StampColumn, String> {
    let (name, source) = match text.split_once('=') {
        Some((name, source)) => (java_trim(name), java_trim(source)),
        None => (java_trim(text), java_trim(text)),
    };
    validate_column_name(name)?;
    validate_column_name(source)?;
    Ok(StampColumn::new(name, source))
}

/// The options of a request, read in Java's order: the stamp columns, then the check that
/// `history` comes alone.
///
/// Behavior: `SnapshotWhenTableGrpcImpl#options`,
/// `io.deephaven.api.snapshot.SnapshotWhenOptions#checkHistory`
fn options(request: &SnapshotWhenTableRequest) -> Result<SnapshotWhenOptions, Status> {
    let stamp_columns = request
        .stamp_columns
        .iter()
        .map(|text| stamp_column(text))
        .collect::<Result<Vec<_>, _>>()
        .map_err(internal_status)?;
    if request.history && (request.initial || request.incremental) {
        return Err(Status::unimplemented(
            "snapshotWhen with history does not currently support incremental nor initial. \
             See https://github.com/deephaven/deephaven-core/issues/3260.",
        ));
    }
    Ok(SnapshotWhenOptions {
        initial: request.initial,
        incremental: request.incremental,
        history: request.history,
        stamp_columns,
    })
}

/// Behavior: `io.deephaven.server.table.ops.SnapshotWhenTableGrpcImpl`
pub struct SnapshotWhen;

impl TableOp for SnapshotWhen {
    type Request = SnapshotWhenTableRequest;

    fn result_id(request: &SnapshotWhenTableRequest) -> Option<&Ticket> {
        request.result_id.as_ref()
    }

    /// The base, then the trigger.
    ///
    /// Behavior: `SnapshotWhenTableGrpcImpl#refs`
    fn sources(request: &SnapshotWhenTableRequest) -> Vec<TableReference> {
        vec![source(&request.base_id), source(&request.trigger_id)]
    }

    /// Behavior: `SnapshotWhenTableGrpcImpl#validateRequest`
    fn validate(request: &SnapshotWhenTableRequest) -> Result<(), Status> {
        let Some(base) = &request.base_id else {
            return Err(missing_field("SnapshotWhenTableRequest", "base_id", 2));
        };
        let Some(trigger) = &request.trigger_id else {
            return Err(missing_field("SnapshotWhenTableRequest", "trigger_id", 3));
        };
        validate_reference(base)?;
        validate_reference(trigger)?;
        options(request).map(|_| ())
    }

    /// Behavior: `SnapshotWhenTableGrpcImpl#create`
    fn create(
        _context: &OpContext,
        request: &SnapshotWhenTableRequest,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        let options = options(request)?;
        Ok(snapshot_when(&sources[0], &sources[1], &options)?)
    }
}
