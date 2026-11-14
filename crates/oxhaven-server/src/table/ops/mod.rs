//! The table operations: one handler per kind of `BatchTableRequest.Operation`.
//!
//! # Adding an operation
//!
//! 1. Add one file here, say `sort.rs`, with a unit struct that implements [`TableOp`]:
//!
//!    ```text
//!    use super::prelude::*;
//!    use oxhaven_proto::grpc::SortTableRequest;
//!
//!    pub struct Handler;
//!
//!    impl TableOp for Handler {
//!        type Request = SortTableRequest;
//!
//!        fn result_id(request: &SortTableRequest) -> Option<&Ticket> {
//!            request.result_id.as_ref()
//!        }
//!
//!        fn sources(request: &SortTableRequest) -> Vec<TableReference> {
//!            vec![source(&request.source_id)]
//!        }
//!
//!        fn create(
//!            context: &OpContext,
//!            request: &SortTableRequest,
//!            sources: &[Arc<dyn Table>],
//!        ) -> Result<Arc<dyn Table>, ExportError> {
//!            let parent = &sources[0];
//!            require_columns(parent.definition(), request.sorts.iter().map(|s| &s.column_name))?;
//!            Ok(oxhaven_engine::ops::sort::sort(parent, ...)?)
//!        }
//!    }
//!    ```
//!
//! 2. Add one line to the `handlers!` list at the bottom of this file:
//!    `sort { Sort => Handler }`. A file that serves several operations lists them all:
//!    `select { Update => Update, View => View }`.
//!
//! Nothing else changes: the one-shot RPC, `Batch`, the export, the schema in the response
//! and the status of an operation that has no handler all come from the service.
//!
//! # What a handler gets and what it returns
//!
//! - `sources` holds the tables named by [`TableOp::sources`], in that order, already
//!   resolved from tickets or batch offsets. A source that is not a table never reaches
//!   the handler.
//! - `create` runs on an export thread. If any source is refreshing it runs under the
//!   update graph's shared lock, so the handler reads a consistent state and no cycle runs
//!   between its read and its subscription.
//! - Errors decide the status code the client sees, which the oracle compares with Java's:
//!   - `?` on a `TableFailure`, a `FormulaError` or the error of an engine operator gives
//!     what Java gives for an exception out of the engine: `INVALID_ARGUMENT` with only an
//!     error id, the text in the log. The conversions are in one place, in
//!     [`support`](super::support).
//!   - An `unported!` panic in the engine is answered `UNIMPLEMENTED` with the name of the
//!     Java behavior that is missing, so a request cannot take the server down. Where the
//!     handler can see that a request needs something that is not ported, it says so
//!     itself with [`not_ported`](super::support::not_ported).
//!   - A `Status` is sent as it is. Use one where the Java handler throws a status itself;
//!     [`require_columns`](super::support::require_columns) is the common case
//!     (`FAILED_PRECONDITION`, as `SortTableGrpcImpl` and `SelectDistinctGrpcImpl` do).
//!   - [`TableOp::validate`] runs before the export exists, on the request alone. A status
//!     from it fails the RPC without consuming the result ticket, and fails a whole batch.
//!
//! Behavior: `io.deephaven.server.table.ops.GrpcTableOperation`, `io.deephaven.server.table.TableModule`

// AsOfJoin is deprecated in the protocol and still named
#![allow(deprecated)]

use super::support::OpContext;
use crate::session::ExportError;
use oxhaven_contracts::table::Table;
use oxhaven_proto::grpc::batch_table_request::operation::Op;
use oxhaven_proto::grpc::{TableReference, Ticket};
use std::sync::Arc;
use tonic::Status;

/// What handler files import.
pub mod prelude {
    pub use super::super::support::{
        compile, internal_status, java_trim, missing_columns, missing_field, not_ported,
        parse_assignment, require_columns, source, validate_column_name, validate_reference,
        OpContext,
    };
    pub use super::TableOp;
    pub use crate::session::ExportError;
    pub use oxhaven_contracts::table::Table;
    pub use oxhaven_proto::grpc::{TableReference, Ticket};
    pub use std::sync::Arc;
    pub use tonic::Status;
}

/// One table operation. Implemented on a unit struct; see the module documentation.
///
/// Behavior: `io.deephaven.server.table.ops.GrpcTableOperation`
pub trait TableOp: 'static {
    /// The request message, as it appears in the one-shot RPC and in a batch.
    type Request: Send + 'static;

    /// The ticket to export the result under. A batch operation may leave it unset.
    ///
    /// Behavior: `GrpcTableOperation#getResultTicket`
    fn result_id(request: &Self::Request) -> Option<&Ticket>;

    /// The tables the operation reads, in the order `create` receives them.
    ///
    /// Behavior: `GrpcTableOperation#getTableReferences`
    fn sources(request: &Self::Request) -> Vec<TableReference> {
        let _ = request;
        Vec::new()
    }

    /// Checks that need only the request. Most checks need the sources and belong in
    /// `create`.
    ///
    /// Behavior: `GrpcTableOperation#validateRequest`
    fn validate(request: &Self::Request) -> Result<(), Status> {
        let _ = request;
        Ok(())
    }

    /// Builds the result.
    ///
    /// Behavior: `GrpcTableOperation#create`
    fn create(
        context: &OpContext,
        request: &Self::Request,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError>;
}

/// A request bound to its handler, with the request type erased.
pub(super) trait BoundOp: Send {
    /// The result ticket's bytes; empty if the request has none.
    fn result_id(&self) -> &[u8];
    fn sources(&self) -> Vec<TableReference>;
    fn validate(&self) -> Result<(), Status>;
    fn create(
        &self,
        context: &OpContext,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError>;
}

struct Bound<H: TableOp> {
    request: H::Request,
}

impl<H: TableOp> BoundOp for Bound<H> {
    fn result_id(&self) -> &[u8] {
        H::result_id(&self.request).map_or(&[], |ticket| ticket.ticket.as_slice())
    }

    fn sources(&self) -> Vec<TableReference> {
        H::sources(&self.request)
    }

    fn validate(&self) -> Result<(), Status> {
        H::validate(&self.request)
    }

    fn create(
        &self,
        context: &OpContext,
        sources: &[Arc<dyn Table>],
    ) -> Result<Arc<dyn Table>, ExportError> {
        H::create(context, &self.request, sources)
    }
}

/// The name of an operation as Java's `BatchTableRequest.Operation.OpCase` spells it.
pub fn op_name(op: &Op) -> &'static str {
    match op {
        Op::EmptyTable(_) => "EMPTY_TABLE",
        Op::TimeTable(_) => "TIME_TABLE",
        Op::DropColumns(_) => "DROP_COLUMNS",
        Op::Update(_) => "UPDATE",
        Op::LazyUpdate(_) => "LAZY_UPDATE",
        Op::View(_) => "VIEW",
        Op::UpdateView(_) => "UPDATE_VIEW",
        Op::Select(_) => "SELECT",
        Op::SelectDistinct(_) => "SELECT_DISTINCT",
        Op::Filter(_) => "FILTER",
        Op::UnstructuredFilter(_) => "UNSTRUCTURED_FILTER",
        Op::Sort(_) => "SORT",
        Op::Head(_) => "HEAD",
        Op::Tail(_) => "TAIL",
        Op::HeadBy(_) => "HEAD_BY",
        Op::TailBy(_) => "TAIL_BY",
        Op::Ungroup(_) => "UNGROUP",
        Op::Merge(_) => "MERGE",
        Op::ComboAggregate(_) => "COMBO_AGGREGATE",
        Op::Flatten(_) => "FLATTEN",
        Op::RunChartDownsample(_) => "RUN_CHART_DOWNSAMPLE",
        Op::CrossJoin(_) => "CROSS_JOIN",
        Op::NaturalJoin(_) => "NATURAL_JOIN",
        Op::ExactJoin(_) => "EXACT_JOIN",
        Op::LeftJoin(_) => "LEFT_JOIN",
        Op::AsOfJoin(_) => "AS_OF_JOIN",
        Op::FetchTable(_) => "FETCH_TABLE",
        Op::ApplyPreviewColumns(_) => "APPLY_PREVIEW_COLUMNS",
        Op::CreateInputTable(_) => "CREATE_INPUT_TABLE",
        Op::UpdateBy(_) => "UPDATE_BY",
        Op::WhereIn(_) => "WHERE_IN",
        Op::AggregateAll(_) => "AGGREGATE_ALL",
        Op::Aggregate(_) => "AGGREGATE",
        Op::Snapshot(_) => "SNAPSHOT",
        Op::SnapshotWhen(_) => "SNAPSHOT_WHEN",
        Op::MetaTable(_) => "META_TABLE",
        Op::RangeJoin(_) => "RANGE_JOIN",
        Op::Aj(_) => "AJ",
        Op::Raj(_) => "RAJ",
        Op::ColumnStatistics(_) => "COLUMN_STATISTICS",
        Op::MultiJoin(_) => "MULTI_JOIN",
        Op::Slice(_) => "SLICE",
    }
}

/// The status of an operation that has no handler yet. Java has a handler for every
/// operation, so this status has no Java counterpart.
fn unimplemented(op: &Op) -> Status {
    Status::unimplemented(format!(
        "TableService operation {} is not implemented",
        op_name(op)
    ))
}

/// Declares the handler files and builds the registry from them.
macro_rules! handlers {
    ($( $module:ident { $( $variant:ident => $handler:ident ),+ $(,)? } )*) => {
        $( mod $module; )*

        /// Binds `op` to its handler, or answers `UNIMPLEMENTED` naming the operation.
        ///
        /// Behavior: `TableServiceGrpcImpl#getOp`
        pub(super) fn bind(op: Op) -> Result<Box<dyn BoundOp>, Status> {
            match op {
                $( $(
                    Op::$variant(request) => {
                        Ok(Box::new(Bound::<$module::$handler> { request }))
                    }
                )+ )*
                #[allow(unreachable_patterns)]
                other => Err(unimplemented(&other)),
            }
        }

        /// The operations that have a handler, by their Java names.
        #[cfg(test)]
        pub(super) const IMPLEMENTED: &[&str] = &[ $( $( stringify!($variant) ),+ ),* ];
    };
}

// One line per handler file: `file { Operation => HandlerType }`.
handlers! {
    apply_preview_columns { ApplyPreviewColumns => Handler }
    aggregate { Aggregate => Aggregate, AggregateAll => AggregateAll }
    create_input_table { CreateInputTable => Handler }
    distinct { SelectDistinct => Handler }
    drop_columns { DropColumns => Handler }
    empty_table { EmptyTable => Handler }
    fetch_table { FetchTable => Handler }
    filter { Filter => Handler }
    flatten { Flatten => Handler }
    head_tail { Head => Head, Tail => Tail }
    join { NaturalJoin => NaturalJoin, ExactJoin => ExactJoin, LeftJoin => LeftJoin }
    merge { Merge => Handler }
    meta_table { MetaTable => Handler }
    select {
        Update => Update,
        LazyUpdate => LazyUpdate,
        View => View,
        UpdateView => UpdateView,
        Select => Select,
    }
    slice { Slice => Handler }
    snapshot { Snapshot => Snapshot, SnapshotWhen => SnapshotWhen }
    sort { Sort => Handler }
    time_table { TimeTable => Handler }
    unstructured_filter { UnstructuredFilter => Handler }
    update_by { UpdateBy => Handler }
}
