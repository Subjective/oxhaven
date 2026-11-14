//! `ExportedTableUpdates`: the sizes of a session's exported tables, as they change.
//!
//! Behavior: `io.deephaven.server.table.ExportedTableUpdateListener`

use super::support::internal_status;
use crate::session::{
    export_id_to_ticket, is_terminal, ticket_to_export_id, ExportState, Session, NON_EXPORT_ID,
};
use oxhaven_contracts::graph::locked;
use oxhaven_contracts::table::{Table, TableFailure, UpdateListener};
use oxhaven_contracts::update::TableUpdate;
use oxhaven_engine::graph::{ParentListener, UpdateHandler};
use oxhaven_proto::grpc::{ExportNotification, ExportedTableUpdateMessage};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use tokio::sync::mpsc;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tonic::Status;

type Output = mpsc::UnboundedSender<Result<ExportedTableUpdateMessage, Status>>;

/// Sends one size message; false once the client is gone.
fn send(output: &Output, export_id: i32, size: i64, failure: Option<String>) -> bool {
    output
        .send(Ok(ExportedTableUpdateMessage {
            export_id: Some(export_id_to_ticket(export_id)),
            size,
            update_failure_message: failure.unwrap_or_default(),
        }))
        .is_ok()
}

/// Reports the size of one refreshing table after each of its updates.
///
/// Behavior: `ExportedTableUpdateListener.ListenerImpl`
struct SizeReporter {
    table: Arc<dyn Table>,
    export_id: i32,
    output: Output,
}

impl UpdateHandler for SizeReporter {
    fn on_update(&self, _update: &TableUpdate) -> Result<(), TableFailure> {
        let size = self.table.row_set().size() as i64;
        send(&self.output, self.export_id, size, None);
        Ok(())
    }

    fn on_failure(&self, failure: &TableFailure) {
        // The client gets the message of the status the error transformer makes.
        let status = internal_status(&failure.message);
        let size = self.table.row_set().size() as i64;
        let message = format!("INVALID_ARGUMENT: {}", status.message());
        send(&self.output, self.export_id, size, Some(message));
    }
}

/// A subscription to one refreshing table. Dropping it unsubscribes.
struct Listening {
    _listener: Arc<dyn UpdateListener>,
    _reporter: Arc<SizeReporter>,
}

/// The state of one `ExportedTableUpdates` call.
struct Subscription {
    session: Weak<Session>,
    output: Output,
    listening: Mutex<HashMap<i32, Listening>>,
}

impl Subscription {
    /// Behavior: `ExportedTableUpdateListener#onNext`
    fn on_notification(&self, notification: &ExportNotification) {
        let Some(ticket) = &notification.ticket else {
            return;
        };
        let Ok(export_id) = ticket_to_export_id(&ticket.ticket, "ticket") else {
            return;
        };
        if export_id == NON_EXPORT_ID {
            // the marker that ends the initial run
            return;
        }
        let state = notification.export_state();
        if state == ExportState::Exported {
            let table = self
                .session
                .upgrade()
                .and_then(|session| session.get_export_if_exists(export_id).ok().flatten())
                .and_then(|export| export.value())
                .and_then(|value| value.as_table().cloned());
            if let Some(table) = table {
                self.on_new_table(export_id, table);
            }
        } else if is_terminal(state) {
            self.listening
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .remove(&export_id);
        }
    }

    /// Sends the table's size now and, if it can change, after every change.
    ///
    /// Behavior: `ExportedTableUpdateListener#onNewTableExport`
    fn on_new_table(&self, export_id: i32, table: Arc<dyn Table>) {
        let graph = match table.update_graph() {
            Some(graph) if table.is_refreshing() => graph,
            _ => {
                send(&self.output, export_id, table.row_set().size() as i64, None);
                return;
            }
        };
        if table.is_failed() {
            let message = "FAILED_PRECONDITION: Exported Table Already Failed".to_string();
            send(&self.output, export_id, -1, Some(message));
            return;
        }
        let mut listening = self
            .listening
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        if listening.contains_key(&export_id) {
            // an export may be announced twice
            return;
        }
        let reporter = Arc::new(SizeReporter {
            table: table.clone(),
            export_id,
            output: self.output.clone(),
        });
        // Under the lock no cycle runs between the size sent here and the subscription, so
        // the first message cannot be overtaken by an update.
        let listener = locked(graph.as_ref(), || {
            let handler: Arc<dyn UpdateHandler> = reporter.clone();
            let listener = ParentListener::subscribe(table.clone(), Arc::downgrade(&handler));
            send(&self.output, export_id, table.row_set().size() as i64, None);
            listener
        });
        listening.insert(
            export_id,
            Listening {
                _listener: listener,
                _reporter: reporter,
            },
        );
    }
}

/// Starts the stream for `session`: the size of every table it has exported, then the size
/// of each table as it is exported and after each of its updates. The stream ends with the
/// session, and the subscription with the client's call.
pub(super) fn subscribe(
    session: &Arc<Session>,
) -> Result<UnboundedReceiverStream<Result<ExportedTableUpdateMessage, Status>>, Status> {
    let mut notifications = session.add_export_listener()?;
    let (output, receiver) = mpsc::unbounded_channel();
    let subscription = Arc::new(Subscription {
        session: Arc::downgrade(session),
        output,
        listening: Mutex::new(HashMap::new()),
    });
    tokio::spawn(async move {
        loop {
            let notification = tokio::select! {
                notification = notifications.recv() => notification,
                () = subscription.output.closed() => None,
            };
            let Some(Ok(notification)) = notification else {
                break;
            };
            // Reading a table may wait for the update graph's lock: not on this thread.
            let subscription = subscription.clone();
            let handled = tokio::task::spawn_blocking(move || {
                subscription.on_notification(&notification);
            });
            if handled.await.is_err() {
                break;
            }
        }
        // Dropping the subscription drops the table listeners and the export listener.
    });
    Ok(UnboundedReceiverStream::new(receiver))
}
