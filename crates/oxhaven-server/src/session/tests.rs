//! Tests of the session module: the export state machine and the session store against a
//! clock and an executor stepped by hand, then the gRPC surface against a real server.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use oxhaven_proto::grpc::ExportNotification;
use tonic::{Code, Status};

use super::*;

const PSK: &str = "s3cret";
const EXPIRE_MS: i64 = 100_000;

/// Queues jobs and runs them when told to.
#[derive(Default)]
struct ManualExecutor {
    jobs: Mutex<VecDeque<Job>>,
    serial_jobs: AtomicUsize,
}

impl ManualExecutor {
    fn run_next(&self) -> bool {
        let job = self.jobs.lock().unwrap().pop_front();
        job.map(|job| job()).is_some()
    }

    /// Runs jobs, including the ones they queue, until none is left.
    fn run_all(&self) {
        while self.run_next() {}
    }

    fn queued(&self) -> usize {
        self.jobs.lock().unwrap().len()
    }
}

impl Executor for ManualExecutor {
    fn run(&self, job: Job) {
        self.jobs.lock().unwrap().push_back(job);
    }

    fn run_serially(&self, job: Job) {
        self.serial_jobs.fetch_add(1, Ordering::SeqCst);
        self.run(job);
    }
}

struct Fixture {
    now: Arc<AtomicI64>,
    executor: Arc<ManualExecutor>,
    store: Arc<SessionStore>,
}

impl Fixture {
    fn new() -> Self {
        let now = Arc::new(AtomicI64::new(1_000_000));
        let executor = Arc::new(ManualExecutor::default());
        let clock: Clock = {
            let now = now.clone();
            Arc::new(move || now.load(Ordering::SeqCst))
        };
        let handlers =
            AuthHandlers::new(vec![AuthHandler::psk(PSK).unwrap(), AuthHandler::Anonymous])
                .unwrap();
        let store = SessionStore::new(clock, executor.clone(), handlers, EXPIRE_MS).unwrap();
        Self {
            now,
            executor,
            store,
        }
    }

    fn session(&self) -> Arc<Session> {
        self.store.new_session(AuthContext::SuperUser)
    }

    fn advance(&self, millis: i64) {
        self.now.fetch_add(millis, Ordering::SeqCst);
    }
}

/// Where a handler's argument lands.
type Captured<T> = Arc<Mutex<Vec<T>>>;

fn capture<T: Send + 'static>() -> (Captured<T>, impl FnOnce(T) + Send + 'static) {
    let captured: Captured<T> = Arc::default();
    let sink = captured.clone();
    (captured, move |value| sink.lock().unwrap().push(value))
}

fn number(value: &ExportValue) -> i32 {
    *value.downcast_ref::<i32>().expect("an i32 export")
}

fn constant(n: i32) -> impl FnOnce(Vec<ExportValue>) -> Result<ExportValue, ExportError> + Send {
    move |_| Ok(ExportValue::object(n))
}

fn sum(inputs: Vec<ExportValue>) -> Result<ExportValue, ExportError> {
    Ok(ExportValue::object(inputs.iter().map(number).sum::<i32>()))
}

/// Everything a listener has received so far, as (export id, state) pairs.
fn drain(listener: &mut ExportNotifications) -> Vec<(i32, ExportState)> {
    drain_full(listener)
        .into_iter()
        .map(|n| {
            (
                ticket_to_export_id(&n.ticket.unwrap().ticket, "test").unwrap(),
                ExportState::try_from(n.export_state).unwrap(),
            )
        })
        .collect()
}

fn drain_full(listener: &mut ExportNotifications) -> Vec<ExportNotification> {
    let mut notifications = Vec::new();
    while let Ok(notification) = listener.try_recv() {
        notifications.push(notification.unwrap());
    }
    notifications
}

use oxhaven_proto::grpc::export_notification::State::{
    Cancelled, DependencyCancelled, DependencyFailed, DependencyReleased, Exported, Failed,
    Pending, Queued, Released, Running, Unknown,
};

mod exports {
    use super::*;

    #[test]
    fn export_passes_through_every_state_and_tells_listeners() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let mut listener = session.add_export_listener().unwrap();
        assert_eq!(drain(&mut listener), [(0, Exported)]);

        let (values, on_success) = capture();
        let export = session
            .new_export(1)
            .unwrap()
            .on_success(on_success)
            .submit(constant(7))
            .unwrap();
        assert_eq!(export.state(), Queued);
        assert_eq!(export.value().map(|v| number(&v)), None);
        assert_eq!(
            drain(&mut listener),
            [(1, Unknown), (1, Pending), (1, Queued)]
        );

        fixture.executor.run_all();
        assert_eq!(export.state(), Exported);
        assert_eq!(export.value().map(|v| number(&v)), Some(7));
        assert_eq!(drain(&mut listener), [(1, Running), (1, Exported)]);
        assert_eq!(
            values
                .lock()
                .unwrap()
                .iter()
                .map(number)
                .collect::<Vec<_>>(),
            [7]
        );
        assert_eq!(export.ticket().ticket, [b'e', 1, 0, 0, 0]);
    }

    #[test]
    fn work_waits_for_dependencies_and_receives_their_values_in_order() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let first = session.new_export(1).unwrap().submit(constant(40)).unwrap();
        // export 3 is required before anything defines it
        let third = session.get_export(3).unwrap();
        assert_eq!(third.state(), Unknown);

        let order = session
            .new_export(2)
            .unwrap()
            .require([first.clone(), third.clone()])
            .submit(|inputs| {
                Ok(ExportValue::object(
                    inputs.iter().map(number).collect::<Vec<_>>(),
                ))
            })
            .unwrap();
        fixture.executor.run_all();
        assert_eq!(first.state(), Exported);
        assert_eq!(order.state(), Pending, "one dependency is still undefined");

        session.new_export(3).unwrap().submit(constant(2)).unwrap();
        assert_eq!(order.state(), Pending);
        fixture.executor.run_all();
        assert_eq!(order.state(), Exported);
        assert_eq!(
            order.value().unwrap().downcast_ref::<Vec<i32>>().unwrap(),
            &[40, 2]
        );
    }

    #[test]
    fn dependency_already_exported_is_used_without_waiting() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let first = session.new_export(1).unwrap().submit(constant(1)).unwrap();
        fixture.executor.run_all();
        let second = session
            .new_export(2)
            .unwrap()
            .require([first.clone(), first])
            .submit(sum)
            .unwrap();
        assert_eq!(second.state(), Queued);
        fixture.executor.run_all();
        assert_eq!(second.value().map(|v| number(&v)), Some(2));
    }

    #[test]
    fn status_failure_reaches_own_handler_and_dependents_unchanged() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let mut listener = session.add_export_listener().unwrap();
        let (own, on_error) = capture::<Status>();
        let failing = session
            .new_export(1)
            .unwrap()
            .on_error(on_error)
            .submit(|_| Err(Status::not_found("no such table").into()))
            .unwrap();
        let (dependent_errors, on_error) = capture::<Status>();
        let dependent = session
            .new_export(2)
            .unwrap()
            .require([failing.clone()])
            .on_error(on_error)
            .submit(sum)
            .unwrap();
        fixture.executor.run_all();

        assert_eq!(failing.state(), Failed);
        assert_eq!(dependent.state(), DependencyFailed);
        for errors in [&own, &dependent_errors] {
            let errors = errors.lock().unwrap();
            assert_eq!(errors.len(), 1);
            assert_eq!(errors[0].code(), Code::NotFound);
            assert_eq!(errors[0].message(), "no such table");
        }

        let notifications = drain_full(&mut listener);
        let terminal = |id: i32| {
            notifications
                .iter()
                .rfind(|n| n.ticket.as_ref().unwrap().ticket == export_id_to_bytes(id))
                .unwrap()
                .clone()
        };
        let (failed, dependency_failed) = (terminal(1), terminal(2));
        assert_eq!(failed.export_state, Failed as i32);
        assert!(!failed.context.is_empty(), "a failure carries its error id");
        assert_eq!(dependency_failed.export_state, DependencyFailed as i32);
        assert_eq!(dependency_failed.context, failed.context);
        // the dependency has its own error, so it is not named as the cause
        assert_eq!(dependency_failed.dependent_handle, "");

        // a dependent defined after the failure sees the same
        let (late_errors, on_error) = capture::<Status>();
        let late = session
            .new_export(3)
            .unwrap()
            .require([failing])
            .on_error(on_error)
            .submit(sum)
            .unwrap();
        assert_eq!(late.state(), DependencyFailed);
        assert_eq!(late_errors.lock().unwrap()[0].code(), Code::NotFound);
        assert_eq!(fixture.executor.queued(), 0);
    }

    #[test]
    fn internal_failure_is_hidden_behind_an_error_id() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let (own, on_error) = capture::<Status>();
        let failing = session
            .new_export(1)
            .unwrap()
            .on_error(on_error)
            .submit(|_| Err(ExportError::Internal("secret detail".to_string())))
            .unwrap();
        let (dependent_errors, on_error) = capture::<Status>();
        session
            .new_export(2)
            .unwrap()
            .require([failing])
            .on_error(on_error)
            .submit(sum)
            .unwrap();
        fixture.executor.run_all();

        let own = own.lock().unwrap();
        assert_eq!(own[0].code(), Code::InvalidArgument);
        assert!(!own[0].message().contains("secret detail"));
        let dependent_errors = dependent_errors.lock().unwrap();
        assert_eq!(dependent_errors[0].code(), Code::FailedPrecondition);
        assert!(dependent_errors[0].message().contains("DEPENDENCY_FAILED"));
        assert!(!dependent_errors[0].message().contains("secret detail"));
    }

    #[test]
    fn panic_in_work_fails_the_export() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let (errors, on_error) = capture::<Status>();
        let export = session
            .new_export(1)
            .unwrap()
            .on_error(on_error)
            .submit(|_| panic!("unported: some.Java#method"))
            .unwrap();
        fixture.executor.run_all();
        assert_eq!(export.state(), Failed);
        assert_eq!(errors.lock().unwrap()[0].code(), Code::InvalidArgument);
    }

    #[test]
    fn cancelling_pending_work_fails_it_and_its_dependents() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let mut listener = session.add_export_listener().unwrap();
        let ran = Arc::new(AtomicUsize::new(0));
        let (errors, on_error) = capture::<Status>();
        let export = {
            let ran = ran.clone();
            session
                .new_export(1)
                .unwrap()
                .on_error(on_error)
                .submit(move |_| {
                    ran.fetch_add(1, Ordering::SeqCst);
                    Ok(ExportValue::unit())
                })
                .unwrap()
        };
        let (dependent_errors, on_error) = capture::<Status>();
        let dependent = session
            .new_export(2)
            .unwrap()
            .require([export.clone()])
            .on_error(on_error)
            .submit(sum)
            .unwrap();

        export.cancel();
        assert_eq!(export.state(), Cancelled);
        fixture.executor.run_all();
        assert_eq!(ran.load(Ordering::SeqCst), 0, "cancelled work must not run");
        assert_eq!(errors.lock().unwrap()[0].code(), Code::Cancelled);
        // the cancelled export had a handler, hence an error id, hence a plain
        // DEPENDENCY_FAILED for its dependent
        assert_eq!(dependent.state(), DependencyFailed);
        assert_eq!(
            dependent_errors.lock().unwrap()[0].code(),
            Code::FailedPrecondition
        );
        // the cancel notification precedes the error id; the dependent's carries it
        let notifications = drain_full(&mut listener);
        let cancelled = notifications
            .iter()
            .find(|n| n.export_state == Cancelled as i32)
            .unwrap();
        assert_eq!(cancelled.context, "");
        let failed = notifications
            .iter()
            .find(|n| n.export_state == DependencyFailed as i32)
            .unwrap();
        assert!(!failed.context.is_empty());
        assert_eq!(
            session.export_count(),
            2,
            "cancelled exports keep their ids"
        );
    }

    #[test]
    fn cancelling_an_undefined_export_names_it_to_its_dependents() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let mut listener = session.add_export_listener().unwrap();
        let undefined = session.get_export(5).unwrap();
        let (errors, on_error) = capture::<Status>();
        let dependent = session
            .new_export(6)
            .unwrap()
            .require([undefined.clone()])
            .on_error(on_error)
            .submit(sum)
            .unwrap();
        undefined.cancel();

        assert_eq!(dependent.state(), DependencyCancelled);
        let errors = errors.lock().unwrap();
        assert_eq!(errors[0].code(), Code::FailedPrecondition);
        assert!(errors[0].message().contains("related parent export id: 5"));
        let notification = drain_full(&mut listener)
            .into_iter()
            .find(|n| n.export_state == DependencyCancelled as i32)
            .unwrap();
        assert_eq!(notification.dependent_handle, "5");
        assert!(!notification.context.is_empty());

        // defining the cancelled id reports the cancel to the new definition
        let (redefined, on_error) = capture::<Status>();
        session
            .new_export(5)
            .unwrap()
            .on_error(on_error)
            .submit(constant(1))
            .unwrap();
        assert_eq!(redefined.lock().unwrap()[0].code(), Code::Cancelled);
        assert_eq!(fixture.executor.queued(), 0);
    }

    #[test]
    fn cancel_during_work_discards_the_result() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let export = session.new_export(1).unwrap();
        let handle = export.export().clone();
        let (successes, on_success) = capture::<ExportValue>();
        let export = export
            .on_success(on_success)
            .submit(move |_| {
                handle.cancel();
                Ok(ExportValue::object(1))
            })
            .unwrap();
        fixture.executor.run_all();
        assert_eq!(export.state(), Cancelled);
        assert!(export.value().is_none());
        assert!(successes.lock().unwrap().is_empty());
    }

    #[test]
    fn released_export_leaves_the_table_and_its_id_cannot_be_reused() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let mut listener = session.add_export_listener().unwrap();
        let export = session.new_export(1).unwrap().submit(constant(9)).unwrap();
        fixture.executor.run_all();
        assert_eq!(session.export_count(), 1);
        drain(&mut listener);

        export.cancel();
        assert_eq!(export.state(), Released);
        assert!(export.value().is_none());
        assert_eq!(session.export_count(), 0);
        assert_eq!(drain(&mut listener), [(1, Released)]);
        // releasing again changes nothing
        export.cancel();
        assert_eq!(drain(&mut listener), []);

        // the id resolves to a stand-in that is not in the table and notifies nobody
        let stand_in = session.get_export(1).unwrap();
        assert_eq!(stand_in.state(), Released);
        assert_eq!(
            session.get_export_if_exists(1).unwrap().unwrap().state(),
            Released
        );
        assert_eq!(session.export_count(), 0);

        let (errors, on_error) = capture::<Status>();
        let dependent = session
            .new_export(2)
            .unwrap()
            .require([stand_in])
            .on_error(on_error)
            .submit(sum)
            .unwrap();
        assert_eq!(dependent.state(), DependencyReleased);
        {
            let errors = errors.lock().unwrap();
            assert_eq!(errors[0].code(), Code::FailedPrecondition);
            assert!(errors[0].message().contains("related parent export id: 1"));
        }
        assert_eq!(
            drain(&mut listener),
            [(2, Unknown), (2, DependencyReleased)],
            "a dependent of a dead dependency is never pending"
        );

        let (errors, on_error) = capture::<Status>();
        let redefined = session
            .new_export(1)
            .unwrap()
            .on_error(on_error)
            .submit(constant(10))
            .unwrap();
        assert_eq!(redefined.state(), Released);
        let errors = errors.lock().unwrap();
        assert_eq!(errors[0].code(), Code::FailedPrecondition);
        assert!(errors[0].message().contains("RELEASED"));
        assert_eq!(fixture.executor.queued(), 0);
        assert_eq!(drain(&mut listener), []);
    }

    #[test]
    fn release_after_a_dependent_took_the_value_does_not_fail_the_dependent() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let first = session.new_export(1).unwrap().submit(constant(5)).unwrap();
        fixture.executor.run_all();
        let second = session
            .new_export(2)
            .unwrap()
            .require([first.clone()])
            .submit(sum)
            .unwrap();
        first.cancel();
        assert_eq!(first.state(), Released);
        fixture.executor.run_all();
        assert_eq!(second.value().map(|v| number(&v)), Some(5));
    }

    #[test]
    fn an_export_is_defined_once() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let export = session.new_export(1).unwrap().submit(constant(1)).unwrap();
        let again = session.new_export(1).unwrap().submit(constant(2));
        assert_eq!(again.unwrap_err().code(), Code::InvalidArgument);
        fixture.executor.run_all();
        assert_eq!(export.value().map(|v| number(&v)), Some(1));
        let after = session.new_export(1).unwrap().submit(constant(3));
        assert_eq!(after.unwrap_err().code(), Code::InvalidArgument);
    }

    #[test]
    fn export_ids_are_validated() {
        let fixture = Fixture::new();
        let session = fixture.session();
        for reserved in [0, -1, i32::MIN] {
            let err = session.new_export(reserved).err().unwrap();
            assert_eq!(err.code(), Code::InvalidArgument, "{reserved}");
        }
        assert_eq!(
            session.get_export(0).unwrap_err().code(),
            Code::InvalidArgument
        );
        assert_eq!(
            session.get_export(-4).unwrap_err().code(),
            Code::FailedPrecondition
        );
        assert!(session.get_export_if_exists(4).unwrap().is_none());
        assert!(session.get_export_if_exists(0).unwrap().is_none());
        assert_eq!(session.export_count(), 0);
        let err = session
            .new_export_for_ticket(&[b'e', 1, 0], "resultId")
            .err()
            .unwrap();
        assert_eq!(err.code(), Code::FailedPrecondition);
    }

    #[test]
    fn server_side_exports_count_down_from_minus_one() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let mut listener = session.add_export_listener().unwrap();
        drain(&mut listener);
        let first = session
            .new_server_side_export(ExportValue::object(11))
            .unwrap();
        let second = session
            .new_server_side_export(ExportValue::object(12))
            .unwrap();
        assert_eq!((first.id(), second.id()), (-1, -2));
        assert_eq!(first.ticket().ticket, [b'e', 0xff, 0xff, 0xff, 0xff]);
        assert_eq!(first.state(), Exported);
        assert_eq!(
            drain(&mut listener),
            [(-1, Unknown), (-1, Exported), (-2, Unknown), (-2, Exported)]
        );
        let resolved = session.get_export(-2).unwrap();
        assert_eq!(resolved.value().map(|v| number(&v)), Some(12));
    }

    #[test]
    fn non_export_runs_work_without_notifying_listeners() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let mut listener = session.add_export_listener().unwrap();
        drain(&mut listener);
        let export = session.new_export(1).unwrap().submit(constant(3)).unwrap();
        let (values, on_success) = capture::<ExportValue>();
        let work = session
            .non_export()
            .unwrap()
            .require([export])
            .on_success(on_success)
            .submit(sum)
            .unwrap();
        assert!(work.is_non_export());
        fixture.executor.run_all();
        assert_eq!(work.state(), Exported);
        assert_eq!(number(&values.lock().unwrap()[0]), 3);
        assert!(drain(&mut listener).iter().all(|(id, _)| *id == 1));
        assert_eq!(session.export_count(), 1);
        // cancelling a finished non-export is a no-op
        work.cancel();
        assert_eq!(work.state(), Exported);
    }

    #[test]
    fn sessionless_exports_are_dependencies_of_any_session() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let ready = Export::ready(ExportValue::object(4));
        let ok = session
            .new_export(1)
            .unwrap()
            .require([ready])
            .submit(sum)
            .unwrap();
        fixture.executor.run_all();
        assert_eq!(ok.value().map(|v| number(&v)), Some(4));

        let failed = Export::failed(Status::permission_denied("no").into());
        let (errors, on_error) = capture::<Status>();
        let dependent = session
            .new_export(2)
            .unwrap()
            .require([failed])
            .on_error(on_error)
            .submit(sum)
            .unwrap();
        assert_eq!(dependent.state(), DependencyFailed);
        assert_eq!(errors.lock().unwrap()[0].code(), Code::PermissionDenied);
    }

    #[test]
    fn serial_work_goes_to_the_serial_queue() {
        let fixture = Fixture::new();
        let session = fixture.session();
        session.new_export(1).unwrap().submit(constant(1)).unwrap();
        assert_eq!(fixture.executor.serial_jobs.load(Ordering::SeqCst), 0);
        session
            .new_export(2)
            .unwrap()
            .requires_serial_queue()
            .submit(constant(1))
            .unwrap();
        assert_eq!(fixture.executor.serial_jobs.load(Ordering::SeqCst), 1);
        assert_eq!(fixture.executor.queued(), 2);
    }

    #[test]
    fn new_listener_is_sent_live_exports_then_the_end_of_refresh_marker() {
        let fixture = Fixture::new();
        let session = fixture.session();
        session.new_export(1).unwrap().submit(constant(1)).unwrap();
        fixture.executor.run_all();
        session.new_export(2).unwrap().submit(constant(2)).unwrap();
        let cancelled = session.new_export(3).unwrap().submit(constant(3)).unwrap();
        cancelled.cancel();
        session.get_export(4).unwrap();

        let mut listener = session.add_export_listener().unwrap();
        let notifications = drain_full(&mut listener);
        let marker = notifications.last().unwrap();
        assert_eq!(marker.ticket.as_ref().unwrap().ticket, [b'e', 0, 0, 0, 0]);
        assert_eq!(marker.export_state, Exported as i32);
        assert_eq!(marker.context, "run is complete");
        let mut listener_again = session.add_export_listener().unwrap();
        // terminal exports are not replayed
        assert_eq!(
            drain(&mut listener_again),
            [(1, Exported), (2, Queued), (4, Unknown), (0, Exported)]
        );
        assert_eq!(session.listener_count(), 2);

        // a listener that goes away is dropped at the next notification
        drop(listener);
        session.get_export(9).unwrap();
        assert_eq!(session.listener_count(), 1);
        assert_eq!(drain(&mut listener_again), [(9, Unknown)]);
    }

    #[test]
    fn end_of_session_cancels_exports_ends_listeners_and_runs_callbacks_once() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let exported = session.new_export(1).unwrap().submit(constant(1)).unwrap();
        fixture.executor.run_all();
        let (errors, on_error) = capture::<Status>();
        let queued = session
            .new_export(2)
            .unwrap()
            .on_error(on_error)
            .submit(constant(2))
            .unwrap();
        let mut listener = session.add_export_listener().unwrap();
        drain(&mut listener);
        let closed = Arc::new(AtomicUsize::new(0));
        let removed = {
            let closed = closed.clone();
            session
                .add_on_close(move || {
                    closed.fetch_add(100, Ordering::SeqCst);
                })
                .unwrap()
        };
        {
            let closed = closed.clone();
            session
                .add_on_close(move || {
                    closed.fetch_add(1, Ordering::SeqCst);
                })
                .unwrap();
        }
        assert!(session.remove_on_close(removed));
        assert!(!session.remove_on_close(removed));

        fixture.store.close_session(&session);
        assert!(session.is_expired());
        assert_eq!(exported.state(), Released);
        assert_eq!(queued.state(), Cancelled);
        assert_eq!(errors.lock().unwrap()[0].code(), Code::Cancelled);
        assert_eq!(drain(&mut listener), [(1, Released), (2, Cancelled)]);
        assert!(
            matches!(
                listener.try_recv(),
                Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
            ),
            "the listener's stream ends with the session"
        );
        assert_eq!(closed.load(Ordering::SeqCst), 1);
        assert_eq!(session.export_count(), 0);

        // queued work finds the session gone
        fixture.executor.run_all();
        assert_eq!(queued.state(), Cancelled);

        fixture.store.close_session(&session);
        assert_eq!(closed.load(Ordering::SeqCst), 1);

        for err in [
            session.get_export(1).unwrap_err(),
            session.get_export_if_exists(1).unwrap_err(),
            session.new_export(3).err().unwrap(),
            session.non_export().err().unwrap(),
            session
                .new_server_side_export(ExportValue::unit())
                .unwrap_err(),
            session.add_export_listener().unwrap_err(),
            session.add_on_close(|| {}).unwrap_err(),
        ] {
            assert_eq!(err.code(), Code::Unauthenticated);
        }
    }

    #[test]
    fn work_queued_when_the_session_ends_is_cancelled_not_run() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let ran = Arc::new(AtomicUsize::new(0));
        let export = {
            let ran = ran.clone();
            session
                .new_export(1)
                .unwrap()
                .submit(move |_| {
                    ran.fetch_add(1, Ordering::SeqCst);
                    Ok(ExportValue::unit())
                })
                .unwrap()
        };
        // the token lapses, but nothing has swept the session yet
        fixture.advance(EXPIRE_MS);
        fixture.executor.run_all();
        assert_eq!(export.state(), Cancelled);
        assert_eq!(ran.load(Ordering::SeqCst), 0);
    }
}

mod store {
    use super::*;

    #[test]
    fn token_lifetime_has_a_floor() {
        let make = |expire_ms| {
            SessionStore::new(
                Arc::new(|| 0),
                Arc::new(ManualExecutor::default()),
                AuthHandlers::default(),
                expire_ms,
            )
        };
        assert!(make(MIN_TOKEN_EXPIRE_MS - 1).is_err());
        assert!(make(MIN_TOKEN_EXPIRE_MS).is_ok());
        assert!(make(i64::MAX).is_err());
    }

    #[test]
    fn token_rotates_only_after_a_fifth_of_its_lifetime() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let first = session.expiration().unwrap();
        assert_eq!(
            first.deadline_millis,
            fixture.now.load(Ordering::SeqCst) + EXPIRE_MS
        );
        assert_eq!(first.bearer(), format!("Bearer {}", first.token));

        fixture.advance(EXPIRE_MS / 5 - 1);
        assert_eq!(fixture.store.refresh_token(&session), Some(first));

        fixture.advance(1);
        let second = fixture.store.refresh_token(&session).unwrap();
        assert_ne!(second.token, first.token);
        assert_eq!(
            second.deadline_millis,
            fixture.now.load(Ordering::SeqCst) + EXPIRE_MS
        );
        assert_eq!(session.expiration(), Some(second));

        // both tokens find the session until the older one lapses
        let find = |token| {
            fixture
                .store
                .session_for_token(token)
                .map(|s| s.id().to_string())
        };
        assert_eq!(find(first.token).as_deref(), Some(session.id()));
        assert_eq!(find(second.token).as_deref(), Some(session.id()));
        fixture.advance(EXPIRE_MS - EXPIRE_MS / 5);
        assert_eq!(find(first.token), None);
        assert_eq!(find(second.token).as_deref(), Some(session.id()));
    }

    #[test]
    fn sweep_forgets_lapsed_tokens_and_ends_sessions_without_a_live_one() {
        let fixture = Fixture::new();
        let kept = fixture.session();
        let lapsing = fixture.session();
        let closed = Arc::new(AtomicUsize::new(0));
        for session in [&kept, &lapsing] {
            let closed = closed.clone();
            session
                .add_on_close(move || {
                    closed.fetch_add(1, Ordering::SeqCst);
                })
                .unwrap();
        }
        let first_deadline = kept.expiration().unwrap().deadline_millis;
        assert_eq!(fixture.store.sweep(), Some(first_deadline));

        fixture.advance(EXPIRE_MS / 2);
        let renewed = fixture.store.refresh_token(&kept).unwrap();
        assert_eq!(fixture.store.sweep(), Some(first_deadline));
        assert_eq!(closed.load(Ordering::SeqCst), 0);

        // the first tokens of both sessions lapse; only `lapsing` has no other
        fixture.advance(EXPIRE_MS / 2);
        assert!(lapsing.is_expired());
        assert_eq!(
            closed.load(Ordering::SeqCst),
            0,
            "nothing ends before the sweep"
        );
        assert_eq!(fixture.store.sweep(), Some(renewed.deadline_millis));
        assert_eq!(closed.load(Ordering::SeqCst), 1);
        assert!(!kept.is_expired());
        assert!(fixture.store.refresh_token(&lapsing).is_none());

        fixture.advance(EXPIRE_MS);
        assert_eq!(fixture.store.sweep(), None);
        assert_eq!(closed.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn closing_a_session_forgets_its_tokens() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let other = fixture.session();
        let first = session.expiration().unwrap();
        fixture.advance(EXPIRE_MS / 2);
        let second = fixture.store.refresh_token(&session).unwrap();
        fixture.store.close_session(&session);
        assert!(fixture.store.session_for_token(first.token).is_none());
        assert!(fixture.store.session_for_token(second.token).is_none());
        assert!(fixture.store.refresh_token(&session).is_none());
        // only the other session's token is left to sweep
        assert_eq!(
            fixture.store.sweep(),
            Some(other.expiration().unwrap().deadline_millis)
        );
    }

    #[test]
    fn authorization_header_finds_or_creates_a_session() {
        let fixture = Fixture::new();
        let store = &fixture.store;
        let psk = store
            .session_for_auth_header(&format!("{PSK_AUTH_TYPE} {PSK}"))
            .unwrap();
        assert_eq!(psk.auth_context(), AuthContext::SuperUser);
        let bearer = psk.expiration().unwrap().bearer();
        let same = store.session_for_auth_header(&bearer).unwrap();
        assert_eq!(same.id(), psk.id());

        // each login is a new session
        let again = store
            .session_for_auth_header(&format!("{PSK_AUTH_TYPE} {PSK}"))
            .unwrap();
        assert_ne!(again.id(), psk.id());

        // credentials may be nested in a bearer value
        let nested = store
            .session_for_auth_header(&format!("Bearer {PSK_AUTH_TYPE} {PSK}"))
            .unwrap();
        assert_eq!(nested.auth_context(), AuthContext::SuperUser);

        let anonymous = store.session_for_auth_header("Anonymous").unwrap();
        assert_eq!(anonymous.auth_context(), AuthContext::Anonymous);
        let nested_anonymous = store.session_for_auth_header("Bearer Anonymous").unwrap();
        assert_eq!(nested_anonymous.auth_context(), AuthContext::Anonymous);

        for refused in [
            format!("{PSK_AUTH_TYPE} wrong"),
            PSK_AUTH_TYPE.to_string(),
            format!("{PSK_AUTH_TYPE}  {PSK}"),
            format!("Bearer {}", uuid::Uuid::new_v4()),
            "Bearer".to_string(),
            "Anonymous x".to_string(),
            "Basic dXNlcjpwYXNz".to_string(),
            PSK.to_string(),
            String::new(),
        ] {
            assert_eq!(
                store.session_for_auth_header(&refused).err(),
                Some(AuthenticationFailed),
                "{refused:?}"
            );
        }

        // a closed session's bearer token is refused
        store.close_session(&psk);
        assert!(store.session_for_auth_header(&bearer).is_err());
    }

    #[test]
    fn shutdown_notifies_termination_listeners_then_ends_sessions() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let mut listener = fixture.store.add_termination_listener(&session).unwrap();
        assert!(listener.try_recv().is_err());
        fixture.store.on_shutdown();
        let notification = listener.try_recv().unwrap().unwrap();
        assert!(!notification.abnormal_termination);
        assert!(session.is_expired());
        // exactly one ending: the session's end does not add an error after the notification
        assert!(matches!(
            listener.try_recv(),
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected)
        ));
    }

    #[test]
    fn termination_listener_fails_when_its_session_ends_first() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let mut listener = fixture.store.add_termination_listener(&session).unwrap();
        fixture.store.close_session(&session);
        let err = listener.try_recv().unwrap().unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated);
        assert_eq!(
            fixture
                .store
                .add_termination_listener(&session)
                .unwrap_err()
                .code(),
            Code::Unauthenticated
        );
    }
}

mod router {
    use super::*;

    fn router() -> TicketRouter {
        TicketRouter::new(vec![Arc::new(ExportTicketResolver)]).unwrap()
    }

    #[test]
    fn routes_must_be_unique() {
        let duplicate = TicketRouter::new(vec![
            Arc::new(ExportTicketResolver),
            Arc::new(ExportTicketResolver),
        ]);
        assert!(duplicate.is_err());
    }

    #[test]
    fn resolve_separates_errors_of_the_call_from_failed_exports() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let router = router();
        let code = |ticket: &[u8]| {
            router
                .resolve(Some(&session), ticket, "sourceId")
                .map(|export| export.state())
                .map_err(|status| status.code())
        };
        assert_eq!(code(b""), Err(Code::FailedPrecondition));
        assert_eq!(code(&[b'z', 1, 0, 0, 0]), Err(Code::FailedPrecondition));
        assert_eq!(code(&[b'e', 1]), Err(Code::FailedPrecondition));
        assert_eq!(code(&[b'e', 1, 0, 0, 0]), Ok(Unknown));
        // the resolver's own refusals come back as failed exports
        assert_eq!(code(&[b'e', 0, 0, 0, 0]), Ok(Failed));
        assert_eq!(code(&[b'e', 0xff, 0xff, 0xff, 0xff]), Ok(Failed));
        let no_session = router
            .resolve(None, &[b'e', 1, 0, 0, 0], "sourceId")
            .unwrap();
        assert_eq!(no_session.state(), Failed);

        assert_eq!(
            router.log_name(&[b'e', 2, 0, 0, 0], "id").unwrap(),
            "export/2"
        );
        assert_eq!(
            router.log_name(b"", "id").unwrap_err().code(),
            Code::InvalidArgument
        );
    }

    #[test]
    fn publish_from_copies_the_source_and_reports_completion() {
        let fixture = Fixture::new();
        let session = fixture.session();
        let router = router();
        let source = session
            .new_server_side_export(ExportValue::object(21))
            .unwrap();
        let (published, on_publish) = capture::<()>();
        let (errors, on_error) = capture::<Status>();
        router
            .publish_from(
                &session,
                &export_id_to_bytes(3),
                "resultId",
                Some(Box::new(move || on_publish(()))),
                on_error,
                source,
            )
            .unwrap();
        assert!(published.lock().unwrap().is_empty());
        fixture.executor.run_all();
        assert_eq!(published.lock().unwrap().len(), 1);
        assert!(errors.lock().unwrap().is_empty());
        let result = session.get_export(3).unwrap();
        assert_eq!(result.value().map(|v| number(&v)), Some(21));

        // a failed source reports through the error handler, not through on_publish
        let (published, on_publish) = capture::<()>();
        let (errors, on_error) = capture::<Status>();
        let missing = router
            .resolve(Some(&session), &export_id_to_bytes(-9), "sourceId")
            .unwrap();
        router
            .publish_from(
                &session,
                &export_id_to_bytes(4),
                "resultId",
                Some(Box::new(move || on_publish(()))),
                on_error,
                missing,
            )
            .unwrap();
        fixture.executor.run_all();
        assert!(published.lock().unwrap().is_empty());
        assert_eq!(errors.lock().unwrap()[0].code(), Code::FailedPrecondition);

        let reserved = router.publish(&session, &export_id_to_bytes(-1), "resultId", None);
        assert_eq!(reserved.err().unwrap().code(), Code::InvalidArgument);
    }
}

mod wire {
    // RefreshSessionToken and NewSession are deprecated in the protocol and still served
    #![allow(deprecated)]

    use super::*;

    use oxhaven_proto::flight::flight_service_client::FlightServiceClient;
    use oxhaven_proto::flight::HandshakeRequest as FlightHandshakeRequest;
    use oxhaven_proto::grpc::session_service_client::SessionServiceClient;
    use oxhaven_proto::grpc::{
        ExportNotificationRequest, ExportRequest, HandshakeRequest, PublishRequest, ReleaseRequest,
        TerminationNotificationRequest, WrappedAuthenticationRequest,
    };
    use prost::Message;
    use tokio::sync::oneshot;
    use tonic::metadata::{MetadataMap, MetadataValue};
    use tonic::transport::Channel;
    use tonic::Request;

    use crate::{serve, ServerConfig, ServerState};

    struct Server {
        state: ServerState,
        channel: Channel,
        shutdown: Option<oneshot::Sender<()>>,
        served: tokio::task::JoinHandle<()>,
    }

    impl Server {
        async fn start() -> Self {
            let state = ServerState::new(ServerConfig {
                auth_handlers: AuthHandlers::new(vec![
                    AuthHandler::psk(PSK).unwrap(),
                    AuthHandler::Anonymous,
                ])
                .unwrap(),
                token_expire_ms: DEFAULT_TOKEN_EXPIRE_MS,
            })
            .unwrap();
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap();
            let (shutdown, stopped) = oneshot::channel::<()>();
            let served = tokio::spawn({
                let state = state.clone();
                async move {
                    serve(state, listener, async {
                        let _ = stopped.await;
                    })
                    .await
                    .unwrap();
                }
            });
            let channel = Channel::from_shared(format!("http://{address}"))
                .unwrap()
                .connect()
                .await
                .unwrap();
            Self {
                state,
                channel,
                shutdown: Some(shutdown),
                served,
            }
        }

        fn sessions(&self) -> SessionServiceClient<Channel> {
            SessionServiceClient::new(self.channel.clone())
        }

        /// Logs in with the pre-shared key and returns the bearer header value.
        async fn login(&self) -> String {
            let response = self
                .sessions()
                .refresh_session_token(with_auth(
                    HandshakeRequest::default(),
                    &format!("{PSK_AUTH_TYPE} {PSK}"),
                ))
                .await
                .unwrap();
            bearer_of(response.metadata()).expect("a bearer token in the response headers")
        }

        fn session(&self, bearer: &str) -> Arc<Session> {
            self.state
                .sessions
                .session_for_auth_header(bearer)
                .expect("a live session for the bearer token")
        }
    }

    fn with_auth<T>(message: T, authorization: &str) -> Request<T> {
        let mut request = Request::new(message);
        request
            .metadata_mut()
            .insert("authorization", authorization.parse().unwrap());
        request
    }

    fn bearer_of(metadata: &MetadataMap) -> Option<String> {
        metadata
            .get("authorization")
            .map(|value| value.to_str().unwrap().to_string())
    }

    fn ticket(id: i32) -> Option<oxhaven_proto::grpc::Ticket> {
        Some(export_id_to_ticket(id))
    }

    fn export_request(source: i32, result: i32) -> ExportRequest {
        ExportRequest {
            source_id: ticket(source),
            result_id: ticket(result),
        }
    }

    #[tokio::test]
    async fn calls_without_credentials_are_unauthenticated() {
        let server = Server::start().await;
        let mut client = server.sessions();
        let err = client
            .refresh_session_token(HandshakeRequest::default())
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated);
        assert!(bearer_of(err.metadata()).is_none());
        let err = client
            .release(ReleaseRequest { id: ticket(1) })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated);
        let err = client
            .export_notifications(ExportNotificationRequest {})
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated);
    }

    #[tokio::test]
    async fn refused_credentials_stop_the_call_before_the_service() {
        let server = Server::start().await;
        let mut client = server.sessions();
        for refused in [
            format!("{PSK_AUTH_TYPE} wrong"),
            "Bearer nonsense".to_string(),
        ] {
            // NewSession never needs a session, so only the layer can produce this message
            let err = client
                .new_session(with_auth(HandshakeRequest::default(), &refused))
                .await
                .unwrap_err();
            assert_eq!(err.code(), Code::Unauthenticated, "{refused}");
            assert_eq!(err.message(), "Authentication details invalid");
        }
    }

    #[tokio::test]
    async fn new_session_without_credentials_is_refused() {
        let server = Server::start().await;
        let err = server
            .sessions()
            .new_session(HandshakeRequest::default())
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated);
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn psk_header_creates_a_session_and_the_response_carries_its_token() {
        let server = Server::start().await;
        let mut client = server.sessions();
        let response = client
            .refresh_session_token(with_auth(
                HandshakeRequest::default(),
                &format!("{PSK_AUTH_TYPE} {PSK}"),
            ))
            .await
            .unwrap();
        let bearer = bearer_of(response.metadata()).unwrap();
        let body = response.into_inner();
        assert_eq!(body.metadata_header, b"authorization");
        assert_eq!(body.session_token, bearer.as_bytes());
        assert_eq!(body.token_expiration_delay_millis, DEFAULT_TOKEN_EXPIRE_MS);
        let session = server.session(&bearer);
        assert_eq!(
            body.token_deadline_time_millis,
            session.expiration().unwrap().deadline_millis
        );
        assert_eq!(session.auth_context(), AuthContext::SuperUser);

        // the bearer token names the same session, and a young token is not rotated
        let again = client
            .refresh_session_token(with_auth(HandshakeRequest::default(), &bearer))
            .await
            .unwrap();
        assert_eq!(bearer_of(again.metadata()).unwrap(), bearer);
        assert_eq!(again.into_inner().session_token, bearer.as_bytes());

        let err = client
            .refresh_session_token(with_auth(
                HandshakeRequest {
                    auth_protocol: 1,
                    payload: Vec::new(),
                },
                &bearer,
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
        // an error response of an authenticated call still carries the token
        assert_eq!(bearer_of(err.metadata()).unwrap(), bearer);
    }

    #[tokio::test]
    async fn anonymous_header_creates_an_anonymous_session() {
        let server = Server::start().await;
        let response = server
            .sessions()
            .refresh_session_token(with_auth(HandshakeRequest::default(), "Anonymous"))
            .await
            .unwrap();
        let bearer = bearer_of(response.metadata()).unwrap();
        assert_eq!(
            server.session(&bearer).auth_context(),
            AuthContext::Anonymous
        );
    }

    #[tokio::test]
    async fn token_is_accepted_from_flight_auth_header_and_cookie() {
        let server = Server::start().await;
        let bearer = server.login().await;
        let token = bearer.strip_prefix("Bearer ").unwrap();
        let mut client = server.sessions();

        let mut flight_auth = Request::new(HandshakeRequest::default());
        flight_auth.metadata_mut().insert_bin(
            "auth-token-bin",
            MetadataValue::from_bytes(token.as_bytes()),
        );
        let response = client.refresh_session_token(flight_auth).await.unwrap();
        assert_eq!(bearer_of(response.metadata()).unwrap(), bearer);
        assert!(response.metadata().get("set-cookie").is_none());

        let mut cookie = Request::new(HandshakeRequest::default());
        cookie.metadata_mut().insert(
            "cookie",
            format!("deephaven-auth-cookie={token}").parse().unwrap(),
        );
        cookie
            .metadata_mut()
            .insert("x-deephaven-auth-cookie-request", "true".parse().unwrap());
        let response = client.refresh_session_token(cookie).await.unwrap();
        assert_eq!(bearer_of(response.metadata()).unwrap(), bearer);
        assert_eq!(
            response
                .metadata()
                .get("set-cookie")
                .unwrap()
                .to_str()
                .unwrap(),
            format!("deephaven-auth-cookie={token}")
        );

        let mut other_cookie = Request::new(HandshakeRequest::default());
        other_cookie
            .metadata_mut()
            .insert("cookie", format!("other={token}").parse().unwrap());
        let err = client
            .refresh_session_token(other_cookie)
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated);
    }

    fn wrapped(auth_type: &str, payload: &[u8]) -> FlightHandshakeRequest {
        FlightHandshakeRequest {
            protocol_version: 0,
            payload: WrappedAuthenticationRequest {
                r#type: auth_type.to_string(),
                payload: payload.to_vec(),
            }
            .encode_to_vec()
            .into(),
        }
    }

    /// Sends the messages, completes, and returns the response payloads or the error.
    async fn flight_handshake(
        server: &Server,
        request: Request<tokio_stream::Iter<std::vec::IntoIter<FlightHandshakeRequest>>>,
    ) -> Result<(Vec<Vec<u8>>, MetadataMap), Status> {
        let response = FlightServiceClient::new(server.channel.clone())
            .handshake(request)
            .await?;
        let metadata = response.metadata().clone();
        let mut stream = response.into_inner();
        let mut payloads = Vec::new();
        while let Some(message) = stream.message().await? {
            payloads.push(message.payload.to_vec());
        }
        Ok((payloads, metadata))
    }

    fn messages(
        messages: Vec<FlightHandshakeRequest>,
    ) -> Request<tokio_stream::Iter<std::vec::IntoIter<FlightHandshakeRequest>>> {
        Request::new(tokio_stream::iter(messages))
    }

    #[tokio::test]
    async fn flight_handshake_logs_in_and_returns_the_bare_token() {
        let server = Server::start().await;
        let (payloads, metadata) = flight_handshake(
            &server,
            messages(vec![wrapped(PSK_AUTH_TYPE, PSK.as_bytes())]),
        )
        .await
        .unwrap();
        assert_eq!(payloads.len(), 1);
        let token = String::from_utf8(payloads[0].clone()).unwrap();
        assert!(uuid::Uuid::parse_str(&token).is_ok(), "{token}");
        // the session did not exist when the call's headers were read
        assert!(bearer_of(&metadata).is_none());
        let session = server.session(&format!("Bearer {token}"));
        assert_eq!(session.auth_context(), AuthContext::SuperUser);

        // Flight Auth 1.0 renewal: the token comes back, in the same session
        let (payloads, _) =
            flight_handshake(&server, messages(vec![wrapped("Bearer", token.as_bytes())]))
                .await
                .unwrap();
        assert_eq!(payloads, [token.clone().into_bytes()]);

        let (payloads, _) = flight_handshake(&server, messages(vec![wrapped("Anonymous", b"")]))
            .await
            .unwrap();
        let anonymous = String::from_utf8(payloads[0].clone()).unwrap();
        assert_ne!(anonymous, token);
        assert_eq!(
            server
                .session(&format!("Bearer {anonymous}"))
                .auth_context(),
            AuthContext::Anonymous
        );
    }

    #[tokio::test]
    async fn flight_handshake_refuses_bad_credentials() {
        let server = Server::start().await;
        let unknown_token = uuid::Uuid::new_v4().to_string();
        let cases: Vec<(Vec<FlightHandshakeRequest>, &str)> = vec![
            (
                vec![wrapped(PSK_AUTH_TYPE, b"wrong")],
                "Authentication details invalid",
            ),
            (
                vec![wrapped("Basic", b"x")],
                "Authentication details invalid",
            ),
            (
                vec![wrapped("Bearer", b"not a uuid")],
                "Authentication details invalid",
            ),
            (
                vec![FlightHandshakeRequest {
                    protocol_version: 0,
                    payload: vec![0xff, 0xff, 0xff].into(),
                }],
                "Authentication details invalid",
            ),
            (
                vec![FlightHandshakeRequest::default()],
                "Authentication details invalid",
            ),
            (vec![], "no authentication details provided"),
            // a well-formed token of no session is not answered at all
            (
                vec![wrapped("Bearer", unknown_token.as_bytes())],
                "no authentication details provided",
            ),
        ];
        for (sent, message) in cases {
            let err = flight_handshake(&server, messages(sent.clone()))
                .await
                .unwrap_err();
            assert_eq!(err.code(), Code::Unauthenticated, "{sent:?}");
            assert_eq!(err.message(), message, "{sent:?}");
        }
    }

    #[tokio::test]
    async fn flight_handshake_of_an_authenticated_call_sends_no_message() {
        let server = Server::start().await;
        let mut request = messages(vec![wrapped(PSK_AUTH_TYPE, b"ignored")]);
        request.metadata_mut().insert(
            "authorization",
            format!("{PSK_AUTH_TYPE} {PSK}").parse().unwrap(),
        );
        let (payloads, metadata) = flight_handshake(&server, request).await.unwrap();
        assert!(payloads.is_empty());
        let bearer = bearer_of(&metadata).unwrap();
        assert_eq!(
            server.session(&bearer).auth_context(),
            AuthContext::SuperUser
        );
    }

    #[tokio::test]
    async fn export_release_and_publish_over_the_wire() {
        let server = Server::start().await;
        let bearer = server.login().await;
        let session = server.session(&bearer);
        let source = session
            .new_server_side_export(ExportValue::object(42))
            .unwrap();
        let mut client = server.sessions();

        client
            .export_from_ticket(with_auth(export_request(source.id(), 1), &bearer))
            .await
            .unwrap();
        let exported = session.get_export(1).unwrap();
        assert_eq!(exported.state(), Exported);
        assert_eq!(exported.value().map(|v| number(&v)), Some(42));

        client
            .publish_from_ticket(with_auth(
                PublishRequest {
                    source_id: ticket(1),
                    result_id: ticket(2),
                },
                &bearer,
            ))
            .await
            .unwrap();
        assert_eq!(
            session.get_export(2).unwrap().value().map(|v| number(&v)),
            Some(42)
        );

        // an id is defined once
        let err = client
            .export_from_ticket(with_auth(export_request(source.id(), 1), &bearer))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);

        client
            .release(with_auth(ReleaseRequest { id: ticket(1) }, &bearer))
            .await
            .unwrap();
        assert_eq!(exported.state(), Released);
        // releasing a released export is quietly accepted
        client
            .release(with_auth(ReleaseRequest { id: ticket(1) }, &bearer))
            .await
            .unwrap();
        assert_eq!(session.get_export(2).unwrap().state(), Exported);

        let err = client
            .export_from_ticket(with_auth(export_request(1, 3), &bearer))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition, "released source");
        assert_eq!(session.get_export(3).unwrap().state(), DependencyReleased);
        let err = client
            .export_from_ticket(with_auth(export_request(2, 1), &bearer))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition, "released result id");
    }

    #[tokio::test]
    async fn bad_export_requests_get_the_reference_status_codes() {
        let server = Server::start().await;
        let bearer = server.login().await;
        let session = server.session(&bearer);
        let mut client = server.sessions();
        let raw = |bytes: &[u8]| {
            Some(oxhaven_proto::grpc::Ticket {
                ticket: bytes.to_vec(),
            })
        };

        let export_cases = [
            (
                ExportRequest {
                    source_id: None,
                    result_id: ticket(1),
                },
                Code::InvalidArgument,
            ),
            (
                ExportRequest {
                    source_id: ticket(1),
                    result_id: None,
                },
                Code::InvalidArgument,
            ),
            // an empty source ticket fails where Java names it, before resolving
            (
                ExportRequest {
                    source_id: raw(b""),
                    result_id: ticket(1),
                },
                Code::InvalidArgument,
            ),
            (
                ExportRequest {
                    source_id: raw(b"q123"),
                    result_id: ticket(1),
                },
                Code::FailedPrecondition,
            ),
            (
                ExportRequest {
                    source_id: raw(&[b'e', 1, 2]),
                    result_id: ticket(1),
                },
                Code::FailedPrecondition,
            ),
            (
                ExportRequest {
                    source_id: ticket(5),
                    result_id: raw(&[b'e', 1, 2]),
                },
                Code::FailedPrecondition,
            ),
            (export_request(5, 0), Code::InvalidArgument),
            (export_request(5, -1), Code::InvalidArgument),
            // a server id that does not exist, through the failed source export
            (export_request(-7, 1), Code::FailedPrecondition),
            (export_request(0, 2), Code::InvalidArgument),
        ];
        for (request, code) in export_cases {
            let err = client
                .export_from_ticket(with_auth(request.clone(), &bearer))
                .await
                .unwrap_err();
            assert_eq!(err.code(), code, "{request:?}");
        }
        assert_eq!(session.get_export(1).unwrap().state(), DependencyFailed);

        let release_cases = [
            (ReleaseRequest { id: None }, Code::InvalidArgument),
            (ReleaseRequest { id: ticket(77) }, Code::Unavailable),
            (ReleaseRequest { id: ticket(0) }, Code::Unavailable),
            (
                ReleaseRequest {
                    id: raw(&[b'e', 1]),
                },
                Code::FailedPrecondition,
            ),
        ];
        for (request, code) in release_cases {
            let err = client
                .release(with_auth(request.clone(), &bearer))
                .await
                .unwrap_err();
            assert_eq!(err.code(), code, "{request:?}");
        }

        let err = client
            .publish_from_ticket(with_auth(
                PublishRequest {
                    source_id: ticket(5),
                    result_id: None,
                },
                &bearer,
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument);
    }

    #[tokio::test]
    async fn export_notifications_stream_follows_an_export_and_ends_with_the_session() {
        let server = Server::start().await;
        let bearer = server.login().await;
        let session = server.session(&bearer);
        let mut client = server.sessions();
        let mut stream = client
            .export_notifications(with_auth(ExportNotificationRequest {}, &bearer))
            .await
            .unwrap()
            .into_inner();
        let marker = stream.message().await.unwrap().unwrap();
        assert_eq!(marker.ticket.unwrap().ticket, [b'e', 0, 0, 0, 0]);
        assert_eq!(marker.context, "run is complete");

        let source = session
            .new_server_side_export(ExportValue::object(1))
            .unwrap();
        client
            .export_from_ticket(with_auth(export_request(source.id(), 8), &bearer))
            .await
            .unwrap();
        let mut seen = Vec::new();
        while seen.last() != Some(&(8, Exported)) {
            let notification = stream.message().await.unwrap().unwrap();
            seen.push((
                ticket_to_export_id(&notification.ticket.unwrap().ticket, "test").unwrap(),
                ExportState::try_from(notification.export_state).unwrap(),
            ));
        }
        assert_eq!(
            seen,
            [
                (-1, Unknown),
                (-1, Exported),
                (8, Unknown),
                (8, Pending),
                (8, Queued),
                (8, Running),
                (8, Exported)
            ]
        );

        client
            .close_session(with_auth(HandshakeRequest::default(), &bearer))
            .await
            .unwrap();
        let mut last = Vec::new();
        while let Some(notification) = stream.message().await.unwrap() {
            last.push(ExportState::try_from(notification.export_state).unwrap());
        }
        assert_eq!(last, [Released, Released], "then the stream completes");
    }

    #[tokio::test]
    async fn closed_session_token_stops_working() {
        let server = Server::start().await;
        let bearer = server.login().await;
        let mut client = server.sessions();
        let response = client
            .close_session(with_auth(HandshakeRequest::default(), &bearer))
            .await
            .unwrap();
        assert!(
            bearer_of(response.metadata()).is_none(),
            "a closed session has no token to return"
        );
        let err = client
            .refresh_session_token(with_auth(HandshakeRequest::default(), &bearer))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated);
        assert_eq!(err.message(), "Authentication details invalid");
    }

    #[tokio::test]
    async fn termination_notification_answers_at_shutdown() {
        let mut server = Server::start().await;
        let bearer = server.login().await;
        let mut client = server.sessions();
        let pending = tokio::spawn({
            let bearer = bearer.clone();
            async move {
                client
                    .termination_notification(with_auth(TerminationNotificationRequest {}, &bearer))
                    .await
            }
        });
        // the call registers its listener before shutdown is requested
        while server.session(&bearer).on_close_count() == 0 {
            tokio::task::yield_now().await;
        }
        server.shutdown.take().unwrap().send(()).unwrap();
        let notification = pending.await.unwrap().unwrap().into_inner();
        assert!(!notification.abnormal_termination);
        drop(server.channel);
        tokio::time::timeout(std::time::Duration::from_secs(30), server.served)
            .await
            .expect("the server stops once its clients are gone")
            .unwrap();
    }

    #[tokio::test]
    async fn termination_notification_fails_when_the_session_is_closed() {
        let server = Server::start().await;
        let bearer = server.login().await;
        let mut client = server.sessions();
        let pending = tokio::spawn({
            let bearer = bearer.clone();
            let mut client = client.clone();
            async move {
                client
                    .termination_notification(with_auth(TerminationNotificationRequest {}, &bearer))
                    .await
            }
        });
        while server.session(&bearer).on_close_count() == 0 {
            tokio::task::yield_now().await;
        }
        client
            .close_session(with_auth(HandshakeRequest::default(), &bearer))
            .await
            .unwrap();
        let err = pending.await.unwrap().unwrap_err();
        assert_eq!(err.code(), Code::Unauthenticated);
    }

    #[tokio::test]
    async fn services_not_ported_answer_unimplemented_and_still_authenticate() {
        use oxhaven_proto::grpc::config_service_client::ConfigServiceClient;
        use oxhaven_proto::grpc::ConfigurationConstantsRequest;

        let server = Server::start().await;
        let err = ConfigServiceClient::new(server.channel.clone())
            .get_configuration_constants(with_auth(
                ConfigurationConstantsRequest {},
                &format!("{PSK_AUTH_TYPE} {PSK}"),
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unimplemented);
        let bearer = bearer_of(err.metadata()).expect("the token of the new session");
        server.session(&bearer);

        let err = FlightServiceClient::new(server.channel.clone())
            .do_get(with_auth(
                oxhaven_proto::flight::Ticket {
                    ticket: export_id_to_bytes(1).into(),
                },
                &bearer,
            ))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::Unimplemented);
    }
}
