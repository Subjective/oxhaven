//! Where export work runs.

use std::sync::mpsc;
use std::sync::Mutex;

/// A unit of export work.
pub type Job = Box<dyn FnOnce() + Send>;

/// Runs export work off the thread that submitted it. Tests substitute an executor they can
/// step by hand.
///
/// Behavior: `io.deephaven.server.util.Scheduler#runImmediately`, `#runSerially`
pub trait Executor: Send + Sync {
    /// Runs `job` soon, concurrently with other jobs.
    fn run(&self, job: Job);

    /// Runs `job` after every job passed to this method before it has finished.
    fn run_serially(&self, job: Job);
}

/// Runs concurrent jobs on the blocking pool of a tokio runtime and serial jobs on one
/// dedicated thread.
pub struct TokioExecutor {
    runtime: tokio::runtime::Handle,
    serial: Mutex<mpsc::Sender<Job>>,
}

impl TokioExecutor {
    pub fn new(runtime: tokio::runtime::Handle) -> Self {
        let (serial, jobs) = mpsc::channel::<Job>();
        std::thread::Builder::new()
            .name("oxhaven-serial-exports".to_string())
            .spawn(move || {
                // ends when the executor, and with it the sender, is dropped
                for job in jobs {
                    job();
                }
            })
            .expect("spawn the serial export thread");
        Self {
            runtime,
            serial: Mutex::new(serial),
        }
    }
}

impl Executor for TokioExecutor {
    fn run(&self, job: Job) {
        self.runtime.spawn_blocking(job);
    }

    fn run_serially(&self, job: Job) {
        let serial = self.serial.lock().unwrap_or_else(|e| e.into_inner());
        // the receiving thread lives as long as this sender
        let _ = serial.send(job);
    }
}
