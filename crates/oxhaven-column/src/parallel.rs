//! Running the pieces of one computation on several threads.
//!
//! No Java counterpart in structure. Java runs the segments of a static `where` and the
//! chunks of a formula column on the threads of its `OperationInitializationThreadPool`
//! (`io.deephaven.engine.table.impl.util.JobScheduler#iterateParallel`); here the pieces of
//! one call run on threads that live for that call, and the calling thread takes part.
//!
//! [`map_pieces`] is for pieces that only compute from values already read: a piece must
//! not take a lock of the engine or read a table. The results come back in piece order
//! whatever the number of threads, so a caller that combines them in that order gets the
//! same answer as on one thread (PORTING.md, rule 7).

use std::cell::Cell;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Mutex, OnceLock, PoisonError};

/// The environment variable that sets the number of threads; see [`threads`].
pub const THREADS_VARIABLE: &str = "OXHAVEN_THREADS";

/// 0 while [`set_threads`] has not been called.
static CONFIGURED: AtomicUsize = AtomicUsize::new(0);

thread_local! {
    /// The number of threads for computations started by this thread, when it is not the
    /// process's: 1 inside a piece, so that pieces do not start threads of their own.
    static OVERRIDE: Cell<Option<usize>> = const { Cell::new(None) };
}

fn default_threads() -> usize {
    static DEFAULT: OnceLock<usize> = OnceLock::new();
    *DEFAULT.get_or_init(|| {
        std::env::var(THREADS_VARIABLE)
            .ok()
            .and_then(|text| text.trim().parse::<usize>().ok())
            .filter(|count| *count > 0)
            .or_else(|| std::thread::available_parallelism().ok().map(usize::from))
            .unwrap_or(1)
    })
}

/// How many threads one computation may use: what [`with_threads`] or [`set_threads`] set,
/// else the value of `OXHAVEN_THREADS` if it is a positive number, else the number of
/// cores available to the process.
pub fn threads() -> usize {
    if let Some(count) = OVERRIDE.with(Cell::get) {
        return count;
    }
    match CONFIGURED.load(Ordering::Relaxed) {
        0 => default_threads(),
        count => count,
    }
}

/// Sets the number of threads for the whole process. 0 restores the default.
pub fn set_threads(count: usize) {
    CONFIGURED.store(count, Ordering::Relaxed);
}

/// Runs `run` with [`threads`] being `count` (at least 1) on this thread.
pub fn with_threads<R>(count: usize, run: impl FnOnce() -> R) -> R {
    struct Restore(Option<usize>);
    impl Drop for Restore {
        fn drop(&mut self) {
            OVERRIDE.with(|cell| cell.set(self.0));
        }
    }
    let _restore = Restore(OVERRIDE.with(|cell| cell.replace(Some(count.max(1)))));
    run()
}

/// `piece(0)`, `piece(1)`, ... `piece(pieces - 1)`, in that order, computed on up to
/// [`threads`] threads. A panic in a piece is raised again on the calling thread.
pub fn map_pieces<T: Send>(pieces: usize, piece: impl Fn(usize) -> T + Sync) -> Vec<T> {
    let workers = threads().min(pieces);
    if workers <= 1 {
        return (0..pieces).map(piece).collect();
    }
    let next = AtomicUsize::new(0);
    let work = || {
        with_threads(1, || {
            let mut done = Vec::new();
            loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                if index >= pieces {
                    return done;
                }
                done.push((index, piece(index)));
            }
        })
    };
    on_workers(workers, pieces, work)
}

/// Runs `work` on `workers` threads, the calling thread among them, and returns the
/// results of the `pieces` pieces they computed between them, in piece order.
fn on_workers<T: Send>(
    workers: usize,
    pieces: usize,
    work: impl Fn() -> Vec<(usize, T)> + Sync,
) -> Vec<T> {
    let mut results: Vec<Option<T>> = (0..pieces).map(|_| None).collect();
    std::thread::scope(|scope| {
        let spawned: Vec<_> = (1..workers).map(|_| scope.spawn(&work)).collect();
        let mut done = work();
        for handle in spawned {
            match handle.join() {
                Ok(more) => done.extend(more),
                Err(panic) => std::panic::resume_unwind(panic),
            }
        }
        for (index, result) in done {
            results[index] = Some(result);
        }
    });
    results
        .into_iter()
        .map(|result| result.expect("every piece was computed"))
        .collect()
}

/// Cuts `out` into pieces of `piece_rows` cells (the last may be shorter) and computes
/// `piece(0, first piece)`, `piece(1, second piece)`, ... on up to [`threads`] threads:
/// each piece writes its own part of `out` and no other. The values the pieces return come
/// back in piece order. A panic in a piece is raised again on the calling thread.
///
/// This is how the pieces of one result are written into memory made once for all of it,
/// instead of each into memory of its own that is then copied.
pub fn fill_pieces<V: Send, T: Send>(
    out: &mut [V],
    piece_rows: usize,
    piece: impl Fn(usize, &mut [V]) -> T + Sync,
) -> Vec<T> {
    let piece_rows = piece_rows.max(1);
    let pieces = out.len().div_ceil(piece_rows);
    let parts = out.chunks_mut(piece_rows).enumerate();
    let workers = threads().min(pieces);
    if workers <= 1 {
        return parts.map(|(index, part)| piece(index, part)).collect();
    }
    // The parts do not overlap, so whichever thread takes one is alone in writing it.
    let parts = Mutex::new(parts);
    let work = || {
        with_threads(1, || {
            let mut done = Vec::new();
            loop {
                let next = parts.lock().unwrap_or_else(PoisonError::into_inner).next();
                let Some((index, part)) = next else {
                    return done;
                };
                done.push((index, piece(index, part)));
            }
        })
    };
    on_workers(workers, pieces, work)
}

/// The pieces `rows` rows are cut into for [`map_pieces`]: `(offset, length)` of each, all
/// `piece_rows` long but the last.
pub fn pieces_of(rows: usize, piece_rows: usize) -> Vec<(usize, usize)> {
    let piece_rows = piece_rows.max(1);
    (0..rows.div_ceil(piece_rows))
        .map(|piece| {
            let offset = piece * piece_rows;
            (offset, piece_rows.min(rows - offset))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn results_come_back_in_piece_order_for_every_thread_count() {
        for count in [1, 2, 3, 4, 9] {
            let squares = with_threads(count, || map_pieces(100, |index| index * index));
            assert_eq!(squares, (0..100).map(|i| i * i).collect::<Vec<_>>());
        }
        assert_eq!(
            with_threads(4, || map_pieces(0, |index| index)),
            Vec::<usize>::new()
        );
    }

    #[test]
    fn pieces_use_other_threads_and_do_not_nest() {
        let main = std::thread::current().id();
        let seen = with_threads(4, || {
            map_pieces(64, |_| {
                std::thread::sleep(std::time::Duration::from_millis(1));
                (std::thread::current().id(), threads())
            })
        });
        assert!(seen.iter().any(|(id, _)| *id != main));
        assert!(seen.iter().all(|(_, inner)| *inner == 1));
        // one thread: everything on the caller
        let seen = with_threads(1, || map_pieces(8, |_| std::thread::current().id()));
        assert!(seen.iter().all(|id| *id == main));
    }

    #[test]
    fn a_panic_in_a_piece_reaches_the_caller_with_its_message() {
        let caught = std::panic::catch_unwind(|| {
            with_threads(3, || {
                map_pieces(10, |index| {
                    if index == 7 {
                        panic!("piece seven");
                    }
                    index
                })
            })
        });
        let panic = caught.expect_err("the piece panicked");
        assert_eq!(panic.downcast_ref::<&str>(), Some(&"piece seven"));
    }

    #[test]
    fn with_threads_restores_the_previous_count() {
        let outer = threads();
        with_threads(3, || {
            assert_eq!(threads(), 3);
            with_threads(0, || assert_eq!(threads(), 1));
            assert_eq!(threads(), 3);
        });
        assert_eq!(threads(), outer);
    }

    #[test]
    fn each_piece_fills_its_own_part_on_any_number_of_threads() {
        let main = std::thread::current().id();
        for count in [1, 2, 3, 4, 9] {
            let mut out = vec![0_usize; 1003];
            let seen = with_threads(count, || {
                fill_pieces(&mut out, 10, |index, part| {
                    std::thread::sleep(std::time::Duration::from_micros(200));
                    for (cell, value) in part.iter_mut().zip(index * 10..) {
                        *cell = value * 3;
                    }
                    (index, part.len(), std::thread::current().id(), threads())
                })
            });
            assert_eq!(out, (0..1003).map(|i| i * 3).collect::<Vec<_>>());
            // In piece order, the last piece shorter, and no piece starts threads.
            assert_eq!(seen.len(), 101);
            for (place, (index, length, _, inner)) in seen.iter().enumerate() {
                let expected = if place == 100 { 3 } else { 10 };
                assert_eq!((*index, *length), (place, expected));
                assert_eq!(*inner, 1);
            }
            assert_eq!(seen.iter().any(|(_, _, id, _)| *id != main), count > 1);
        }
        let mut empty: [u8; 0] = [];
        assert!(fill_pieces(&mut empty, 4, |index, _| index).is_empty());
        let mut three = [0_u8; 3];
        assert_eq!(fill_pieces(&mut three, 0, |_, part| part.len()), vec![1; 3]);
        let caught = std::panic::catch_unwind(|| {
            let mut out = [0_u8; 40];
            with_threads(3, || {
                fill_pieces(&mut out, 4, |index, _| assert!(index != 7, "piece seven"))
            })
        });
        assert!(caught.is_err());
    }

    #[test]
    fn rows_are_cut_into_pieces_that_cover_them() {
        assert_eq!(pieces_of(10, 4), vec![(0, 4), (4, 4), (8, 2)]);
        assert_eq!(pieces_of(8, 4), vec![(0, 4), (4, 4)]);
        assert_eq!(pieces_of(0, 4), vec![]);
        assert_eq!(pieces_of(3, 0), vec![(0, 1), (1, 1), (2, 1)]);
    }
}
