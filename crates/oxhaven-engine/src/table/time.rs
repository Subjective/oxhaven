//! The time table: one row per period of an injected clock.

use super::{failure, QueryTable, RootSource, TableSpec};
use arrow::array::{ArrayRef, TimestampNanosecondArray};
use oxhaven_contracts::graph::{UpdateGraph, UpdateSource};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table, TableAttributes, TableFailure};
use oxhaven_contracts::update::TableUpdate;
use std::sync::{Arc, Mutex, OnceLock, PoisonError, Weak};
use std::time::{SystemTime, UNIX_EPOCH};

/// Where a time table reads the time. Tests and the differential harness inject a clock
/// they control; nothing in the engine reads the wall clock on its own.
pub trait TimeSource: Send + Sync {
    /// Nanoseconds since the epoch, UTC.
    fn now_nanos(&self) -> i64;
}

/// The system's wall clock.
pub struct WallClock;

impl TimeSource for WallClock {
    fn now_nanos(&self) -> i64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(since) => since.as_nanos() as i64,
            Err(before) => -(before.duration().as_nanos() as i64),
        }
    }
}

/// How to build a time table.
pub struct TimeTableOptions {
    /// The time of row 0, in nanoseconds since the epoch. `None` means the first refresh
    /// decides: its time rounded down to a multiple of the period.
    pub start_nanos: Option<i64>,
    /// Nanoseconds between rows. Must be positive.
    pub period_nanos: i64,
    /// Whether each row exists for one cycle only.
    pub blink: bool,
}

/// A refreshing table with one `Instant` column, `Timestamp`, that gains a row for every
/// period that has passed on `clock`: row `k` holds `start + k * period`.
///
/// With a start time, the rows due at construction are there from the beginning. Without
/// one, the table starts empty and its first refresh adds row 0. A table that is not blink is
/// append-only and flat. A blink table removes, on each refresh, the rows it had.
///
/// Behavior: io.deephaven.engine.table.impl.TimeTable
pub fn time_table(
    graph: Arc<dyn UpdateGraph>,
    clock: Arc<dyn TimeSource>,
    options: TimeTableOptions,
) -> Result<Arc<QueryTable>, TableFailure> {
    let TimeTableOptions {
        start_nanos,
        period_nanos,
        blink,
    } = options;
    if period_nanos <= 0 {
        return Err(failure(format!(
            "Invalid time period: {period_nanos} nanoseconds"
        )));
    }
    let column = Arc::new(Timestamps {
        column_type: ColumnType::Instant,
        start: OnceLock::new(),
        period: period_nanos,
    });
    // Behavior: the `refresh(false)` in the TimeTable constructor.
    let mut last_row = -1;
    if let Some(start) = start_nanos {
        column.start.get_or_init(|| start);
        let now = clock.now_nanos();
        if now >= start {
            last_row = elapsed(now, start)? / period_nanos;
        }
    }

    let definition = match TableDefinition::new(vec![ColumnDefinition::new(
        "Timestamp",
        ColumnType::Instant,
    )]) {
        Ok(definition) => definition,
        Err(e) => panic!("{e}"),
    };
    let mut attributes = TableAttributes::default();
    if blink {
        attributes.blink = true;
    } else {
        attributes.add_only = true;
        attributes.append_only = true;
    }
    let mut spec = TableSpec::new(
        definition,
        range(0, last_row),
        vec![column.clone() as Arc<dyn ColumnSource>],
    )
    .with_attributes(attributes);
    spec.flat = !blink;

    Ok(QueryTable::root(graph, spec, |table| RootSource {
        source: Arc::new(Refresher {
            table,
            clock,
            column,
            blink,
            last_row: Mutex::new(last_row),
        }),
        input: None,
    }))
}

/// Nanoseconds from `start` to `now`, which is not before it.
///
/// Behavior: `io.deephaven.time.DateTimeUtils#minus(Instant, Instant)`, which throws where
/// the difference overflows a `long` (`DateTimeUtils#checkOverflowPlus`): a start before
/// 1970 that is further from the clock than about 292 years.
fn elapsed(now: i64, start: i64) -> Result<i64, TableFailure> {
    now.checked_sub(start).ok_or_else(|| {
        failure(format!(
            "Subtracting {start} nanos from {now} would overflow"
        ))
    })
}

/// The rows `first..=last`, or none if `last < first`.
fn range(first: i64, last: i64) -> RowSet {
    if last < first {
        RowSet::empty()
    } else {
        RowSet::from_range(first, last)
    }
}

struct Refresher {
    table: Weak<QueryTable>,
    clock: Arc<dyn TimeSource>,
    column: Arc<Timestamps>,
    blink: bool,
    last_row: Mutex<i64>,
}

impl UpdateSource for Refresher {
    /// Behavior: io.deephaven.engine.table.impl.TimeTable#refresh
    fn refresh(&self) {
        let Some(table) = self.table.upgrade() else {
            return;
        };
        if table.is_failed() {
            return;
        }
        let now = self.clock.now_nanos();
        let period = self.column.period;
        let mut last_row = self.last_row.lock().unwrap_or_else(PoisonError::into_inner);
        let first_new = *last_row + 1;
        match self.column.start.get() {
            None => {
                // Behavior: io.deephaven.function.Numeric#lowerBin(long, long)
                self.column
                    .start
                    .get_or_init(|| now.div_euclid(period) * period);
                *last_row = 0;
            }
            Some(&start) if now >= start => match elapsed(now, start) {
                Ok(elapsed) => *last_row = (*last_row).max(elapsed / period),
                Err(failure) => {
                    drop(last_row);
                    table.fail(failure);
                    return;
                }
            },
            Some(_) => {}
        }
        let added = range(first_new, *last_row);
        let removed = if self.blink {
            (*table.row_set()).clone()
        } else {
            RowSet::empty()
        };
        drop(last_row);
        table.notify(TableUpdate {
            added,
            removed,
            ..TableUpdate::empty()
        });
    }
}

/// The `Timestamp` column: computed from the row key, never stored.
///
/// Behavior: io.deephaven.engine.table.impl.TimeTable.SyntheticInstantSource
struct Timestamps {
    column_type: ColumnType,
    /// The time of row 0. Unset until the first refresh of a table made without a start.
    start: OnceLock<i64>,
    period: i64,
}

impl ColumnSource for Timestamps {
    fn column_type(&self) -> &ColumnType {
        &self.column_type
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        let start = match self.start.get() {
            Some(start) => *start,
            None if rows.is_empty() => 0,
            None => return Err(failure("the time table has no rows yet")),
        };
        let values: Vec<i64> = rows.iter().map(|key| start + self.period * key).collect();
        Ok(Arc::new(
            TimestampNanosecondArray::from(values).with_timezone("UTC"),
        ))
    }

    /// A row's value never changes.
    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.chunk(rows)
    }
}
