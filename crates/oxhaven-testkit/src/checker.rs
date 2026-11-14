//! The self-consistency checker: a listener that checks every update a table delivers.

use crate::compare::{array_difference, diff_tables, read_columns, with_lock, TableSnapshot};
use crate::graph::lock;
use crate::recorder::{current_step, listener_satisfied};
use arrow::array::{ArrayRef, UInt64Array};
use arrow::compute::take;
use oxhaven_contracts::graph::{ClockPhase, Dependency};
use oxhaven_contracts::rowset::RowKey;
use oxhaven_contracts::table::{Table, TableFailure, UpdateListener};
use oxhaven_contracts::update::TableUpdate;
use std::fmt;
use std::sync::{Arc, Mutex};

/// As in Java's `TableUpdateValidator`, the first few problems are kept and the rest dropped.
const MAX_ISSUES: usize = 10;

type Recompute = Box<dyn Fn() -> Arc<dyn Table> + Send + Sync>;

/// What a [`ConsistencyChecker`] found wrong with its table, oldest first.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ConsistencyError {
    pub issues: Vec<String>,
}

impl fmt::Display for ConsistencyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "table is inconsistent:")?;
        for issue in &self.issues {
            write!(f, "\n - {issue}")?;
        }
        Ok(())
    }
}

impl std::error::Error for ConsistencyError {}

/// Checks that a table's updates describe what actually happened to it.
///
/// The checker keeps its own copy of the table's rows and values as of the end of each cycle.
/// When an update arrives it checks the update, the table's previous state and the table's
/// current state against that copy; see the crate documentation for the list. Problems are
/// collected, not thrown, because a panic inside a cycle would hide them behind a broken
/// graph: call [`verify`](Self::verify) or [`assert_consistent`](Self::assert_consistent)
/// after every cycle. Once a problem is found it stays reported.
///
/// Behavior: io.deephaven.engine.table.impl.TableUpdateValidator and
/// io.deephaven.engine.testutil.EvalNugget#validate. Unlike the Java validator it is not
/// itself a table, and it also checks previous values of rows the update does not touch.
///
/// Nothing should wait on a checker as a [`Dependency`]: it can report itself satisfied
/// while its update is still queued, because the contracts give a listener no way to see
/// that.
pub struct ConsistencyChecker {
    table: Arc<dyn Table>,
    recompute: Option<Recompute>,
    state: Mutex<State>,
}

struct State {
    /// The table as of the end of the last cycle, or `None` if it could not be read.
    expected: Option<TableSnapshot>,
    issues: Vec<String>,
    updates_seen: u64,
    ran_step: Option<u64>,
}

impl ConsistencyChecker {
    /// Starts checking `table`. Keep the returned value: dropping it stops the checks.
    ///
    /// Works on any table, static or refreshing, and may be called between cycles or from
    /// inside one.
    pub fn attach(table: Arc<dyn Table>) -> Arc<Self> {
        Self::attach_inner(table, None)
    }

    /// As [`attach`](Self::attach), and [`verify`](Self::verify) also compares the table with
    /// the table `recompute` returns, by [`diff_tables`]. `recompute` builds the same result
    /// from scratch out of the current state of the inputs; it runs between cycles.
    ///
    /// Behavior: io.deephaven.engine.testutil.EvalNugget#validate
    pub fn attach_with_recompute(
        table: Arc<dyn Table>,
        recompute: impl Fn() -> Arc<dyn Table> + Send + Sync + 'static,
    ) -> Arc<Self> {
        Self::attach_inner(table, Some(Box::new(recompute)))
    }

    fn attach_inner(table: Arc<dyn Table>, recompute: Option<Recompute>) -> Arc<Self> {
        with_lock(&*table.clone(), || {
            // Inside a cycle, a table that is not yet satisfied may still deliver this step's
            // update, which starts from the previous state. A satisfied table will not, and
            // its current state is where the next update starts.
            let update_may_follow = table.update_graph().is_some_and(|graph| {
                let clock = graph.clock();
                clock.phase == ClockPhase::Updating && !table.satisfied(clock.step)
            });
            let snapshot = if update_may_follow {
                TableSnapshot::previous(&*table)
            } else {
                TableSnapshot::current(&*table)
            };
            let mut issues = Vec::new();
            let expected = match snapshot {
                Ok(snapshot) => Some(snapshot),
                Err(e) => {
                    issues.push(format!("cannot read the table: {e}"));
                    None
                }
            };
            let checker = Arc::new(Self {
                table,
                recompute,
                state: Mutex::new(State {
                    expected,
                    issues,
                    updates_seen: 0,
                    ran_step: None,
                }),
            });
            let listener: Arc<dyn UpdateListener> = checker.clone();
            checker.table.add_listener(&listener);
            checker
        })
    }

    /// The number of updates received so far.
    pub fn updates_seen(&self) -> u64 {
        lock(&self.state).updates_seen
    }

    /// Runs the between-cycle checks and returns every problem found since the checker was
    /// attached. Call after each cycle, not from inside one.
    pub fn verify(&self) -> Result<(), ConsistencyError> {
        let table = &*self.table;
        with_lock(table, || {
            let graph = table.update_graph();
            if graph.is_some_and(|graph| graph.clock().phase == ClockPhase::Updating) {
                return Err(ConsistencyError {
                    issues: vec!["verify was called inside a cycle".to_string()],
                });
            }
            let expected = lock(&self.state).expected.clone();
            let mut issues = Vec::new();
            let mut now = None;
            if let Some(expected) = expected {
                now = self.check_idle(&expected, &mut issues);
            }
            if let Some(recompute) = &self.recompute {
                if let Some(difference) = diff_tables(&*recompute(), table) {
                    issues.push(format!("table differs from its recompute: {difference}"));
                }
            }

            let mut state = lock(&self.state);
            let step = current_step(table);
            for issue in issues {
                note(&mut state.issues, step, issue);
            }
            // Start the next cycle from what the table holds now, so that one silent change
            // is reported once.
            state.expected = now;
            if state.issues.is_empty() {
                Ok(())
            } else {
                Err(ConsistencyError {
                    issues: state.issues.clone(),
                })
            }
        })
    }

    /// Panics with every problem found if [`verify`](Self::verify) fails.
    #[track_caller]
    pub fn assert_consistent(&self) {
        if let Err(e) = self.verify() {
            panic!("{e}");
        }
    }

    /// Between cycles the table must hold what it held when its last update was checked, and
    /// its previous state must equal its current state. Returns the table's current state.
    ///
    /// Behavior: io.deephaven.engine.table.impl.TableUpdateValidator#deepValidation
    fn check_idle(
        &self,
        expected: &TableSnapshot,
        issues: &mut Vec<String>,
    ) -> Option<TableSnapshot> {
        let table = &*self.table;
        let now = match TableSnapshot::current(table) {
            Ok(now) => now,
            Err(e) => {
                issues.push(format!("cannot read the table: {e}"));
                return None;
            }
        };
        if now.rows != expected.rows {
            issues.push(format!(
                "row set changed without an update: was {:?}, now {:?}",
                expected.rows, now.rows
            ));
        } else {
            self.compare_columns(
                "changed without an update",
                &expected.columns,
                &now.columns,
                &now.rows.iter().collect::<Vec<_>>(),
                issues,
            );
        }
        let prev_rows = table.prev_row_set();
        if *prev_rows != now.rows {
            issues.push(format!(
                "previous row set {prev_rows:?} differs from the row set {:?} between cycles",
                now.rows
            ));
        } else {
            match read_columns(table, &now.rows, true) {
                Err(e) => issues.push(format!("cannot read previous values: {e}")),
                Ok(prev) => self.compare_columns(
                    "previous value differs from the current value between cycles",
                    &now.columns,
                    &prev,
                    &now.rows.iter().collect::<Vec<_>>(),
                    issues,
                ),
            }
        }
        Some(now)
    }

    /// Checks one update against `before`, the table at the end of the last cycle. Returns
    /// the table's current state.
    ///
    /// Behavior: io.deephaven.engine.table.impl.TableUpdateValidator#onUpdate, with
    /// `aggressiveUpdateValidation` on.
    fn check_update(
        &self,
        before: &TableSnapshot,
        update: &TableUpdate,
        issues: &mut Vec<String>,
    ) -> Option<TableSnapshot> {
        let table = &*self.table;
        let before_keys: Vec<RowKey> = before.rows.iter().collect();

        // The previous state must still read as it was.
        let prev_rows = table.prev_row_set();
        if *prev_rows != before.rows {
            issues.push(format!(
                "previous row set is {prev_rows:?} but the table held {:?} at the end of the \
                 last cycle",
                before.rows
            ));
        }
        match read_columns(table, &before.rows, true) {
            Err(e) => issues.push(format!("cannot read previous values: {e}")),
            Ok(prev) => self.compare_columns(
                "previous value is not the value at the end of the last cycle",
                &before.columns,
                &prev,
                &before_keys,
                issues,
            ),
        }

        let rows = table.row_set();
        let now = match read_columns(table, &rows, false) {
            Ok(columns) => TableSnapshot {
                rows: (*rows).clone(),
                columns,
            },
            Err(e) => {
                issues.push(format!("cannot read the table: {e}"));
                return None;
            }
        };

        // The update must be a valid change from the old rows to the new rows.
        match update.apply_to(&before.rows) {
            Err(e) => issues.push(format!(
                "update is not valid for the previous row set {:?}: {e}",
                before.rows
            )),
            Ok(expected) if expected != now.rows => issues.push(format!(
                "update yields rows {expected:?} but the table has {:?}",
                now.rows
            )),
            Ok(_) => self.check_unmodified(before, &now, update, issues),
        }
        Some(now)
    }

    /// Every value the update does not claim to have added or modified must be the value the
    /// row had before, at the key it had before the shifts.
    fn check_unmodified(
        &self,
        before: &TableSnapshot,
        now: &TableSnapshot,
        update: &TableUpdate,
        issues: &mut Vec<String>,
    ) {
        let unshift = update.shifted.inverse();
        for c in 0..now.columns.len() {
            let claimed_modified = update.modified_columns.contains(c);
            let kept: Vec<RowKey> = now
                .rows
                .iter()
                .filter(|&key| {
                    !update.added.contains(key)
                        && !(claimed_modified && update.modified.contains(key))
                })
                .collect();
            let was: UInt64Array = kept
                .iter()
                .map(|&key| before.rows.find(unshift.apply_to_key(key)))
                .collect();
            let is: UInt64Array = kept.iter().map(|&key| now.rows.find(key)).collect();
            let taken = take(before.columns[c].as_ref(), &was, None)
                .and_then(|was| Ok((was, take(now.columns[c].as_ref(), &is, None)?)));
            match taken {
                Ok((was, is)) => self.compare_column(
                    c,
                    "value changed in a row or column the update does not report as modified",
                    &was,
                    &is,
                    &kept,
                    issues,
                ),
                Err(e) => issues.push(format!("cannot compare column {c}: {e}")),
            }
        }
    }

    fn compare_columns(
        &self,
        what: &str,
        expected: &[ArrayRef],
        actual: &[ArrayRef],
        keys: &[RowKey],
        issues: &mut Vec<String>,
    ) {
        for (c, (expected, actual)) in expected.iter().zip(actual).enumerate() {
            self.compare_column(c, what, expected, actual, keys, issues);
        }
    }

    /// Compares one column's values at `keys`, in the current key space.
    fn compare_column(
        &self,
        column: usize,
        what: &str,
        expected: &ArrayRef,
        actual: &ArrayRef,
        keys: &[RowKey],
        issues: &mut Vec<String>,
    ) {
        let place = |i: usize| format!("row key {}", keys[i]);
        if let Some(difference) = array_difference(expected, actual, place) {
            let name = &self.table.definition().columns()[column].name;
            issues.push(format!("column {name}: {what}: {difference}"));
        }
    }
}

fn note(issues: &mut Vec<String>, step: Option<u64>, issue: String) {
    if issues.len() < MAX_ISSUES {
        issues.push(match step {
            Some(step) => format!("step {step}: {issue}"),
            None => issue,
        });
    }
}

impl Dependency for ConsistencyChecker {
    fn satisfied(&self, step: u64) -> bool {
        let ran_step = lock(&self.state).ran_step;
        listener_satisfied(&*self.table, ran_step, step)
    }
}

impl UpdateListener for ConsistencyChecker {
    fn on_update(&self, update: &TableUpdate) -> Result<(), TableFailure> {
        let step = current_step(&*self.table);
        let before = {
            let mut state = lock(&self.state);
            state.updates_seen += 1;
            state.ran_step = step;
            state.expected.take()
        };
        let mut issues = Vec::new();
        let now = match &before {
            Some(before) => self.check_update(before, update, &mut issues),
            // The table could not be read before, which is already reported. Try again.
            None => TableSnapshot::current(&*self.table).ok(),
        };
        let mut state = lock(&self.state);
        for issue in issues {
            note(&mut state.issues, step, issue);
        }
        state.expected = now;
        Ok(())
    }

    fn on_failure(&self, failure: &TableFailure) {
        let step = current_step(&*self.table);
        let mut state = lock(&self.state);
        state.ran_step = step;
        note(&mut state.issues, step, format!("table failed: {failure}"));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        definition, list_of, longs, strings, ReferenceGraph, ReferenceTable, ScriptedTable,
        TableChange,
    };
    use arrow::array::new_null_array;
    use oxhaven_contracts::graph::{UpdateGraph, UpdateSource};
    use oxhaven_contracts::rowset::RowSet;
    use oxhaven_contracts::schema::{ColumnType, TableDefinition};
    use oxhaven_contracts::shift::{RowSetShiftData, ShiftRange};
    use oxhaven_contracts::table::{ColumnSource, TableAttributes};
    use oxhaven_contracts::update::ModifiedColumnSet;

    fn sym_qty() -> TableDefinition {
        definition(&[("Sym", ColumnType::String), ("Qty", ColumnType::Long)])
    }

    /// Rows 0, 1, 5, 6 holding a..d and 10..40.
    fn scripted(graph: &Arc<ReferenceGraph>) -> Arc<ScriptedTable> {
        ScriptedTable::new(
            graph.clone(),
            sym_qty(),
            RowSet::from_keys([0, 1, 5, 6]),
            vec![strings(["a", "b", "c", "d"]), longs([10, 20, 30, 40])],
        )
    }

    fn update() -> TableUpdate {
        TableUpdate::empty()
    }

    fn keys<const N: usize>(keys: [RowKey; N]) -> RowSet {
        RowSet::from_keys(keys)
    }

    /// The problems the checker reports after one cycle that applies `change` and reports
    /// `reported`.
    fn issues_for(change: TableChange, reported: TableUpdate) -> Vec<String> {
        let graph = ReferenceGraph::new();
        let script = scripted(&graph);
        let checker = ConsistencyChecker::attach(script.table());
        script.push_misreported(change, reported);
        graph.run_cycle();
        assert_eq!(checker.updates_seen(), 1);
        checker.verify().err().map(|e| e.issues).unwrap_or_default()
    }

    #[track_caller]
    fn assert_one_issue(issues: Vec<String>, expected: &str) {
        assert_eq!(issues.len(), 1, "{issues:#?}");
        assert!(issues[0].contains(expected), "{issues:#?}");
    }

    #[test]
    fn truthful_updates_of_every_kind_pass() {
        let graph = ReferenceGraph::new();
        let script = scripted(&graph);
        let checker = ConsistencyChecker::attach(script.table());
        script.push(
            TableChange::new()
                .remove([1])
                .shift(5, 6, -3)
                .add([1, 9], vec![strings(["x", "y"]), longs([7, 8])])
                .modify([0, 3], [(1, longs([11, 41]))]),
        );
        script.push(TableChange::new().shift(0, 3, 100));
        script.push(TableChange::new().modify([100, 9], [(0, strings(["p", "q"]))]));
        for seen in 1..=3 {
            graph.run_cycle();
            assert_eq!(checker.verify(), Ok(()));
            assert_eq!(checker.updates_seen(), seen);
        }
        // A cycle without an update is fine too.
        graph.run_cycle();
        assert_eq!(checker.verify(), Ok(()));
        assert_eq!(checker.updates_seen(), 3);
    }

    #[test]
    fn honest_change_reported_as_itself_has_no_issues() {
        // The control for the tests below: the same helper, with a truthful report.
        let change = TableChange::new().remove([0]);
        let reported = change.update.clone();
        assert_eq!(issues_for(change, reported), Vec::<String>::new());
    }

    #[test]
    fn update_that_removes_a_row_that_was_not_there_is_caught() {
        let issues = issues_for(
            TableChange::new().remove([0]),
            TableUpdate {
                removed: keys([0, 9]),
                ..update()
            },
        );
        assert_one_issue(
            issues,
            "step 1: update is not valid for the previous row set RowSet{0-1,5-6}: removed rows \
             not in previous: RowSet{9}",
        );
    }

    #[test]
    fn update_that_adds_a_row_that_already_exists_is_caught() {
        let issues = issues_for(
            TableChange::new().remove([0]),
            TableUpdate {
                removed: keys([0]),
                added: keys([1]),
                ..update()
            },
        );
        assert_one_issue(issues, "added rows already present: RowSet{1}");
    }

    #[test]
    fn update_that_modifies_rows_without_columns_is_caught() {
        let issues = issues_for(
            TableChange::new().modify([0], [(1, longs([11]))]),
            TableUpdate {
                modified: keys([0]),
                ..update()
            },
        );
        assert_one_issue(
            issues,
            "modified rows and modified columns must both be empty",
        );
    }

    #[test]
    fn update_that_yields_another_row_set_is_caught() {
        let issues = issues_for(
            TableChange::new().remove([0]),
            TableUpdate {
                removed: keys([1]),
                ..update()
            },
        );
        assert_one_issue(
            issues,
            "update yields rows RowSet{0,5-6} but the table has RowSet{1,5-6}",
        );
    }

    #[test]
    fn unreported_add_is_caught() {
        let issues = issues_for(
            TableChange::new()
                .remove([0])
                .add([9], vec![strings(["x"]), longs([1])]),
            TableUpdate {
                removed: keys([0]),
                ..update()
            },
        );
        assert_one_issue(
            issues,
            "update yields rows RowSet{1,5-6} but the table has RowSet{1,5-6,9}",
        );
    }

    #[test]
    fn shift_reported_with_the_wrong_delta_is_caught() {
        let shifted = |delta| {
            RowSetShiftData::new(vec![ShiftRange {
                first: 5,
                last: 6,
                delta,
            }])
        };
        let issues = issues_for(
            TableChange::new().shift(5, 6, 2),
            TableUpdate {
                shifted: shifted(3).unwrap(),
                ..update()
            },
        );
        assert_one_issue(
            issues,
            "update yields rows RowSet{0-1,8-9} but the table has RowSet{0-1,7-8}",
        );
    }

    #[test]
    fn row_changed_but_not_reported_as_modified_is_caught() {
        let issues = issues_for(
            TableChange::new().modify([0, 1], [(1, longs([11, 21]))]),
            TableUpdate {
                modified: keys([0]),
                modified_columns: ModifiedColumnSet::of([1]),
                ..update()
            },
        );
        assert_one_issue(
            issues,
            "column Qty: value changed in a row or column the update does not report as \
             modified: row key 1: expected 20 but found 21",
        );
    }

    /// Rows 0, 1, 5 of a vector column holding `[1, 2]`, null and `[]`, and the checker's
    /// problems after one cycle that applies `change` and reports `reported`.
    fn list_issues_for(change: TableChange, reported: Option<TableUpdate>) -> Vec<String> {
        let vector = ColumnType::vector(ColumnType::Long);
        let graph = ReferenceGraph::new();
        let script = ScriptedTable::new(
            graph.clone(),
            definition(&[("V", vector.clone())]),
            RowSet::from_keys([0, 1, 5]),
            vec![list_of(&vector, [Some(2), None, Some(0)], longs([1, 2]))],
        );
        let checker = ConsistencyChecker::attach(script.table());
        match reported {
            Some(reported) => script.push_misreported(change, reported),
            None => script.push(change),
        }
        graph.run_cycle();
        assert_eq!(checker.updates_seen(), 1);
        checker.verify().err().map(|e| e.issues).unwrap_or_default()
    }

    fn long_lists<const N: usize, const M: usize>(
        lengths: [Option<usize>; N],
        elements: [i64; M],
    ) -> ArrayRef {
        let vector = ColumnType::vector(ColumnType::Long);
        list_of(&vector, lengths, longs(elements))
    }

    #[test]
    fn truthful_updates_of_a_list_column_pass() {
        let change = TableChange::new()
            .remove([1])
            .shift(5, 5, -3)
            .add([7, 9], vec![long_lists([None, Some(1)], [4])])
            .modify([0], [(0, long_lists([Some(3)], [1, 2, 3]))]);
        assert_eq!(list_issues_for(change, None), Vec::<String>::new());
    }

    #[test]
    fn list_cell_changed_but_not_reported_as_modified_is_caught() {
        let reported = || {
            Some(TableUpdate {
                modified: keys([0]),
                modified_columns: ModifiedColumnSet::of([0]),
                ..update()
            })
        };
        let unreported = "column V: value changed in a row or column the update does not \
                          report as modified: ";
        // A null cell that became an empty one.
        let change =
            TableChange::new().modify([0, 1], [(0, long_lists([Some(2), Some(0)], [1, 2]))]);
        assert_one_issue(
            list_issues_for(change, reported()),
            &format!("{unreported}row key 1: expected null but found []"),
        );
        // A cell that gained an element.
        let change =
            TableChange::new().modify([0, 5], [(0, long_lists([Some(2), Some(1)], [1, 2, 3]))]);
        assert_one_issue(
            list_issues_for(change, reported()),
            &format!("{unreported}row key 5: expected [] but found [3]"),
        );
        // A cell with another element in the same place.
        let change = TableChange::new().modify([0], [(0, long_lists([Some(2)], [1, 9]))]);
        let issues = list_issues_for(
            change,
            Some(TableUpdate {
                modified: keys([5]),
                modified_columns: ModifiedColumnSet::of([0]),
                ..update()
            }),
        );
        assert_one_issue(
            issues,
            &format!("{unreported}row key 0: expected [1, 2] but found [1, 9]"),
        );
    }

    #[test]
    fn column_changed_but_not_in_the_modified_column_set_is_caught() {
        let issues = issues_for(
            TableChange::new().modify([0], [(0, strings(["z"])), (1, longs([11]))]),
            TableUpdate {
                modified: keys([0]),
                modified_columns: ModifiedColumnSet::of([1]),
                ..update()
            },
        );
        assert_one_issue(
            issues,
            "column Sym: value changed in a row or column the update does not report as \
             modified: row key 0: expected a but found z",
        );
    }

    #[test]
    fn values_that_do_not_follow_the_reported_shift_are_caught() {
        // Row 5 is removed and row 6 moves down to 5. Reporting it as the removal of row 6
        // gives the right row set, but row 5 now holds what row 6 held.
        let issues = issues_for(
            TableChange::new().remove([5]).shift(6, 6, -1),
            TableUpdate {
                removed: keys([6]),
                ..update()
            },
        );
        assert_eq!(issues.len(), 2, "{issues:#?}");
        assert!(
            issues[0].contains("column Sym: value changed"),
            "{issues:#?}"
        );
        assert!(issues[0].contains("row key 5: expected c but found d"));
        assert!(
            issues[1].contains("column Qty: value changed"),
            "{issues:#?}"
        );
        assert!(issues[1].contains("row key 5: expected 30 but found 40"));
    }

    /// How a [`Faulty`] table breaks the contract.
    #[derive(Clone, Copy, PartialEq)]
    enum Fault {
        None,
        PrevValuesAreCurrent,
        PrevRowSetIsCurrent,
        NeverNotifies,
        CannotReadPrev,
        /// Between cycles the previous row set is still the one the table started with.
        PrevRowSetLagsWhenIdle,
        /// Between cycles every previous value reads as null.
        PrevValuesLostWhenIdle,
    }

    fn idle(table: &ReferenceTable) -> bool {
        table.update_graph().unwrap().clock().phase == ClockPhase::Idle
    }

    /// A table that is not a `ReferenceTable`: it forwards to one, with one fault.
    struct Faulty {
        inner: Arc<ReferenceTable>,
        fault: Fault,
    }

    struct FaultyColumn {
        table: Arc<ReferenceTable>,
        inner: Arc<dyn ColumnSource>,
        fault: Fault,
    }

    impl ColumnSource for FaultyColumn {
        fn column_type(&self) -> &ColumnType {
            self.inner.column_type()
        }
        fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
            self.inner.chunk(rows)
        }
        fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
            match self.fault {
                Fault::PrevValuesAreCurrent => self.inner.chunk(rows),
                Fault::CannotReadPrev => Err(TableFailure {
                    message: "disk on fire".into(),
                }),
                Fault::PrevValuesLostWhenIdle if idle(&self.table) => Ok(new_null_array(
                    &self.column_type().arrow_type(),
                    rows.size() as usize,
                )),
                _ => self.inner.prev_chunk(rows),
            }
        }
    }

    impl Dependency for Faulty {
        fn satisfied(&self, step: u64) -> bool {
            self.inner.satisfied(step)
        }
    }

    impl Table for Faulty {
        fn definition(&self) -> &TableDefinition {
            self.inner.definition()
        }
        fn row_set(&self) -> Arc<RowSet> {
            self.inner.row_set()
        }
        fn prev_row_set(&self) -> Arc<RowSet> {
            match self.fault {
                Fault::PrevRowSetIsCurrent => self.inner.row_set(),
                Fault::PrevRowSetLagsWhenIdle if idle(&self.inner) => Arc::new(keys([0, 1, 5, 6])),
                _ => self.inner.prev_row_set(),
            }
        }
        fn column(&self, index: usize) -> Arc<dyn ColumnSource> {
            Arc::new(FaultyColumn {
                table: self.inner.clone(),
                inner: self.inner.column(index),
                fault: self.fault,
            })
        }
        fn is_refreshing(&self) -> bool {
            true
        }
        fn attributes(&self) -> TableAttributes {
            self.inner.attributes()
        }
        fn update_graph(&self) -> Option<Arc<dyn UpdateGraph>> {
            self.inner.update_graph()
        }
        fn add_listener(&self, listener: &Arc<dyn UpdateListener>) {
            if self.fault != Fault::NeverNotifies {
                self.inner.add_listener(listener);
            }
        }
    }

    /// The problems reported for a faulty table after one cycle that applies `change`.
    fn issues_with_fault(fault: Fault, change: TableChange) -> Vec<String> {
        let graph = ReferenceGraph::new();
        let script = scripted(&graph);
        let table: Arc<dyn Table> = Arc::new(Faulty {
            inner: script.table(),
            fault,
        });
        let checker = ConsistencyChecker::attach(table);
        script.push(change);
        graph.run_cycle();
        let expected_updates = if fault == Fault::NeverNotifies { 0 } else { 1 };
        assert_eq!(checker.updates_seen(), expected_updates);
        checker.verify().err().map(|e| e.issues).unwrap_or_default()
    }

    fn modify_and_remove() -> TableChange {
        TableChange::new()
            .remove([6])
            .modify([1], [(1, longs([21]))])
    }

    #[test]
    fn checker_accepts_any_table_that_keeps_the_contract() {
        let issues = issues_with_fault(Fault::None, modify_and_remove());
        assert_eq!(issues, Vec::<String>::new());
    }

    #[test]
    fn previous_values_that_already_show_the_change_are_caught() {
        let issues = issues_with_fault(
            Fault::PrevValuesAreCurrent,
            TableChange::new().modify([1], [(1, longs([21]))]),
        );
        assert_one_issue(
            issues,
            "column Qty: previous value is not the value at the end of the last cycle: row key \
             1: expected 20 but found 21",
        );
    }

    #[test]
    fn previous_row_set_that_already_shows_the_change_is_caught() {
        let issues = issues_with_fault(Fault::PrevRowSetIsCurrent, modify_and_remove());
        assert_one_issue(
            issues,
            "previous row set is RowSet{0-1,5} but the table held RowSet{0-1,5-6} at the end \
             of the last cycle",
        );
    }

    #[test]
    fn unreadable_previous_values_are_caught() {
        let issues = issues_with_fault(Fault::CannotReadPrev, modify_and_remove());
        // Once when the update arrives and once between cycles.
        assert_eq!(
            issues,
            ["step 1: cannot read previous values: disk on fire"; 2]
        );
    }

    #[test]
    fn previous_state_that_differs_from_current_between_cycles_is_caught() {
        let issues = issues_with_fault(Fault::PrevRowSetLagsWhenIdle, modify_and_remove());
        assert_one_issue(
            issues,
            "previous row set RowSet{0-1,5-6} differs from the row set RowSet{0-1,5} between \
             cycles",
        );
        let issues = issues_with_fault(Fault::PrevValuesLostWhenIdle, modify_and_remove());
        assert_eq!(issues.len(), 2, "{issues:#?}");
        assert!(issues[0].contains(
            "column Sym: previous value differs from the current value between cycles: row key \
             0: expected a but found null"
        ));
        assert!(issues[1].contains("column Qty: previous value differs"));
    }

    #[test]
    fn change_without_any_update_is_caught_between_cycles() {
        let issues = issues_with_fault(Fault::NeverNotifies, modify_and_remove());
        assert_one_issue(
            issues,
            "row set changed without an update: was RowSet{0-1,5-6}, now RowSet{0-1,5}",
        );
        let issues = issues_with_fault(
            Fault::NeverNotifies,
            TableChange::new().modify([1], [(1, longs([21]))]),
        );
        assert_one_issue(
            issues,
            "column Qty: changed without an update: row key 1: expected 20 but found 21",
        );
    }

    #[test]
    fn problem_stays_reported_and_later_cycles_are_still_checked() {
        let graph = ReferenceGraph::new();
        let script = scripted(&graph);
        let checker = ConsistencyChecker::attach(script.table());
        script.push_misreported(
            TableChange::new().remove([0]),
            TableUpdate {
                removed: keys([1]),
                ..update()
            },
        );
        graph.run_cycle();
        assert_eq!(checker.verify().unwrap_err().issues.len(), 1);

        // The checker carries on from the table's real state: a truthful cycle adds nothing.
        script.push(TableChange::new().remove([1]));
        graph.run_cycle();
        assert_eq!(checker.verify().unwrap_err().issues.len(), 1);

        script.push_misreported(TableChange::new().remove([5]), update());
        graph.run_cycle();
        let error = checker.verify().unwrap_err();
        assert_eq!(error.issues.len(), 2, "{error}");
        assert!(error.issues[1].starts_with("step 3: update yields rows RowSet{5-6}"));
        assert!(error
            .to_string()
            .starts_with("table is inconsistent:\n - step 1: "));
    }

    #[test]
    fn only_the_first_ten_problems_are_kept() {
        let graph = ReferenceGraph::new();
        let script = scripted(&graph);
        let checker = ConsistencyChecker::attach(script.table());
        for key in 10..22 {
            let add = TableChange::new().add([key], vec![strings(["x"]), longs([1])]);
            script.push_misreported(add, update());
            graph.run_cycle();
        }
        assert_eq!(checker.updates_seen(), 12);
        assert_eq!(checker.verify().unwrap_err().issues.len(), MAX_ISSUES);
    }

    #[test]
    #[should_panic(expected = "table is inconsistent:\n - step 1: table failed: boom")]
    fn failed_table_is_reported() {
        let graph = ReferenceGraph::new();
        let script = scripted(&graph);
        let checker = ConsistencyChecker::attach(script.table());
        script.push_failure("boom");
        graph.run_cycle();
        checker.assert_consistent();
    }

    /// A from-scratch copy of `source` with flat row keys, keeping rows whose Qty passes.
    fn recompute(source: &Arc<ReferenceTable>, keep: fn(i64) -> bool) -> Arc<dyn Table> {
        let data = TableSnapshot::current(&**source).unwrap();
        let qty = crate::testing::long_values(&data.columns[1]);
        let kept: UInt64Array = (0..qty.len() as u64)
            .filter(|&i| qty[i as usize].is_some_and(keep))
            .map(Some)
            .collect();
        let columns: Vec<ArrayRef> = data
            .columns
            .iter()
            .map(|column| take(column.as_ref(), &kept, None).unwrap())
            .collect();
        ReferenceTable::new_static(sym_qty(), RowSet::flat(kept.len() as u64), columns)
    }

    #[test]
    fn table_is_compared_with_its_recompute_after_every_cycle() {
        let graph = ReferenceGraph::new();
        let script = scripted(&graph);
        let source = script.table();
        let same = ConsistencyChecker::attach_with_recompute(source.clone(), {
            let source = source.clone();
            move || recompute(&source, |_| true)
        });
        // This recompute disagrees once a Qty above 40 appears.
        let differs = ConsistencyChecker::attach_with_recompute(source.clone(), {
            let source = source.clone();
            move || recompute(&source, |qty| qty <= 40)
        });
        assert_eq!(same.verify(), Ok(()));
        assert_eq!(differs.verify(), Ok(()));

        script.push(TableChange::new().add([3], vec![strings(["e"]), longs([50])]));
        graph.run_cycle();
        assert_eq!(same.verify(), Ok(()));
        assert_eq!(
            differs.verify().unwrap_err().issues,
            ["step 1: table differs from its recompute: expected 4 rows but found 5"]
        );
    }

    #[test]
    fn static_table_can_be_checked() {
        let table = ReferenceTable::new_static(
            sym_qty(),
            RowSet::flat(1),
            vec![strings(["a"]), longs([1])],
        );
        let other = ReferenceTable::new_static(
            sym_qty(),
            RowSet::flat(1),
            vec![strings(["a"]), longs([2])],
        );
        let checker =
            ConsistencyChecker::attach_with_recompute(table.clone(), move || other.clone());
        assert_eq!(
            checker.verify().unwrap_err().issues,
            ["table differs from its recompute: column Qty, position 0: expected 2 but found 1"]
        );
        assert_eq!(ConsistencyChecker::attach(table).verify(), Ok(()));
    }

    /// Attaches a checker to a table from inside a cycle.
    struct AttachInCycle {
        table: Arc<ReferenceTable>,
        checker: Mutex<Option<Arc<ConsistencyChecker>>>,
        verified_in_cycle: Mutex<Option<Result<(), ConsistencyError>>>,
    }

    impl UpdateSource for AttachInCycle {
        fn refresh(&self) {
            let mut checker = lock(&self.checker);
            if checker.is_none() {
                let attached = ConsistencyChecker::attach(self.table.clone());
                *lock(&self.verified_in_cycle) = Some(attached.verify());
                *checker = Some(attached);
            }
        }
    }

    #[test]
    fn checker_attached_inside_a_cycle_starts_from_the_right_state() {
        // One source attaches before the table changes in the cycle, the other after.
        let graph = ReferenceGraph::new();
        let attach = |table: Arc<ReferenceTable>| {
            let source = Arc::new(AttachInCycle {
                table,
                checker: Mutex::default(),
                verified_in_cycle: Mutex::default(),
            });
            let dyn_source: Arc<dyn UpdateSource> = source.clone();
            graph.add_source(&dyn_source);
            source
        };
        // Sources refresh in registration order: `before` is registered ahead of the source
        // that changes its table, `after` behind the script that changes its table.
        let early_table = ReferenceTable::new_refreshing(
            graph.clone(),
            sym_qty(),
            RowSet::empty(),
            vec![strings([]), longs([])],
        );
        let before = attach(early_table.clone());
        struct Add(Arc<ReferenceTable>);
        impl UpdateSource for Add {
            fn refresh(&self) {
                if self.0.row_set().is_empty() {
                    let add = TableChange::new().add([0], vec![strings(["a"]), longs([1])]);
                    self.0.apply(&add).unwrap();
                }
            }
        }
        let add: Arc<dyn UpdateSource> = Arc::new(Add(early_table));
        graph.add_source(&add);

        let script = scripted(&graph);
        script.push(modify_and_remove());
        let after = attach(script.table());

        graph.run_cycle();
        for source in [&before, &after] {
            assert_eq!(
                lock(&source.verified_in_cycle)
                    .clone()
                    .unwrap()
                    .unwrap_err()
                    .issues,
                ["verify was called inside a cycle"]
            );
        }
        let (before, after) = (
            lock(&before.checker).clone().unwrap(),
            lock(&after.checker).clone().unwrap(),
        );
        // Attached before the change: the checker starts from the old state and gets the update.
        assert_eq!(before.updates_seen(), 1);
        assert_eq!(before.verify(), Ok(()));
        // Attached after the change: no update arrives, and the new state is the start.
        assert_eq!(after.updates_seen(), 0);
        assert_eq!(after.verify(), Ok(()));
    }
}
