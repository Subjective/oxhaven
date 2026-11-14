//! A source table that plays back a script, one entry per cycle.

use crate::graph::lock;
use crate::table::{ReferenceTable, TableChange};
use arrow::array::ArrayRef;
use oxhaven_contracts::graph::{UpdateGraph, UpdateSource};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::TableDefinition;
use oxhaven_contracts::update::TableUpdate;
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

enum Entry {
    Change(TableChange),
    Failure(String),
    /// Applies the change but tells listeners that the update is what happened.
    Misreported(TableChange, TableUpdate),
}

/// A refreshing [`ReferenceTable`] and the script that drives it.
///
/// Each cycle takes the oldest entry pushed and applies it to the table; a cycle with nothing
/// pushed leaves the table alone. Keep the `ScriptedTable` alive for as long as the script
/// should run: the graph holds it weakly, as it does every source.
pub struct ScriptedTable {
    table: Arc<ReferenceTable>,
    script: Mutex<VecDeque<Entry>>,
}

impl ScriptedTable {
    /// Creates the table with its initial rows and registers the script as a source of
    /// `graph`.
    ///
    /// # Panics
    /// If `columns` does not match `definition` and `rows`.
    pub fn new(
        graph: Arc<dyn UpdateGraph>,
        definition: TableDefinition,
        rows: RowSet,
        columns: Vec<ArrayRef>,
    ) -> Arc<Self> {
        let table = ReferenceTable::new_refreshing(graph.clone(), definition, rows, columns);
        let scripted = Arc::new(Self {
            table,
            script: Mutex::default(),
        });
        let source: Arc<dyn UpdateSource> = scripted.clone();
        graph.add_source(&source);
        scripted
    }

    /// The table the script drives.
    pub fn table(&self) -> Arc<ReferenceTable> {
        self.table.clone()
    }

    /// Schedules `change` for the first cycle that has no entry yet.
    pub fn push(&self, change: TableChange) {
        lock(&self.script).push_back(Entry::Change(change));
    }

    /// Schedules the table to fail with `message`.
    pub fn push_failure(&self, message: &str) {
        lock(&self.script).push_back(Entry::Failure(message.to_string()));
    }

    /// Schedules `change`, but with `reported` delivered to listeners in place of its update.
    ///
    /// This makes a table that breaks the contract on purpose. It exists to show that a check
    /// catches a bad update: the checker's own tests use it, and a test of another validating
    /// listener can too. Never use it to drive an operator under test.
    pub fn push_misreported(&self, change: TableChange, reported: TableUpdate) {
        lock(&self.script).push_back(Entry::Misreported(change, reported));
    }
}

impl UpdateSource for ScriptedTable {
    /// # Panics
    /// If the table refuses the entry. A script that does not fit its table is a broken test.
    fn refresh(&self) {
        let entry = lock(&self.script).pop_front();
        let result = match entry {
            None => return,
            Some(Entry::Change(change)) => self.table.apply(&change),
            Some(Entry::Failure(message)) => self.table.fail(&message),
            Some(Entry::Misreported(change, reported)) => {
                self.table.apply_reporting(&change, Some(reported))
            }
        };
        if let Err(e) = result {
            panic!("scripted table refused its script: {e}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{definition, longs, Recorder, ReferenceGraph};
    use oxhaven_contracts::schema::ColumnType;
    use oxhaven_contracts::table::Table;

    fn scripted(graph: &Arc<ReferenceGraph>) -> Arc<ScriptedTable> {
        ScriptedTable::new(
            graph.clone(),
            definition(&[("V", ColumnType::Long)]),
            RowSet::flat(2),
            vec![longs([1, 2])],
        )
    }

    #[test]
    fn script_plays_one_entry_per_cycle_then_rests() {
        let graph = ReferenceGraph::new();
        let script = scripted(&graph);
        let table = script.table();
        let recorder = Recorder::attach(table.clone());
        script.push(TableChange::new().add([2], vec![longs([3])]));
        script.push(TableChange::new().remove([0]));

        graph.run_cycle();
        assert_eq!(*table.row_set(), RowSet::flat(3));
        graph.run_cycle();
        assert_eq!(*table.row_set(), RowSet::from_keys([1, 2]));
        graph.run_cycle();
        assert_eq!(*table.row_set(), RowSet::from_keys([1, 2]));
        assert_eq!(recorder.events().len(), 2);
    }

    #[test]
    #[should_panic(expected = "refused its script: invalid update: removed rows not in previous")]
    fn script_that_does_not_fit_the_table_panics() {
        let graph = ReferenceGraph::new();
        let script = scripted(&graph);
        script.push(TableChange::new().remove([7]));
        graph.run_cycle();
    }

    #[test]
    fn dropping_the_script_stops_it() {
        let graph = ReferenceGraph::new();
        let script = scripted(&graph);
        let table = script.table();
        script.push(TableChange::new().remove([0]));
        drop(script);
        graph.run_cycle();
        assert_eq!(*table.row_set(), RowSet::flat(2));
    }

    #[test]
    fn misreported_change_is_applied_but_another_update_is_delivered() {
        let graph = ReferenceGraph::new();
        let script = scripted(&graph);
        let table = script.table();
        let recorder = Recorder::attach(table.clone());
        let lie = TableUpdate {
            removed: RowSet::from_keys([1]),
            ..TableUpdate::empty()
        };
        script.push_misreported(TableChange::new().remove([0]), lie.clone());
        graph.run_cycle();
        assert_eq!(*table.row_set(), RowSet::from_keys([1]));
        assert_eq!(recorder.events(), [Ok(lie)]);
    }
}
