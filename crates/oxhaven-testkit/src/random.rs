//! A seeded generator of valid table changes.

use crate::table::TableChange;
use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, StringArray, TimestampNanosecondArray, UInt16Array,
};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::shift::{RowSetShiftData, ShiftRange};
use oxhaven_contracts::table::Table;
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::sync::Arc;

/// Generates random [`TableChange`]s that are always valid for the table they are made for:
/// removes, shifts, modifies and adds, with values for every column type.
///
/// The same seed gives the same sequence of changes for the same sequence of tables. Values
/// come from a small domain with about one null in ten, so that grouping, joining and sorting
/// see duplicates and nulls.
///
/// In the spirit of Java's `GenerateTableUpdates#generateShiftAwareTableUpdates`, but not a
/// port: the two do not produce the same updates for a seed. One deliberate difference is
/// that a shift here never lands on a surviving row, where Java removes ("blats") the rows a
/// shift would land on.
pub struct RandomUpdates {
    state: u64,
    /// Whether to generate each kind of change. All are on by default.
    pub adds: bool,
    pub removes: bool,
    pub modifies: bool,
    pub shifts: bool,
}

impl RandomUpdates {
    pub fn new(seed: u64) -> Self {
        Self {
            state: seed,
            adds: true,
            removes: true,
            modifies: true,
            shifts: true,
        }
    }

    /// A change that is valid for `table` as it is now. Each kind of change touches up to
    /// about `target_size` rows. Generate between cycles, one change per cycle.
    ///
    /// # Panics
    /// If the table has a column type this generator has no values for.
    pub fn next_change(&mut self, table: &dyn Table, target_size: u64) -> TableChange {
        let target_size = target_size.max(1);
        let columns = table.definition().columns();
        let mut keys: Vec<RowKey> = table.row_set().iter().collect();

        // Removes, in the key space before the change.
        let removed = if self.removes {
            // At most half the rows, so that a small table tends to grow.
            let count = self
                .below(target_size + 1)
                .min((keys.len() as u64).div_ceil(2));
            self.take_random(&mut keys, count)
        } else {
            Vec::new()
        };

        // Shifts of runs of surviving rows. Each run lands strictly between the row below it
        // (where that row ends up) and the row above it (where that row is now), so a shift
        // never reorders rows and never lands on a row.
        let mut shifts = Vec::new();
        if self.shifts && self.below(2) == 0 {
            let reach = 2 * target_size as i64;
            let mut floor: RowKey = -1;
            let mut i = 0;
            while i < keys.len() {
                if self.below(4) != 0 {
                    floor = keys[i];
                    i += 1;
                    continue;
                }
                let j = (i + self.below(4) as usize).min(keys.len() - 1);
                let (first, last) = (keys[i], keys[j]);
                let lowest = (floor + 1 - first).max(-reach);
                let highest = keys
                    .get(j + 1)
                    .map_or(reach, |next| next - 1 - last)
                    .min(reach);
                // Any delta in `lowest..=highest` except zero; zero is always in that range.
                let mut delta = 0;
                if lowest < highest {
                    delta = lowest + self.below((highest - lowest) as u64) as i64;
                    if delta >= 0 {
                        delta += 1;
                    }
                    shifts.push(ShiftRange { first, last, delta });
                }
                floor = last + delta;
                i = j + 1;
            }
        }
        let shifted = match RowSetShiftData::new(shifts) {
            Ok(shifted) => shifted,
            Err(e) => panic!("generated an invalid shift: {e}"),
        };
        let mut keys: Vec<RowKey> = keys.iter().map(|&k| shifted.apply_to_key(k)).collect();

        // Modifies, in the key space after the change, of at least one column.
        let mut modified = Vec::new();
        let mut modified_columns = Vec::new();
        if self.modifies && !columns.is_empty() {
            let count = self.below(2 * target_size).min(keys.len() as u64);
            let mut candidates = keys.clone();
            modified = self.take_random(&mut candidates, count);
            if !modified.is_empty() {
                let must = self.below(columns.len() as u64) as usize;
                modified_columns = (0..columns.len())
                    .filter(|&c| c == must || self.below(2) == 0)
                    .collect();
            }
        }

        // Adds, at keys that are free after the change.
        let mut added = Vec::new();
        if self.adds {
            let count = self.below(target_size + 1);
            let space = keys.iter().max().map_or(0, |k| k + 1) as u64 + 2 * target_size;
            for _ in 0..count {
                let key = self.below(space) as RowKey;
                if !keys.contains(&key) {
                    keys.push(key);
                    added.push(key);
                }
            }
        }

        let added = RowSet::from_keys(added);
        let modified = RowSet::from_keys(modified);
        let added_values = if added.is_empty() {
            Vec::new()
        } else {
            columns
                .iter()
                .map(|c| self.values(&c.column_type, added.size()))
                .collect()
        };
        let modified_values = modified_columns
            .iter()
            .map(|&c| (c, self.values(&columns[c].column_type, modified.size())))
            .collect();
        TableChange {
            update: TableUpdate {
                added,
                removed: RowSet::from_keys(removed),
                modified,
                shifted,
                modified_columns: ModifiedColumnSet::of(modified_columns),
            },
            added_values,
            modified_values,
        }
    }

    /// `size` random values for a column of `column_type`.
    ///
    /// # Panics
    /// If this generator has no values for `column_type`.
    pub fn values(&mut self, column_type: &ColumnType, size: u64) -> ArrayRef {
        let n = size as usize;
        match column_type {
            ColumnType::Boolean => Arc::new(self.column::<_, BooleanArray>(n, |r| r.below(2) == 1)),
            ColumnType::Byte => Arc::new(self.column::<_, Int8Array>(n, |r| r.small() as i8)),
            ColumnType::Char => {
                Arc::new(self.column::<_, UInt16Array>(n, |r| b'a' as u16 + r.below(8) as u16))
            }
            ColumnType::Short => Arc::new(self.column::<_, Int16Array>(n, |r| r.small() as i16)),
            ColumnType::Int => Arc::new(self.column::<_, Int32Array>(n, |r| r.small() as i32)),
            ColumnType::Long => Arc::new(self.column::<_, Int64Array>(n, |r| r.small())),
            ColumnType::Float => {
                Arc::new(self.column::<_, Float32Array>(n, |r| r.fraction() as f32))
            }
            ColumnType::Double => Arc::new(self.column::<_, Float64Array>(n, |r| r.fraction())),
            ColumnType::String => {
                let letters = ["a", "b", "c", "d", "e", "f", "g", "h"];
                Arc::new(self.column::<_, StringArray>(n, |r| letters[r.below(8) as usize]))
            }
            ColumnType::Instant => {
                let nanos = self.column::<_, TimestampNanosecondArray>(n, |r| {
                    1_700_000_000_000_000_000 + r.small() * 1_000_000_000
                });
                Arc::new(nanos.with_timezone("UTC"))
            }
            ColumnType::Vector(element) | ColumnType::Array(element) => {
                // About one null cell in ten, and otherwise zero to three elements, about
                // one in ten of them null, so that empty and null cells both occur.
                let lengths: Vec<Option<usize>> = (0..n)
                    .map(|_| (self.below(10) != 0).then(|| self.below(4) as usize))
                    .collect();
                let total: usize = lengths.iter().flatten().sum();
                let elements = self.values(element, total as u64);
                crate::list_of(column_type, lengths, elements)
            }
            other => panic!("RandomUpdates has no values for column type {other:?}"),
        }
    }

    /// `n` values from `value`, about one in ten replaced by null.
    fn column<T, A: FromIterator<Option<T>>>(
        &mut self,
        n: usize,
        mut value: impl FnMut(&mut Self) -> T,
    ) -> A {
        (0..n)
            .map(|_| (self.below(10) != 0).then(|| value(self)))
            .collect()
    }

    /// An integer in `-10..10`.
    fn small(&mut self) -> i64 {
        self.below(20) as i64 - 10
    }

    /// A multiple of 0.25 in `-2.5..2.5`, or occasionally NaN.
    fn fraction(&mut self) -> f64 {
        match self.below(21) {
            20 => f64::NAN,
            n => (n as f64 - 10.0) / 4.0,
        }
    }

    /// Removes `count` random keys from `keys`, which stays in order, and returns them.
    fn take_random(&mut self, keys: &mut Vec<RowKey>, count: u64) -> Vec<RowKey> {
        (0..count)
            .map(|_| keys.remove(self.below(keys.len() as u64) as usize))
            .collect()
    }

    /// A uniform-enough integer in `0..bound`, by SplitMix64. `bound` must not be zero.
    fn below(&mut self, bound: u64) -> u64 {
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        (z ^ (z >> 31)) % bound
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{definition, ConsistencyChecker, ReferenceGraph, ReferenceTable, ScriptedTable};
    use arrow::array::{Array, ListArray};
    use oxhaven_contracts::graph::UpdateGraph;
    use oxhaven_contracts::schema::TableDefinition;

    fn every_type() -> TableDefinition {
        definition(&[
            ("Boolean", ColumnType::Boolean),
            ("Byte", ColumnType::Byte),
            ("Char", ColumnType::Char),
            ("Short", ColumnType::Short),
            ("Int", ColumnType::Int),
            ("Long", ColumnType::Long),
            ("Float", ColumnType::Float),
            ("Double", ColumnType::Double),
            ("String", ColumnType::String),
            ("Instant", ColumnType::Instant),
            ("Ints", ColumnType::vector(ColumnType::Int)),
            ("Doubles", ColumnType::array(ColumnType::Double)),
            ("Strings", ColumnType::vector(ColumnType::String)),
            (
                "Nested",
                ColumnType::vector(ColumnType::array(ColumnType::Boolean)),
            ),
        ])
    }

    fn empty_table(graph: &Arc<ReferenceGraph>) -> Arc<ScriptedTable> {
        let definition = every_type();
        let columns = definition
            .columns()
            .iter()
            .map(|c| arrow::array::new_empty_array(&c.column_type.arrow_type()))
            .collect();
        ScriptedTable::new(graph.clone(), definition, RowSet::empty(), columns)
    }

    /// What `steps` random cycles from `seed` did, as counts of cycles with each kind of change.
    #[derive(Default, Debug, PartialEq)]
    struct Kinds {
        adds: u32,
        removes: u32,
        modifies: u32,
        shifts: u32,
    }

    fn run(
        mut random: RandomUpdates,
        steps: u32,
    ) -> (Kinds, Vec<TableUpdate>, Arc<ReferenceTable>) {
        let graph = ReferenceGraph::new();
        let script = empty_table(&graph);
        let table = script.table();
        let checker = ConsistencyChecker::attach(table.clone());
        let mut kinds = Kinds::default();
        let mut updates = Vec::new();
        for _ in 0..steps {
            let change = random.next_change(&*table, 8);
            let update = change.update.clone();
            // Valid on its own terms, before the table gets a say.
            let expected = update.apply_to(&table.row_set()).unwrap();
            kinds.adds += u32::from(!update.added.is_empty());
            kinds.removes += u32::from(!update.removed.is_empty());
            kinds.modifies += u32::from(!update.modified.is_empty());
            kinds.shifts += u32::from(!update.shifted.is_empty());
            script.push(change);
            // A refused script panics here.
            graph.run_cycle();
            assert_eq!(*table.row_set(), expected);
            checker.assert_consistent();
            updates.push(update);
        }
        (kinds, updates, table)
    }

    #[test]
    fn every_generated_change_is_valid_and_every_kind_occurs() {
        for seed in 0..50 {
            let (kinds, _, _) = run(RandomUpdates::new(seed), 40);
            assert!(
                kinds.adds > 5 && kinds.removes > 5 && kinds.modifies > 5 && kinds.shifts > 5,
                "seed {seed}: {kinds:?}"
            );
        }
    }

    #[test]
    fn same_seed_gives_the_same_changes_and_another_seed_does_not() {
        let (_, a, _) = run(RandomUpdates::new(7), 20);
        let (_, b, _) = run(RandomUpdates::new(7), 20);
        let (_, c, _) = run(RandomUpdates::new(8), 20);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn kinds_can_be_turned_off() {
        let only_adds = RandomUpdates {
            removes: false,
            modifies: false,
            shifts: false,
            ..RandomUpdates::new(3)
        };
        let (kinds, _, _) = run(only_adds, 30);
        assert!(kinds.adds > 5, "{kinds:?}");
        assert_eq!(
            Kinds {
                adds: kinds.adds,
                ..Kinds::default()
            },
            kinds
        );

        let no_adds = RandomUpdates {
            adds: false,
            ..RandomUpdates::new(3)
        };
        assert_eq!(run(no_adds, 30).0, Kinds::default());
    }

    #[test]
    fn values_have_the_column_type_and_some_nulls() {
        let mut random = RandomUpdates::new(1);
        for column in every_type().columns() {
            let values = random.values(&column.column_type, 200);
            assert_eq!(values.data_type(), &column.column_type.arrow_type());
            assert_eq!(values.len(), 200);
            let nulls = values.null_count();
            assert!((5..60).contains(&nulls), "{}: {nulls} nulls", column.name);
        }
    }

    #[test]
    fn list_values_have_null_cells_empty_cells_and_null_elements() {
        let mut random = RandomUpdates::new(1);
        let values = random.values(&ColumnType::vector(ColumnType::Int), 200);
        let lists = values.as_any().downcast_ref::<ListArray>().unwrap();
        let null_cells = lists.null_count();
        let lengths: Vec<usize> = (0..200)
            .filter(|&row| lists.is_valid(row))
            .map(|row| lists.value(row).len())
            .collect();
        let empty_cells = lengths.iter().filter(|&&length| length == 0).count();
        assert!((5..60).contains(&null_cells), "{null_cells} null cells");
        assert!(
            (10..100).contains(&empty_cells),
            "{empty_cells} empty cells"
        );
        assert_eq!(lengths.iter().max(), Some(&3));
        // Every element belongs to a cell that is not null.
        assert_eq!(lists.values().len(), lengths.iter().sum::<usize>());
        let null_elements = lists.values().null_count();
        assert!((5..80).contains(&null_elements), "{null_elements} nulls");
    }

    #[test]
    fn shifts_move_rows_into_free_space_only() {
        // A dense table leaves no room between rows, so shifts can only move the top run up
        // or be absent; a sparse one allows moves in both directions.
        let mut random = RandomUpdates {
            adds: false,
            removes: false,
            modifies: false,
            ..RandomUpdates::new(11)
        };
        let table = |rows: RowSet| {
            let size = rows.size();
            let long = definition(&[("V", ColumnType::Long)]);
            ReferenceTable::new_static(long, rows, vec![crate::longs(0..size as i64)])
        };
        let dense = table(RowSet::flat(20));
        let sparse = table(RowSet::from_keys((0..20).map(|k| k * 10)));
        let (mut up, mut down) = (0, 0);
        for _ in 0..200 {
            let update = random.next_change(&*dense, 4).update;
            assert!(update
                .shifted
                .ranges()
                .iter()
                .all(|s| s.delta > 0 && s.last == 19));
            update.apply_to(&dense.row_set()).unwrap();

            let update = random.next_change(&*sparse, 4).update;
            let after = update.apply_to(&sparse.row_set()).unwrap();
            assert_eq!(after.size(), 20);
            up += update
                .shifted
                .ranges()
                .iter()
                .filter(|s| s.delta > 0)
                .count();
            down += update
                .shifted
                .ranges()
                .iter()
                .filter(|s| s.delta < 0)
                .count();
        }
        assert!(up > 20 && down > 20, "up {up}, down {down}");
    }
}
