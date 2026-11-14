use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::shift::{RowSetShiftData, ShiftError, ShiftRange};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate, UpdateError};

/// The modified rows of `update` in the previous key space.
///
/// Behavior: io.deephaven.engine.table.impl.TableUpdateImpl#getModifiedPreShift
pub fn modified_pre_shift(update: &TableUpdate) -> Result<RowSet, ShiftError> {
    update.shifted.inverse().apply(&update.modified)
}

/// The row set a table had before `update` produced `current`.
///
/// The inverse of [`TableUpdate::apply_to`]: un-shift the rows that were not added, then put
/// the removed rows back.
pub fn previous_row_set(current: &RowSet, update: &TableUpdate) -> Result<RowSet, ShiftError> {
    let survivors = update
        .shifted
        .inverse()
        .apply(&current.minus(&update.added))?;
    Ok(survivors.union(&update.removed))
}

/// One update with the effect of `first` followed by `second`, for a table whose row set was
/// `prev` before `first`.
///
/// The result's shifts are stated only for rows that exist: `prev` rows that neither update
/// removes. Java also emits shifts across the gaps between rows; the two forms move every
/// existing row identically, and shift ranges are not compared (`PARITY.md` A4).
///
/// A row modified by `first` and removed by `second` leaves `modified`; if that empties
/// `modified`, the modified columns are emptied too, where Java keeps the stale column set
/// until `BaseTable#notifyListeners` repairs it.
///
/// An error means the inputs were not consecutive valid updates.
///
/// Behavior: io.deephaven.engine.table.impl.util.UpdateCoalescer#update
pub fn compose(
    prev: &RowSet,
    first: &TableUpdate,
    second: &TableUpdate,
) -> Result<TableUpdate, UpdateError> {
    let shift_error = UpdateError::Shift;

    // Rows `first` added and `second` removed never existed as far as a listener can tell.
    let added_then_removed = first.added.intersect(&second.removed);
    let added = first.added.minus(&second.removed);
    let modified = first.modified.minus(&second.removed);

    // The rest of `second.removed` existed before `first`; state it in that key space.
    let removed_by_second = first
        .shifted
        .inverse()
        .apply(&second.removed.minus(&added_then_removed))
        .map_err(shift_error)?;
    let removed = first.removed.union(&removed_by_second);
    let survivors = prev.minus(&removed);

    let mut added = second.shifted.apply(&added).map_err(shift_error)?;
    let mut modified = second.shifted.apply(&modified).map_err(shift_error)?;
    let shifted =
        compose_shifts(&survivors, &first.shifted, &second.shifted).map_err(shift_error)?;

    // A row cannot be modified relative to a state in which it did not exist.
    let modified_by_second = second.modified.minus(&added);
    let mut modified_columns = if first.modified.is_empty() {
        ModifiedColumnSet::empty()
    } else {
        first.modified_columns.clone()
    };
    if !modified_by_second.is_empty() {
        modified.insert(&modified_by_second);
        modified_columns = modified_columns.union(&second.modified_columns);
    }
    if modified.is_empty() {
        modified_columns = ModifiedColumnSet::empty();
    }

    // A key `first` removed may be added again by `second`; it is then both removed and added.
    added.insert(&second.added);

    Ok(TableUpdate {
        added,
        removed,
        modified,
        shifted,
        modified_columns,
    })
}

/// The shifts that move each row of `rows` by `first` and then by `second`.
fn compose_shifts(
    rows: &RowSet,
    first: &RowSetShiftData,
    second: &RowSetShiftData,
) -> Result<RowSetShiftData, ShiftError> {
    let mut out = Vec::new();
    for range in rows.ranges() {
        for a in split_by_delta(range.first(), range.last(), first) {
            for b in split_by_delta(a.first + a.delta, a.last + a.delta, second) {
                out.push(ShiftRange {
                    first: b.first - a.delta,
                    last: b.last - a.delta,
                    delta: a.delta + b.delta,
                });
            }
        }
    }
    RowSetShiftData::new(out)
}

/// Cuts `first..=last` into consecutive pieces that `shifts` moves by one delta each. Keys
/// outside every shift range get a delta of zero.
fn split_by_delta(first: RowKey, last: RowKey, shifts: &RowSetShiftData) -> Vec<ShiftRange> {
    let ranges = shifts.ranges();
    let mut next = ranges.partition_point(|s| s.last < first);
    let mut out = Vec::new();
    let mut at = first;
    loop {
        let (end, delta) = match ranges.get(next) {
            Some(s) if s.first <= at => {
                next += 1;
                (s.last.min(last), s.delta)
            }
            Some(s) if s.first <= last => (s.first - 1, 0),
            _ => (last, 0),
        };
        out.push(ShiftRange {
            first: at,
            last: end,
            delta,
        });
        if end == last {
            return out;
        }
        at = end + 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::btree_map::Entry;
    use std::collections::BTreeMap;

    fn keys(keys: impl IntoIterator<Item = RowKey>) -> RowSet {
        RowSet::from_keys(keys)
    }

    fn shifts(ranges: &[(RowKey, RowKey, i64)]) -> RowSetShiftData {
        RowSetShiftData::new(
            ranges
                .iter()
                .map(|&(first, last, delta)| ShiftRange { first, last, delta })
                .collect(),
        )
        .unwrap()
    }

    #[test]
    fn modified_pre_shift_undoes_the_shift_for_modified_rows() {
        let update = TableUpdate {
            modified: keys([3, 15, 40]),
            modified_columns: ModifiedColumnSet::All,
            shifted: shifts(&[(10, 19, 5), (50, 59, -10)]),
            ..TableUpdate::empty()
        };
        assert_eq!(modified_pre_shift(&update).unwrap(), keys([3, 10, 50]));

        let unshifted = TableUpdate {
            shifted: RowSetShiftData::empty(),
            ..update
        };
        assert_eq!(modified_pre_shift(&unshifted).unwrap(), unshifted.modified);
    }

    #[test]
    fn previous_row_set_inverts_apply_to() {
        let prev = keys([0, 1, 2, 3, 10]);
        let update = TableUpdate {
            removed: keys([1]),
            shifted: shifts(&[(2, 3, -1), (10, 10, 5)]),
            added: keys([3, 20]),
            ..TableUpdate::empty()
        };
        let current = update.apply_to(&prev).unwrap();
        assert_eq!(current, keys([0, 1, 2, 3, 15, 20]));
        assert_eq!(previous_row_set(&current, &update).unwrap(), prev);
    }

    #[test]
    fn a_row_added_then_removed_disappears() {
        let prev = keys([0]);
        let first = TableUpdate {
            added: keys([5, 6]),
            ..TableUpdate::empty()
        };
        let second = TableUpdate {
            removed: keys([5]),
            ..TableUpdate::empty()
        };
        let both = compose(&prev, &first, &second).unwrap();
        assert_eq!(
            both,
            TableUpdate {
                added: keys([6]),
                ..TableUpdate::empty()
            }
        );
    }

    #[test]
    fn a_later_remove_is_stated_in_the_original_key_space() {
        let prev = keys([10, 11]);
        let first = TableUpdate {
            shifted: shifts(&[(10, 11, 10)]),
            ..TableUpdate::empty()
        };
        let second = TableUpdate {
            removed: keys([21]),
            ..TableUpdate::empty()
        };
        let both = compose(&prev, &first, &second).unwrap();
        assert_eq!(both.removed, keys([11]));
        assert_eq!(both.shifted, shifts(&[(10, 10, 10)]));
        assert_eq!(both.apply_to(&prev).unwrap(), keys([20]));
    }

    #[test]
    fn shifts_accumulate_and_cancel_per_row() {
        let prev = keys([0, 10, 20, 30]);
        let first = TableUpdate {
            shifted: shifts(&[(10, 10, 5), (20, 20, 5)]),
            ..TableUpdate::empty()
        };
        // Moves the row now at 15 back to 10 and the row at 30 on to 40.
        let second = TableUpdate {
            shifted: shifts(&[(15, 15, -5), (30, 30, 10)]),
            ..TableUpdate::empty()
        };
        let both = compose(&prev, &first, &second).unwrap();
        assert_eq!(both.shifted, shifts(&[(20, 20, 5), (30, 30, 10)]));
        assert_eq!(both.apply_to(&prev).unwrap(), keys([0, 10, 25, 40]));
    }

    #[test]
    fn modifies_merge_but_never_cover_an_added_row() {
        let prev = keys([0, 1]);
        let first = TableUpdate {
            added: keys([5]),
            modified: keys([0]),
            modified_columns: ModifiedColumnSet::of([0]),
            ..TableUpdate::empty()
        };
        let second = TableUpdate {
            shifted: shifts(&[(5, 5, 2)]),
            modified: keys([1, 7]),
            modified_columns: ModifiedColumnSet::of([2]),
            ..TableUpdate::empty()
        };
        let both = compose(&prev, &first, &second).unwrap();
        assert_eq!(both.added, keys([7]));
        assert_eq!(both.modified, keys([0, 1]));
        assert_eq!(both.modified_columns, ModifiedColumnSet::of([0, 2]));
        assert!(both.shifted.is_empty());
    }

    #[test]
    fn modified_columns_empty_out_when_the_modified_row_is_removed() {
        let prev = keys([0, 1]);
        let first = TableUpdate {
            modified: keys([1]),
            modified_columns: ModifiedColumnSet::of([3]),
            ..TableUpdate::empty()
        };
        let second = TableUpdate {
            removed: keys([1]),
            ..TableUpdate::empty()
        };
        let both = compose(&prev, &first, &second).unwrap();
        assert!(both.modified.is_empty() && both.modified_columns.is_empty());
        assert_eq!(both.apply_to(&prev).unwrap(), keys([0]));
    }

    #[test]
    fn a_removed_key_can_be_added_again() {
        let prev = keys([4]);
        let first = TableUpdate {
            removed: keys([4]),
            ..TableUpdate::empty()
        };
        let second = TableUpdate {
            added: keys([4]),
            ..TableUpdate::empty()
        };
        let both = compose(&prev, &first, &second).unwrap();
        assert_eq!((&both.removed, &both.added), (&keys([4]), &keys([4])));
        assert_eq!(both.apply_to(&prev).unwrap(), keys([4]));
    }

    #[test]
    fn inconsistent_inputs_are_an_error() {
        // `second` shifts key 5 onto key 6, which `first` added and nothing removed.
        let first = TableUpdate {
            added: keys([5, 6]),
            ..TableUpdate::empty()
        };
        let second = TableUpdate {
            shifted: shifts(&[(5, 5, 1)]),
            ..TableUpdate::empty()
        };
        assert!(matches!(
            compose(&RowSet::empty(), &first, &second),
            Err(UpdateError::Shift(ShiftError::Collision(6)))
        ));
    }

    /// A fixed pseudo-random sequence, so that every run tests the same cases.
    struct Lcg(u64);

    impl Lcg {
        fn below(&mut self, bound: u64) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (self.0 >> 33) % bound
        }
    }

    /// A table as rows with identities, so that a composed update can be checked against the
    /// two updates applied one after the other.
    #[derive(Clone)]
    struct Model {
        /// Row key to row identity.
        rows: BTreeMap<RowKey, u32>,
        /// Identities of rows that were modified while they existed.
        modified: Vec<u32>,
    }

    /// Changes `model` at random and returns the update that describes the change.
    fn random_update(rng: &mut Lcg, model: &mut Model, next_id: &mut u32) -> TableUpdate {
        let before: Vec<(RowKey, u32)> = model.rows.iter().map(|(k, id)| (*k, *id)).collect();
        let removed: Vec<RowKey> = before
            .iter()
            .filter(|_| rng.below(4) == 0)
            .map(|(k, _)| *k)
            .collect();
        let mut rows = BTreeMap::new();
        let mut ranges = Vec::new();
        let mut floor: RowKey = 0;
        for (key, id) in before.iter().filter(|(k, _)| !removed.contains(k)) {
            let new_key = (key + rng.below(7) as i64 - 3).max(floor);
            floor = new_key + 1;
            rows.insert(new_key, *id);
            ranges.push(ShiftRange {
                first: *key,
                last: *key,
                delta: new_key - key,
            });
        }
        let modified: Vec<RowKey> = rows.keys().copied().filter(|_| rng.below(3) == 0).collect();
        for key in &modified {
            let id = rows[key];
            if !model.modified.contains(&id) {
                model.modified.push(id);
            }
        }
        let mut added = Vec::new();
        for _ in 0..rng.below(4) {
            let key = rng.below(40) as RowKey;
            if let Entry::Vacant(slot) = rows.entry(key) {
                slot.insert(*next_id);
                *next_id += 1;
                added.push(key);
            }
        }
        model.rows = rows;
        TableUpdate {
            added: keys(added),
            removed: keys(removed),
            modified_columns: if modified.is_empty() {
                ModifiedColumnSet::empty()
            } else {
                ModifiedColumnSet::of([rng.below(3) as usize])
            },
            modified: keys(modified),
            shifted: RowSetShiftData::new(ranges).unwrap(),
        }
    }

    #[test]
    fn composed_update_equals_the_two_updates_in_sequence() {
        let mut rng = Lcg(42);
        let mut shifted_cases = 0;
        for case in 0..500 {
            let mut next_id = 0;
            let mut start = Model {
                rows: BTreeMap::new(),
                modified: Vec::new(),
            };
            for _ in 0..rng.below(12) {
                if let Entry::Vacant(slot) = start.rows.entry(rng.below(40) as RowKey) {
                    slot.insert(next_id);
                    next_id += 1;
                }
            }
            let original_ids = next_id;
            let prev: RowSet = start.rows.keys().copied().collect();

            let mut end = start.clone();
            let first = random_update(&mut rng, &mut end, &mut next_id);
            let middle: RowSet = end.rows.keys().copied().collect();
            assert_eq!(first.apply_to(&prev).unwrap(), middle, "case {case}");
            let second = random_update(&mut rng, &mut end, &mut next_id);
            let current: RowSet = end.rows.keys().copied().collect();
            assert_eq!(second.apply_to(&middle).unwrap(), current, "case {case}");

            let both = compose(&prev, &first, &second).unwrap();
            assert_eq!(both.apply_to(&prev).unwrap(), current, "case {case}");
            if !both.shifted.is_empty() {
                shifted_cases += 1;
            }

            let mut expect_added = Vec::new();
            let mut expect_modified = Vec::new();
            for (key, id) in &end.rows {
                if *id >= original_ids {
                    expect_added.push(*key);
                } else if end.modified.contains(id) {
                    expect_modified.push(*key);
                }
            }
            let mut expect_removed = Vec::new();
            for (key, id) in &start.rows {
                match end.rows.iter().find(|(_, end_id)| *end_id == id) {
                    None => expect_removed.push(*key),
                    Some((end_key, _)) => {
                        assert_eq!(both.shifted.apply_to_key(*key), *end_key, "case {case}")
                    }
                }
            }
            assert_eq!(both.added, keys(expect_added), "case {case}");
            assert_eq!(both.removed, keys(expect_removed), "case {case}");
            assert_eq!(both.modified, keys(expect_modified), "case {case}");
            assert_eq!(
                previous_row_set(&current, &both).unwrap(),
                prev,
                "case {case}"
            );
        }
        assert!(shifted_cases > 100, "the generator must exercise shifts");
    }
}
