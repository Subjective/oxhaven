use oxhaven_contracts::update::ModifiedColumnSet;
use std::collections::{BTreeMap, BTreeSet};

/// Translates a parent table's modified columns into a child table's.
///
/// Each parent column maps to the child columns that depend on it. A parent column with no
/// entry affects no child column.
#[derive(Clone, PartialEq, Eq, Debug, Default)]
pub struct ModifiedColumnTransformer {
    children_of: BTreeMap<usize, BTreeSet<usize>>,
    all_children: BTreeSet<usize>,
}

impl ModifiedColumnTransformer {
    /// Builds a transformer from `(parent column, child columns)` pairs. A parent column may
    /// appear more than once; its child columns accumulate.
    ///
    /// Behavior: io.deephaven.engine.table.ModifiedColumnSet#newTransformer
    pub fn new<C>(mapping: impl IntoIterator<Item = (usize, C)>) -> Self
    where
        C: IntoIterator<Item = usize>,
    {
        let mut out = Self::default();
        for (parent, children) in mapping {
            let children: BTreeSet<usize> = children.into_iter().collect();
            out.all_children.extend(children.iter().copied());
            out.children_of.entry(parent).or_default().extend(children);
        }
        out
    }

    /// Maps column `i` of the parent to column `i` of the child, for `columns` columns.
    ///
    /// Behavior: io.deephaven.engine.table.ModifiedColumnSet#newIdentityTransformer
    pub fn identity(columns: usize) -> Self {
        Self::new((0..columns).map(|c| (c, [c])))
    }

    /// The child columns that may have changed when `input` changed in the parent.
    ///
    /// `ModifiedColumnSet::All` yields every child column the transformer can reach, not
    /// `All`: child columns that depend on no parent column stay clean.
    ///
    /// Behavior: io.deephaven.engine.table.ModifiedColumnSet.Transformer#transform
    pub fn transform(&self, input: &ModifiedColumnSet) -> ModifiedColumnSet {
        match input {
            ModifiedColumnSet::All => ModifiedColumnSet::Columns(self.all_children.clone()),
            ModifiedColumnSet::Columns(dirty) => ModifiedColumnSet::Columns(
                dirty
                    .iter()
                    .filter_map(|parent| self.children_of.get(parent))
                    .flatten()
                    .copied()
                    .collect(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dirty_parent_columns_mark_their_child_columns() {
        let t = ModifiedColumnTransformer::new([(0, vec![0, 3]), (2, vec![1]), (0, vec![4])]);
        assert_eq!(
            t.transform(&ModifiedColumnSet::of([0])),
            ModifiedColumnSet::of([0, 3, 4])
        );
        assert_eq!(
            t.transform(&ModifiedColumnSet::of([0, 2])),
            ModifiedColumnSet::of([0, 1, 3, 4])
        );
    }

    #[test]
    fn unmapped_and_empty_inputs_give_an_empty_set() {
        let t = ModifiedColumnTransformer::new([(0, vec![0]), (2, vec![1])]);
        assert!(t.transform(&ModifiedColumnSet::of([1, 7])).is_empty());
        assert!(t.transform(&ModifiedColumnSet::empty()).is_empty());
    }

    #[test]
    fn all_maps_to_every_reachable_child_column_only() {
        let t = ModifiedColumnTransformer::new([(0, vec![0]), (2, vec![1, 5])]);
        assert_eq!(
            t.transform(&ModifiedColumnSet::All),
            ModifiedColumnSet::of([0, 1, 5])
        );
    }

    #[test]
    fn identity_copies_columns_in_range() {
        let t = ModifiedColumnTransformer::identity(3);
        assert_eq!(
            t.transform(&ModifiedColumnSet::of([1, 2, 9])),
            ModifiedColumnSet::of([1, 2])
        );
        assert_eq!(
            t.transform(&ModifiedColumnSet::All),
            ModifiedColumnSet::of([0, 1, 2])
        );
    }
}
