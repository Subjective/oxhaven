//! The snapshot of a view: key table rows to directives, directives to a tree, and the
//! depth-first walk of the expanded nodes that counts every row and reads those of the
//! viewport.

use super::{
    failure, Action, Expandable, HierarchicalError, Hierarchy, NodeId, NodeKey, Parent,
    DEPTH_COLUMN, EXPANDED_COLUMN,
};
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Int32Array, UInt32Array};
use arrow::datatypes::Int8Type;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use std::collections::HashMap;
use std::sync::Arc;

/// Rows of the walk of a view.
#[derive(Clone, Debug)]
pub struct Snapshot {
    /// The number of rows of the whole walk.
    pub size: u64,
    /// The number of rows returned.
    pub rows: u64,
    /// The values of the requested columns, in the order they were requested.
    pub columns: Vec<ArrayRef>,
}

/// What the default expansions and then the rows of the key table ask for: one entry per
/// node key, in the order of each key's last row.
///
/// Behavior: HierarchicalTableImpl#extractKeyTableNodeDirectives and
/// #accumulateKeyTableDirective
pub(super) fn directives(
    hierarchy: &dyn Hierarchy,
    key_table: Option<&dyn Table>,
    action_column: Option<&str>,
) -> Result<Vec<(NodeKey, Action)>, HierarchicalError> {
    let mut ordered: Vec<Option<(NodeKey, Action)>> = Vec::new();
    let mut last: HashMap<NodeKey, usize> = HashMap::new();
    let mut add = |key: NodeKey, action: Action| {
        if let Some(earlier) = last.insert(key.clone(), ordered.len()) {
            ordered[earlier] = None;
        }
        ordered.push(Some((key, action)));
    };
    for key in hierarchy.default_expansions() {
        add(key, Action::Expand);
    }
    if let Some(key_table) = key_table {
        let keys = hierarchy.node_keys(key_table)?;
        let actions = match action_column {
            None => vec![Action::Expand; keys.len()],
            Some(name) => {
                let definition = key_table.definition();
                let index = definition
                    .index_of(name)
                    .filter(|&index| definition.columns()[index].column_type == ColumnType::Byte)
                    .ok_or_else(|| {
                        HierarchicalError::IllegalArgument(format!(
                            "The key table has no byte column named {name}"
                        ))
                    })?;
                let values = key_table.column(index).chunk(&key_table.row_set())?;
                let values = values.as_primitive_opt::<Int8Type>().ok_or_else(|| {
                    failure("the action column of a key table is not a column of bytes")
                })?;
                values
                    .iter()
                    // Java's null byte is a value like any other here, and not an action
                    .map(|value| Action::of_wire(value.unwrap_or(i8::MIN)))
                    .collect::<Result<Vec<_>, _>>()?
            }
        };
        for (key, action) in keys.into_iter().zip(actions) {
            add(key, action);
        }
    }
    Ok(ordered.into_iter().flatten().collect())
}

/// A directive in the tree of directives. `action` is `None` for a node that is there
/// only because a descendant has a directive: Java's `Linkage`.
struct Linked {
    key: NodeKey,
    node: NodeId,
    row_in_parent: Option<RowKey>,
    action: Option<Action>,
    children: Vec<usize>,
    slot_in_parent: usize,
}

/// The directives as a tree, and the index of the root's, if the root has one.
///
/// Behavior: HierarchicalTableImpl#linkKeyTableNodeDirectives
fn link(
    hierarchy: &dyn Hierarchy,
    directives: Vec<(NodeKey, Action)>,
) -> Result<(Vec<Linked>, Option<usize>), HierarchicalError> {
    let mut arena: Vec<Linked> = Vec::new();
    let mut by_key: HashMap<NodeKey, usize> = HashMap::new();
    let mut root = None;
    // Takes the directives below a replaced one out of the map; they stay in the arena,
    // unreachable. In a tree whose rows name each other as parents, a directive can be
    // below itself: Java then finds the directive that replaced it in the map and takes
    // that for a state it cannot be in.
    // Behavior: HierarchicalTableImpl#removeOldChildren and its assertion
    fn forget(
        arena: &[Linked],
        by_key: &mut HashMap<NodeKey, usize>,
        removed: usize,
    ) -> Result<(), HierarchicalError> {
        for &child in &arena[removed].children {
            if by_key.remove(&arena[child].key) != Some(child) {
                return Err(failure(
                    "Assertion failed: asserted linkedDirectives.remove(childDirective.nodeKey) \
                     == childDirective",
                )
                .into());
            }
            forget(arena, by_key, child)?;
        }
        Ok(())
    }
    for (key, action) in directives {
        // A key that names no node now: left over from other data or another filter.
        let Some(located) = hierarchy.locate(&key)? else {
            continue;
        };
        let mut linked = arena.len();
        arena.push(Linked {
            key: key.clone(),
            node: located.node,
            row_in_parent: located.row_in_parent,
            action: Some(action),
            children: Vec::new(),
            slot_in_parent: 0,
        });
        // A directive replaces the linkage made for it by a descendant's directive, and
        // with it what hung below.
        let mut removed = by_key.insert(key.clone(), linked);
        if let Some(removed) = removed {
            forget(&arena, &mut by_key, removed)?;
        }
        let mut key = key;
        loop {
            let parent_key = match hierarchy.parent(&key)? {
                Parent::None => {
                    root = Some(linked);
                    break;
                }
                Parent::Orphan => break,
                Parent::Key(parent_key) => parent_key,
            };
            let (parent, known) = match by_key.get(&parent_key) {
                Some(&parent) => (parent, true),
                None => {
                    // A node that is not there now may be known when its parent, which
                    // is not there either, is not: then nothing leads to it.
                    let Some(located) = hierarchy.locate(&parent_key)? else {
                        break;
                    };
                    arena.push(Linked {
                        key: parent_key.clone(),
                        node: located.node,
                        row_in_parent: located.row_in_parent,
                        action: None,
                        children: Vec::new(),
                        slot_in_parent: 0,
                    });
                    by_key.insert(parent_key.clone(), arena.len() - 1);
                    (arena.len() - 1, false)
                }
            };
            match removed.take() {
                Some(removed) => {
                    let slot = arena[removed].slot_in_parent;
                    arena[linked].slot_in_parent = slot;
                    arena[parent].children[slot] = linked;
                }
                None => {
                    arena[linked].slot_in_parent = arena[parent].children.len();
                    arena[parent].children.push(linked);
                }
            }
            if known {
                // its ancestors were linked when it was made
                break;
            }
            linked = parent;
            key = parent_key;
        }
    }
    Ok((arena, root))
}

/// The rows of a node's table in the order they are shown.
enum Shown {
    /// In the order of the table.
    AsStored(Arc<RowSet>),
    /// In the order the hierarchy sorts them.
    Sorted(Vec<RowKey>),
}

impl Shown {
    fn size(&self) -> u64 {
        match self {
            Shown::AsStored(rows) => rows.size(),
            Shown::Sorted(rows) => rows.len() as u64,
        }
    }

    fn iter(&self) -> Box<dyn Iterator<Item = RowKey> + '_> {
        match self {
            Shown::AsStored(rows) => Box::new(rows.iter()),
            Shown::Sorted(rows) => Box::new(rows.iter().copied()),
        }
    }

    /// The position of each of `rows` that is shown.
    fn positions(&self, rows: &[RowKey]) -> Vec<Option<u64>> {
        match self {
            Shown::AsStored(shown) => rows.iter().map(|&row| shown.find(row)).collect(),
            Shown::Sorted(shown) => {
                let wanted: HashMap<RowKey, usize> = rows
                    .iter()
                    .enumerate()
                    .map(|(at, &row)| (row, at))
                    .collect();
                let mut positions = vec![None; rows.len()];
                if !wanted.is_empty() {
                    for (position, row) in shown.iter().enumerate() {
                        if let Some(&at) = wanted.get(row) {
                            positions[at] = Some(position as u64);
                        }
                    }
                }
                positions
            }
        }
    }

    /// Positions `start..end`: the rows as a row set, and for rows that are not shown in
    /// the order of their keys, where in that row set each shown row is.
    fn range(&self, start: u64, end: u64) -> (RowSet, Option<Vec<u32>>) {
        match self {
            Shown::AsStored(rows) => (rows.sub_set_by_position_range(start, end), None),
            Shown::Sorted(rows) => {
                let shown = &rows[start as usize..end as usize];
                let mut by_key: Vec<u32> = (0..shown.len() as u32).collect();
                by_key.sort_unstable_by_key(|&at| shown[at as usize]);
                let mut places = vec![0; shown.len()];
                for (place, &at) in by_key.iter().enumerate() {
                    places[at as usize] = place as u32;
                }
                let stored = by_key.iter().map(|&at| shown[at as usize]);
                (RowSet::from_keys(stored), Some(places))
            }
        }
    }
}

/// A run of rows of one node's table inside the viewport.
struct Segment {
    table: Arc<dyn Table>,
    /// The rows, which are read in the order of their keys.
    rows: RowSet,
    /// Where in `rows` each row of the run is, when the run is not in the order of its
    /// keys.
    places: Option<Vec<u32>>,
    depth: u32,
    expandable: Expandable,
    /// Whether the last row of `rows` is followed by its children.
    last_expanded: bool,
}

/// The deepest the walk goes. Java has no limit and visits a node from the visit of its
/// parent: on a tree that is deep enough its stack overflows and the process ends.
/// Here such a snapshot fails.
const MAX_DEPTH: u32 = 200;

struct Walk<'a> {
    hierarchy: &'a dyn Hierarchy,
    arena: Vec<Linked>,
    first: u64,
    count: u64,
    /// Rows walked so far.
    visited: u64,
    /// The depth of the rows of the node being visited.
    depth: u32,
    /// How many further levels are expanded without a directive; `u32::MAX` for all.
    expanding: u32,
    segments: Vec<Segment>,
}

impl Walk<'_> {
    /// Counts positions `start..end` of `rows` and keeps the part inside the viewport.
    ///
    /// Behavior: HierarchicalTableImpl#consumeRowsUntilNextExpansion
    fn run(
        &mut self,
        table: &Arc<dyn Table>,
        rows: &Shown,
        start: u64,
        end: u64,
        expandable: Expandable,
        last_expanded: bool,
    ) {
        if end <= start {
            return;
        }
        let from = self.visited;
        let to = from + (end - start);
        self.visited = to;
        let low = from.max(self.first);
        let high = to.min(self.first.saturating_add(self.count));
        if low >= high {
            return;
        }
        let (rows, places) = rows.range(start + (low - from), start + (high - from));
        self.segments.push(Segment {
            table: table.clone(),
            rows,
            places,
            depth: self.depth,
            expandable,
            last_expanded: last_expanded && high == to,
        });
    }

    /// Behavior: HierarchicalTableImpl#visitExpandedNode
    fn visit(
        &mut self,
        node: NodeId,
        action: Option<Action>,
        child_directives: &[usize],
    ) -> Result<(), HierarchicalError> {
        let Some(table) = self.hierarchy.children(node)? else {
            return Ok(());
        };
        if table.row_set().is_empty() {
            return Ok(());
        }
        if self.depth >= MAX_DEPTH {
            return Err(failure(format!(
                "unported: io.deephaven.engine.table.impl.hierarchical.HierarchicalTableImpl#visitExpandedNode: \
                 a walk more than {MAX_DEPTH} levels deep"
            ))
            .into());
        }
        let outer = self.expanding;
        // Behavior: SnapshotStateImpl#filling, as it is when the visit starts
        let filling = self
            .visited
            .min(self.first.saturating_add(self.count))
            .saturating_sub(self.first)
            < self.count;
        self.depth += 1;
        let rows = match self.hierarchy.order(self.depth, table.as_ref())? {
            Some(sorted) => Shown::Sorted(sorted),
            None => Shown::AsStored(table.row_set()),
        };
        let expandable = self.hierarchy.rows_expandable(self.depth);
        let can_expand = expandable != Expandable::None;
        // Behavior: SnapshotStateImpl#updateExpansionTypeAndValidateAction
        self.expanding = match action {
            Some(Action::Expand) => 0,
            Some(Action::ExpandAll) if can_expand => u32::MAX,
            Some(Action::ExpandToDepth(levels)) if can_expand => levels,
            Some(Action::ExpandAll | Action::ExpandToDepth(_)) => 0,
            None if !can_expand => 0,
            None if self.expanding == u32::MAX => u32::MAX,
            None => self.expanding.saturating_sub(1),
            Some(Action::Contract) => {
                return Err(failure("the walk visited a contracted node").into());
            }
        };
        // The child directives to act on, each with the position of its row, in row
        // order. Behavior: SnapshotStateImpl#shouldSkipChildDirective,
        // NodeTableState#filterKeyAndSortChildDirectives
        let mut children: Vec<(u64, usize)> = Vec::new();
        if can_expand {
            let acting: Vec<(usize, RowKey)> = child_directives
                .iter()
                .filter_map(|&child| {
                    let directive = &self.arena[child];
                    let acts = self.expanding > 0
                        || !matches!(directive.action, None | Some(Action::Contract));
                    directive
                        .row_in_parent
                        .filter(|_| acts)
                        .map(|row| (child, row))
                })
                .collect();
            let keys: Vec<RowKey> = acting.iter().map(|&(_, row)| row).collect();
            for (&(child, _), position) in acting.iter().zip(rows.positions(&keys)) {
                match position {
                    Some(position) => children.push((position, child)),
                    // Java looks the row up in the sorted table while it still fills the
                    // viewport, and takes a row that is not there for an inconsistency.
                    // Behavior: HierarchicalTableImpl.LinkedDirective#setRowKeyInParentSorted
                    None if filling && matches!(rows, Shown::Sorted(_)) => {
                        return Err(failure(
                            "Failed to perform sort reverse lookup: an expanded node has \
                             no row in its sorted parent now",
                        )
                        .into());
                    }
                    None => {}
                }
            }
            children.sort_unstable();
        }
        let size = rows.size();
        let mut start = 0;
        if self.expanding > 0 {
            // every row is expanded unless a directive contracts it
            let mut next = children.iter().peekable();
            for (position, row) in rows.iter().enumerate() {
                let position = position as u64;
                let directive = next
                    .next_if(|(at, _)| *at == position)
                    .map(|&(_, child)| child);
                if directive.is_some_and(|child| self.arena[child].action == Some(Action::Contract))
                {
                    continue;
                }
                self.run(&table, &rows, start, position + 1, expandable, true);
                start = position + 1;
                match directive {
                    Some(child) => self.visit_directive(child)?,
                    None => {
                        let child = NodeId {
                            depth: node.depth + 1,
                            row,
                        };
                        self.visit(child, None, &[])?;
                    }
                }
            }
        } else {
            for (position, child) in children {
                self.run(&table, &rows, start, position + 1, expandable, true);
                start = position + 1;
                self.visit_directive(child)?;
            }
        }
        self.run(&table, &rows, start, size, expandable, false);
        self.depth -= 1;
        self.expanding = outer;
        Ok(())
    }

    fn visit_directive(&mut self, directive: usize) -> Result<(), HierarchicalError> {
        let children = std::mem::take(&mut self.arena[directive].children);
        let (node, action) = (self.arena[directive].node, self.arena[directive].action);
        self.visit(node, action, &children)
    }
}

/// Rows `first .. first + count` of the walk that `directives` describe, and its size.
///
/// Behavior: HierarchicalTableImpl#snapshotData and
/// #traverseExpansionsAndFillSnapshotChunks, without the retries: the caller holds the
/// update graph's lock.
pub(super) fn snapshot(
    hierarchy: &dyn Hierarchy,
    directives: Vec<(NodeKey, Action)>,
    columns: &[usize],
    first: u64,
    count: u64,
) -> Result<Snapshot, HierarchicalError> {
    let (arena, root) = link(hierarchy, directives)?;
    let mut walk = Walk {
        hierarchy,
        arena,
        first,
        count,
        visited: 0,
        depth: 0,
        expanding: 0,
        segments: Vec::new(),
    };
    // A root that is not expanded shows nothing.
    if let Some(root) = root {
        if !matches!(walk.arena[root].action, None | Some(Action::Contract)) {
            walk.visit_directive(root)?;
        }
    }
    let definition = hierarchy.columns();
    let mut data = Vec::with_capacity(columns.len());
    for &column in columns {
        let column_type = definition
            .get(column)
            .map(|column| column.column_type.arrow_type())
            .ok_or_else(|| {
                HierarchicalError::IllegalArgument(format!(
                    "Requested column {column} of {} columns",
                    definition.len()
                ))
            })?;
        let mut parts: Vec<ArrayRef> = Vec::with_capacity(walk.segments.len());
        for segment in &walk.segments {
            let length = segment.rows.size() as usize;
            parts.push(match column {
                DEPTH_COLUMN => Arc::new(Int32Array::from(vec![segment.depth as i32; length])),
                EXPANDED_COLUMN => {
                    let expanded = |row: usize| segment.last_expanded && row + 1 == length;
                    Arc::new(match segment.expandable {
                        Expandable::None => BooleanArray::new_null(length),
                        Expandable::All => (0..length).map(|row| Some(expanded(row))).collect(),
                        Expandable::ByRow => {
                            let stored =
                                hierarchy.expandable_rows(segment.table.as_ref(), &segment.rows)?;
                            (0..length)
                                .map(|row| {
                                    let place = match &segment.places {
                                        Some(places) => places[row] as usize,
                                        None => row,
                                    };
                                    stored[place].then(|| expanded(row))
                                })
                                .collect::<BooleanArray>()
                        }
                    })
                }
                _ => {
                    let stored = hierarchy.fill(
                        segment.depth,
                        segment.table.as_ref(),
                        &segment.rows,
                        column,
                    )?;
                    match &segment.places {
                        None => stored,
                        Some(places) => {
                            arrow::compute::take(&stored, &UInt32Array::from(places.clone()), None)
                                .map_err(|error| failure(error.to_string()))?
                        }
                    }
                }
            });
        }
        data.push(if parts.is_empty() {
            arrow::array::new_empty_array(&column_type)
        } else {
            let parts: Vec<&dyn Array> = parts.iter().map(|part| part.as_ref()).collect();
            arrow::compute::concat(&parts).map_err(|error| failure(error.to_string()))?
        });
    }
    Ok(Snapshot {
        size: walk.visited,
        rows: walk
            .segments
            .iter()
            .map(|segment| segment.rows.size())
            .sum(),
        columns: data,
    })
}
