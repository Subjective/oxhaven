//! Trees and their views. The rows of the scripted cases, and the failures, are what
//! Java 42.9 was observed to answer (`oracle/scenarios/python/tree_*.py`).

use super::tests::text;
use super::*;
use crate::graph::SteppedUpdateGraph;
use crate::ops::filter::CheckedFilters;
use crate::ops::sort::SortColumn;
use arrow::array::{Array, Int32Array, Int8Array};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_expr::Compiler;
use oxhaven_testkit::{
    at_least_for, definition, strings, test_seeds, ConsistencyChecker, RandomUpdates,
    ReferenceTable, ScriptedTable, TableChange,
};
use std::collections::{BTreeMap, BTreeSet};

fn numbers(values: &[Option<i32>]) -> ArrayRef {
    Arc::new(Int32Array::from(values.to_vec()))
}

const COLUMNS: [(&str, ColumnType); 3] = [
    ("Id", ColumnType::Int),
    ("Parent", ColumnType::Int),
    ("Name", ColumnType::String),
];

/// Rows of identifier, parent (0 for none) and name.
fn values(rows: &[(i32, i32, &str)]) -> Vec<ArrayRef> {
    let ids: Vec<Option<i32>> = rows.iter().map(|row| Some(row.0)).collect();
    let parents: Vec<Option<i32>> = rows
        .iter()
        .map(|row| (row.1 != 0).then_some(row.1))
        .collect();
    vec![
        numbers(&ids),
        numbers(&parents),
        strings(rows.iter().map(|row| row.2)),
    ]
}

fn people(rows: &[(i32, i32, &str)]) -> Arc<dyn Table> {
    ReferenceTable::new_static(
        definition(&COLUMNS),
        RowSet::flat(rows.len() as u64),
        values(rows),
    )
}

/// The table of the scenario `tree_static`: 7 is an orphan with a child, 8 its own
/// parent.
fn family() -> Arc<dyn Table> {
    people(&[
        (1, 0, "a"),
        (2, 0, "b"),
        (3, 1, "c"),
        (4, 1, "d"),
        (5, 3, "e"),
        (6, 2, "f"),
        (7, 99, "g"),
        (8, 8, "h"),
        (9, 7, "i"),
    ])
}

const EXPAND_ALL: i8 = 3;
const CONTRACT: i8 = 4;

/// A key table: identifiers (0 for the root) and actions.
fn keys(rows: &[(i32, i8)]) -> Arc<dyn Table> {
    let ids: Vec<Option<i32>> = rows
        .iter()
        .map(|row| (row.0 != 0).then_some(row.0))
        .collect();
    ReferenceTable::new_static(
        definition(&[("Id", ColumnType::Int), ("Action", ColumnType::Byte)]),
        RowSet::flat(rows.len() as u64),
        vec![
            numbers(&ids),
            Arc::new(Int8Array::from_iter_values(rows.iter().map(|row| row.1))),
        ],
    )
}

fn snapshot_of(tree: &Arc<TreeTable>, rows: &[(i32, i8)]) -> Result<Snapshot, HierarchicalError> {
    let hierarchy: Arc<dyn Hierarchy> = tree.clone();
    View::new(hierarchy, Some(keys(rows)), Some("Action".to_string())).snapshot(None, 0, 10_000)
}

/// The rows of the walk as `depth`, `+` expanded, `-` not, `.` cannot be, and the
/// identifier.
fn shown(tree: &Arc<TreeTable>, rows: &[(i32, i8)]) -> String {
    let snapshot = snapshot_of(tree, rows).unwrap();
    text(&snapshot)
        .iter()
        .map(|row| {
            let cells: Vec<&str> = row.split('|').collect();
            let expanded = match cells[1] {
                "true" => "+",
                "false" => "-",
                _ => ".",
            };
            format!("{}{expanded}{}", cells[0], cells[2])
        })
        .collect::<Vec<_>>()
        .join(" ")
}

fn everything(tree: &Arc<TreeTable>) -> String {
    shown(tree, &[(0, EXPAND_ALL)])
}

fn tree_of(source: &Arc<dyn Table>) -> Arc<TreeTable> {
    Arc::new(tree(source, "Id", "Parent").unwrap())
}

fn promoted(source: &Arc<dyn Table>) -> Arc<TreeTable> {
    tree_of(&promote_orphans(source, "Id", "Parent", &Compiler).unwrap())
}

#[test]
fn the_rows_without_a_parent_are_under_the_root() {
    let tree = tree_of(&family());
    let hierarchy: Arc<dyn Hierarchy> = tree.clone();
    let snapshot = View::new(hierarchy, None, None)
        .snapshot(None, 0, 100)
        .unwrap();
    assert_eq!(text(&snapshot), ["1|false|1|-|a", "1|false|2|-|b"]);
    let names: Vec<&str> = tree
        .columns()
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    assert_eq!(names, ["__DEPTH__", "__EXPANDED__", "Id", "Parent", "Name"]);
}

#[test]
fn orphans_are_left_out_unless_promoted() {
    let source = family();
    assert_eq!(everything(&tree_of(&source)), "1+1 2+3 3.5 2.4 1+2 2.6");
    assert_eq!(
        everything(&promoted(&source)),
        "1+1 2+3 3.5 2.4 1+2 2.6 1+7 2.9"
    );
    // observed on Java 42.9: the promotion keeps the columns a source restricts its
    // sorts to, in the order of the set its view collects them in
    let restricted = crate::ops::sort::restrict_sort_to(&source, &["Name", "Id"]).unwrap();
    let names = |tree: &TreeTable| tree.sortable_columns().map(|names| names.join(","));
    assert_eq!(names(&tree_of(&restricted)).as_deref(), Some("Name,Id"));
    assert_eq!(names(&promoted(&restricted)).as_deref(), Some("Id,Name"));
    assert_eq!(names(&promoted(&source)), None);
}

#[test]
fn a_key_table_names_nodes_by_identifier() {
    let tree = promoted(&family());
    // 6 has no children and 42 is no row
    assert_eq!(
        shown(&tree, &[(1, 1), (3, 1), (7, 1), (6, 1), (42, 1)]),
        "1+1 2+3 3.5 2.4 1-2 1+7 2.9"
    );
    let tree = tree_of(&family());
    assert_eq!(shown(&tree, &[(0, 0x71)]), "1+1 2-3 2.4 1+2 2.6");
    assert_eq!(
        shown(&tree, &[(0, EXPAND_ALL), (3, CONTRACT)]),
        "1+1 2-3 2.4 1+2 2.6"
    );
}

/// Observed on Java 42.9: `Failed to find ancestor for existing key table directive`,
/// `INVALID_ARGUMENT` for the view.
#[test]
fn a_key_of_an_orphan_with_children_fails_the_snapshot() {
    let error = snapshot_of(&tree_of(&family()), &[(7, 1)]).unwrap_err();
    assert!(
        error.to_string().contains("Failed to find ancestor"),
        "{error}"
    );
    // an orphan without children is no node
    let tree = tree_of(&people(&[(1, 0, "a"), (2, 9, "b")]));
    assert_eq!(shown(&tree, &[(2, 1)]), "1.1");
}

/// Observed on Java 42.9: rows that are each other's ancestors are not shown, a key of
/// one of them does nothing, and keys of two of them fail the snapshot with an
/// assertion of `removeOldChildren`.
#[test]
fn rows_in_a_cycle_are_not_reached() {
    let source = people(&[
        (1, 0, "a"),
        (2, 2, "self"),
        (3, 2, "c"),
        (4, 5, "x"),
        (5, 4, "y"),
    ]);
    for tree in [tree_of(&source), promoted(&source)] {
        assert_eq!(everything(&tree), "1.1");
        assert_eq!(shown(&tree, &[(2, 1)]), "1.1");
        assert_eq!(shown(&tree, &[(4, 1)]), "1.1");
        let error = snapshot_of(&tree, &[(4, 1), (5, 1)]).unwrap_err();
        assert!(
            error.to_string().contains("linkedDirectives.remove"),
            "{error}"
        );
    }
}

/// Observed on Java 42.9: a row without an identifier can be expanded, and its children
/// are the rows of the root. Expanded to a depth Java shows the root's rows again with
/// depths that are not theirs, and with every level expanded its stack overflows and
/// the server stops. Neither is reproduced.
#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.hierarchical.TreeTableImpl#makeChildNodeIdLookup"
)]
fn the_expansion_of_a_row_without_an_identifier_is_not_ported() {
    let source: Arc<dyn Table> = ReferenceTable::new_static(
        definition(&COLUMNS),
        RowSet::flat(3),
        vec![
            numbers(&[None, Some(1), Some(2)]),
            numbers(&[None, None, Some(1)]),
            strings(["n", "a", "b"]),
        ],
    );
    let tree = tree_of(&source);
    assert_eq!(shown(&tree, &[]), "1-- 1-1");
    assert_eq!(shown(&tree, &[(1, 1)]), "1-- 1+1 2.2");
    let _ = snapshot_of(&tree, &[(0, 0x71)]);
}

/// A chain of rows deeper than the walk goes fails the snapshot and not the process.
#[test]
fn a_tree_deeper_than_the_walk_goes_fails_the_snapshot() {
    let ids: Vec<Option<i32>> = (1..=300).map(Some).collect();
    let parents: Vec<Option<i32>> = (0..300).map(|id| (id > 0).then_some(id)).collect();
    let source: Arc<dyn Table> = ReferenceTable::new_static(
        definition(&COLUMNS),
        RowSet::flat(300),
        vec![numbers(&ids), numbers(&parents), strings(vec!["x"; 300])],
    );
    let tree = tree_of(&source);
    assert_eq!(shown(&tree, &[(0, 0x73)]), "1+1 2+2 3+3 4-4");
    let error = snapshot_of(&tree, &[(0, EXPAND_ALL)]).unwrap_err();
    assert!(error.to_string().contains("levels deep"), "{error}");
}

#[test]
fn java_refusals_of_a_tree() {
    let source = family();
    let refusal = |id: &str, parent: &str| tree(&source, id, parent).err().unwrap().to_string();
    assert_eq!(
        refusal("Nope", "Parent"),
        "tree identifier column: Unknown column names [Nope], available column names are \
         [Id, Parent, Name]"
    );
    assert_eq!(
        refusal("Id", "Nope"),
        "tree parent column: Unknown column names [Nope], available column names are \
         [Id, Parent, Name]"
    );
    assert_eq!(
        refusal("Id", "Name"),
        "tree parent and identifier columns must have the same data type, but parent is \
         [Name, class java.lang.String] and identifier is [Id, int]"
    );
    let twice = people(&[(1, 0, "a"), (2, 1, "b"), (2, 1, "c")]);
    let error = tree(&twice, "Id", "Parent").err().unwrap().to_string();
    assert!(error.contains("outputs=[__ROW_KEY__]"), "{error}");
    // every row is its own parent
    assert_eq!(
        everything(&Arc::new(tree(&source, "Id", "Id").unwrap())),
        ""
    );
    // a key table without the identifier column, or with it in another type
    let tree = tree_of(&source);
    let hierarchy: Arc<dyn Hierarchy> = tree.clone();
    let wrong = ReferenceTable::new_static(
        definition(&[("Id", ColumnType::String)]),
        RowSet::flat(1),
        vec![strings(["1"])],
    );
    assert!(View::new(hierarchy, Some(wrong), None)
        .snapshot(None, 0, 10)
        .is_err());
}

fn ticking(graph: &Arc<SteppedUpdateGraph>, rows: &[(i32, i32, &str)]) -> Arc<ScriptedTable> {
    ScriptedTable::new(
        graph.clone(),
        definition(&COLUMNS),
        RowSet::flat(rows.len() as u64),
        values(rows),
    )
}

/// The steps of the scenario `tree_ticking`, as Java 42.9 answered them.
#[test]
fn a_ticking_tree_follows_its_source() {
    let graph = SteppedUpdateGraph::new();
    let source = ticking(
        &graph,
        &[
            (1, 0, "a"),
            (2, 0, "b"),
            (3, 1, "c"),
            (4, 3, "d"),
            (5, 9, "e"),
        ],
    );
    let parent: Arc<dyn Table> = source.table();
    let (plain, raised) = (tree_of(&parent), promoted(&parent));
    let chosen = [(1, 1), (3, 1), (9, 1), (5, 1)];
    assert_eq!(everything(&plain), "1+1 2+3 3.4 1.2");
    assert_eq!(everything(&raised), "1+1 2+3 3.4 1.2 1.5");
    assert_eq!(shown(&plain, &chosen), "1+1 2+3 3.4 1.2");
    assert_eq!(shown(&raised, &chosen), "1+1 2+3 3.4 1.2 1.5");

    // 9 appears under 2, so 5 is no orphan; 4 moves under 2; 6 is new under 5
    source.push(
        TableChange::new()
            .add([5, 6], values(&[(9, 2, "nine"), (6, 5, "f")]))
            .modify([3], [(1, numbers(&[Some(2)])), (2, strings(["d2"]))]),
    );
    graph.run_cycle();
    for tree in [&plain, &raised] {
        assert_eq!(everything(tree), "1+1 2.3 1+2 2.4 2+9 3+5 4.6");
        // 3 has no children now, and nothing expands 2
        assert_eq!(shown(tree, &chosen), "1+1 2.3 1-2");
    }

    // 1 goes: 3 is an orphan, and a key of it, which had children, fails
    source.push(TableChange::new().remove([0]));
    graph.run_cycle();
    assert_eq!(everything(&plain), "1+2 2.4 2+9 3+5 4.6");
    assert_eq!(everything(&raised), "1+2 2.4 2+9 3+5 4.6 1.3");
    let error = snapshot_of(&plain, &chosen).unwrap_err();
    assert!(
        error.to_string().contains("Failed to find ancestor"),
        "{error}"
    );
    assert_eq!(shown(&raised, &chosen), "1-2 1.3");

    // 1 comes back under 3: the two are a cycle, promoted or not
    source.push(TableChange::new().add([0], values(&[(1, 3, "cycle")])));
    graph.run_cycle();
    for tree in [&plain, &raised] {
        assert_eq!(everything(tree), "1+2 2.4 2+9 3+5 4.6");
        let error = snapshot_of(tree, &chosen).unwrap_err();
        assert!(
            error.to_string().contains("linkedDirectives.remove"),
            "{error}"
        );
    }

    source.push(TableChange::new().modify([0], [(1, numbers(&[None])), (2, strings(["back"]))]));
    graph.run_cycle();
    for tree in [&plain, &raised] {
        assert_eq!(everything(tree), "1+1 2.3 1+2 2.4 2+9 3+5 4.6");
        assert_eq!(shown(tree, &chosen), "1+1 2.3 1-2");
    }
}

/// Observed on Java 42.9: a second row with an identifier fails the aggregation that
/// finds rows by identifier, and nothing else. The tree goes on showing its rows, and
/// what it knows of identifiers stays as it was.
#[test]
fn a_second_row_with_an_identifier_stops_the_lookup_and_nothing_else() {
    let graph = SteppedUpdateGraph::new();
    let source = ticking(&graph, &[(1, 0, "a"), (2, 1, "b"), (3, 1, "c")]);
    let parent: Arc<dyn Table> = source.table();
    let tree = tree_of(&parent);
    source.push(TableChange::new().add([3], values(&[(2, 0, "again")])));
    graph.run_cycle();
    assert_eq!(everything(&tree), "1+1 2.2 2.3 1.2");
    source.push(TableChange::new().add([4, 5], values(&[(7, 0, "g"), (8, 7, "h")])));
    graph.run_cycle();
    assert_eq!(everything(&tree), "1+1 2.2 2.3 1.2 1+7 2.8");
    // 7 came after the lookup stopped: a key of it finds no row
    assert_eq!(shown(&tree, &[(7, 1), (1, 1)]), "1+1 2.2 2.3 1.2 1-7");
}

const VALUED: [(&str, ColumnType); 3] = [
    ("Id", ColumnType::Int),
    ("Parent", ColumnType::Int),
    ("V", ColumnType::Int),
];

/// The table of the scenario `tree_sorted`.
fn valued() -> Arc<dyn Table> {
    ReferenceTable::new_static(
        definition(&VALUED),
        RowSet::flat(7),
        vec![
            numbers(&[
                Some(1),
                Some(2),
                Some(3),
                Some(4),
                Some(5),
                Some(6),
                Some(7),
            ]),
            numbers(&[None, None, Some(1), Some(1), Some(3), Some(3), Some(2)]),
            numbers(&[
                Some(5),
                Some(9),
                Some(3),
                Some(8),
                Some(-7),
                Some(2),
                Some(1),
            ]),
        ],
    )
}

fn filtered(tree: &TreeTable, filters: &[&str]) -> Arc<TreeTable> {
    let checked = CheckedFilters::parse(filters, tree.source().definition(), &Compiler).unwrap();
    Arc::new(tree.with_filters(&checked, &Compiler).unwrap())
}

#[test]
fn a_sort_orders_the_children_of_every_node() {
    let tree = tree_of(&valued());
    let descending = Arc::new(tree.with_sorts(&[SortColumn::descending("V")]));
    assert_eq!(everything(&descending), "1+2 2.7 1+1 2.4 2+3 3.6 3.5");
    assert_eq!(
        shown(&descending, &[(1, 1), (3, 1)]),
        "1-2 1+1 2.4 2+3 3.6 3.5"
    );
    let absolute = Arc::new(tree.with_sorts(&[SortColumn::ascending("V").absolute()]));
    assert_eq!(everything(&absolute), "1+1 2+3 3.6 3.5 2.4 1+2 2.7");
    assert!(tree.check_sorts(&[SortColumn::ascending("V")]).is_ok());
    assert_eq!(
        tree.check_sorts(&[SortColumn::ascending("W")])
            .unwrap_err()
            .to_string(),
        "Unknown column names [W], available column names are [Id, Parent, V]"
    );
}

#[test]
fn a_filter_keeps_the_ancestors_of_the_rows_that_pass() {
    let tree = tree_of(&valued());
    let small = filtered(&tree, &["V < 3"]);
    assert_eq!(everything(&small), "1+1 2+3 3.5 3.6 1+2 2.7");
    // a second filter reads the rows the first kept, ancestors among them
    assert_eq!(
        everything(&filtered(&small, &["V > 0"])),
        "1+1 2+3 3.6 1+2 2.7"
    );
    assert_eq!(everything(&filtered(&tree, &["V > 100"])), "");
    let sorted = Arc::new(tree.with_sorts(&[SortColumn::descending("V")]));
    assert_eq!(
        everything(&filtered(&sorted, &["V < 3"])),
        "1+2 2.7 1+1 2+3 3.6 3.5"
    );
}

/// What a tree of `rows` (identifier, parent, value by row key) shows with every level
/// expanded: each row as [`shown`] writes it.
fn expected_walk(rows: &BTreeMap<RowKey, (i32, Option<i32>, Option<i32>)>) -> String {
    let mut children: BTreeMap<Option<i32>, Vec<i32>> = BTreeMap::new();
    for (id, parent, _) in rows.values() {
        children.entry(*parent).or_default().push(*id);
    }
    fn walk(
        children: &BTreeMap<Option<i32>, Vec<i32>>,
        parent: Option<i32>,
        depth: usize,
        out: &mut Vec<String>,
    ) {
        for id in children.get(&parent).into_iter().flatten() {
            let expandable = children.contains_key(&Some(*id));
            out.push(format!("{depth}{}{id}", if expandable { "+" } else { "." }));
            walk(children, Some(*id), depth + 1, out);
        }
    }
    let mut out = Vec::new();
    walk(&children, None, 1, &mut out);
    out.join(" ")
}

fn read(source: &dyn Table) -> BTreeMap<RowKey, (i32, Option<i32>, Option<i32>)> {
    let rows = source.row_set();
    let columns: Vec<ArrayRef> = (0..3)
        .map(|index| source.column(index).chunk(&rows).unwrap())
        .collect();
    let column = |index: usize| {
        columns[index]
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap()
    };
    rows.iter()
        .enumerate()
        .map(|(at, row)| {
            let value = |index: usize| column(index).is_valid(at).then(|| column(index).value(at));
            (row, (column(0).value(at), value(1), value(2)))
        })
        .collect()
}

/// Random changes of a source whose identifiers stay unique: after every cycle the tree,
/// the tree with orphans promoted and the filtered tree show what a tree made anew of
/// the rows shows, and the filtered source is consistent and equal to one made anew.
fn random_tree(seed: u64) -> u64 {
    let graph = SteppedUpdateGraph::new();
    let mut random = RandomUpdates::new(seed);
    let mut state = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    let mut below = move |bound: u64| {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state % bound
    };
    let mut next_id = 1;
    let mut fresh = |count: usize| -> ArrayRef {
        let ids: Vec<Option<i32>> = (0..count)
            .map(|_| {
                next_id += 1;
                Some(next_id)
            })
            .collect();
        numbers(&ids)
    };
    // a parent: none, an identifier that was given out, or one that never is
    let mut parents = |count: usize, issued: i32| -> ArrayRef {
        let parents: Vec<Option<i32>> = (0..count)
            .map(|_| match below(8) {
                0 | 1 => None,
                2 => Some(-1),
                _ => Some(2 + below(issued.max(1) as u64) as i32),
            })
            .collect();
        numbers(&parents)
    };
    let source = ScriptedTable::new(
        graph.clone(),
        definition(&VALUED),
        RowSet::flat(12),
        vec![
            fresh(12),
            parents(12, 12),
            random.values(&ColumnType::Int, 12),
        ],
    );
    let parent: Arc<dyn Table> = source.table();
    let plain = tree_of(&parent);
    let raised = promoted(&parent);
    let positive = filtered(&plain, &["V > 0"]);
    let checker = {
        let parent = parent.clone();
        ConsistencyChecker::attach_with_recompute(positive.source().clone(), move || {
            filtered(&tree_of(&parent), &["V > 0"]).source().clone()
        })
    };
    let mut issued = 12;
    let mut rows_seen = 0;
    for cycle in 0..30 {
        let mut change = random.next_change(&*parent, 16);
        let added = change.update.added.size() as usize;
        if added > 0 {
            change.added_values[0] = fresh(added);
            change.added_values[1] = parents(added, issued);
            issued += added as i32;
        }
        let modified = change.update.modified.size() as usize;
        if let Some(values) = change.modified_values.get_mut(&0) {
            *values = fresh(modified);
            issued += modified as i32;
        }
        if let Some(values) = change.modified_values.get_mut(&1) {
            *values = parents(modified, issued);
        }
        source.push(change);
        graph.run_cycle();
        checker
            .verify()
            .unwrap_or_else(|error| panic!("seed {seed}, cycle {cycle}: {error:?}"));
        let rows = read(&*parent);
        assert_eq!(
            everything(&plain),
            expected_walk(&rows),
            "seed {seed}, cycle {cycle}"
        );
        let ids: BTreeSet<i32> = rows.values().map(|row| row.0).collect();
        let lifted = rows
            .iter()
            .map(|(row, (id, parent, value))| {
                (
                    *row,
                    (*id, parent.filter(|parent| ids.contains(parent)), *value),
                )
            })
            .collect();
        assert_eq!(
            everything(&raised),
            expected_walk(&lifted),
            "seed {seed}, cycle {cycle}"
        );
        // the rows that pass and their ancestors
        let by_id: BTreeMap<i32, RowKey> =
            rows.iter().map(|(row, cells)| (cells.0, *row)).collect();
        let mut kept: BTreeSet<RowKey> = rows
            .iter()
            .filter(|(_, cells)| cells.2.is_some_and(|value| value > 0))
            .map(|(row, _)| *row)
            .collect();
        let mut level: Vec<RowKey> = kept.iter().copied().collect();
        while let Some(row) = level.pop() {
            let ancestor = rows[&row].1.and_then(|parent| by_id.get(&parent));
            if let Some(ancestor) = ancestor {
                if kept.insert(*ancestor) {
                    level.push(*ancestor);
                }
            }
        }
        let passing = rows
            .iter()
            .filter(|(row, _)| kept.contains(row))
            .map(|(row, cells)| (*row, *cells))
            .collect();
        let walk = everything(&positive);
        assert_eq!(walk, expected_walk(&passing), "seed {seed}, cycle {cycle}");
        rows_seen += walk.split(' ').count() as u64;
    }
    rows_seen
}

#[test]
fn random_changes_keep_trees_right() {
    let mut rows = 0;
    for seed in test_seeds(0..40) {
        rows += random_tree(seed);
    }
    assert!(rows >= at_least_for(2000, 0..40), "only {rows} rows");
}

/// The last cell of every row of the walk with every node expanded.
fn last_cells(tree: &Arc<TreeTable>) -> Vec<String> {
    text(&snapshot_of(tree, &[(0, EXPAND_ALL)]).unwrap())
        .iter()
        .map(|row| row.rsplit('|').next().unwrap().to_string())
        .collect()
}

const RED: &str = "143833713130660064";
const BLUE: &str = "72058689286103264";

fn with_ops(
    tree: &Arc<TreeTable>,
    record: impl FnOnce(NodeOperations) -> Result<NodeOperations, HierarchicalError>,
) -> Result<Arc<TreeTable>, HierarchicalError> {
    let operations = record(tree.node_operations())?;
    tree.with_node_operations(&operations).map(Arc::new)
}

fn texts(items: &[&str]) -> Vec<String> {
    items.iter().map(|item| item.to_string()).collect()
}

#[test]
fn formats_add_columns_to_the_nodes() {
    let tree = tree_of(&valued());
    let formatted = with_ops(&tree, |ops| {
        ops.format_columns(&texts(&["V = V > 2 ? RED : BLUE"]), &Compiler)
    })
    .unwrap();
    let names: Vec<&str> = formatted
        .snapshot_definition()
        .columns()
        .iter()
        .map(|column| column.name.as_str())
        .collect();
    assert_eq!(
        names,
        [
            "__DEPTH__",
            "__EXPANDED__",
            "Id",
            "Parent",
            "V",
            "V__TABLE_STYLE_FORMAT"
        ]
    );
    assert_eq!(everything(&formatted), everything(&tree));
    // as Java 42.9 answered
    assert_eq!(
        last_cells(&formatted),
        [RED, RED, BLUE, BLUE, RED, RED, BLUE]
    );
    // the rows that a condition holds for, and 0 for the others
    let rows = with_ops(&tree, |ops| {
        ops.format_columns(
            &[crate::ops::select::format::conditional_format(
                "*", "V > 2", "PINK",
            )],
            &Compiler,
        )
    })
    .unwrap();
    let pink = "144045691226816512";
    assert_eq!(last_cells(&rows), [pink, pink, "0", "0", pink, pink, "0"]);
    // a filter of the source keeps the formats
    let checked = CheckedFilters::parse(&["V > 4"], tree.source().definition(), &Compiler).unwrap();
    let source_filtered = Arc::new(formatted.with_filters(&checked, &Compiler).unwrap());
    assert_eq!(everything(&source_filtered), "1+1 2.4 1.2");
    assert_eq!(last_cells(&source_filtered), [RED, RED, RED]);
}

#[test]
fn a_node_filter_keeps_the_rows_of_a_node_that_pass() {
    let tree = tree_of(&valued());
    let big = with_ops(&tree, |ops| {
        ops.where_filters(&texts(&["V > 2"]), &Compiler)
    })
    .unwrap();
    // 3 and 2 have children, none of which passes: they cannot be expanded
    assert_eq!(everything(&big), "1+1 2.3 2.4 1.2");
    let two = with_ops(&tree, |ops| {
        ops.where_filters(&texts(&["V > 2", "V < 9"]), &Compiler)
    })
    .unwrap();
    assert_eq!(everything(&two), "1+1 2.3 2.4");
    // the position of a row is its position in its node
    let later = with_ops(&tree, |ops| {
        ops.where_filters(&texts(&["i > 0"]), &Compiler)
    })
    .unwrap();
    assert_eq!(everything(&later), "1.2");
    // a key of a row that does not pass expands nothing
    let over_four = with_ops(&tree, |ops| {
        ops.where_filters(&texts(&["V > 4"]), &Compiler)
    })
    .unwrap();
    assert_eq!(shown(&over_four, &[(1, 1), (3, 1)]), "1+1 2.4 1.2");
    // sorted too: a static node has no reverse lookup of its sort, and Java passes the
    // key over (a ticking one fails the view: the scenario `tree_formatted`)
    let sorted = with_ops(&tree, |ops| {
        Ok(ops
            .where_filters(&texts(&["V > 4"]), &Compiler)?
            .sort(&[SortColumn::descending("V")]))
    })
    .unwrap();
    assert_eq!(shown(&sorted, &[(1, 1), (3, 1)]), "1.2 1+1 2.4");
    // a filter may read a formatting column recorded before it
    let by_format = with_ops(&tree, |ops| {
        ops.format_columns(&texts(&["V = RED"]), &Compiler)?
            .where_filters(&texts(&["V__TABLE_STYLE_FORMAT != 0"]), &Compiler)
    })
    .unwrap();
    assert_eq!(everything(&by_format), everything(&tree));
    assert!(matches!(
        tree.node_operations()
            .where_filters(&texts(&["Nope > 1"]), &Compiler),
        Err(HierarchicalError::Formula(_))
    ));
    assert!(matches!(
        tree.node_operations()
            .where_filters(&texts(&["V_[i - 1] > 1"]), &Compiler),
        Err(HierarchicalError::Unsupported(_))
    ));
}

/// Behavior: TreeNodeOperationsRecorder#withOperations, as Java 42.9 answered it
#[test]
fn formats_are_refused_for_nodes_that_have_operations() {
    let tree = tree_of(&valued());
    let incompatible = |result: Result<TreeTable, HierarchicalError>| {
        assert_eq!(
            result.err().map(|error| error.to_string()),
            Some(
                "Incompatible operation recorders; compatible recorders must be created from \
                 the same table"
                    .to_string()
            )
        );
    };
    let format = |ops: NodeOperations| ops.format_columns(&texts(&["V = RED"]), &Compiler);
    let sorted = tree.with_sorts(&[SortColumn::ascending("V")]);
    incompatible(sorted.with_node_operations(&format(tree.node_operations()).unwrap()));
    let filtered = with_ops(&tree, |ops| {
        ops.where_filters(&texts(&["V > 2"]), &Compiler)
    })
    .unwrap();
    incompatible(filtered.with_node_operations(&format(tree.node_operations()).unwrap()));
    let formatted = with_ops(&tree, format).unwrap();
    incompatible(
        formatted.with_node_operations(&tree.node_operations().sort(&[SortColumn::ascending("V")])),
    );
    // filters add no column, and a recorder of the formatted tree has its columns
    let again = with_ops(&filtered, |ops| {
        ops.where_filters(&texts(&["V < 9"]), &Compiler)
    })
    .unwrap();
    assert_eq!(everything(&again), "1+1 2.3 2.4");
    let refmt = with_ops(&formatted, |ops| {
        ops.format_columns(&texts(&["V = BLUE"]), &Compiler)?
            .where_filters(&texts(&["V < 9"]), &Compiler)
    })
    .unwrap();
    assert_eq!(everything(&refmt), "1+1 2+3 3.5 3.6 2.4");
    assert_eq!(last_cells(&refmt), [BLUE; 5]);
}
