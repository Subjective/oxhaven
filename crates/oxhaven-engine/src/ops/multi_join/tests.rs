//! Unit tests of the multi join: the static result and its refusals, then each kind of
//! change of its tables.

use super::{multi_join, JoinAddition, JoinMatch, MultiJoinError, MultiJoinInput};
use crate::ops::sort::testing::{frozen, graph, keys, scripted, static_table, Columns};
use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int32Array, Int64Array, Int8Array,
    TimestampNanosecondArray,
};
use arrow::compute::cast;
use arrow::datatypes::DataType;
use arrow::util::display::{ArrayFormatter, FormatOptions};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowKey;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_testkit::{
    ints, longs, strings, ConsistencyChecker, Recorder, ScriptedTable, TableChange, TableSnapshot,
};
use std::sync::Arc;

fn opt_ints(values: &[Option<i32>]) -> ArrayRef {
    Arc::new(Int32Array::from(values.to_vec()))
}

fn input(table: &Arc<dyn Table>, on: &[&str], add: &[&str]) -> MultiJoinInput {
    let pair = |text: &&str| match text.split_once('=') {
        Some((left, right)) => (left.to_string(), right.to_string()),
        None => (text.to_string(), text.to_string()),
    };
    MultiJoinInput {
        table: table.clone(),
        on: on
            .iter()
            .map(pair)
            .map(|(left, right)| JoinMatch::new(left, right))
            .collect(),
        add: add
            .iter()
            .map(pair)
            .map(|(name, source)| JoinAddition::renamed(name, source))
            .collect(),
    }
}

fn names(table: &dyn Table) -> Vec<&str> {
    let columns = table.definition().columns().iter();
    columns.map(|column| column.name.as_str()).collect()
}

/// The rows of `table` in order, each as its cells joined by commas. An instant is written
/// as its nanoseconds.
fn rows(table: &dyn Table) -> Vec<String> {
    let snapshot = TableSnapshot::current(table).unwrap();
    let options = FormatOptions::default().with_null("null");
    let columns: Vec<ArrayRef> = snapshot
        .columns
        .iter()
        .map(|values| match values.data_type() {
            DataType::Timestamp(_, _) => cast(values, &DataType::Int64).unwrap(),
            _ => values.clone(),
        })
        .collect();
    let formatters: Vec<ArrayFormatter> = columns
        .iter()
        .map(|values| ArrayFormatter::try_new(values.as_ref(), &options).unwrap())
        .collect();
    (0..snapshot.rows.size() as usize)
        .map(|row| {
            let cells: Vec<String> = formatters
                .iter()
                .map(|column| column.value(row).to_string())
                .collect();
            cells.join(",")
        })
        .collect()
}

/// `K` (int) and a string column `name`.
fn tagged(name: &'static str, keys: &[Option<i32>], tags: &[&str]) -> Arc<dyn Table> {
    static_table(&[
        ("K", ColumnType::Int, opt_ints(keys)),
        (name, ColumnType::String, strings(tags.iter().copied())),
    ])
}

fn refusal(inputs: &[MultiJoinInput]) -> MultiJoinError {
    multi_join(inputs).err().expect("a refusal")
}

// ---- The static result ---------------------------------------------------------------------

#[test]
fn one_row_per_key_in_order_of_first_appearance_with_nulls_for_missing_rows() {
    let a = tagged(
        "A",
        &[Some(1), Some(2), None, Some(4)],
        &["a1", "a2", "an", "a4"],
    );
    let b = tagged("B", &[Some(2), Some(3), Some(1)], &["b2", "b3", "b1"]);
    let c = tagged("C", &[Some(3), Some(5), None], &["c3", "c5", "cn"]);
    let result = multi_join(&[
        MultiJoinInput::new(a.clone(), &["K"]),
        MultiJoinInput::new(b.clone(), &["K"]),
        MultiJoinInput::new(c, &["K"]),
    ])
    .unwrap();
    assert_eq!(names(&*result), ["K", "A", "B", "C"]);
    assert_eq!(
        rows(&*result),
        [
            "1,a1,b1,null",
            "2,a2,b2,null",
            "null,an,null,cn",
            "4,a4,null,null",
            "3,null,b3,c3",
            "5,null,null,c5",
        ]
    );
    assert_eq!(keys(&result.row_set()), [0, 1, 2, 3, 4, 5]);
    assert!(!result.is_refreshing() && !result.is_flat());
    assert_eq!(result.attributes(), Default::default());

    // The first table decides the order.
    let swapped = multi_join(&[
        MultiJoinInput::new(b, &["K"]),
        MultiJoinInput::new(a, &["K"]),
    ])
    .unwrap();
    assert_eq!(names(&*swapped), ["K", "B", "A"]);
    assert_eq!(
        rows(&*swapped),
        [
            "2,b2,a2",
            "3,b3,null",
            "1,b1,a1",
            "null,null,an",
            "4,null,a4"
        ]
    );
}

#[test]
fn nan_keys_are_one_key_and_the_zeros_are_one_key_shown_as_first_seen() {
    let doubles =
        |values: &[Option<f64>]| -> ArrayRef { Arc::new(Float64Array::from(values.to_vec())) };
    let other_nan = f64::from_bits(0xfff8_0000_0000_0001);
    let a = static_table(&[
        (
            "K",
            ColumnType::Double,
            doubles(&[Some(-0.0), Some(f64::NAN), None]),
        ),
        ("A", ColumnType::Long, longs([1, 2, 3])),
    ]);
    let b = static_table(&[
        (
            "K",
            ColumnType::Double,
            doubles(&[Some(other_nan), Some(0.0), None, Some(1.5)]),
        ),
        ("B", ColumnType::Long, longs([10, 20, 30, 40])),
    ]);
    let result = multi_join(&[
        MultiJoinInput::new(a, &["K"]),
        MultiJoinInput::new(b, &["K"]),
    ])
    .unwrap();
    assert_eq!(
        rows(&*result),
        ["-0.0,1,20", "NaN,2,10", "null,3,30", "1.5,null,40"]
    );
}

#[test]
fn keys_and_added_columns_can_be_renamed_and_chosen() {
    let a = tagged("A", &[Some(1), Some(2)], &["a1", "a2"]);
    let c = static_table(&[
        ("Key", ColumnType::Int, ints([2, 3])),
        ("C", ColumnType::Long, longs([20, 30])),
        ("Extra", ColumnType::String, strings(["x", "y"])),
    ]);
    let result = multi_join(&[
        input(&a, &["K"], &["Name=A"]),
        input(&c, &["K=Key"], &["C"]),
    ])
    .unwrap();
    assert_eq!(names(&*result), ["K", "Name", "C"]);
    assert_eq!(rows(&*result), ["1,a1,null", "2,a2,20", "3,null,30"]);

    // Without a list, every column that is not the source of a key is added, under its
    // own name: `Key` is not, `Extra` is.
    let all = multi_join(&[input(&a, &["K"], &[]), input(&c, &["K=Key"], &[])]).unwrap();
    assert_eq!(names(&*all), ["K", "A", "C", "Extra"]);

    // The first table names the key columns of the result.
    let named = multi_join(&[input(&c, &["Id=Key"], &["C"]), input(&a, &["Id=K"], &[])]).unwrap();
    assert_eq!(names(&*named), ["Id", "C", "A"]);
    assert_eq!(rows(&*named), ["2,20,a2", "3,30,null", "1,null,a1"]);
}

#[test]
fn several_key_columns_may_be_given_in_any_order() {
    let p = static_table(&[
        ("K", ColumnType::Int, ints([1, 1, 2])),
        ("J", ColumnType::String, strings(["x", "y", "x"])),
        ("P", ColumnType::Long, longs([10, 11, 20])),
    ]);
    let q = static_table(&[
        ("K", ColumnType::Int, ints([1, 2, 3])),
        ("J", ColumnType::String, strings(["y", "x", "z"])),
        ("Q", ColumnType::Long, longs([100, 200, 300])),
    ]);
    let result = multi_join(&[
        MultiJoinInput::new(p, &["K", "J"]),
        MultiJoinInput::new(q, &["J", "K"]),
    ])
    .unwrap();
    assert_eq!(names(&*result), ["K", "J", "P", "Q"]);
    assert_eq!(
        rows(&*result),
        ["1,x,10,null", "1,y,11,100", "2,x,20,200", "3,z,null,300"]
    );
}

#[test]
fn one_table_and_empty_tables_are_joined() {
    let a = tagged("A", &[Some(2), Some(1)], &["a2", "a1"]);
    let e = tagged("E", &[], &[]);
    let one = multi_join(&[MultiJoinInput::new(a.clone(), &["K"])]).unwrap();
    assert_eq!(rows(&*one), ["2,a2", "1,a1"]);
    let with_empty = multi_join(&[
        MultiJoinInput::new(e.clone(), &["K"]),
        MultiJoinInput::new(a, &["K"]),
    ])
    .unwrap();
    assert_eq!(names(&*with_empty), ["K", "E", "A"]);
    assert_eq!(rows(&*with_empty), ["2,null,a2", "1,null,a1"]);
    let empty = multi_join(&[MultiJoinInput::new(e, &["K"])]).unwrap();
    assert!(empty.row_set().is_empty());
}

#[test]
fn an_instant_key_joins_a_long_key_and_a_boolean_key_a_byte_key() {
    let instants: ArrayRef =
        Arc::new(TimestampNanosecondArray::from(vec![Some(5), None]).with_timezone("UTC"));
    let t = static_table(&[
        ("K", ColumnType::Instant, instants),
        ("A", ColumnType::Long, longs([1, 2])),
    ]);
    let l = static_table(&[
        (
            "K",
            ColumnType::Long,
            Arc::new(Int64Array::from(vec![None, Some(7), Some(5)])),
        ),
        ("B", ColumnType::Long, longs([10, 20, 30])),
    ]);
    let result = multi_join(&[
        MultiJoinInput::new(t.clone(), &["K"]),
        MultiJoinInput::new(l.clone(), &["K"]),
    ])
    .unwrap();
    assert_eq!(
        result.definition().columns()[0].column_type,
        ColumnType::Instant
    );
    assert_eq!(rows(&*result), ["5,1,30", "null,2,10", "7,null,20"]);
    let result = multi_join(&[
        MultiJoinInput::new(l, &["K"]),
        MultiJoinInput::new(t, &["K"]),
    ]);
    assert_eq!(
        rows(&*result.unwrap()),
        ["null,10,2", "7,20,null", "5,30,1"]
    );

    let booleans: ArrayRef = Arc::new(BooleanArray::from(vec![Some(true), None]));
    let z = static_table(&[
        ("K", ColumnType::Boolean, booleans),
        ("A", ColumnType::Long, longs([1, 2])),
    ]);
    let bytes: ArrayRef = Arc::new(Int8Array::from(vec![Some(0), Some(1), None, Some(3)]));
    let y = static_table(&[
        ("K", ColumnType::Byte, bytes),
        ("B", ColumnType::Long, longs([10, 20, 30, 40])),
    ]);
    let result = multi_join(&[
        MultiJoinInput::new(z.clone(), &["K"]),
        MultiJoinInput::new(y.clone(), &["K"]),
    ])
    .unwrap();
    // A byte that is neither 0 nor 1 is its own key, shown as `byteAsBoolean` shows it.
    assert_eq!(
        rows(&*result),
        ["true,1,20", "null,2,30", "false,null,10", "true,null,40"]
    );
    let result = multi_join(&[
        MultiJoinInput::new(y, &["K"]),
        MultiJoinInput::new(z, &["K"]),
    ]);
    assert_eq!(
        rows(&*result.unwrap()),
        ["0,10,null", "1,20,1", "null,30,2", "3,40,null"]
    );
}

// ---- Refusals ------------------------------------------------------------------------------

#[test]
fn a_join_of_no_tables_is_refused() {
    assert_eq!(refusal(&[]), MultiJoinError::NoInputs);
    assert_eq!(
        MultiJoinError::NoInputs.to_string(),
        "At least one table must be included in MultiJoinTable."
    );
}

#[test]
fn two_rows_of_one_key_in_a_table_are_refused() {
    let dup = tagged("A", &[Some(1), Some(1)], &["d1", "d2"]);
    let b = tagged("B", &[Some(2)], &["b2"]);
    let error = refusal(&[
        MultiJoinInput::new(b.clone(), &["K"]),
        MultiJoinInput::new(dup.clone(), &["K"]),
    ]);
    assert_eq!(
        error,
        MultiJoinError::DuplicateKey {
            key: "1".to_string(),
            table: 1
        }
    );
    assert_eq!(error.to_string(), "Duplicate key found for 1 in table 1.");
    // Also alone, and for the null key.
    assert!(matches!(
        refusal(&[MultiJoinInput::new(dup, &["K"])]),
        MultiJoinError::DuplicateKey { table: 0, .. }
    ));
    let nulls = tagged("A", &[None, Some(1), None], &["x", "y", "z"]);
    assert_eq!(
        refusal(&[MultiJoinInput::new(nulls, &["K"])]),
        MultiJoinError::DuplicateKey {
            key: "null".to_string(),
            table: 0
        }
    );
}

#[test]
fn an_added_column_may_come_from_one_table_only_and_may_not_be_a_key() {
    let a = tagged("A", &[Some(1)], &["a1"]);
    let again = tagged("A", &[Some(2)], &["a2"]);
    let error = refusal(&[
        MultiJoinInput::new(a.clone(), &["K"]),
        MultiJoinInput::new(again.clone(), &["K"]),
    ]);
    assert_eq!(
        error,
        MultiJoinError::ColumnConflict {
            column: "A".to_string(),
            earlier: Some(0),
            table: 1
        }
    );
    assert_eq!(error.to_string(), "Column A defined in table 0 and table 1");

    let error = refusal(&[input(&a, &["K"], &["K=A"])]);
    assert_eq!(
        error.to_string(),
        "Column K defined in table key columns and table 0"
    );
    let error = refusal(&[input(&a, &["K"], &["X=A", "X=K"])]);
    assert_eq!(error.to_string(), "Column X defined in table 0 and table 0");
    // Renamed apart, the two tables join.
    let joined = multi_join(&[input(&a, &["K"], &[]), input(&again, &["K"], &["A2=A"])]);
    assert_eq!(rows(&*joined.unwrap()), ["1,a1,null", "2,null,a2"]);
}

#[test]
fn every_table_must_name_the_key_columns_of_the_first() {
    let a = tagged("A", &[Some(1)], &["a1"]);
    let b = tagged("B", &[Some(1)], &["b1"]);
    let error = refusal(&[input(&a, &["K"], &[]), input(&b, &["J=K"], &[])]);
    assert_eq!(
        error,
        MultiJoinError::KeyColumnMismatch {
            table: 1,
            first: vec!["K".to_string()],
            this: vec!["J".to_string()]
        }
    );
    // A different number of key columns is such a mismatch.
    assert!(matches!(
        refusal(&[input(&a, &["K"], &[]), input(&b, &["K", "B"], &[])]),
        MultiJoinError::KeyColumnMismatch { table: 1, .. }
    ));
    assert!(matches!(
        refusal(&[input(&a, &["K"], &[]), input(&b, &[], &[])]),
        MultiJoinError::KeyColumnMismatch { table: 1, .. }
    ));
}

#[test]
fn key_columns_must_have_the_types_of_the_first_table() {
    let a = tagged("A", &[Some(1)], &["a1"]);
    let s = static_table(&[
        ("K", ColumnType::String, strings(["1"])),
        ("B", ColumnType::Long, longs([1])),
    ]);
    assert_eq!(
        refusal(&[
            MultiJoinInput::new(a, &["K"]),
            MultiJoinInput::new(s, &["K"])
        ]),
        MultiJoinError::KeyTypeMismatch {
            table: 1,
            first: vec![ColumnType::Int],
            this: vec![ColumnType::String]
        }
    );
}

/// Java compares the key types of a table with the first table's as sets, so the same
/// types in another order pass, and reading a row of the table then fails.
#[test]
fn key_types_in_another_order_are_refused_when_the_table_has_a_row() {
    let p = |rows: usize| {
        static_table(&[
            ("K", ColumnType::Int, ints(0..rows as i32)),
            (
                "J",
                ColumnType::String,
                strings(["x"].into_iter().take(rows)),
            ),
        ])
    };
    let q = |rows: usize| {
        static_table(&[
            (
                "K",
                ColumnType::String,
                strings(["x"].into_iter().take(rows)),
            ),
            ("J", ColumnType::Int, ints(0..rows as i32)),
            ("Q", ColumnType::Long, longs(0..rows as i64)),
        ])
    };
    let join = |p: Arc<dyn Table>, q: Arc<dyn Table>| {
        multi_join(&[input(&p, &["K", "J"], &[]), input(&q, &["K", "J"], &[])])
    };
    assert_eq!(
        join(p(1), q(1)).err(),
        Some(MultiJoinError::KeyTypeOrder {
            table: 1,
            column: "K".to_string(),
            first: ColumnType::Int,
            this: ColumnType::String
        })
    );
    let result = join(p(1), q(0)).unwrap();
    assert_eq!(names(&*result), ["K", "J", "Q"]);
    assert_eq!(rows(&*result), ["0,x,null"]);
}

#[test]
fn unknown_columns_are_refused_a_key_first_and_an_added_column_after_duplicates() {
    let a = tagged("A", &[Some(1)], &["a1"]);
    let dup = tagged("B", &[Some(1), Some(1)], &["d1", "d2"]);
    let unknown_key = MultiJoinError::NoSuchColumn {
        table: 1,
        column: "Nope".to_string(),
    };
    // Before anything else about the tables.
    assert_eq!(
        refusal(&[input(&dup, &["J=K"], &[]), input(&a, &["K=Nope"], &[])]),
        unknown_key
    );
    assert_eq!(
        refusal(&[input(&a, &["K"], &["X=Nope"])]),
        MultiJoinError::NoSuchColumn {
            table: 0,
            column: "Nope".to_string()
        }
    );
    // Java looks for the added columns once the keys are read.
    assert!(matches!(
        refusal(&[input(&a, &["K"], &["X=Nope"]), input(&dup, &["K"], &[])]),
        MultiJoinError::DuplicateKey { table: 1, .. }
    ));
}

#[test]
fn tables_of_two_update_graphs_are_refused() {
    let columns: Columns = &[
        ("K", ColumnType::Int, ints([1])),
        ("A", ColumnType::String, strings(["a"])),
    ];
    let other: Columns = &[
        ("K", ColumnType::Int, ints([1])),
        ("B", ColumnType::String, strings(["b"])),
    ];
    let a: Arc<dyn Table> = scripted(&graph(), [0], columns).table();
    let b: Arc<dyn Table> = scripted(&graph(), [0], other).table();
    assert_eq!(
        refusal(&[
            MultiJoinInput::new(a.clone(), &["K"]),
            MultiJoinInput::new(b, &["K"])
        ]),
        MultiJoinError::UpdateGraphConflict
    );
    // A static table has no graph to conflict with.
    let fixed = static_table(other);
    let joined = multi_join(&[
        MultiJoinInput::new(a, &["K"]),
        MultiJoinInput::new(fixed, &["K"]),
    ]);
    assert!(joined.unwrap().is_refreshing());
}

// ---- Updates -------------------------------------------------------------------------------

enum Side<'a> {
    Fixed(Columns<'a>),
    Live(&'a [RowKey], Columns<'a>),
}

/// A join with a recorder and a consistency checker on its result.
struct Join {
    graph: Arc<dyn UpdateGraph>,
    scripts: Vec<Option<Arc<ScriptedTable>>>,
    tables: Vec<Arc<dyn Table>>,
    result: Arc<dyn Table>,
    recorder: Arc<Recorder>,
    checker: Arc<ConsistencyChecker>,
}

impl Join {
    fn new(sides: Vec<Side>, inputs: impl Fn(&[Arc<dyn Table>]) -> Vec<MultiJoinInput>) -> Self {
        let graph = graph();
        let mut scripts = Vec::new();
        let mut tables: Vec<Arc<dyn Table>> = Vec::new();
        for side in sides {
            match side {
                Side::Fixed(columns) => {
                    scripts.push(None);
                    tables.push(static_table(columns));
                }
                Side::Live(keys, columns) => {
                    let script = scripted(&graph, keys.iter().copied(), columns);
                    tables.push(script.table());
                    scripts.push(Some(script));
                }
            }
        }
        let result = multi_join(&inputs(&tables)).expect("a join");
        let recorder = Recorder::attach(result.clone());
        let checker = ConsistencyChecker::attach(result.clone());
        checker.assert_consistent();
        Self {
            graph,
            scripts,
            tables,
            result,
            recorder,
            checker,
        }
    }

    fn run(&self, changes: Vec<(usize, TableChange)>) -> Option<Result<TableUpdate, TableFailure>> {
        let before = self.recorder.events().len();
        for (table, change) in changes {
            let script = self.scripts[table].as_ref().expect("a live table");
            script.push(change);
        }
        self.graph.run_cycle();
        let mut events = self.recorder.events().split_off(before);
        assert!(events.len() <= 1, "more than one event in a cycle");
        events.pop()
    }

    /// Applies the changes in one cycle and returns the update the result delivered.
    fn step(&self, changes: Vec<(usize, TableChange)>) -> Option<TableUpdate> {
        let event = self.run(changes);
        self.checker.assert_consistent();
        event.map(|event| event.expect("an update"))
    }

    fn change(&self, table: usize, change: TableChange) -> Option<TableUpdate> {
        self.step(vec![(table, change)])
    }

    /// Applies the changes in one cycle and returns the failure the result delivered.
    fn fail(&self, changes: Vec<(usize, TableChange)>) -> String {
        let event = self.run(changes).expect("a failure");
        assert!(self.result.is_failed());
        event.expect_err("a failure").message
    }

    fn rows(&self) -> Vec<String> {
        rows(&*self.result)
    }
}

const A_COLUMNS: usize = 1;
const B_COLUMN: usize = 2;
const X_COLUMN: usize = 3;

fn a_row(key: i32, tag: &str) -> Vec<ArrayRef> {
    vec![ints([key]), strings([tag])]
}

fn b_row(key: i32, value: i64, tag: &str) -> Vec<ArrayRef> {
    vec![ints([key]), longs([value]), strings([tag]), ints([0])]
}

fn a_columns() -> [(&'static str, ColumnType, ArrayRef); 2] {
    [
        ("K", ColumnType::Int, ints([1, 2, 3])),
        ("A", ColumnType::String, strings(["a1", "a2", "a3"])),
    ]
}

fn b_columns() -> [(&'static str, ColumnType, ArrayRef); 4] {
    [
        ("K", ColumnType::Int, ints([2, 4])),
        ("B", ColumnType::Long, longs([200, 400])),
        ("X", ColumnType::String, strings(["x", "y"])),
        ("Unseen", ColumnType::Int, ints([0, 0])),
    ]
}

fn a_and_b(tables: &[Arc<dyn Table>]) -> Vec<MultiJoinInput> {
    vec![
        input(&tables[0], &["K"], &[]),
        input(&tables[1], &["K"], &["B", "X"]),
    ]
}

/// `A` (`K`, `A`) at rows 10, 20, 30 with keys 1, 2, 3 and `B` (`K`, `B`, `X`, `Unseen`) at
/// rows 5, 6 with keys 2, 4, of which the join shows `B` and `X`. Both change.
fn both_live() -> Join {
    let join = Join::new(
        vec![
            Side::Live(&[10, 20, 30], &a_columns()),
            Side::Live(&[5, 6], &b_columns()),
        ],
        a_and_b,
    );
    assert_eq!(
        join.rows(),
        [
            "1,a1,null,null",
            "2,a2,200,x",
            "3,a3,null,null",
            "4,null,400,y"
        ]
    );
    assert!(join.result.is_refreshing() && !join.result.is_flat());
    join
}

fn only(update: &TableUpdate) -> (Vec<RowKey>, Vec<RowKey>, Vec<RowKey>) {
    assert!(update.shifted.is_empty());
    (
        keys(&update.added),
        keys(&update.removed),
        keys(&update.modified),
    )
}

#[test]
fn a_row_of_a_known_key_fills_its_row_and_a_new_key_is_appended() {
    let join = both_live();
    let update = join
        .change(0, TableChange::new().add([40], a_row(4, "a4")))
        .unwrap();
    assert_eq!(only(&update), (vec![], vec![], vec![3]));
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([A_COLUMNS]));
    assert_eq!(join.rows()[3], "4,a4,400,y");

    let update = join
        .change(1, TableChange::new().add([7], b_row(9, 900, "z")))
        .unwrap();
    assert_eq!(only(&update), (vec![4], vec![], vec![]));
    assert_eq!(join.rows()[4], "9,null,900,z");
    assert_eq!(keys(&join.result.row_set()), [0, 1, 2, 3, 4]);
}

#[test]
fn a_row_stays_while_any_table_has_its_key_and_comes_back_at_its_old_place() {
    let join = both_live();
    let update = join.change(1, TableChange::new().remove([5])).unwrap();
    assert_eq!(only(&update), (vec![], vec![], vec![1]));
    assert_eq!(
        update.modified_columns,
        ModifiedColumnSet::of([B_COLUMN, X_COLUMN])
    );
    assert_eq!(join.rows()[1], "2,a2,null,null");

    // No table has key 4 any more.
    let update = join.change(1, TableChange::new().remove([6])).unwrap();
    assert_eq!(only(&update), (vec![], vec![3], vec![]));
    assert_eq!(keys(&join.result.row_set()), [0, 1, 2]);

    let update = join
        .change(1, TableChange::new().add([8], b_row(7, 700, "s")))
        .unwrap();
    assert_eq!(only(&update), (vec![4], vec![], vec![]));
    // Key 4 again, from the other table: before key 7, where it was.
    let update = join
        .change(0, TableChange::new().add([50], a_row(4, "back")))
        .unwrap();
    assert_eq!(only(&update), (vec![3], vec![], vec![]));
    assert_eq!(
        join.rows(),
        [
            "1,a1,null,null",
            "2,a2,null,null",
            "3,a3,null,null",
            "4,back,null,null",
            "7,null,700,s"
        ]
    );
}

#[test]
fn a_modify_reports_the_shown_columns_it_changed_and_nothing_for_others() {
    let join = both_live();
    let update = join
        .change(1, TableChange::new().modify([5], [(1, longs([201]))]))
        .unwrap();
    assert_eq!(only(&update), (vec![], vec![], vec![1]));
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([B_COLUMN]));
    assert_eq!(join.rows()[1], "2,a2,201,x");

    assert_eq!(
        join.change(1, TableChange::new().modify([5], [(3, ints([9]))])),
        None
    );
    let update = join
        .change(0, TableChange::new().modify([10], [(1, strings(["z"]))]))
        .unwrap();
    assert_eq!(only(&update), (vec![], vec![], vec![0]));
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([A_COLUMNS]));
}

#[test]
fn a_key_change_moves_a_row_between_the_rows_of_its_old_and_new_key() {
    let join = both_live();
    // Key 1 is left without a row; key 4 gets the row.
    let update = join
        .change(0, TableChange::new().modify([10], [(0, ints([4]))]))
        .unwrap();
    assert_eq!(only(&update), (vec![], vec![0], vec![3]));
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([A_COLUMNS]));
    assert_eq!(join.rows(), ["2,a2,200,x", "3,a3,null,null", "4,a1,400,y"]);

    // To a key no table has had: key 2 keeps its other row.
    let update = join
        .change(0, TableChange::new().modify([20], [(0, ints([8]))]))
        .unwrap();
    assert_eq!(only(&update), (vec![4], vec![], vec![1]));
    assert_eq!(join.rows()[0], "2,null,200,x");
    assert_eq!(join.rows()[3], "8,a2,null,null");

    // Two rows swap keys in one cycle: no key is ever duplicated.
    let update = join
        .change(0, TableChange::new().modify([20, 30], [(0, ints([3, 8]))]))
        .unwrap();
    assert_eq!(only(&update), (vec![], vec![], vec![2, 4]));
    assert_eq!(
        join.rows(),
        [
            "2,null,200,x",
            "3,a2,null,null",
            "4,a1,400,y",
            "8,a3,null,null"
        ]
    );
}

#[test]
fn a_shift_changes_nothing_and_later_changes_reach_the_shifted_rows() {
    let join = both_live();
    assert_eq!(join.change(1, TableChange::new().shift(5, 6, 10)), None);
    assert_eq!(join.rows()[1], "2,a2,200,x");
    let update = join
        .change(1, TableChange::new().modify([15], [(1, longs([201]))]))
        .unwrap();
    assert_eq!(only(&update), (vec![], vec![], vec![1]));
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([B_COLUMN]));
    assert_eq!(join.rows()[1], "2,a2,201,x");

    // A row shifted and modified in one cycle reports every column of its table, as Java
    // does for a row that is not where it was.
    let change = TableChange::new()
        .shift(15, 16, 4)
        .modify([20], [(2, strings(["w"]))]);
    let update = join.change(1, change).unwrap();
    assert_eq!(only(&update), (vec![], vec![], vec![3]));
    assert_eq!(
        update.modified_columns,
        ModifiedColumnSet::of([B_COLUMN, X_COLUMN])
    );
    assert_eq!(join.rows()[3], "4,null,400,w");

    // Removes, shifts and a key change together.
    let change = TableChange::new()
        .remove([19])
        .shift(20, 20, -19)
        .modify([1], [(0, ints([3]))]);
    let update = join.change(1, change).unwrap();
    assert_eq!(only(&update), (vec![], vec![3], vec![1, 2]));
    assert_eq!(
        join.rows(),
        ["1,a1,null,null", "2,a2,null,null", "3,a3,400,w"]
    );
}

#[test]
fn a_row_replaced_by_another_of_its_key_in_one_cycle_is_modified() {
    let join = both_live();
    let change = TableChange::new().remove([5]).add([9], b_row(2, 222, "q"));
    let update = join.change(1, change).unwrap();
    assert_eq!(only(&update), (vec![], vec![], vec![1]));
    assert_eq!(
        update.modified_columns,
        ModifiedColumnSet::of([B_COLUMN, X_COLUMN])
    );
    assert_eq!(join.rows()[1], "2,a2,222,q");
    // Also at the row key of the row it replaces.
    let change = TableChange::new().remove([9]).add([9], b_row(2, 333, "r"));
    let update = join.change(1, change).unwrap();
    assert_eq!(only(&update), (vec![], vec![], vec![1]));
    assert_eq!(join.rows()[1], "2,a2,333,r");
}

#[test]
fn a_second_row_of_a_key_fails_the_result() {
    let join = both_live();
    let message = join.fail(vec![(0, TableChange::new().add([40], a_row(2, "dup")))]);
    assert_eq!(message, "Duplicate key found for 2 in table 0.");

    let join = both_live();
    let message = join.fail(vec![(1, TableChange::new().modify([6], [(0, ints([2]))]))]);
    assert_eq!(message, "Duplicate key found for 2 in table 1.");
}

#[test]
fn changes_of_several_tables_in_one_cycle_make_one_update() {
    let join = both_live();
    let update = join
        .step(vec![
            (0, TableChange::new().add([40], a_row(9, "a9"))),
            (1, TableChange::new().add([7], b_row(9, 900, "z"))),
        ])
        .unwrap();
    assert_eq!(only(&update), (vec![4], vec![], vec![]));
    assert_eq!(join.rows()[4], "9,a9,900,z");

    // Each table changes another row of the result.
    let update = join
        .step(vec![
            (0, TableChange::new().modify([10], [(1, strings(["n"]))])),
            (1, TableChange::new().remove([5])),
        ])
        .unwrap();
    assert_eq!(only(&update), (vec![], vec![], vec![0, 1]));
    assert_eq!(
        update.modified_columns,
        ModifiedColumnSet::of([A_COLUMNS, B_COLUMN, X_COLUMN])
    );
}

/// The tables in turn, and of one table the rows whose key changed before the added rows.
#[test]
fn new_keys_of_one_cycle_are_ordered_by_table_then_rekeyed_rows_then_added_rows() {
    let join = both_live();
    let update = join
        .step(vec![
            (
                0,
                TableChange::new()
                    .add([5], a_row(12, "a12"))
                    .modify([10], [(0, ints([11]))]),
            ),
            (
                1,
                TableChange::new().add(
                    [1, 2],
                    vec![
                        ints([14, 13]),
                        longs([1, 2]),
                        strings(["p", "q"]),
                        ints([0, 0]),
                    ],
                ),
            ),
        ])
        .unwrap();
    assert_eq!(only(&update), (vec![4, 5, 6, 7], vec![0], vec![]));
    let rows = join.rows();
    assert_eq!(
        rows[3..],
        [
            "11,a1,null,null",
            "12,a12,null,null",
            "14,null,1,p",
            "13,null,2,q"
        ]
    );
}

#[test]
fn a_static_table_keeps_its_rows_while_the_others_change() {
    let join = Join::new(
        vec![
            Side::Live(&[10, 20, 30], &a_columns()),
            Side::Fixed(&b_columns()),
        ],
        a_and_b,
    );
    assert!(join.result.is_refreshing());
    // Key 2 loses its row of `A` and keeps its row of `B`; key 1 has no other row.
    let update = join.change(0, TableChange::new().remove([10, 20])).unwrap();
    assert_eq!(only(&update), (vec![], vec![0], vec![1]));
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([A_COLUMNS]));
    assert_eq!(
        join.rows(),
        ["2,null,200,x", "3,a3,null,null", "4,null,400,y"]
    );

    let all_static = multi_join(&a_and_b(&[
        frozen(&*join.tables[0]),
        join.tables[1].clone(),
    ]));
    assert!(!all_static.unwrap().is_refreshing());
}

#[test]
fn a_table_joined_twice_is_read_twice() {
    let join = Join::new(vec![Side::Live(&[10, 20, 30], &a_columns())], |tables| {
        vec![
            input(&tables[0], &["K"], &[]),
            input(&tables[0], &["K"], &["Again=A"]),
        ]
    });
    assert_eq!(join.rows(), ["1,a1,a1", "2,a2,a2", "3,a3,a3"]);
    let change = TableChange::new()
        .remove([10])
        .add([40], a_row(7, "a7"))
        .modify([20], [(1, strings(["n"]))]);
    let update = join.change(0, change).unwrap();
    assert_eq!(only(&update), (vec![3], vec![0], vec![1]));
    assert_eq!(join.rows(), ["2,n,n", "3,a3,a3", "7,a7,a7"]);
}

#[test]
fn a_table_that_fails_fails_the_result() {
    let join = both_live();
    join.scripts[1].as_ref().unwrap().push_failure("boom");
    let event = join.run(Vec::new()).expect("a failure");
    assert_eq!(event.expect_err("a failure").message, "boom");
    assert!(join.result.is_failed());
    // A join of a failed table is refused.
    assert!(matches!(
        multi_join(&a_and_b(&join.tables)),
        Err(MultiJoinError::Table(_))
    ));
}

/// A refreshing table whose key types are the first table's in another order passes while
/// it is empty, as in Java, and fails the result with its first row.
#[test]
fn key_types_in_another_order_fail_the_result_with_the_first_row() {
    let p: Columns = &[
        ("K", ColumnType::Int, ints([1])),
        ("J", ColumnType::String, strings(["x"])),
    ];
    let q: Columns = &[
        ("K", ColumnType::String, strings([])),
        ("J", ColumnType::Int, ints([])),
        ("Q", ColumnType::Long, longs([])),
    ];
    let join = Join::new(vec![Side::Fixed(p), Side::Live(&[], q)], |tables| {
        vec![
            input(&tables[0], &["K", "J"], &[]),
            input(&tables[1], &["K", "J"], &[]),
        ]
    });
    assert_eq!(join.rows(), ["1,x,null"]);
    let row = vec![strings(["x"]), ints([1]), longs([5])];
    let message = join.fail(vec![(1, TableChange::new().add([0], row))]);
    assert!(message.starts_with("Key column K of table 1"), "{message}");
}

// ---- Without key columns -------------------------------------------------------------------

fn no_keys(tables: &[Arc<dyn Table>]) -> Vec<MultiJoinInput> {
    vec![
        input(&tables[0], &[], &[]),
        input(&tables[1], &[], &["B", "X"]),
    ]
}

#[test]
fn without_key_columns_the_result_is_the_one_row_of_each_table() {
    let a = tagged("A", &[Some(1)], &["a1"]);
    let b = tagged("B", &[Some(2)], &["b2"]);
    let e = tagged("E", &[], &[]);
    let join = |tables: &[&Arc<dyn Table>]| {
        let inputs: Vec<MultiJoinInput> = tables
            .iter()
            .enumerate()
            .map(|(index, table)| {
                let key = format!("K{index}=K");
                input(
                    table,
                    &[],
                    &[key.as_str(), table.definition().columns()[1].name.as_str()],
                )
            })
            .collect();
        multi_join(&inputs)
    };
    let result = join(&[&a, &e, &b]).unwrap();
    assert_eq!(names(&*result), ["K0", "A", "K1", "E", "K2", "B"]);
    assert_eq!(rows(&*result), ["1,a1,null,null,2,b2"]);
    assert_eq!(keys(&result.row_set()), [0]);
    assert!(!result.is_flat());
    // No table has a row: no row.
    let f = tagged("F", &[], &[]);
    assert!(join(&[&e, &f]).unwrap().row_set().is_empty());

    let two = tagged("B", &[Some(2), Some(3)], &["b2", "b3"]);
    let error = join(&[&a, &two]).err().unwrap();
    assert_eq!(
        error,
        MultiJoinError::ZeroKeyRows {
            table: 1,
            in_cycle: false
        }
    );
    assert_eq!(
        error.to_string(),
        "Duplicate rows for table 1 on zero-key multiJoin."
    );
    // Java counts the rows of a table before it looks for its added columns, and the
    // tables in turn.
    assert!(matches!(
        multi_join(&[input(&two, &[], &["Nope"]), input(&a, &[], &[])]),
        Err(MultiJoinError::ZeroKeyRows { table: 0, .. })
    ));
    assert!(matches!(
        multi_join(&[input(&a, &[], &["Nope"]), input(&two, &[], &[])]),
        Err(MultiJoinError::NoSuchColumn { table: 0, .. })
    ));
}

#[test]
fn without_key_columns_the_row_follows_the_tables() {
    let empty_a: Columns = &[
        ("K", ColumnType::Int, ints([])),
        ("A", ColumnType::String, strings([])),
    ];
    let one_b = [
        ("K2", ColumnType::Int, ints([2])),
        ("B", ColumnType::Long, longs([200])),
        ("X", ColumnType::String, strings(["x"])),
        ("Unseen", ColumnType::Int, ints([0])),
    ];
    let join = Join::new(
        vec![Side::Live(&[], empty_a), Side::Live(&[5], &one_b)],
        no_keys,
    );
    assert_eq!(names(&*join.result), ["K", "A", "B", "X"]);
    assert_eq!(join.rows(), ["null,null,200,x"]);

    // The other table's row arrives: the row is modified, in every column of that table.
    let update = join
        .change(0, TableChange::new().add([3], a_row(1, "a1")))
        .unwrap();
    assert_eq!(only(&update), (vec![], vec![], vec![0]));
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([0, 1]));
    assert_eq!(join.rows(), ["1,a1,200,x"]);

    let update = join
        .change(1, TableChange::new().modify([5], [(2, strings(["w"]))]))
        .unwrap();
    assert_eq!(only(&update), (vec![], vec![], vec![0]));
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([3]));
    assert_eq!(
        join.change(1, TableChange::new().modify([5], [(3, ints([4]))])),
        None
    );
    // A shifted row is still the table's row.
    assert_eq!(join.change(1, TableChange::new().shift(5, 5, 4)), None);
    assert_eq!(join.rows(), ["1,a1,200,w"]);
    let change = TableChange::new()
        .shift(9, 9, -8)
        .modify([1], [(1, longs([201]))]);
    let update = join.change(1, change).unwrap();
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([2]));
    assert_eq!(join.rows(), ["1,a1,201,w"]);

    // The row goes with the last row of the tables and comes back with the next.
    let update = join.change(1, TableChange::new().remove([1])).unwrap();
    assert_eq!(only(&update), (vec![], vec![], vec![0]));
    assert_eq!(join.rows(), ["1,a1,null,null"]);
    let update = join.change(0, TableChange::new().remove([3])).unwrap();
    assert_eq!(only(&update), (vec![], vec![0], vec![]));
    assert!(join.rows().is_empty());
    let update = join
        .change(0, TableChange::new().add([8], a_row(7, "a7")))
        .unwrap();
    assert_eq!(only(&update), (vec![0], vec![], vec![]));
    assert_eq!(join.rows(), ["7,a7,null,null"]);

    // A table's one row may be replaced, and a second row fails the result.
    let change = TableChange::new().remove([8]).add([9], a_row(8, "a8"));
    let update = join.change(0, change).unwrap();
    assert_eq!(only(&update), (vec![], vec![], vec![0]));
    assert_eq!(join.rows(), ["8,a8,null,null"]);
    let message = join.fail(vec![(0, TableChange::new().add([12], a_row(9, "a9")))]);
    assert_eq!(message, "Multiple rows in 0 for zero-key multiJoin.");
}
