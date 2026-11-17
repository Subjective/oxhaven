//! Unit tests of the joins: the static result, then each kind of change of either table.

use super::{exact_join, natural_join, JoinAddition, JoinError, JoinMatch, NaturalJoinType};
use crate::ops::sort::testing::{
    blink_table, cells, frozen, graph, keys, scripted, shifts, static_table, Columns,
};
use crate::table::append_only_input_table;
use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
    Int8Array, RecordBatch, StringArray, TimestampNanosecondArray, UInt16Array,
};
use arrow::datatypes::{Field, Schema};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use oxhaven_testkit::{
    definition, ints, longs, strings, ConsistencyChecker, Recorder, ReferenceTable, ScriptedTable,
    TableChange,
};
use std::sync::Arc;

use NaturalJoinType::{ErrorOnDuplicate, ExactlyOneMatch, FirstMatch, LastMatch};

fn opt_ints(values: &[Option<i32>]) -> ArrayRef {
    Arc::new(Int32Array::from(values.to_vec()))
}

fn on(columns: &[&str]) -> Vec<JoinMatch> {
    columns
        .iter()
        .map(|column| JoinMatch::same(*column))
        .collect()
}

fn add(columns: &[&str]) -> Vec<JoinAddition> {
    let columns = columns.iter();
    columns.map(|column| JoinAddition::same(*column)).collect()
}

/// The values of the column `name`, in row order, as text.
fn column(table: &dyn Table, name: &str) -> Vec<String> {
    let index = table.definition().index_of(name);
    cells(table, index.unwrap_or_else(|| panic!("no column {name}")))
}

fn names(table: &dyn Table) -> Vec<&str> {
    let columns = table.definition().columns().iter();
    columns.map(|column| column.name.as_str()).collect()
}

/// A left table `K` (int), `Tag` and a right table `K` (int), `V` (long).
fn int_tables(left: &[Option<i32>], right: &[Option<i32>]) -> (Arc<dyn Table>, Arc<dyn Table>) {
    let tags: Vec<String> = (0..left.len()).map(|i| format!("l{i}")).collect();
    let left = static_table(&[
        ("K", ColumnType::Int, opt_ints(left)),
        (
            "Tag",
            ColumnType::String,
            strings(tags.iter().map(String::as_str)),
        ),
    ]);
    let values = longs((0..right.len() as i64).map(|i| 100 + i));
    let right = static_table(&[
        ("K", ColumnType::Int, opt_ints(right)),
        ("V", ColumnType::Long, values),
    ]);
    (left, right)
}

fn join_v(
    left: &Arc<dyn Table>,
    right: &Arc<dyn Table>,
    join_type: NaturalJoinType,
) -> Result<Vec<String>, JoinError> {
    let result = natural_join(left, right, &on(&["K"]), &add(&["V"]), join_type)?;
    Ok(column(&*result, "V"))
}

// ---- The static result ---------------------------------------------------------------------

#[test]
fn left_rows_keep_their_order_and_keys_and_get_the_matching_right_row() {
    let left: Arc<dyn Table> = ReferenceTable::new_static(
        definition(&[("K", ColumnType::Int), ("Tag", ColumnType::String)]),
        RowSet::from_keys([3, 7, 20, 21]),
        vec![ints([2, 9, 1, 2]), strings(["a", "b", "c", "d"])],
    );
    let right = static_table(&[
        ("K", ColumnType::Int, ints([1, 2, 5])),
        ("V", ColumnType::Long, longs([10, 20, 50])),
        ("W", ColumnType::String, strings(["x", "y", "z"])),
    ]);
    let result = natural_join(&left, &right, &on(&["K"]), &[], ErrorOnDuplicate).unwrap();
    assert_eq!(names(&*result), ["K", "Tag", "V", "W"]);
    assert_eq!(keys(&result.row_set()), [3, 7, 20, 21]);
    assert_eq!(column(&*result, "Tag"), ["a", "b", "c", "d"]);
    assert_eq!(column(&*result, "V"), ["20", "null", "10", "20"]);
    assert_eq!(column(&*result, "W"), ["y", "null", "x", "y"]);
    assert!(!result.is_refreshing() && !result.is_flat());
}

#[test]
fn a_null_key_matches_a_null_key() {
    let (left, right) = int_tables(&[None, Some(1), None], &[Some(1), None]);
    assert_eq!(
        join_v(&left, &right, ErrorOnDuplicate).unwrap(),
        ["101", "100", "101"]
    );
}

#[test]
fn nan_matches_nan_and_negative_zero_matches_zero() {
    let other_nan = f64::from_bits(0xfff8_0000_0000_0001);
    let left = static_table(&[(
        "K",
        ColumnType::Double,
        Arc::new(Float64Array::from(vec![
            Some(other_nan),
            Some(-0.0),
            Some(0.0),
            None,
            Some(f64::INFINITY),
            Some(1.5),
        ])),
    )]);
    let right = static_table(&[
        (
            "K",
            ColumnType::Double,
            Arc::new(Float64Array::from(vec![
                Some(0.0),
                Some(f64::NAN),
                None,
                Some(f64::NEG_INFINITY),
            ])),
        ),
        ("V", ColumnType::Long, longs([1, 2, 3, 4])),
    ]);
    assert_eq!(
        join_v(&left, &right, ErrorOnDuplicate).unwrap(),
        ["2", "1", "1", "3", "null", "null"]
    );
    // The two zeros are one key, so both of them on the right are duplicates.
    let zeros = static_table(&[
        (
            "K",
            ColumnType::Double,
            Arc::new(Float64Array::from(vec![Some(0.0), Some(-0.0)])),
        ),
        ("V", ColumnType::Long, longs([1, 2])),
    ]);
    assert!(matches!(
        join_v(&left, &zeros, ErrorOnDuplicate),
        Err(JoinError::DuplicateRightKey(_))
    ));
    assert_eq!(
        join_v(&left, &zeros, LastMatch).unwrap(),
        ["null", "2", "2", "null", "null", "null"]
    );
}

/// For every type a match column can have: a left table of two values and a null, and a
/// right table of the null, the second value and a value of its own.
#[test]
fn every_column_type_is_a_key() {
    let instants = |values: Vec<Option<i64>>| -> ArrayRef {
        Arc::new(TimestampNanosecondArray::from(values).with_timezone("UTC"))
    };
    let cases: Vec<(ColumnType, ArrayRef, ArrayRef)> = vec![
        (
            ColumnType::Boolean,
            Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
            Arc::new(BooleanArray::from(vec![None, Some(false), Some(false)])),
        ),
        (
            ColumnType::Byte,
            Arc::new(Int8Array::from(vec![Some(1), Some(-127), None])),
            Arc::new(Int8Array::from(vec![None, Some(-127), Some(3)])),
        ),
        (
            ColumnType::Char,
            Arc::new(UInt16Array::from(vec![Some(97), Some(0xFFFE), None])),
            Arc::new(UInt16Array::from(vec![None, Some(0xFFFE), Some(98)])),
        ),
        (
            ColumnType::Short,
            Arc::new(Int16Array::from(vec![Some(1), Some(-32767), None])),
            Arc::new(Int16Array::from(vec![None, Some(-32767), Some(3)])),
        ),
        (
            ColumnType::Int,
            opt_ints(&[Some(1), Some(i32::MAX), None]),
            opt_ints(&[None, Some(i32::MAX), Some(3)]),
        ),
        (
            ColumnType::Long,
            Arc::new(Int64Array::from(vec![Some(1), Some(i64::MAX), None])),
            Arc::new(Int64Array::from(vec![None, Some(i64::MAX), Some(3)])),
        ),
        (
            ColumnType::Float,
            Arc::new(Float32Array::from(vec![Some(1.5), Some(f32::NAN), None])),
            Arc::new(Float32Array::from(vec![None, Some(f32::NAN), Some(3.0)])),
        ),
        (
            ColumnType::Double,
            Arc::new(Float64Array::from(vec![Some(1.5), Some(f64::NAN), None])),
            Arc::new(Float64Array::from(vec![None, Some(f64::NAN), Some(3.0)])),
        ),
        (
            ColumnType::String,
            Arc::new(StringArray::from(vec![Some("a"), Some(""), None])),
            Arc::new(StringArray::from(vec![None, Some(""), Some("c")])),
        ),
        (
            ColumnType::Instant,
            instants(vec![Some(1), Some(-5), None]),
            instants(vec![None, Some(-5), Some(3)]),
        ),
    ];
    for (column_type, left, right) in cases {
        let left = static_table(&[("K", column_type.clone(), left)]);
        let right = static_table(&[
            ("K", column_type.clone(), right),
            ("V", ColumnType::Long, longs([10, 20, 30])),
        ]);
        // The third right row repeats the second key only for `Boolean`.
        let join_type = if column_type == ColumnType::Boolean {
            FirstMatch
        } else {
            ErrorOnDuplicate
        };
        assert_eq!(
            join_v(&left, &right, join_type).unwrap(),
            ["null", "20", "10"],
            "{column_type:?}"
        );
    }
}

#[test]
fn every_match_column_must_be_equal() {
    let left = static_table(&[
        (
            "A",
            ColumnType::Int,
            opt_ints(&[Some(1), Some(1), None, Some(2)]),
        ),
        (
            "B",
            ColumnType::String,
            Arc::new(StringArray::from(vec![Some("x"), Some("y"), None, None])),
        ),
    ]);
    let right = static_table(&[
        (
            "B",
            ColumnType::String,
            Arc::new(StringArray::from(vec![Some("y"), None, Some("x"), None])),
        ),
        (
            "A",
            ColumnType::Int,
            opt_ints(&[Some(1), None, Some(2), Some(1)]),
        ),
        ("V", ColumnType::Long, longs([10, 20, 30, 40])),
    ]);
    let result = natural_join(&left, &right, &on(&["A", "B"]), &[], ErrorOnDuplicate).unwrap();
    assert_eq!(names(&*result), ["A", "B", "V"]);
    assert_eq!(column(&*result, "V"), ["null", "10", "20", "null"]);
    // The same pairs, with the right columns named in the other order.
    let pairs = [JoinMatch::new("B", "B"), JoinMatch::new("A", "A")];
    let result = natural_join(&left, &right, &pairs, &add(&["V"]), ErrorOnDuplicate).unwrap();
    assert_eq!(column(&*result, "V"), ["null", "10", "20", "null"]);
}

#[test]
fn duplicate_right_keys_fail_only_for_a_key_the_left_table_has() {
    let (left, right) = int_tables(
        &[Some(1), Some(4), None],
        &[Some(2), Some(1), Some(2), Some(7), Some(7)],
    );
    // Keys 2 and 7 are duplicated, and no left row has them.
    assert_eq!(
        join_v(&left, &right, ErrorOnDuplicate).unwrap(),
        ["101", "null", "null"]
    );
    let (left, _) = int_tables(&[Some(1), Some(2)], &[]);
    let error = join_v(&left, &right, ErrorOnDuplicate).unwrap_err();
    assert_eq!(
        error,
        JoinError::DuplicateRightKey("Natural Join found duplicate right key for 2".into())
    );
    assert_eq!(join_v(&left, &right, ExactlyOneMatch).unwrap_err(), error);
}

#[test]
fn first_and_last_match_take_the_first_and_last_right_row_in_table_order() {
    let (left, right) = int_tables(
        &[Some(2), None, Some(1), Some(9)],
        &[Some(2), Some(1), Some(2), None, Some(2), None],
    );
    assert_eq!(
        join_v(&left, &right, FirstMatch).unwrap(),
        ["100", "103", "101", "null"]
    );
    assert_eq!(
        join_v(&left, &right, LastMatch).unwrap(),
        ["104", "105", "101", "null"]
    );
}

#[test]
fn an_exact_join_needs_one_right_row_for_every_left_row() {
    let (left, right) = int_tables(&[Some(1), Some(2), Some(1)], &[Some(2), Some(1), Some(3)]);
    let result = exact_join(&left, &right, &on(&["K"]), &add(&["V"])).unwrap();
    assert_eq!(column(&*result, "V"), ["101", "100", "101"]);

    let (left, _) = int_tables(&[Some(1), Some(4)], &[]);
    assert_eq!(
        exact_join(&left, &right, &on(&["K"]), &add(&["V"])).err(),
        Some(JoinError::NoRightRow(
            "Tables don't have one-to-one mapping - no mappings for key 4.".into()
        ))
    );
    // A natural join of the same tables gives the row a null.
    assert_eq!(
        join_v(&left, &right, ErrorOnDuplicate).unwrap(),
        ["101", "null"]
    );
    // With several match columns the message lists the key.
    let pairs = [JoinMatch::same("K"), JoinMatch::new("K", "K")];
    let error = exact_join(&left, &right, &pairs, &add(&["V"]))
        .err()
        .unwrap();
    assert_eq!(
        error.to_string(),
        "Tables don't have one-to-one mapping - no mappings for key [4, 4]."
    );
}

#[test]
fn added_columns_default_to_the_right_columns_not_named_like_a_left_match_column() {
    let left = static_table(&[
        ("K", ColumnType::Int, ints([1, 2])),
        ("A", ColumnType::Int, ints([1, 2])),
    ]);
    let right = static_table(&[
        ("B", ColumnType::Int, ints([2, 1])),
        ("K", ColumnType::Long, longs([10, 20])),
        ("W", ColumnType::Long, longs([7, 8])),
    ]);
    // The filter uses the left name of the match: the right table's own K is left out and
    // its match column B is added.
    let pairs = [JoinMatch::new("K", "B")];
    let result = natural_join(&left, &right, &pairs, &[], ErrorOnDuplicate).unwrap();
    assert_eq!(names(&*result), ["K", "A", "B", "W"]);
    assert_eq!(column(&*result, "B"), ["1", "2"]);
    assert_eq!(column(&*result, "W"), ["8", "7"]);
    // Matching the left A instead leaves the right K in, and it clashes with the left K.
    let pairs = [JoinMatch::new("A", "B")];
    assert_eq!(
        natural_join(&left, &right, &pairs, &[], ErrorOnDuplicate).err(),
        Some(JoinError::ConflictingColumns(vec!["K".into()]))
    );
}

#[test]
fn added_columns_can_be_renamed_and_the_last_of_one_name_wins() {
    let left = static_table(&[("K", ColumnType::Int, ints([1, 2]))]);
    let right = static_table(&[
        ("K", ColumnType::Int, ints([2, 1])),
        ("V", ColumnType::Long, longs([10, 20])),
        ("W", ColumnType::String, strings(["x", "y"])),
    ]);
    let additions = [
        JoinAddition::renamed("X", "V"),
        JoinAddition::renamed("RightKey", "K"),
        JoinAddition::renamed("X", "W"),
    ];
    let result = natural_join(&left, &right, &on(&["K"]), &additions, ErrorOnDuplicate).unwrap();
    assert_eq!(names(&*result), ["K", "X", "RightKey"]);
    assert_eq!(
        result.definition().columns()[1].column_type,
        ColumnType::String
    );
    assert_eq!(column(&*result, "X"), ["y", "x"]);
    assert_eq!(column(&*result, "RightKey"), ["1", "2"]);
}

#[test]
fn bad_arguments_are_errors() {
    let left = static_table(&[
        ("K", ColumnType::Int, ints([1])),
        ("S", ColumnType::String, strings(["a"])),
    ]);
    let right = static_table(&[
        ("K", ColumnType::Int, ints([1])),
        ("V", ColumnType::Long, longs([10])),
    ]);
    let join = |on: &[JoinMatch], add: &[JoinAddition]| {
        natural_join(&left, &right, on, add, ErrorOnDuplicate).err()
    };
    let missing = |name: &str| Some(JoinError::NoSuchColumn(vec![name.to_string()]));
    // Java looks for conflicts first, and the default additions here include the right `K`.
    assert_eq!(
        join(&[JoinMatch::new("Nope", "K")], &[]),
        Some(JoinError::ConflictingColumns(vec!["K".into()]))
    );
    assert_eq!(
        join(&[JoinMatch::new("Nope", "K")], &add(&["V"])),
        missing("Nope")
    );
    assert_eq!(join(&[JoinMatch::new("K", "Nope")], &[]), missing("Nope"));
    assert_eq!(join(&on(&["K"]), &add(&["Nope"])), missing("Nope"));
    assert_eq!(
        join(&on(&["K"]), &[JoinAddition::renamed("S", "V")]),
        Some(JoinError::ConflictingColumns(vec!["S".into()]))
    );
    assert_eq!(
        join(&on(&["K"]), &[JoinAddition::renamed("K", "V")]),
        Some(JoinError::ConflictingColumns(vec!["K".into()]))
    );
    assert_eq!(
        join(&[JoinMatch::new("S", "K")], &add(&["V"])),
        Some(JoinError::MismatchedTypes {
            left: "S".into(),
            left_type: ColumnType::String,
            right: "K".into(),
            right_type: ColumnType::Int,
        })
    );
}

/// As recorded from Java for the same tables, in `testdata/corpus/join_zero_keys`.
#[test]
fn without_match_columns_every_left_row_matches_every_right_row() {
    let left = static_table(&[("A", ColumnType::Int, ints([1, 2]))]);
    let no_left = static_table(&[("A", ColumnType::Int, ints([]))]);
    let right = |size: i64| static_table(&[("V", ColumnType::Long, longs(10..10 + size))]);
    let join = |left: &Arc<dyn Table>, size: i64, join_type| {
        natural_join(left, &right(size), &[], &[], join_type).map(|table| column(&*table, "V"))
    };
    let types = [ErrorOnDuplicate, FirstMatch, LastMatch, ExactlyOneMatch];
    for join_type in types {
        assert_eq!(join(&left, 1, join_type).unwrap(), ["10", "10"]);
    }
    // No right row: nulls, except in an exact join.
    for join_type in [ErrorOnDuplicate, FirstMatch, LastMatch] {
        assert_eq!(join(&left, 0, join_type).unwrap(), ["null", "null"]);
    }
    assert_eq!(
        join(&left, 0, ExactlyOneMatch).unwrap_err().to_string(),
        "exactJoin with zero key columns must have exactly one row in the right hand side table!"
    );
    // Two right rows: duplicates for the two types that refuse them, and Java's failed
    // cast for the two that would choose, whether or not there is a left row.
    for join_type in [ErrorOnDuplicate, ExactlyOneMatch] {
        assert_eq!(
            join(&left, 2, join_type).unwrap_err().to_string(),
            "naturalJoin with zero key columns may not have more than one row in the right \
             hand side table!"
        );
        assert_eq!(join(&no_left, 2, join_type).unwrap(), Vec::<String>::new());
    }
    for join_type in [FirstMatch, LastMatch] {
        for left in [&left, &no_left] {
            assert!(matches!(
                join(left, 2, join_type),
                Err(JoinError::ZeroKeyStaticRight(_))
            ));
        }
    }
}

#[test]
fn a_static_result_is_add_only_and_append_only() {
    let (left, right) = int_tables(&[Some(1)], &[Some(1)]);
    let result = natural_join(&left, &right, &on(&["K"]), &[], ErrorOnDuplicate).unwrap();
    let attributes = result.attributes();
    assert!(attributes.add_only && attributes.append_only && !attributes.blink);
}

// ---- Ticking -------------------------------------------------------------------------------

/// A table of a ticking test: static, or scripted at the given row keys.
enum Side<'a> {
    Fixed(Columns<'a>),
    Live(&'a [RowKey], Columns<'a>),
}

type Build = dyn Fn(&Arc<dyn Table>, &Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync;

/// A join of two tables, with every update recorded and checked.
struct Pair {
    graph: Arc<dyn UpdateGraph>,
    left: Option<Arc<ScriptedTable>>,
    right: Option<Arc<ScriptedTable>>,
    result: Arc<dyn Table>,
    recorder: Arc<Recorder>,
    checker: Arc<ConsistencyChecker>,
}

impl Pair {
    /// `build` on the two tables; after every step the result must equal `build` on static
    /// copies of them.
    fn new(
        left: Side,
        right: Side,
        build: impl Fn(&Arc<dyn Table>, &Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync + 'static,
    ) -> Self {
        Self::make(left, right, Arc::new(build), true)
    }

    /// As [`Pair::new`], for a result that differs from the join of static copies.
    fn unlike_static(
        left: Side,
        right: Side,
        build: impl Fn(&Arc<dyn Table>, &Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync + 'static,
    ) -> Self {
        Self::make(left, right, Arc::new(build), false)
    }

    fn make(left: Side, right: Side, build: Arc<Build>, recompute: bool) -> Self {
        let graph = graph();
        let table = |side: Side| -> (Option<Arc<ScriptedTable>>, Arc<dyn Table>) {
            match side {
                Side::Fixed(columns) => (None, static_table(columns)),
                Side::Live(keys, columns) => {
                    let script = scripted(&graph, keys.iter().copied(), columns);
                    let table: Arc<dyn Table> = script.table();
                    (Some(script), table)
                }
            }
        };
        let (left, left_table) = table(left);
        let (right, right_table) = table(right);
        let result = build(&left_table, &right_table);
        let recorder = Recorder::attach(result.clone());
        let checker = if recompute {
            ConsistencyChecker::attach_with_recompute(result.clone(), move || {
                build(&frozen(&*left_table), &frozen(&*right_table))
            })
        } else {
            ConsistencyChecker::attach(result.clone())
        };
        checker.assert_consistent();
        Self {
            graph,
            left,
            right,
            result,
            recorder,
            checker,
        }
    }

    fn run(
        &self,
        left: Option<TableChange>,
        right: Option<TableChange>,
    ) -> Option<Result<TableUpdate, TableFailure>> {
        let before = self.recorder.events().len();
        if let Some(change) = left {
            self.left.as_ref().expect("a live left table").push(change);
        }
        if let Some(change) = right {
            self.right
                .as_ref()
                .expect("a live right table")
                .push(change);
        }
        self.graph.run_cycle();
        let mut events = self.recorder.events().split_off(before);
        assert!(events.len() <= 1, "more than one event in a cycle");
        events.pop()
    }

    /// Applies the changes in one cycle and returns the update the result delivered.
    ///
    /// # Panics
    /// If the result failed, is then inconsistent, or differs from the join of static
    /// copies of the tables.
    fn step(&self, left: Option<TableChange>, right: Option<TableChange>) -> Option<TableUpdate> {
        let event = self.run(left, right);
        self.checker.assert_consistent();
        event.map(|event| event.expect("an update"))
    }

    fn left(&self, change: TableChange) -> Option<TableUpdate> {
        self.step(Some(change), None)
    }

    fn right(&self, change: TableChange) -> Option<TableUpdate> {
        self.step(None, Some(change))
    }

    /// Applies the changes in one cycle and returns the failure the result delivered.
    fn fails(&self, left: Option<TableChange>, right: Option<TableChange>) -> String {
        let event = self.run(left, right).expect("a failure");
        assert!(self.result.is_failed());
        event.expect_err("a failure").message
    }

    fn v(&self) -> Vec<String> {
        column(&*self.result, "V")
    }
}

fn natural(
    join_type: NaturalJoinType,
) -> impl Fn(&Arc<dyn Table>, &Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync + 'static {
    move |left, right| natural_join(left, right, &on(&["K"]), &[], join_type).unwrap()
}

/// Left columns `K` (int) and `Tag` (string).
fn left_columns(keys: &[i32], tags: &[&str]) -> [(&'static str, ColumnType, ArrayRef); 2] {
    [
        ("K", ColumnType::Int, ints(keys.iter().copied())),
        ("Tag", ColumnType::String, strings(tags.iter().copied())),
    ]
}

/// Right columns `K` (int), `V` (long) and `Other` (string), which the tests do not add.
fn right_columns(keys: &[i32], values: &[i64]) -> [(&'static str, ColumnType, ArrayRef); 3] {
    let other = vec!["o"; keys.len()];
    [
        ("K", ColumnType::Int, ints(keys.iter().copied())),
        ("V", ColumnType::Long, longs(values.iter().copied())),
        ("Other", ColumnType::String, strings(other)),
    ]
}

fn left_row(key: i32, tag: &str) -> Vec<ArrayRef> {
    vec![ints([key]), strings([tag])]
}

fn right_row(key: i32, value: i64) -> Vec<ArrayRef> {
    vec![ints([key]), longs([value]), strings(["o"])]
}

/// `K`, `V` joined without `Other`: result columns K, Tag, V.
fn kv(
    join_type: NaturalJoinType,
) -> impl Fn(&Arc<dyn Table>, &Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync + 'static {
    move |left, right| natural_join(left, right, &on(&["K"]), &add(&["V"]), join_type).unwrap()
}

const TAG: usize = 1;
const V: usize = 2;

/// Both tables live: left rows 1, 2, 1 at keys 10, 20, 30 and right rows 1, 2 at 5, 6.
fn both_live(join_type: NaturalJoinType) -> Pair {
    Pair::new(
        Side::Live(&[10, 20, 30], &left_columns(&[1, 2, 1], &["a", "b", "c"])),
        Side::Live(&[5, 6], &right_columns(&[1, 2], &[100, 200])),
        kv(join_type),
    )
}

#[test]
fn left_adds_and_removes_pass_through_with_their_matches() {
    let pair = both_live(ErrorOnDuplicate);
    assert!(pair.result.is_refreshing());
    let update = pair
        .left(
            TableChange::new()
                .remove([20])
                .add([15, 40], vec![ints([2, 7]), strings(["d", "e"])]),
        )
        .unwrap();
    assert_eq!(keys(&update.added), [15, 40]);
    assert_eq!(keys(&update.removed), [20]);
    assert!(update.modified.is_empty() && update.modified_columns.is_empty());
    assert_eq!(pair.v(), ["100", "200", "100", "null"]);
}

#[test]
fn a_left_modify_reports_added_columns_only_when_the_key_changed() {
    let pair = both_live(ErrorOnDuplicate);
    let update = pair
        .left(TableChange::new().modify([10], [(TAG, strings(["z"]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [10]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([TAG]));

    let update = pair
        .left(TableChange::new().modify([10, 30], [(0, ints([2, 1]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [10, 30]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([0, V]));
    assert_eq!(pair.v(), ["200", "200", "100"]);

    // A key column written with the values it had: the rows are modified, their matches
    // are not.
    let update = pair
        .left(TableChange::new().modify([10, 30], [(0, ints([2, 1]))]))
        .unwrap();
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([0]));

    // To a key without a right row.
    pair.left(TableChange::new().modify([20], [(0, ints([9]))]));
    assert_eq!(pair.v(), ["200", "null", "100"]);
}

#[test]
fn left_shifts_pass_through_and_matches_follow_the_rows() {
    let pair = both_live(ErrorOnDuplicate);
    let update = pair
        .left(
            TableChange::new()
                .remove([10])
                .shift(20, 30, -15)
                .add([40], left_row(2, "d"))
                .modify([15], [(0, ints([2]))]),
        )
        .unwrap();
    assert_eq!(shifts(&update), [(20, 30, -15)]);
    assert_eq!(keys(&pair.result.row_set()), [5, 15, 40]);
    assert_eq!(keys(&update.modified), [15]);
    assert_eq!(pair.v(), ["200", "200", "200"]);
    // A later change of the right table reaches the rows at their new keys.
    let update = pair.right(TableChange::new().remove([6])).unwrap();
    assert_eq!(keys(&update.modified), [5, 15, 40]);
    assert_eq!(pair.v(), ["null", "null", "null"]);
}

#[test]
fn a_right_add_or_remove_modifies_the_left_rows_of_its_key_only() {
    let pair = both_live(ErrorOnDuplicate);
    // A key no left row has.
    assert_eq!(
        pair.right(TableChange::new().add([7], right_row(9, 900))),
        None
    );
    let update = pair.right(TableChange::new().remove([5])).unwrap();
    assert!(update.added.is_empty() && update.removed.is_empty() && update.shifted.is_empty());
    assert_eq!(keys(&update.modified), [10, 30]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([V]));
    assert_eq!(pair.v(), ["null", "200", "null"]);
    let update = pair
        .right(TableChange::new().add([1], right_row(1, 101)))
        .unwrap();
    assert_eq!(keys(&update.modified), [10, 30]);
    assert_eq!(pair.v(), ["101", "200", "101"]);
}

#[test]
fn a_right_modify_reports_the_added_columns_it_changed() {
    let pair = both_live(ErrorOnDuplicate);
    let update = pair
        .right(TableChange::new().modify([6], [(1, longs([201]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [20]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([V]));
    assert_eq!(pair.v(), ["100", "201", "100"]);
    // A column the join does not add.
    assert_eq!(
        pair.right(TableChange::new().modify([6], [(2, strings(["p"]))])),
        None
    );
}

#[test]
fn a_right_key_change_moves_the_match_from_the_old_key_to_the_new() {
    let pair = both_live(ErrorOnDuplicate);
    let update = pair
        .right(TableChange::new().modify([5], [(0, ints([7]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [10, 30]);
    assert_eq!(pair.v(), ["null", "200", "null"]);
    let update = pair
        .right(TableChange::new().modify([6], [(0, ints([1]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [10, 20, 30]);
    assert_eq!(pair.v(), ["200", "null", "200"]);
    // Two rows swap keys in one cycle: no key is ever duplicated.
    pair.right(TableChange::new().modify([5], [(0, ints([2]))]));
    let update = pair
        .right(TableChange::new().modify([5, 6], [(0, ints([1, 2]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [10, 20, 30]);
    assert_eq!(pair.v(), ["100", "200", "100"]);
}

#[test]
fn a_right_shift_changes_nothing_in_the_result() {
    let pair = both_live(ErrorOnDuplicate);
    assert_eq!(pair.right(TableChange::new().shift(5, 6, 10)), None);
    assert_eq!(pair.v(), ["100", "200", "100"]);
    // The shifted rows are still the matches: a modify at the new key arrives.
    let update = pair
        .right(
            TableChange::new()
                .shift(16, 16, 4)
                .modify([15], [(1, longs([111]))]),
        )
        .unwrap();
    assert_eq!(keys(&update.modified), [10, 30]);
    assert_eq!(pair.v(), ["111", "200", "111"]);
    // Shifted and modified in one cycle.
    let update = pair
        .right(
            TableChange::new()
                .shift(20, 20, 1)
                .modify([21], [(1, longs([222]))]),
        )
        .unwrap();
    assert_eq!(keys(&update.modified), [20]);
    assert_eq!(pair.v(), ["111", "222", "111"]);
}

/// Java compares the row key of the new match with that of the old and reports nothing
/// here (`ModifiedSlotUpdater#accept`); the values did change.
#[test]
fn a_right_row_replaced_at_its_row_key_modifies_its_left_rows() {
    let pair = both_live(ErrorOnDuplicate);
    let update = pair
        .right(TableChange::new().remove([5]).add([5], right_row(1, 555)))
        .unwrap();
    assert_eq!(keys(&update.modified), [10, 30]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([V]));
    assert_eq!(pair.v(), ["555", "200", "555"]);
}

#[test]
fn changes_of_both_tables_in_one_cycle_make_one_update() {
    let pair = both_live(ErrorOnDuplicate);
    let update = pair
        .step(
            Some(
                TableChange::new()
                    .remove([10])
                    .shift(20, 30, 5)
                    .add([1], left_row(3, "d"))
                    .modify([25], [(TAG, strings(["y"]))]),
            ),
            Some(
                TableChange::new()
                    .remove([6])
                    .add([8], right_row(3, 300))
                    .modify([5], [(1, longs([101]))]),
            ),
        )
        .unwrap();
    assert_eq!(keys(&update.added), [1]);
    assert_eq!(keys(&update.removed), [10]);
    assert_eq!(shifts(&update), [(20, 30, 5)]);
    // Row 25 by both tables, row 35 by the right table; the removed and the added row are
    // not modified.
    assert_eq!(keys(&update.modified), [25, 35]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([TAG, V]));
    assert_eq!(pair.v(), ["300", "null", "101"]);
}

#[test]
fn a_right_change_that_reaches_no_left_row_delivers_nothing() {
    let pair = both_live(ErrorOnDuplicate);
    // A key no left row has arrives, changes and leaves.
    assert_eq!(
        pair.right(TableChange::new().add([1], right_row(9, 900))),
        None
    );
    let modify = TableChange::new().modify([1], [(1, longs([901]))]);
    assert_eq!(pair.right(modify), None);
    assert_eq!(pair.right(TableChange::new().remove([1])), None);
    // A column that is not added changes in a row that is a match.
    let modify = TableChange::new().modify([5], [(2, strings(["p"]))]);
    assert_eq!(pair.right(modify), None);
    assert_eq!(pair.v(), ["100", "200", "100"]);
}

#[test]
fn a_duplicate_right_key_fails_the_result_once_a_left_row_has_the_key() {
    let pair = both_live(ErrorOnDuplicate);
    // Key 9: duplicated, and no left row has it.
    let nines = vec![ints([9, 9]), longs([1, 2]), strings(["o", "o"])];
    assert_eq!(pair.right(TableChange::new().add([1, 2], nines)), None);
    // One duplicate leaves in the cycle in which a left row with the key arrives.
    pair.step(
        Some(TableChange::new().add([40], left_row(9, "d"))),
        Some(TableChange::new().remove([1])),
    );
    assert_eq!(pair.v(), ["100", "200", "100", "2"]);
    // A second row for a key that left rows have.
    let message = pair.fails(None, Some(TableChange::new().add([7], right_row(2, 201))));
    assert_eq!(message, "Natural Join found duplicate right key for 2");
}

#[test]
fn a_left_row_arriving_at_a_duplicated_key_fails_the_result() {
    let pair = Pair::new(
        Side::Live(&[10], &left_columns(&[1], &["a"])),
        Side::Live(&[5, 6, 7], &right_columns(&[1, 2, 2], &[100, 200, 201])),
        kv(ErrorOnDuplicate),
    );
    let message = pair.fails(Some(TableChange::new().add([11], left_row(2, "b"))), None);
    assert_eq!(message, "Natural Join found duplicate right key for 2");

    // The same with a static right table, and by a key change.
    let pair = Pair::new(
        Side::Live(&[10], &left_columns(&[1], &["a"])),
        Side::Fixed(&right_columns(&[1, 2, 2], &[100, 200, 201])),
        kv(ErrorOnDuplicate),
    );
    let message = pair.fails(
        Some(TableChange::new().modify([10], [(0, ints([2]))])),
        None,
    );
    assert_eq!(message, "Natural Join found duplicate right key for 2");
}

#[test]
fn a_replaced_duplicate_in_one_cycle_does_not_fail() {
    let pair = both_live(ErrorOnDuplicate);
    // The row of key 1 leaves and another arrives; the row of key 2 changes key and
    // another row takes its key.
    let update = pair
        .right(
            TableChange::new()
                .remove([5])
                .add(
                    [1, 2],
                    vec![ints([1, 2]), longs([101, 202]), strings(["o", "o"])],
                )
                .modify([6], [(0, ints([8]))]),
        )
        .unwrap();
    assert_eq!(keys(&update.modified), [10, 20, 30]);
    assert_eq!(pair.v(), ["101", "202", "101"]);
}

#[test]
fn first_match_follows_the_first_right_row_of_a_key() {
    let pair = both_live(FirstMatch);
    // A later row of key 1: not the first, nothing changes.
    assert_eq!(
        pair.right(TableChange::new().add([9], right_row(1, 109))),
        None
    );
    // An earlier row is the new first.
    let update = pair
        .right(TableChange::new().add([2], right_row(1, 102)))
        .unwrap();
    assert_eq!(keys(&update.modified), [10, 30]);
    assert_eq!(pair.v(), ["102", "200", "102"]);
    // A modify of a row that is not the first.
    pair.right(TableChange::new().modify([9], [(1, longs([999]))]));
    assert_eq!(pair.v(), ["102", "200", "102"]);
    // The first leaves: the next one takes over.
    pair.right(TableChange::new().remove([2]));
    assert_eq!(pair.v(), ["100", "200", "100"]);
    pair.right(TableChange::new().remove([5]));
    assert_eq!(pair.v(), ["999", "200", "999"]);
    pair.right(TableChange::new().remove([9]));
    assert_eq!(pair.v(), ["null", "200", "null"]);
}

#[test]
fn last_match_follows_the_last_right_row_of_a_key() {
    let pair = both_live(LastMatch);
    assert_eq!(
        pair.right(TableChange::new().add([2], right_row(1, 102))),
        None
    );
    let update = pair
        .right(TableChange::new().add([9], right_row(1, 109)))
        .unwrap();
    assert_eq!(keys(&update.modified), [10, 30]);
    assert_eq!(pair.v(), ["109", "200", "109"]);
    // Shifting the duplicates keeps their order and so the match.
    assert_eq!(pair.right(TableChange::new().shift(5, 9, 20)), None);
    assert_eq!(pair.v(), ["109", "200", "109"]);
    // The last changes key and becomes the last of key 2.
    pair.right(TableChange::new().modify([29], [(0, ints([2]))]));
    assert_eq!(pair.v(), ["100", "109", "100"]);
    // A left row arriving at a duplicated key gets its last row.
    pair.left(TableChange::new().add([40], left_row(2, "d")));
    assert_eq!(pair.v(), ["100", "109", "100", "109"]);
}

#[test]
fn an_exact_join_fails_when_a_left_row_loses_or_lacks_its_right_row() {
    let exact = |left: &Arc<dyn Table>, right: &Arc<dyn Table>| {
        exact_join(left, right, &on(&["K"]), &add(&["V"])).unwrap()
    };
    let start = || {
        Pair::new(
            Side::Live(&[10, 20], &left_columns(&[1, 2], &["a", "b"])),
            Side::Live(&[5, 6, 7], &right_columns(&[1, 2, 3], &[100, 200, 300])),
            exact,
        )
    };
    let no_mapping =
        |key: i32| format!("Tables don't have one-to-one mapping - no mappings for key {key}.");

    let pair = start();
    // A right row no left row has may go; left rows may come and go.
    assert_eq!(pair.right(TableChange::new().remove([7])), None);
    pair.left(TableChange::new().remove([20]).add([30], left_row(1, "c")));
    assert_eq!(pair.v(), ["100", "100"]);
    assert_eq!(
        pair.fails(None, Some(TableChange::new().remove([5]))),
        no_mapping(1)
    );

    let pair = start();
    let added = TableChange::new().add([30], left_row(9, "c"));
    assert_eq!(pair.fails(Some(added), None), no_mapping(9));

    let pair = start();
    let rekeyed = TableChange::new().modify([10], [(0, ints([9]))]);
    assert_eq!(pair.fails(Some(rekeyed), None), no_mapping(9));

    // The right row arrives in the same cycle as the left row that needs it, and a right
    // row leaves in the cycle in which its last left row leaves.
    let pair = start();
    pair.step(
        Some(TableChange::new().remove([20]).add([30], left_row(9, "c"))),
        Some(TableChange::new().remove([6]).add([8], right_row(9, 900))),
    );
    assert_eq!(pair.v(), ["100", "900"]);

    let pair = start();
    let message = pair.fails(None, Some(TableChange::new().add([8], right_row(2, 201))));
    assert_eq!(message, "Natural Join found duplicate right key for 2");
}

/// As recorded from Java in `testdata/corpus/ticking_join_failures`: with a static right table
/// the rows a left table adds or re-keys later are not checked.
#[test]
fn an_exact_join_with_a_static_right_table_does_not_check_later_left_rows() {
    let pair = Pair::unlike_static(
        Side::Live(&[10, 20], &left_columns(&[1, 2], &["a", "b"])),
        Side::Fixed(&right_columns(&[1, 2, 3], &[100, 200, 300])),
        |left, right| exact_join(left, right, &on(&["K"]), &add(&["V"])).unwrap(),
    );
    pair.left(TableChange::new().add([30], left_row(9, "c")));
    assert_eq!(pair.v(), ["100", "200", "null"]);
    pair.left(TableChange::new().modify([10], [(0, ints([8]))]));
    assert_eq!(pair.v(), ["null", "200", "null"]);
    assert!(!pair.result.is_failed());
}

#[test]
fn a_static_left_table_follows_a_live_right_table() {
    let pair = Pair::new(
        Side::Fixed(&left_columns(&[1, 2, 1, 4], &["a", "b", "c", "d"])),
        Side::Live(&[5], &right_columns(&[1], &[100])),
        kv(ErrorOnDuplicate),
    );
    assert!(pair.result.is_refreshing());
    let update = pair
        .right(TableChange::new().shift(5, 5, 3).add(
            [1, 2],
            vec![ints([2, 9]), longs([200, 900]), strings(["o", "o"])],
        ))
        .unwrap();
    assert_eq!(keys(&update.modified), [1]);
    assert_eq!(pair.v(), ["100", "200", "100", "null"]);
    pair.right(TableChange::new().modify([8], [(0, ints([4])), (1, longs([400]))]));
    assert_eq!(pair.v(), ["null", "200", "null", "400"]);
    let message = pair.fails(None, Some(TableChange::new().add([3], right_row(4, 401))));
    assert_eq!(message, "Natural Join found duplicate right key for 4");
}

#[test]
fn a_live_left_table_follows_a_static_right_table() {
    let pair = Pair::new(
        Side::Live(&[10], &left_columns(&[1], &["a"])),
        Side::Fixed(&right_columns(&[1, 2], &[100, 200])),
        kv(ErrorOnDuplicate),
    );
    let update = pair
        .left(
            TableChange::new()
                .shift(10, 10, -5)
                .add([20, 30], vec![ints([2, 9]), strings(["b", "c"])])
                .modify([5], [(0, ints([2]))]),
        )
        .unwrap();
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([0, V]));
    assert_eq!(pair.v(), ["200", "200", "null"]);
    pair.left(TableChange::new().remove([5, 20]));
    assert_eq!(pair.v(), ["null"]);
}

#[test]
fn without_match_columns_the_result_follows_the_one_right_row() {
    let zero = |join_type: NaturalJoinType| {
        move |left: &Arc<dyn Table>, right: &Arc<dyn Table>| {
            natural_join(left, right, &[], &add(&["V"]), join_type).unwrap()
        }
    };
    let start = |join_type: NaturalJoinType| {
        Pair::new(
            Side::Live(&[10, 20], &left_columns(&[1, 2], &["a", "b"])),
            Side::Live(&[5], &right_columns(&[7], &[100])),
            zero(join_type),
        )
    };
    let pair = start(ErrorOnDuplicate);
    assert_eq!(pair.v(), ["100", "100"]);
    let update = pair
        .right(TableChange::new().remove([5]).add([9], right_row(8, 900)))
        .unwrap();
    assert_eq!(keys(&update.modified), [10, 20]);
    assert_eq!(pair.v(), ["900", "900"]);
    pair.step(
        Some(TableChange::new().add([30], left_row(3, "c"))),
        Some(TableChange::new().remove([9])),
    );
    assert_eq!(pair.v(), ["null", "null", "null"]);
    pair.right(TableChange::new().add([1], right_row(1, 11)));
    let message = pair.fails(None, Some(TableChange::new().add([2], right_row(1, 12))));
    assert!(
        message.starts_with("naturalJoin with zero key columns"),
        "{message}"
    );

    let pair = start(ExactlyOneMatch);
    let message = pair.fails(None, Some(TableChange::new().remove([5])));
    assert!(
        message.starts_with("exactJoin with zero key columns"),
        "{message}"
    );
    // The join of static copies is the one Java refuses, so there is nothing to compare with.
    let start = |join_type: NaturalJoinType| {
        Pair::unlike_static(
            Side::Live(&[10, 20], &left_columns(&[1, 2], &["a", "b"])),
            Side::Live(&[5], &right_columns(&[7], &[100])),
            zero(join_type),
        )
    };
    let pair = start(LastMatch);

    pair.right(TableChange::new().add(
        [2, 8],
        vec![ints([0, 0]), longs([2, 8]), strings(["o", "o"])],
    ));
    assert_eq!(pair.v(), ["8", "8"]);
    let pair = start(FirstMatch);
    pair.right(TableChange::new().add(
        [2, 8],
        vec![ints([0, 0]), longs([2, 8]), strings(["o", "o"])],
    ));
    assert_eq!(pair.v(), ["2", "2"]);
}

/// A static right table and no match columns: unlike with match columns, Java checks an
/// exact join on every change of the left table (`checkRightTableSizeZeroKeys`).
#[test]
fn without_match_columns_a_static_right_table_is_checked_when_left_rows_arrive() {
    let empty: [RowKey; 0] = [];
    let start = |right_rows: usize, join_type: NaturalJoinType| {
        let right_keys = vec![7; right_rows];
        let right_values: Vec<i64> = (0..right_rows as i64).collect();
        Pair::new(
            Side::Live(&empty, &left_columns(&[], &[])),
            Side::Fixed(&right_columns(&right_keys, &right_values)),
            move |left, right| natural_join(left, right, &[], &add(&["V"]), join_type).unwrap(),
        )
    };
    let row = || Some(TableChange::new().add([1], left_row(1, "a")));
    let message = start(0, ExactlyOneMatch).fails(row(), None);
    assert!(
        message.starts_with("exactJoin with zero key columns"),
        "{message}"
    );
    let message = start(2, ExactlyOneMatch).fails(row(), None);
    assert!(
        message.starts_with("naturalJoin with zero key columns"),
        "{message}"
    );
    let message = start(2, ErrorOnDuplicate).fails(row(), None);
    assert!(
        message.starts_with("naturalJoin with zero key columns"),
        "{message}"
    );
    let pair = start(1, ExactlyOneMatch);
    pair.step(row(), None);
    assert_eq!(pair.v(), ["0"]);
}

#[test]
fn a_failure_of_either_table_fails_the_result() {
    for side in [0, 1] {
        let pair = both_live(ErrorOnDuplicate);
        let script = if side == 0 { &pair.left } else { &pair.right };
        script.as_ref().unwrap().push_failure("broken");
        pair.graph.run_cycle();
        assert_eq!(
            pair.recorder.events().pop(),
            Some(Err(TableFailure {
                message: "broken".into()
            }))
        );
        assert!(pair.result.is_failed());
        // A join of a failed table is refused.
        let failed = pair.result.clone();
        let (other, _) = int_tables(&[Some(1)], &[]);
        assert!(matches!(
            natural_join(&other, &failed, &on(&["K"]), &add(&["V"]), ErrorOnDuplicate),
            Err(JoinError::Table(_))
        ));
    }
}

#[test]
fn refreshing_tables_of_two_graphs_are_refused() {
    let (one, other) = (graph(), graph());
    let left: Arc<dyn Table> = scripted(&one, [1], &left_columns(&[1], &["a"])).table();
    let right: Arc<dyn Table> = scripted(&other, [1], &right_columns(&[1], &[1])).table();
    assert_eq!(
        natural_join(&left, &right, &on(&["K"]), &add(&["V"]), ErrorOnDuplicate).err(),
        Some(JoinError::UpdateGraphConflict)
    );
}

// ---- Attributes ----------------------------------------------------------------------------

fn append_only(graph: &Arc<dyn UpdateGraph>, columns: &[(&str, ColumnType)]) -> Arc<dyn Table> {
    append_only_input_table(graph.clone(), definition(columns), None).unwrap()
}

fn append(table: &Arc<dyn Table>, columns: Columns) {
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, column_type, _)| Field::new(*name, column_type.arrow_type(), true))
        .collect();
    let arrays = columns
        .iter()
        .map(|(_, _, values)| values.clone())
        .collect();
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).unwrap();
    table.input_table().unwrap().add(batch).unwrap();
}

#[test]
fn the_result_is_append_only_when_the_left_table_is_and_the_right_cannot_modify_it() {
    let graph = graph();
    let left = append_only(
        &graph,
        &[("K", ColumnType::Int), ("Tag", ColumnType::String)],
    );
    let right = append_only(&graph, &[("K", ColumnType::Int), ("V", ColumnType::Long)]);
    let (keyed, fixed) = int_tables(&[Some(1)], &[Some(1), Some(2)]);
    let flags = |left: &Arc<dyn Table>, right: &Arc<dyn Table>, join_type| {
        let result = natural_join(left, right, &on(&["K"]), &add(&["V"]), join_type).unwrap();
        let attributes = result.attributes();
        (attributes.add_only, attributes.append_only)
    };
    // A static right table never modifies the result.
    assert_eq!(flags(&left, &fixed, ErrorOnDuplicate), (true, true));
    // A refreshing one can, by giving a left row its right row later ...
    assert_eq!(flags(&left, &right, ErrorOnDuplicate), (false, false));
    assert_eq!(flags(&left, &right, FirstMatch), (false, false));
    // ... except in an exact join, when it only adds rows.
    assert_eq!(flags(&left, &right, ExactlyOneMatch), (true, true));
    // A static left table counts as append-only.
    assert_eq!(flags(&keyed, &right, ErrorOnDuplicate), (false, false));
    append(
        &right,
        &[
            ("K", ColumnType::Int, ints([1])),
            ("V", ColumnType::Long, longs([5])),
        ],
    );
    assert_eq!(flags(&keyed, &right, ExactlyOneMatch), (true, true));

    // The append-only result takes the rows the left table appends.
    let result = natural_join(&left, &fixed, &on(&["K"]), &add(&["V"]), ErrorOnDuplicate).unwrap();
    let checker = ConsistencyChecker::attach(result.clone());
    append(&left, &left_columns(&[2, 5], &["a", "b"]));
    checker.assert_consistent();
    assert_eq!(column(&*result, "V"), ["101", "null"]);
    assert!(!result.is_failed());
}

/// Behavior: io.deephaven.engine.table.impl.NaturalJoinHelper#naturalJoin, which copies the
/// attributes of the left table only, by io.deephaven.engine.table.impl.BaseTable#copyAttributes
/// with `CopyAttributeOperation.Join`; `INPUT_TABLE_ATTRIBUTE` is one of them.
#[test]
fn the_result_has_the_input_table_of_the_left_table_only() {
    let graph = graph();
    let left = append_only(
        &graph,
        &[("K", ColumnType::Int), ("Tag", ColumnType::String)],
    );
    let right = append_only(&graph, &[("K", ColumnType::Int), ("V", ColumnType::Long)]);
    append(
        &right,
        &[
            ("K", ColumnType::Int, ints([1, 7])),
            ("V", ColumnType::Long, longs([10, 70])),
        ],
    );
    let (fixed_left, fixed_right) = int_tables(&[Some(1)], &[Some(1), Some(2)]);
    let writes_to = |result: &Arc<dyn Table>, input: &Arc<dyn Table>| {
        let (found, own) = (result.input_table(), input.input_table().unwrap());
        found.is_some_and(|found| {
            std::ptr::eq(
                Arc::as_ptr(&found) as *const (),
                Arc::as_ptr(&own) as *const (),
            )
        })
    };
    let join = |left: &Arc<dyn Table>, right: &Arc<dyn Table>, join_type| {
        natural_join(left, right, &on(&["K"]), &add(&["V"]), join_type).unwrap()
    };
    for join_type in [ErrorOnDuplicate, FirstMatch, LastMatch] {
        assert!(writes_to(&join(&left, &right, join_type), &left));
        assert!(writes_to(&join(&left, &fixed_right, join_type), &left));
        assert!(join(&fixed_left, &right, join_type).input_table().is_none());
        assert!(join(&fixed_left, &fixed_right, join_type)
            .input_table()
            .is_none());
    }
    let exact = exact_join(&left, &right, &on(&["K"]), &add(&["V"])).unwrap();
    assert!(writes_to(&exact, &left));
    assert!(exact_join(&fixed_left, &right, &on(&["K"]), &add(&["V"]))
        .unwrap()
        .input_table()
        .is_none());
    // A join of a join still writes to the first left table.
    let again = natural_join(
        &exact,
        &right,
        &on(&["K"]),
        &[JoinAddition::renamed("V2", "V")],
        FirstMatch,
    )
    .unwrap();
    assert!(writes_to(&again, &left));

    // A row written through the result's input table arrives in the result.
    let checker = ConsistencyChecker::attach(exact.clone());
    append(&exact, &left_columns(&[7], &["g"]));
    checker.assert_consistent();
    assert_eq!(column(&*exact, "V"), ["70"]);
}

#[test]
fn the_result_of_a_blink_left_table_is_a_blink_table() {
    let graph = graph();
    let (left, blink) = blink_table(
        &graph,
        &[("K", ColumnType::Int), ("Tag", ColumnType::String)],
    );
    let right = scripted(&graph, [5], &right_columns(&[1], &[100]));
    let right_table: Arc<dyn Table> = right.table();
    let result = natural_join(
        &left,
        &right_table,
        &on(&["K"]),
        &add(&["V"]),
        ErrorOnDuplicate,
    )
    .unwrap();
    assert!(result.attributes().blink);
    let checker = ConsistencyChecker::attach(result.clone());
    blink.push(left_row(1, "a"));
    graph.run_cycle();
    checker.assert_consistent();
    assert_eq!(column(&*result, "V"), ["100"]);
    // The right row changes in the cycle in which the blink rows leave and others arrive.
    blink.push(vec![ints([1, 2]), strings(["b", "c"])]);
    right.push(TableChange::new().modify([5], [(1, longs([101]))]));
    graph.run_cycle();
    checker.assert_consistent();
    assert_eq!(column(&*result, "V"), ["101", "null"]);
    graph.run_cycle();
    checker.assert_consistent();
    assert!(result.row_set().is_empty() && !result.is_failed());
}

/// The default additions are columns like any other: a change of one is a change of it alone.
#[test]
fn the_default_additions_follow_changes_of_their_right_columns() {
    let pair = Pair::new(
        Side::Live(&[10], &left_columns(&[1], &["a"])),
        Side::Live(&[5], &right_columns(&[1], &[100])),
        natural(ErrorOnDuplicate),
    );
    assert_eq!(names(&*pair.result), ["K", "Tag", "V", "Other"]);
    let update = pair
        .right(TableChange::new().modify([5], [(2, strings(["p"]))]))
        .unwrap();
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([3]));
}

/// Left rows at row keys far apart, for which the redirection is not an array, and then at
/// row keys that an array holds again.
#[test]
fn a_left_table_of_row_keys_far_apart_follows_both_tables() {
    const FAR: RowKey = 1 << 40;
    let pair = Pair::new(
        Side::Live(
            &[10, FAR, 2 * FAR],
            &left_columns(&[1, 2, 1], &["a", "b", "c"]),
        ),
        Side::Live(&[5, 6], &right_columns(&[1, 2], &[100, 200])),
        kv(ErrorOnDuplicate),
    );
    assert_eq!(pair.v(), ["100", "200", "100"]);
    pair.left(
        TableChange::new()
            .remove([10])
            .add([3 * FAR], left_row(2, "d")),
    );
    assert_eq!(pair.v(), ["200", "100", "200"]);
    let update = pair.right(TableChange::new().remove([5])).unwrap();
    assert_eq!(keys(&update.modified), [2 * FAR]);
    assert_eq!(pair.v(), ["200", "null", "200"]);
    pair.left(TableChange::new().shift(FAR, 3 * FAR, -FAR + 7));
    assert_eq!(keys(&pair.result.row_set()), [7, FAR + 7, 2 * FAR + 7]);
    assert_eq!(pair.v(), ["200", "null", "200"]);
    pair.left(TableChange::new().remove([FAR + 7, 2 * FAR + 7]));
    pair.step(
        Some(TableChange::new().add([8, 9], vec![ints([1, 2]), strings(["e", "f"])])),
        Some(TableChange::new().add([5], right_row(1, 101))),
    );
    assert_eq!(pair.v(), ["200", "101", "200"]);
}

/// A static table whose rows are not at `0..size`: the redirection of the result is made
/// in one piece for whatever row keys the left table has.
#[test]
fn a_static_left_table_of_any_row_keys_is_joined() {
    const FAR: RowKey = 1 << 40;
    let right = static_table(&right_columns(&[1, 2], &[100, 200]));
    for rows in [
        vec![0, 1, 2, 3],
        vec![3, 5, 9, 4000],
        vec![0, 7, FAR, FAR + 1],
    ] {
        let left: Arc<dyn Table> = ReferenceTable::new_static(
            definition(&[("K", ColumnType::Int), ("Tag", ColumnType::String)]),
            RowSet::from_keys(rows.iter().copied()),
            vec![ints([2, 9, 1, 2]), strings(["a", "b", "c", "d"])],
        );
        assert_eq!(keys(&left.row_set()), rows);
        let joined = natural_join(&left, &right, &on(&["K"]), &add(&["V"]), ErrorOnDuplicate);
        let joined = joined.unwrap();
        assert_eq!(keys(&joined.row_set()), rows);
        assert_eq!(column(&*joined, "V"), ["200", "null", "100", "200"]);
    }
}

/// With a static right table the join has no slot for a key that table lacks, and still
/// tells a row that moved between two such keys from one that kept its key.
#[test]
fn a_left_row_moving_between_keys_without_right_rows_reports_the_added_columns() {
    let pair = Pair::new(
        Side::Live(&[10, 20], &left_columns(&[7, 1], &["a", "b"])),
        Side::Fixed(&right_columns(&[1, 2], &[100, 200])),
        kv(ErrorOnDuplicate),
    );
    let update = pair
        .left(TableChange::new().modify([10], [(0, ints([8]))]))
        .unwrap();
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([0, V]));
    assert_eq!(pair.v(), ["null", "100"]);
    // The key column written with the values it had.
    let update = pair
        .left(TableChange::new().modify([10, 20], [(0, ints([8, 1]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [10, 20]);
    assert_eq!(update.modified_columns, ModifiedColumnSet::of([0]));
    assert_eq!(pair.v(), ["null", "100"]);
}

/// When one cycle makes the join refuse several keys, the failure names the least of
/// them, whatever order the keys arrived in.
#[test]
fn a_cycle_that_refuses_several_keys_fails_with_the_least_of_them() {
    for right_keys in [[3, 1, 2], [1, 2, 3], [2, 3, 1]] {
        let pair = Pair::new(
            Side::Live(&[10, 20, 30], &left_columns(&[3, 2, 1], &["a", "b", "c"])),
            Side::Live(&[5, 6, 7], &right_columns(&right_keys, &[100, 200, 300])),
            |left, right| exact_join(left, right, &on(&["K"]), &add(&["V"])).unwrap(),
        );
        let message = pair.fails(None, Some(TableChange::new().remove([5, 6, 7])));
        assert_eq!(
            message,
            "Tables don't have one-to-one mapping - no mappings for key 1."
        );
    }
    // A duplicate of one key and the loss of another: the lesser key, whichever it is.
    let start = |right_keys: [i32; 2]| {
        Pair::new(
            Side::Live(&[10, 20], &left_columns(&[4, 6], &["a", "b"])),
            Side::Live(&[5, 6], &right_columns(&right_keys, &[100, 200])),
            |left, right| exact_join(left, right, &on(&["K"]), &add(&["V"])).unwrap(),
        )
    };
    let pair = start([6, 4]);
    let change = TableChange::new().remove([5]).add([9], right_row(4, 400));
    assert_eq!(
        pair.fails(None, Some(change)),
        "Natural Join found duplicate right key for 4"
    );
    let pair = start([4, 6]);
    let change = TableChange::new().remove([5]).add([9], right_row(6, 600));
    assert_eq!(
        pair.fails(None, Some(change)),
        "Tables don't have one-to-one mapping - no mappings for key 4."
    );
}

/// Each kind of key a join hashes in its own way, with both tables changing: a string, a
/// double and a float with their NaNs and zeros, and several columns. Null keys match in
/// all of them.
#[test]
fn every_kind_of_key_follows_changes_of_both_tables() {
    // The values a key column takes, by number: 0 is null, 1 and 2 are equal, and 3 and 4
    // are equal for the floating point types.
    type Make = fn(&[usize]) -> ArrayRef;
    let text: Make = |picks| {
        let values = [None, Some("a"), Some("a"), Some(""), Some("b")];
        Arc::new(StringArray::from_iter(
            picks.iter().map(|pick| values[*pick]),
        ))
    };
    let double: Make = |picks| {
        let other_nan = f64::from_bits(0xfff8_0000_0000_0001);
        let values = [None, Some(f64::NAN), Some(other_nan), Some(0.0), Some(-0.0)];
        Arc::new(Float64Array::from_iter(
            picks.iter().map(|pick| values[*pick]),
        ))
    };
    let float: Make = |picks| {
        let values = [None, Some(f32::NAN), Some(-f32::NAN), Some(-0.0), Some(0.0)];
        Arc::new(Float32Array::from_iter(
            picks.iter().map(|pick| values[*pick]),
        ))
    };
    let flag: Make = |picks| {
        let values = [None, Some(true), Some(true), Some(false), Some(false)];
        Arc::new(BooleanArray::from_iter(
            picks.iter().map(|pick| values[*pick]),
        ))
    };
    // The key columns of each case, and whether its keys 3 and 4 are one key.
    let cases: Vec<(Vec<(&'static str, ColumnType, Make)>, bool)> = vec![
        (vec![("K", ColumnType::String, text)], false),
        (vec![("K", ColumnType::Double, double)], true),
        (vec![("K", ColumnType::Float, float)], true),
        (
            vec![
                ("K", ColumnType::Double, double),
                ("L", ColumnType::String, text),
            ],
            false,
        ),
        (
            vec![
                ("K", ColumnType::Boolean, flag),
                ("L", ColumnType::Float, float),
            ],
            true,
        ),
    ];
    for (key_columns, same) in &cases {
        let names: Vec<&str> = key_columns.iter().map(|(name, _, _)| *name).collect();
        let name = format!("{names:?} of {:?}", key_columns[0].1);
        let columns = |picks: &[usize], values: &[i64]| {
            let mut columns: Vec<(&'static str, ColumnType, ArrayRef)> = key_columns
                .iter()
                .map(|(name, column_type, make)| (*name, column_type.clone(), make(picks)))
                .collect();
            columns.push(("V", ColumnType::Long, longs(values.iter().copied())));
            columns
        };
        let rows = |picks: &[usize], values: &[i64]| -> Vec<ArrayRef> {
            let columns = columns(picks, values).into_iter();
            columns.map(|(_, _, values)| values).collect()
        };
        let rekey = |pick: usize| -> Vec<(usize, ArrayRef)> {
            let columns = key_columns.iter().enumerate();
            columns
                .map(|(index, (_, _, make))| (index, make(&[pick])))
                .collect()
        };
        let on = on(&names);
        let build = move |left: &Arc<dyn Table>, right: &Arc<dyn Table>| {
            let add = [JoinAddition::renamed("RV", "V")];
            natural_join(left, right, &on, &add, ErrorOnDuplicate).unwrap()
        };
        // Left rows with the keys null, 1, 2 and 3; right rows with the keys 2 and 4.
        let pair = Pair::new(
            Side::Live(&[10, 20, 30, 40], &columns(&[0, 1, 2, 3], &[0, 0, 0, 0])),
            Side::Live(&[5, 6], &columns(&[2, 4], &[200, 400])),
            build,
        );
        let rv = || column(&*pair.result, "RV");
        let of_four = |value: &'static str| if *same { value } else { "null" };
        assert_eq!(rv(), ["null", "200", "200", of_four("400")], "{name}");

        // The null key arrives on the right, and the right row of key 2 is replaced by
        // one of key 1 at another row key.
        let change = TableChange::new()
            .remove([5])
            .add([7, 8], rows(&[0, 1], &[1, 100]));
        let update = pair.right(change).unwrap();
        assert_eq!(keys(&update.modified), [10, 20, 30], "{name}");
        assert_eq!(rv(), ["1", "100", "100", of_four("400")], "{name}");

        // A left row is written with the other form of its key: the key is the same.
        let update = pair
            .left(TableChange::new().modify([20], rekey(2)))
            .unwrap();
        let key_indices = 0..key_columns.len();
        assert_eq!(
            update.modified_columns,
            ModifiedColumnSet::of(key_indices),
            "{name}"
        );
        // Then it takes the null key, and a row with key 4 arrives.
        pair.left(
            TableChange::new()
                .modify([20], rekey(0))
                .add([50], rows(&[4], &[0])),
        );
        assert_eq!(rv(), ["1", "1", "100", of_four("400"), "400"], "{name}");
        pair.right(
            TableChange::new()
                .remove([6, 7])
                .add([9], rows(&[3], &[300])),
        );
        assert_eq!(
            rv(),
            ["null", "null", "100", "300", of_four("300")],
            "{name}"
        );
    }
}
