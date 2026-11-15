use super::testing::{
    blink_table, cells, check_random, graph, keys, keys_of, operation, shifts, static_table, tags,
    Ticking,
};
use super::{head, reverse, slice, sort, tail, SortColumn, SortError};
use crate::ops::merge::flatten;
use crate::table::is_copy;
use arrow::array::{
    ArrayRef, BooleanArray, Float32Array, Float64Array, Int32Array, Int8Array, StringArray,
    TimestampNanosecondArray, UInt16Array,
};
use oxhaven_contracts::rowset::{RowRange, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_testkit::{ints, strings, ConsistencyChecker, TableChange};
use std::sync::Arc;

const LETTERS: [&str; 10] = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];

/// A static table of `values` in column `K` and the letters `a`, `b`, ... in column `Tag`.
fn tagged(column_type: ColumnType, values: ArrayRef) -> Arc<dyn Table> {
    let letters = strings(LETTERS[..values.len()].iter().copied());
    static_table(&[
        ("K", column_type, values),
        ("Tag", ColumnType::String, letters),
    ])
}

fn int_values(values: &[Option<i32>]) -> ArrayRef {
    Arc::new(Int32Array::from(values.to_vec()))
}

fn sorted_tags(table: &Arc<dyn Table>, columns: &[SortColumn]) -> String {
    tags(&*sort(table, columns).unwrap()).concat()
}

fn ascending() -> [SortColumn; 1] {
    [SortColumn::ascending("K")]
}

fn descending() -> [SortColumn; 1] {
    [SortColumn::descending("K")]
}

fn absolute() -> [SortColumn; 1] {
    [SortColumn::ascending("K").absolute()]
}

// ---- The order of values -------------------------------------------------------------------

#[test]
fn ints_sort_with_nulls_first_and_ties_in_parent_order() {
    let table = tagged(
        ColumnType::Int,
        int_values(&[Some(3), None, Some(1), Some(3), None, Some(2)]),
    );
    assert_eq!(sorted_tags(&table, &ascending()), "becfad");
    // Descending is the negated comparison, not the reverse: ties stay in parent order.
    assert_eq!(sorted_tags(&table, &descending()), "adfcbe");
}

#[test]
fn doubles_sort_with_nan_greatest_and_one_zero() {
    let values = [
        Some(f64::NAN),
        Some(1.0),
        None,
        Some(f64::NEG_INFINITY),
        Some(f64::INFINITY),
        Some(0.0),
        Some(-0.0),
        Some(-1.0),
        // A NaN with another bit pattern is the same value.
        Some(f64::from_bits(0xfff8_0000_0000_0001)),
    ];
    let table = tagged(
        ColumnType::Double,
        Arc::new(Float64Array::from(values.to_vec())),
    );
    assert_eq!(sorted_tags(&table, &ascending()), "cdhfgbeai");
    assert_eq!(sorted_tags(&table, &descending()), "aiebfghdc");
}

#[test]
fn floats_sort_like_doubles() {
    let values = [Some(f32::NAN), Some(0.0), Some(-0.0), None, Some(-1.5)];
    let table = tagged(
        ColumnType::Float,
        Arc::new(Float32Array::from(values.to_vec())),
    );
    assert_eq!(sorted_tags(&table, &ascending()), "debca");
}

#[test]
fn strings_sort_by_utf16_code_units() {
    // U+10000 is the surrogate pair D800 DC00 in UTF-16, which is below U+E000; in UTF-8
    // and by code point it is above.
    let values = [
        Some("b"),
        Some(""),
        None,
        Some("B"),
        Some("\u{e000}"),
        Some("\u{10000}"),
        Some("a"),
        Some("ab"),
    ];
    let table = tagged(
        ColumnType::String,
        Arc::new(StringArray::from(values.to_vec())),
    );
    assert_eq!(sorted_tags(&table, &ascending()), "cbdghafe");
    assert_eq!(sorted_tags(&table, &descending()), "efahgdbc");
}

#[test]
fn chars_booleans_bytes_and_instants_sort_with_nulls_first() {
    let chars = [Some(b'b' as u16), None, Some(b'A' as u16), Some(0x3b1)];
    let table = tagged(
        ColumnType::Char,
        Arc::new(UInt16Array::from(chars.to_vec())),
    );
    assert_eq!(sorted_tags(&table, &ascending()), "bcad");

    let booleans = [Some(true), None, Some(false), Some(true)];
    let table = tagged(
        ColumnType::Boolean,
        Arc::new(BooleanArray::from(booleans.to_vec())),
    );
    assert_eq!(sorted_tags(&table, &ascending()), "bcad");
    assert_eq!(sorted_tags(&table, &descending()), "adcb");

    let bytes = [Some(5i8), Some(-7), None, Some(127)];
    let table = tagged(ColumnType::Byte, Arc::new(Int8Array::from(bytes.to_vec())));
    assert_eq!(sorted_tags(&table, &ascending()), "cbad");

    let instants = [Some(2_000i64), None, Some(-5), Some(1_000)];
    let instants = TimestampNanosecondArray::from(instants.to_vec()).with_timezone("UTC");
    let table = tagged(ColumnType::Instant, Arc::new(instants));
    assert_eq!(sorted_tags(&table, &ascending()), "bcda");
}

#[test]
fn later_columns_break_ties_each_in_its_own_direction() {
    let table = static_table(&[
        ("A", ColumnType::Int, ints([2, 1, 2, 1, 2])),
        ("B", ColumnType::String, strings(["x", "y", "z", "x", "x"])),
        (
            "Tag",
            ColumnType::String,
            strings(["a", "b", "c", "d", "e"]),
        ),
    ]);
    let by = |columns: &[SortColumn]| sorted_tags(&table, columns);
    assert_eq!(
        by(&[SortColumn::ascending("A"), SortColumn::descending("B")]),
        "bdcae"
    );
    assert_eq!(
        by(&[SortColumn::descending("A"), SortColumn::ascending("B")]),
        "aecdb"
    );
    assert_eq!(
        by(&[SortColumn::ascending("B"), SortColumn::ascending("A")]),
        "daebc"
    );
    // The same column twice: the first use decides.
    assert_eq!(
        by(&[SortColumn::descending("A"), SortColumn::ascending("A")]),
        "acebd"
    );
}

#[test]
fn an_absolute_sort_ignores_the_sign() {
    let table = tagged(
        ColumnType::Int,
        int_values(&[Some(-3), Some(2), None, Some(1), Some(-1), Some(i32::MIN)]),
    );
    // The minimum value is Java's null, and its absolute value is null.
    assert_eq!(sorted_tags(&table, &absolute()), "cfdeba");
    assert_eq!(
        sorted_tags(&table, &[SortColumn::descending("K").absolute()]),
        "abdecf"
    );

    let bytes = [Some(-128i8), Some(-2), Some(1)];
    let table = tagged(ColumnType::Byte, Arc::new(Int8Array::from(bytes.to_vec())));
    assert_eq!(sorted_tags(&table, &absolute()), "acb");

    let doubles = [
        Some(-2.5),
        Some(f64::NAN),
        Some(1.0),
        Some(-0.0),
        None,
        Some(-1.0),
    ];
    let table = tagged(
        ColumnType::Double,
        Arc::new(Float64Array::from(doubles.to_vec())),
    );
    assert_eq!(sorted_tags(&table, &absolute()), "edcfab");
}

#[test]
fn an_absolute_sort_of_a_type_without_abs_is_an_error() {
    for (column_type, values) in [
        (ColumnType::String, strings(["a"])),
        (
            ColumnType::Boolean,
            Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
        ),
    ] {
        let table = tagged(column_type.clone(), values);
        match sort(&table, &absolute()) {
            Err(SortError::AbsoluteNotSupported {
                column,
                column_type: found,
            }) => {
                assert_eq!(column, "K");
                assert_eq!(found, column_type);
            }
            other => panic!("expected an error, found {:?}", other.map(|_| ())),
        }
    }
}

#[test]
fn unknown_sort_columns_are_all_named() {
    let table = tagged(ColumnType::Int, ints([1]));
    let columns = [
        SortColumn::ascending("X"),
        SortColumn::ascending("K"),
        SortColumn::descending("Y"),
    ];
    let error = sort(&table, &columns).map(|_| ()).unwrap_err();
    assert_eq!(error, SortError::NoSuchColumn(vec!["X".into(), "Y".into()]));
    assert_eq!(error.to_string(), "Unknown column names [X, Y]");
}

#[test]
fn a_static_sort_is_flat_or_the_parent_itself() {
    let table = tagged(ColumnType::Int, ints([2, 1, 3]));
    let sorted = sort(&table, &ascending()).unwrap();
    assert!(sorted.is_flat());
    assert!(!sorted.is_refreshing());
    assert_eq!(keys_of(&*sorted), [0, 1, 2]);
    assert_eq!(cells(&*sorted, 0), ["1", "2", "3"]);

    // Without sort columns, and when the rows are in order already.
    assert!(Arc::ptr_eq(&sort(&table, &[]).unwrap(), &table));
    assert!(Arc::ptr_eq(&sort(&sorted, &ascending()).unwrap(), &sorted));
    assert!(!Arc::ptr_eq(
        &sort(&sorted, &descending()).unwrap(),
        &sorted
    ));

    let empty = tagged(ColumnType::Int, ints([]));
    assert_eq!(sort(&empty, &ascending()).unwrap().row_set().size(), 0);
}

// ---- Incremental sort ----------------------------------------------------------------------

fn source_columns() -> [(&'static str, ColumnType, ArrayRef); 2] {
    [
        ("K", ColumnType::Int, ints([3, 1, 3, 2])),
        ("Tag", ColumnType::String, strings(["a", "b", "c", "d"])),
    ]
}

/// The rows a..d with K = 3, 1, 3, 2 at the keys 10, 20, 30, 40, sorted by K: `bdac`.
fn ticking_sort() -> Ticking {
    let ticking = Ticking::new([10, 20, 30, 40], &source_columns(), |table| {
        sort(table, &ascending()).unwrap()
    });
    assert_eq!(tags(&*ticking.result).concat(), "bdac");
    assert!(ticking.result.is_refreshing());
    // Java does not mark the result of an incremental sort flat.
    assert!(!ticking.result.is_flat());
    ticking
}

fn rows(k: impl IntoIterator<Item = i32>, tag: &[&str]) -> Vec<ArrayRef> {
    vec![ints(k), strings(tag.iter().copied())]
}

#[test]
fn added_rows_are_placed_by_key_and_after_earlier_ties() {
    let t = ticking_sort();
    let update = t
        .step(TableChange::new().add([5, 25], rows([0, 3], &["f", "e"])))
        .unwrap();
    // Key 25 lies between the equal rows at 10 and 30.
    assert_eq!(tags(&*t.result).concat(), "fbdaec");
    assert_eq!(keys(&update.added), [0, 4]);
    assert!(update.removed.is_empty() && update.modified.is_empty());
    assert_eq!(shifts(&update), [(0, 2, 1), (3, 3, 2)]);
}

#[test]
fn removed_rows_leave_and_later_rows_close_the_gap() {
    let t = ticking_sort();
    let update = t.step(TableChange::new().remove([20])).unwrap();
    assert_eq!(tags(&*t.result).concat(), "dac");
    assert_eq!(keys(&update.removed), [0]);
    assert!(update.added.is_empty() && update.modified.is_empty());
    assert_eq!(shifts(&update), [(1, 3, -1)]);
}

#[test]
fn a_modified_column_that_is_not_sorted_by_keeps_the_row_in_place() {
    let t = ticking_sort();
    let update = t
        .step(TableChange::new().modify([30], [(1, strings(["z"]))]))
        .unwrap();
    assert_eq!(tags(&*t.result).concat(), "bdaz");
    assert_eq!(keys(&update.modified), [3]);
    assert!(update.added.is_empty() && update.removed.is_empty());
    assert!(update.shifted.is_empty());
    assert!(update.modified_columns.contains(1) && !update.modified_columns.contains(0));
}

/// Java looks for the row's place among the previous values, the row's own included, and
/// so finds a place before the row: `fixtures/sort_modified_rows.json`, step 1.
#[test]
fn a_sort_value_that_moves_before_its_old_one_is_a_remove_and_an_add_even_in_place() {
    let t = ticking_sort();
    // d at key 40 goes from 2 to 1: equal to b at key 20, and still after it.
    let update = t
        .step(TableChange::new().modify([40], [(0, ints([1]))]))
        .unwrap();
    assert_eq!(tags(&*t.result).concat(), "bdac");
    assert_eq!(cells(&*t.result, 0), ["1", "1", "3", "3"]);
    assert_eq!(keys(&update.removed), [1]);
    assert_eq!(keys(&update.added), [1]);
    assert!(update.modified.is_empty());
    assert!(update.shifted.is_empty());
}

/// `fixtures/sort_modified_rows.json`, steps 2 and 3.
#[test]
fn a_sort_value_that_moves_after_its_old_one_or_stays_is_a_modify_in_place() {
    let t = ticking_sort();
    t.step(TableChange::new().modify([40], [(0, ints([1]))]));
    for value in [2, 2] {
        // d goes from 1 back to 2, still before a, and then from 2 to 2.
        let update = t
            .step(TableChange::new().modify([40], [(0, ints([value]))]))
            .unwrap();
        assert_eq!(tags(&*t.result).concat(), "bdac");
        assert_eq!(cells(&*t.result, 0), ["1", "2", "3", "3"]);
        assert_eq!(keys(&update.modified), [1]);
        assert!(update.added.is_empty() && update.removed.is_empty());
        assert!(update.shifted.is_empty());
        assert!(update.modified_columns.contains(0));
    }
}

/// `fixtures/sort_modified_rows.json`, step 7.
#[test]
fn a_row_added_between_the_old_and_new_value_of_a_row_in_place_moves_that_row() {
    let t = ticking_sort();
    // c at key 30 goes from 3 to 9 and stays last; e comes between its old and new value.
    let change = TableChange::new()
        .add([50], rows([5], &["e"]))
        .modify([30], [(0, ints([9]))]);
    let update = t.step(change).unwrap();
    assert_eq!(tags(&*t.result).concat(), "bdaec");
    assert_eq!(keys(&update.removed), [3]);
    assert_eq!(keys(&update.added), [3, 4]);
    assert!(update.modified.is_empty());
}

/// `fixtures/sort_modified_rows.json`, step 8.
#[test]
fn a_row_that_stays_is_a_modify_beside_a_row_that_moves() {
    let t = ticking_sort();
    // a goes to the front, c from 3 to 4 stays where it is.
    let update = t
        .step(TableChange::new().modify([10, 30], [(0, ints([0, 4]))]))
        .unwrap();
    assert_eq!(tags(&*t.result).concat(), "abdc");
    assert_eq!(keys(&update.removed), [2]);
    assert_eq!(keys(&update.added), [0]);
    assert_eq!(keys(&update.modified), [3]);
}

#[test]
fn a_modified_sort_value_that_moves_is_a_remove_and_an_add() {
    let t = ticking_sort();
    let update = t
        .step(TableChange::new().modify([20], [(0, ints([5]))]))
        .unwrap();
    assert_eq!(tags(&*t.result).concat(), "dacb");
    assert_eq!(keys(&update.removed), [0]);
    assert_eq!(keys(&update.added), [3]);
    assert!(update.modified.is_empty());
    assert_eq!(shifts(&update), [(1, 3, -1)]);
}

/// `fixtures/sort_modified_rows.json`, step 6.
#[test]
fn rows_that_swap_places_are_both_removed_and_added() {
    let t = ticking_sort();
    // b and d exchange their values: d, b, a, c.
    let update = t
        .step(TableChange::new().modify([20, 40], [(0, ints([2, 1]))]))
        .unwrap();
    assert_eq!(tags(&*t.result).concat(), "dbac");
    // d goes to the front. b finds its place at itself, but d, queued just before it,
    // goes to that place too, so b follows it.
    assert!(update.modified.is_empty());
    assert_eq!(keys(&update.removed), [0, 1]);
    assert_eq!(keys(&update.added), [0, 1]);
}

#[test]
fn a_shift_of_the_parent_changes_nothing_in_the_result() {
    let t = ticking_sort();
    assert!(t.step(TableChange::new().shift(30, 40, 5)).is_none());
    assert_eq!(tags(&*t.result).concat(), "bdac");
    // The result follows the rows to their new keys.
    let update = t
        .step(TableChange::new().modify([45], [(1, strings(["z"]))]))
        .unwrap();
    assert_eq!(tags(&*t.result).concat(), "bzac");
    assert_eq!(keys(&update.modified), [1]);
}

#[test]
fn removes_shifts_adds_and_modifies_in_one_update() {
    let t = ticking_sort();
    let change = TableChange::new()
        .remove([10])
        .shift(30, 40, -5)
        .add([40], rows([2], &["e"]))
        .modify([25], [(0, ints([0]))]);
    let update = t.step(change).unwrap();
    // 20 b 1, 25 c 0, 35 d 2, 40 e 2.
    assert_eq!(tags(&*t.result).concat(), "cbde");
    assert_eq!(keys(&update.removed), [2, 3]);
    assert_eq!(keys(&update.added), [0, 3]);
    assert!(update.modified.is_empty());
    assert_eq!(shifts(&update), [(0, 1, 1)]);
}

#[test]
fn an_empty_cycle_delivers_nothing() {
    let t = ticking_sort();
    assert!(t.step(TableChange::new()).is_none());
    assert_eq!(tags(&*t.result).concat(), "bdac");
}

#[test]
fn a_sort_fails_with_its_parent() {
    let t = ticking_sort();
    let failure = t.fail("boom");
    assert!(failure.message.contains("boom"), "{failure}");
    assert!(t.result.is_failed());
    // A failed table cannot be sorted.
    let source: Arc<dyn Table> = t.script.table();
    for table in [&source, &t.result] {
        assert!(matches!(
            sort(table, &descending()),
            Err(SortError::Table(_))
        ));
        assert!(matches!(head(table, 1), Err(SortError::Table(_))));
        assert!(matches!(reverse(table), Err(SortError::Table(_))));
    }
    assert!(matches!(
        sort(&source, &ascending()),
        Err(SortError::Table(_))
    ));
    // By the one column it is sorted by, the result is returned as it is, before Java
    // looks at anything else (QueryTable#sort and LiveAttributeMap#prepareReturnThis).
    let again = sort(&t.result, &ascending()).unwrap();
    assert!(Arc::ptr_eq(&again, &t.result));
}

#[test]
fn everything_removed_and_added_again() {
    let t = ticking_sort();
    let update = t.step(TableChange::new().remove([10, 20, 30, 40])).unwrap();
    assert_eq!(keys(&update.removed), [0, 1, 2, 3]);
    assert_eq!(t.result.row_set().size(), 0);
    let update = t
        .step(TableChange::new().add([7, 8], rows([9, 4], &["x", "y"])))
        .unwrap();
    assert_eq!(keys(&update.added), [0, 1]);
    assert_eq!(tags(&*t.result).concat(), "yx");
}

// ---- Blink tables --------------------------------------------------------------------------

const BLINK_COLUMNS: [(&str, ColumnType); 2] =
    [("K", ColumnType::Int), ("Tag", ColumnType::String)];

#[test]
fn the_sort_of_a_blink_table_is_each_cycle_sorted() {
    let graph = graph();
    let (table, blink) = blink_table(&graph, &BLINK_COLUMNS);
    let sorted = sort(&table, &descending()).unwrap();
    assert!(sorted.attributes().blink);
    assert!(sorted.is_flat());
    let checker = ConsistencyChecker::attach(sorted.clone());

    blink.push(rows([2, 3, 1, 3], &["a", "b", "c", "d"]));
    graph.run_cycle();
    assert_eq!(tags(&*sorted).concat(), "bdac");
    blink.push(rows([5, 6], &["e", "f"]));
    graph.run_cycle();
    assert_eq!(tags(&*sorted).concat(), "fe");
    graph.run_cycle();
    assert_eq!(sorted.row_set().size(), 0);
    graph.run_cycle();
    blink.push(rows([1], &["g"]));
    graph.run_cycle();
    assert_eq!(tags(&*sorted).concat(), "g");
    checker.assert_consistent();
    assert_eq!(checker.updates_seen(), 4);
}

#[test]
fn the_head_of_a_blink_table_keeps_the_first_rows_it_ever_showed() {
    let graph = graph();
    let (table, blink) = blink_table(&graph, &BLINK_COLUMNS);
    let first = head(&table, 3).unwrap();
    let attributes = first.attributes();
    assert!(attributes.add_only && attributes.append_only && !attributes.blink);
    assert!(first.is_flat());
    let checker = ConsistencyChecker::attach(first.clone());

    blink.push(rows([1, 2], &["a", "b"]));
    graph.run_cycle();
    assert_eq!(tags(&*first).concat(), "ab");
    blink.push(rows([3, 4], &["c", "d"]));
    graph.run_cycle();
    assert_eq!(tags(&*first).concat(), "abc");
    blink.push(rows([5], &["e"]));
    graph.run_cycle();
    graph.run_cycle();
    assert_eq!(tags(&*first).concat(), "abc");
    assert_eq!(keys_of(&*first), [0, 1, 2]);
    checker.assert_consistent();
    assert_eq!(checker.updates_seen(), 2);

    // Started while the blink table shows rows, the head begins with them.
    blink.push(rows([6, 7], &["f", "g"]));
    graph.run_cycle();
    assert_eq!(tags(&*head(&table, 1).unwrap()).concat(), "f");
}

#[test]
fn the_tail_of_a_blink_table_keeps_the_latest_rows() {
    let graph = graph();
    let (table, blink) = blink_table(&graph, &BLINK_COLUMNS);
    let last = tail(&table, 3).unwrap();
    let attributes = last.attributes();
    assert!(!attributes.add_only && !attributes.append_only && !attributes.blink);
    let checker = ConsistencyChecker::attach(last.clone());

    blink.push(rows([1, 2], &["a", "b"]));
    graph.run_cycle();
    assert_eq!(tags(&*last).concat(), "ab");
    // The rows stay when the blink table drops them.
    graph.run_cycle();
    assert_eq!(tags(&*last).concat(), "ab");
    blink.push(rows([3, 4], &["c", "d"]));
    graph.run_cycle();
    assert_eq!(tags(&*last).concat(), "bcd");
    assert_eq!(keys_of(&*last), [1, 2, 3]);
    // More rows in one cycle than the tail holds.
    blink.push(rows([5, 6, 7, 8, 9], &["e", "f", "g", "h", "i"]));
    graph.run_cycle();
    assert_eq!(tags(&*last).concat(), "ghi");
    assert_eq!(keys_of(&*last), [6, 7, 8]);
    checker.assert_consistent();
    assert_eq!(checker.updates_seen(), 3);
}

#[test]
fn a_blink_table_has_no_slice_and_an_empty_head_is_static() {
    let graph = graph();
    let (table, _blink) = blink_table(&graph, &BLINK_COLUMNS);
    match slice(&table, 0, 2) {
        Err(SortError::Unsupported(message)) => assert!(message.contains("Blink tables")),
        other => panic!("expected an error, found {:?}", other.map(|_| ())),
    }
    for empty in [head(&table, 0).unwrap(), tail(&table, 0).unwrap()] {
        assert!(!empty.is_refreshing());
        assert_eq!(empty.row_set().size(), 0);
    }
    assert!(matches!(
        tail(&table, i32::MAX as i64),
        Err(SortError::IllegalArgument(_))
    ));
    // A reversed blink table is a blink table.
    assert!(reverse(&table).unwrap().attributes().blink);
}

// ---- head, tail, slice, reverse ------------------------------------------------------------

fn five() -> Arc<dyn Table> {
    static_table(&[(
        "Tag",
        ColumnType::String,
        strings(["a", "b", "c", "d", "e"]),
    )])
}

#[test]
fn head_tail_and_slice_of_a_static_table() {
    let table = five();
    let of = |result: Result<Arc<dyn Table>, SortError>| tags(&*result.unwrap()).concat();
    assert_eq!(of(head(&table, 2)), "ab");
    assert_eq!(of(head(&table, 9)), "abcde");
    assert_eq!(of(tail(&table, 2)), "de");
    assert_eq!(of(tail(&table, 9)), "abcde");
    assert_eq!(of(slice(&table, 1, 3)), "bc");
    assert_eq!(of(slice(&table, 2, -1)), "cd");
    assert_eq!(of(slice(&table, -3, -1)), "cd");
    assert_eq!(of(slice(&table, -3, 0)), "cde");
    assert_eq!(of(slice(&table, 3, 99)), "de");
    assert_eq!(of(slice(&table, 4, -4)), "");
    assert_eq!(of(slice(&table, 2, 2)), "");
    assert_eq!(of(head(&table, 0)), "");
    assert_eq!(of(tail(&table, 0)), "");
    assert!(!head(&table, 2).unwrap().is_refreshing());
}

#[test]
fn sizes_and_positions_that_are_refused() {
    let table = five();
    let message = |result: Result<Arc<dyn Table>, SortError>| match result {
        Err(SortError::IllegalArgument(message)) => message,
        other => panic!("expected an error, found {:?}", other.map(|_| ())),
    };
    assert_eq!(message(head(&table, -1)), "size must be >= 0 (found: -1)");
    assert_eq!(message(tail(&table, -2)), "size must be >= 0 (found: -2)");
    assert_eq!(
        message(slice(&table, 3, 1)),
        "Can not slice with a first position (3) after last position (1)"
    );
    assert!(message(slice(&table, -1, -3)).starts_with("Can not slice"));
}

#[test]
fn the_attributes_of_a_slice() {
    assert!(!head(&five(), 2).unwrap().is_flat());
    let table = flatten(&five()).unwrap();
    assert!(table.is_flat());
    // A slice from the first row of a flat table is flat.
    assert!(head(&table, 2).unwrap().is_flat());
    assert!(slice(&table, 0, -1).unwrap().is_flat());
    assert!(!tail(&table, 2).unwrap().is_flat());
    assert!(!slice(&table, 1, 3).unwrap().is_flat());

    let t = Ticking::new([10, 20], &source_columns_of(2), |table| {
        head(table, 1).unwrap()
    });
    let attributes = t.result.attributes();
    assert!(!attributes.add_only && !attributes.append_only && !attributes.blink);
    assert!(!t.result.is_flat());
}

fn source_columns_of(size: usize) -> [(&'static str, ColumnType, ArrayRef); 2] {
    [
        ("K", ColumnType::Int, ints(0..size as i32)),
        (
            "Tag",
            ColumnType::String,
            strings(LETTERS[..size].iter().copied()),
        ),
    ]
}

fn ticking_slice(
    build: impl Fn(&Arc<dyn Table>) -> Arc<dyn Table> + Send + Sync + 'static,
) -> Ticking {
    Ticking::new([10, 20, 30, 40], &source_columns_of(4), build)
}

#[test]
fn a_head_follows_its_parent() {
    let t = ticking_slice(|table| head(table, 2).unwrap());
    assert_eq!(tags(&*t.result).concat(), "ab");

    let update = t
        .step(TableChange::new().add([5], rows([9], &["e"])))
        .unwrap();
    assert_eq!(tags(&*t.result).concat(), "ea");
    assert_eq!(
        (keys(&update.added), keys(&update.removed)),
        (vec![5], vec![20])
    );

    // A row after the head changes nothing.
    assert!(t
        .step(TableChange::new().add([50], rows([9], &["f"])))
        .is_none());
    assert!(t.step(TableChange::new().remove([40])).is_none());
    assert!(t
        .step(TableChange::new().modify([30], [(1, strings(["z"]))]))
        .is_none());

    let update = t.step(TableChange::new().remove([5])).unwrap();
    assert_eq!(tags(&*t.result).concat(), "ab");
    assert_eq!(
        (keys(&update.added), keys(&update.removed)),
        (vec![20], vec![5])
    );

    let update = t.step(TableChange::new().shift(10, 50, 100)).unwrap();
    assert_eq!(keys_of(&*t.result), [110, 120]);
    assert!(update.added.is_empty() && update.removed.is_empty());
    assert!(!update.shifted.is_empty());

    let update = t
        .step(TableChange::new().modify([110, 150], [(1, strings(["y", "x"]))]))
        .unwrap();
    assert_eq!(keys(&update.modified), [110]);
    assert_eq!(tags(&*t.result).concat(), "yb");
}

#[test]
fn a_shift_can_carry_a_row_out_of_a_head() {
    let t = ticking_slice(|table| head(table, 2).unwrap());
    // 20 moves to 25 and a new row takes its place in the first two.
    let change = TableChange::new()
        .shift(20, 20, 5)
        .add([15], rows([9], &["e"]));
    let update = t.step(change).unwrap();
    assert_eq!(tags(&*t.result).concat(), "ae");
    // The row that left is named by the key it had.
    assert_eq!(
        (keys(&update.added), keys(&update.removed)),
        (vec![15], vec![20])
    );
}

#[test]
fn a_tail_follows_its_parent() {
    let t = ticking_slice(|table| tail(table, 2).unwrap());
    assert_eq!(tags(&*t.result).concat(), "cd");
    let update = t
        .step(TableChange::new().add([50], rows([9], &["e"])))
        .unwrap();
    assert_eq!(tags(&*t.result).concat(), "de");
    assert_eq!(
        (keys(&update.added), keys(&update.removed)),
        (vec![50], vec![30])
    );
    assert!(t.step(TableChange::new().remove([10])).is_none());
    let update = t.step(TableChange::new().remove([40, 50])).unwrap();
    assert_eq!(tags(&*t.result).concat(), "bc");
    assert_eq!(keys(&update.added), [20, 30]);
    assert_eq!(keys(&update.removed), [40, 50]);
    // Fewer rows than the size.
    t.step(TableChange::new().remove([20])).unwrap();
    assert_eq!(tags(&*t.result).concat(), "c");
}

#[test]
fn a_slice_counted_from_both_ends_follows_its_parent() {
    let t = ticking_slice(|table| slice(table, 1, -1).unwrap());
    assert_eq!(tags(&*t.result).concat(), "bc");
    let update = t
        .step(TableChange::new().add([50], rows([9], &["e"])))
        .unwrap();
    assert_eq!(tags(&*t.result).concat(), "bcd");
    assert_eq!(keys(&update.added), [40]);
    let update = t.step(TableChange::new().remove([10])).unwrap();
    assert_eq!(tags(&*t.result).concat(), "cd");
    assert_eq!(keys(&update.removed), [20]);
}

#[test]
fn slices_fail_with_their_parent() {
    let t = ticking_slice(|table| slice(table, 1, 3).unwrap());
    t.fail("boom");
    assert!(t.result.is_failed());
}

/// The Java server shows the difference in what follows a `reverse`: the `flatten` of one
/// is a new table and not a copy, and an `update` of a join with one is not flat
/// (`oracle/bin/harness fuzz`, seeds 1381 and 1622 over the default operations).
#[test]
fn the_reverse_of_a_static_table_has_the_row_keys_java_gives_it() {
    let reversed = reverse(&five()).unwrap();
    // The row keys 0..=4 as they lie below the pivot 65536.
    let rows = RowSet::from_ranges([RowRange::new(65532, 65536)]);
    assert_eq!(*reversed.row_set(), rows);
    assert_eq!(tags(&*reversed).concat(), "edcba");

    let flat = flatten(&reversed).unwrap();
    assert!(flat.is_flat() && !is_copy(&*flat));
    assert_eq!(*flat.row_set(), RowSet::flat(5));
    assert_eq!(tags(&*flat).concat(), "edcba");

    // Rows at keys with gaps keep the gaps, and a reverse of a reverse counts from a
    // larger pivot: 2 * 65536 - 1.
    let some = slice(&five(), 1, 4).unwrap();
    let reversed = reverse(&some).unwrap();
    assert_eq!(keys(&reversed.row_set()), [65533, 65534, 65535]);
    assert_eq!(tags(&*reversed).concat(), "dcb");
    let twice = reverse(&reversed).unwrap();
    assert_eq!(keys(&twice.row_set()), [65536, 65537, 65538]);
    assert_eq!(tags(&*twice).concat(), "bcd");

    let none = reverse(&head(&five(), 0).unwrap()).unwrap();
    assert!(none.row_set().is_empty());
}

#[test]
fn reverse_shows_the_last_row_first() {
    let table = five();
    let reversed = reverse(&table).unwrap();
    assert_eq!(tags(&*reversed).concat(), "edcba");
    assert!(!reversed.is_flat());
    assert!(!reversed.is_refreshing());

    let t = ticking_slice(|table| reverse(table).unwrap());
    assert_eq!(tags(&*t.result).concat(), "dcba");
    assert!(!t.result.is_flat());
    let update = t
        .step(TableChange::new().add([5, 50], rows([8, 9], &["e", "f"])))
        .unwrap();
    assert_eq!(tags(&*t.result).concat(), "fdcbae");
    assert_eq!(update.added.size(), 2);
    let update = t
        .step(TableChange::new().modify([20], [(1, strings(["z"]))]))
        .unwrap();
    assert_eq!(update.modified.size(), 1);
    assert!(t.step(TableChange::new().shift(30, 50, 7)).is_none());
    t.step(TableChange::new().remove([10])).unwrap();
    t.fail("boom");
    assert!(t.result.is_failed());
}

// ---- Random updates ------------------------------------------------------------------------

const RANDOM_COLUMNS: [(&str, ColumnType); 6] = [
    ("K", ColumnType::Int),
    ("D", ColumnType::Double),
    ("S", ColumnType::String),
    ("B", ColumnType::Boolean),
    ("C", ColumnType::Char),
    ("T", ColumnType::Instant),
];

fn by(columns: &'static [(&'static str, bool)]) -> impl Fn(&Arc<dyn Table>) -> Arc<dyn Table> {
    move |table| {
        let columns: Vec<SortColumn> = columns
            .iter()
            .map(|(name, descending)| match descending {
                true => SortColumn::descending(*name),
                false => SortColumn::ascending(*name),
            })
            .collect();
        sort(table, &columns).unwrap()
    }
}

#[test]
fn random_updates_through_sorts() {
    let sorts: [(&str, &'static [(&'static str, bool)]); 8] = [
        ("K", &[("K", false)]),
        ("K desc", &[("K", true)]),
        ("D", &[("D", false)]),
        ("S desc, K", &[("S", true), ("K", false)]),
        ("B, C desc", &[("B", false), ("C", true)]),
        ("T, D desc", &[("T", false), ("D", true)]),
        ("C, B, K", &[("C", false), ("B", false), ("K", false)]),
        ("D desc", &[("D", true)]),
    ];
    let mut operations: Vec<_> = sorts
        .into_iter()
        .map(|(name, columns)| operation(name, move |tables| by(columns)(&tables[0])))
        .collect();
    operations.push(operation("abs K, S", |tables| {
        let columns = [
            SortColumn::ascending("K").absolute(),
            SortColumn::ascending("S"),
        ];
        sort(&tables[0], &columns).unwrap()
    }));
    operations.push(operation("abs D desc", |tables| {
        sort(&tables[0], &[SortColumn::descending("D").absolute()]).unwrap()
    }));
    operations.push(operation("sort of sort", |tables| {
        by(&[("S", false)])(&by(&[("K", true)])(&tables[0]))
    }));
    check_random(0..40, 30, 1, &RANDOM_COLUMNS, &operations);
}

#[test]
fn random_updates_through_slices() {
    let slices: [(&str, i64, i64); 9] = [
        ("head 3", 0, 3),
        ("head 1", 0, 1),
        ("tail 3", -3, 0),
        ("tail 1", -1, 0),
        ("slice 1 4", 1, 4),
        ("slice -4 -1", -4, -1),
        ("slice 2 -2", 2, -2),
        ("slice -3 5", -3, 5),
        ("slice 0 -1", 0, -1),
    ];
    let mut operations: Vec<_> = slices
        .into_iter()
        .map(|(name, first, last)| {
            operation(name, move |tables| slice(&tables[0], first, last).unwrap())
        })
        .collect();
    operations.push(operation("head", |tables| head(&tables[0], 4).unwrap()));
    operations.push(operation("tail", |tables| tail(&tables[0], 4).unwrap()));
    operations.push(operation("reverse", |tables| reverse(&tables[0]).unwrap()));
    operations.push(operation("reverse of reverse", |tables| {
        reverse(&reverse(&tables[0]).unwrap()).unwrap()
    }));
    check_random(0..40, 30, 1, &RANDOM_COLUMNS, &operations);
}

#[test]
fn random_updates_through_chains() {
    let operations = [
        operation("head of sort", |tables| {
            head(&by(&[("K", false)])(&tables[0]), 3).unwrap()
        }),
        operation("tail of sort", |tables| {
            tail(&by(&[("D", true), ("S", false)])(&tables[0]), 2).unwrap()
        }),
        operation("sort of head", |tables| {
            by(&[("S", false)])(&head(&tables[0], 4).unwrap())
        }),
        operation("sort of tail", |tables| {
            by(&[("K", true)])(&tail(&tables[0], 3).unwrap())
        }),
        operation("slice of reverse of sort", |tables| {
            let reversed = reverse(&by(&[("C", false)])(&tables[0])).unwrap();
            slice(&reversed, 1, -1).unwrap()
        }),
        operation("sort of flatten", |tables| {
            by(&[("B", false), ("K", false)])(&flatten(&tables[0]).unwrap())
        }),
        operation("flatten of sort", |tables| {
            flatten(&by(&[("T", true)])(&tables[0])).unwrap()
        }),
        operation("head of flatten of tail", |tables| {
            head(&flatten(&tail(&tables[0], 4).unwrap()).unwrap(), 2).unwrap()
        }),
    ];
    check_random(0..40, 30, 1, &RANDOM_COLUMNS, &operations);
}
