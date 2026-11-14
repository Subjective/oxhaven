//! Unit tests of the range join: the range match as text, every refusal and their order,
//! and the columns of the result at the seam. The ranges themselves are compared with
//! Java in `corpus.rs`.

use super::grouped::{join, Column, Joined};
use super::{
    range_join, RangeAggregation, RangeEndRule, RangeJoinError, RangeJoinMatch, RangeStartRule,
};
use crate::ops::agg::Pair;
use crate::ops::join::JoinMatch;
use crate::ops::sort::testing::{graph, scripted, static_table, Columns};
use crate::table::in_memory_table;
use arrow::array::{
    Array, ArrayRef, Float64Array, Int32Array, Int64Array, ListArray, RecordBatch, UInt16Array,
};
use arrow::datatypes::{Field, Schema};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_testkit::{definition, ints, longs, strings, ReferenceTable, TableSnapshot};
use std::sync::Arc;

fn opt_ints(values: &[Option<i32>]) -> ArrayRef {
    Arc::new(Int32Array::from(values.to_vec()))
}

/// A static table with the given columns at `keys`.
fn table_at(keys: impl IntoIterator<Item = RowKey>, columns: Columns) -> Arc<dyn Table> {
    let names: Vec<(&str, ColumnType)> = columns
        .iter()
        .map(|(name, column_type, _)| (*name, column_type.clone()))
        .collect();
    let values = columns.iter().map(|(_, _, values)| values.clone());
    ReferenceTable::new_static(
        definition(&names),
        RowSet::from_keys(keys),
        values.collect(),
    )
}

/// A left table `Id`, `K`, `Lo`, `Hi`, `F` (double).
fn left_columns(
    rows: &[(&str, Option<i32>, Option<i32>)],
) -> Vec<(&'static str, ColumnType, ArrayRef)> {
    let ids = 1..=rows.len() as i32;
    vec![
        ("Id", ColumnType::Int, ints(ids)),
        (
            "K",
            ColumnType::String,
            strings(rows.iter().map(|row| row.0)),
        ),
        (
            "Lo",
            ColumnType::Int,
            opt_ints(&rows.iter().map(|row| row.1).collect::<Vec<_>>()),
        ),
        (
            "Hi",
            ColumnType::Int,
            opt_ints(&rows.iter().map(|row| row.2).collect::<Vec<_>>()),
        ),
        (
            "F",
            ColumnType::Double,
            Arc::new(Float64Array::from_iter_values(
                (0..rows.len()).map(|row| row as f64),
            )),
        ),
    ]
}

/// A right table `K`, `P`, `R` (int, counting from 1), `N` (long, `P` again), `S`.
fn right_columns(rows: &[(&str, Option<i32>)]) -> Vec<(&'static str, ColumnType, ArrayRef)> {
    let ids = 1..=rows.len() as i32;
    let texts: Vec<String> = rows.iter().map(|row| format!("{:?}", row.1)).collect();
    vec![
        (
            "K",
            ColumnType::String,
            strings(rows.iter().map(|row| row.0)),
        ),
        (
            "P",
            ColumnType::Int,
            opt_ints(&rows.iter().map(|row| row.1).collect::<Vec<_>>()),
        ),
        ("R", ColumnType::Int, ints(ids)),
        (
            "N",
            ColumnType::Long,
            Arc::new(
                rows.iter()
                    .map(|row| row.1.map(i64::from))
                    .collect::<Int64Array>(),
            ),
        ),
        (
            "S",
            ColumnType::String,
            strings(texts.iter().map(String::as_str)),
        ),
    ]
}

fn left() -> Arc<dyn Table> {
    static_table(&left_columns(&[
        ("a", Some(1), Some(5)),
        ("a", Some(3), Some(3)),
        ("b", Some(0), Some(9)),
    ]))
}

fn right() -> Arc<dyn Table> {
    static_table(&right_columns(&[
        ("a", Some(1)),
        ("a", Some(3)),
        ("a", Some(5)),
        ("b", Some(2)),
    ]))
}

/// A right table whose bucket `a` is out of order at its second row.
fn descending_right() -> Arc<dyn Table> {
    static_table(&right_columns(&[
        ("a", Some(5)),
        ("a", Some(3)),
        ("b", Some(2)),
    ]))
}

fn on(columns: &[&str]) -> Vec<JoinMatch> {
    columns
        .iter()
        .map(|column| JoinMatch::same(*column))
        .collect()
}

fn inclusive() -> RangeJoinMatch {
    RangeJoinMatch::parse("Lo <= P <= Hi").unwrap()
}

fn group(pairs: &[(&str, &str)]) -> RangeAggregation {
    RangeAggregation::group(
        pairs
            .iter()
            .map(|(output, input)| Pair::new(*output, *input)),
    )
}

/// The refusal of a range join with the usual arguments.
fn refusal(
    left: &Arc<dyn Table>,
    right: &Arc<dyn Table>,
    exact: &[JoinMatch],
    range: &str,
    aggregations: &[RangeAggregation],
) -> RangeJoinError {
    let range = RangeJoinMatch::parse(range).unwrap();
    match range_join(left, right, exact, &range, aggregations) {
        Err(error) => error,
        Ok(_) => panic!("not refused"),
    }
}

/// The arrays of the `group` column `name`, at `rows` or at every row: `None` for a null
/// array, else the elements as text.
fn arrays(joined: &Joined, name: &str, rows: Option<&RowSet>) -> Vec<Option<Vec<String>>> {
    let column = joined.columns.iter().find(|(column, _)| column == name);
    let Some((_, Column::Group(group))) = column else {
        panic!("no group column {name}");
    };
    let all = joined.left.row_set();
    let lists = group.chunk(rows.unwrap_or(&all)).unwrap();
    let lists = lists.as_any().downcast_ref::<ListArray>().unwrap();
    assert_eq!(lists.value_type(), group.element_type().arrow_type());
    (0..lists.len())
        .map(|row| {
            (!lists.is_null(row)).then(|| {
                let elements = lists.value(row);
                let text = arrow::util::display::ArrayFormatter::try_new(
                    elements.as_ref(),
                    &arrow::util::display::FormatOptions::default().with_null("null"),
                )
                .unwrap();
                (0..elements.len())
                    .map(|element| text.value(element).to_string())
                    .collect()
            })
        })
        .collect()
}

fn some(elements: &[&str]) -> Option<Vec<String>> {
    Some(elements.iter().map(|element| element.to_string()).collect())
}

fn names(joined: &Joined) -> Vec<&str> {
    joined
        .columns
        .iter()
        .map(|(name, _)| name.as_str())
        .collect()
}

#[test]
fn a_range_match_is_read_from_text_and_written_back() {
    use RangeEndRule::{GreaterThan, GreaterThanOrEqual, GreaterThanOrEqualAllowFollowing};
    use RangeStartRule::{LessThan, LessThanOrEqual, LessThanOrEqualAllowPreceding};
    let cases = [
        ("Lo < P < Hi", LessThan, GreaterThan),
        ("Lo <= P < Hi", LessThanOrEqual, GreaterThan),
        ("Lo < P <= Hi", LessThan, GreaterThanOrEqual),
        ("Lo <= P <= Hi", LessThanOrEqual, GreaterThanOrEqual),
        (
            "<- Lo <= P <= Hi",
            LessThanOrEqualAllowPreceding,
            GreaterThanOrEqual,
        ),
        (
            "<- Lo <= P < Hi",
            LessThanOrEqualAllowPreceding,
            GreaterThan,
        ),
        (
            "Lo <= P <= Hi ->",
            LessThanOrEqual,
            GreaterThanOrEqualAllowFollowing,
        ),
        (
            "Lo < P <= Hi ->",
            LessThan,
            GreaterThanOrEqualAllowFollowing,
        ),
        (
            "<- Lo <= P <= Hi ->",
            LessThanOrEqualAllowPreceding,
            GreaterThanOrEqualAllowFollowing,
        ),
    ];
    for (text, start_rule, end_rule) in cases {
        let parsed = RangeJoinMatch::parse(text).unwrap();
        let built = RangeJoinMatch::new("Lo", start_rule, "P", end_rule, "Hi").unwrap();
        assert_eq!(parsed, built, "{text}");
        assert_eq!(parsed.to_string(), text);
        assert_eq!(
            (parsed.left_start(), parsed.right_range(), parsed.left_end()),
            ("Lo", "P", "Hi")
        );
        assert_eq!(
            (parsed.start_rule(), parsed.end_rule()),
            (start_rule, end_rule)
        );
    }
    // Spaces are free, also none at all and line breaks; names may hold `$` and `_`.
    let dense = RangeJoinMatch::parse("<-a$<=_b1<=C_->").unwrap();
    assert_eq!(dense.to_string(), "<- a$ <= _b1 <= C_ ->");
    let spread = RangeJoinMatch::parse(" \t<-  Lo\n<=  P <=\tHi  ->\r\n").unwrap();
    assert_eq!(spread.to_string(), "<- Lo <= P <= Hi ->");
}

#[test]
fn a_range_match_that_java_cannot_read_is_refused() {
    let refused = |text: &str| match RangeJoinMatch::parse(text) {
        Err(RangeJoinError::InvalidMatch(message)) => message,
        other => panic!("{text}: {other:?}"),
    };
    for text in [
        "",
        "Lo <= P",
        "Lo >= P >= Hi",
        "Lo <= P <= Hi <= X",
        "Lo = P = Hi",
        "Lo < = P <= Hi",
        "1Lo <= P <= Hi",
        "Lo <= P <= Hi -> x",
        "< - Lo <= P <= Hi",
        "Lo <= P <= Hi - >",
        "Lo <== P <= Hi",
        "L o <= P <= Hi",
        "Lo <= P <= Hi\u{a0}",
    ] {
        let message = refused(text);
        assert!(
            message.starts_with(&format!(
                "Input expression {text} does not match expected pattern"
            )),
            "{text}: {message}"
        );
    }
    assert_eq!(
        refused("<- Lo < P <= Hi"),
        "Error parsing expression <- Lo < P <= Hi: <- operator for \"allow preceding\" may \
         only be combined with <= start match expression"
    );
    assert_eq!(
        refused("Lo <= P < Hi ->"),
        "Error parsing expression Lo <= P < Hi ->: -> operator for \"allow following\" may \
         only be combined with <= end match expression"
    );
    // The first of the two is reported.
    assert!(refused("<- Lo < P < Hi ->").contains("allow preceding"));
    let same = "RangeJoinMatch must have distinct ColumnNames for leftStartColumn() and \
                leftEndColumn() in order to properly define a range, Lo specified for both fields";
    assert_eq!(refused("Lo <= P <= Lo"), same);
    let built = RangeJoinMatch::new(
        "Lo",
        RangeStartRule::LessThan,
        "P",
        RangeEndRule::GreaterThan,
        "Lo",
    );
    assert_eq!(built, Err(RangeJoinError::InvalidMatch(same.to_string())));
    // The range column may be named like a bound.
    assert!(RangeJoinMatch::parse("Lo <= Lo <= Hi").is_ok());
}

#[test]
fn a_refreshing_table_on_either_side_is_refused() {
    let graph = graph();
    let ticking_left: Arc<dyn Table> = scripted(
        &graph,
        0..3,
        &left_columns(&[
            ("a", Some(1), Some(5)),
            ("a", Some(3), Some(3)),
            ("b", Some(0), Some(9)),
        ]),
    )
    .table();
    let ticking_right: Arc<dyn Table> = scripted(
        &graph,
        0..2,
        &right_columns(&[("a", Some(1)), ("a", Some(3))]),
    )
    .table();
    let aggregations = [group(&[("Rs", "R")])];
    let (static_left, static_right) = (left(), right());
    let cases = [
        (
            &ticking_left,
            &static_right,
            true,
            false,
            "left table is refreshing, right table is static",
        ),
        (
            &static_left,
            &ticking_right,
            false,
            true,
            "left table is static, right table is refreshing",
        ),
        (
            &ticking_left,
            &ticking_right,
            true,
            true,
            "left table is refreshing, right table is refreshing",
        ),
    ];
    for (left, right, left_refreshing, right_refreshing, sides) in cases {
        let error = refusal(left, right, &on(&["K"]), "Lo <= P <= Hi", &aggregations);
        assert_eq!(
            error,
            RangeJoinError::Refreshing {
                left: left_refreshing,
                right: right_refreshing
            }
        );
        assert_eq!(
            error.to_string(),
            format!("rangeJoin only supports static (not refreshing) inputs at this time: {sides}")
        );
    }
    // Before anything else is looked at.
    let error = refusal(
        &ticking_left,
        &right(),
        &on(&["Nope"]),
        "X <= Y <= Z",
        &[RangeAggregation::Other("sum".into())],
    );
    assert!(matches!(error, RangeJoinError::Refreshing { .. }));
}

#[test]
fn exact_matches_must_name_columns_of_one_type_and_every_fault_is_listed() {
    let aggregations = [group(&[("Rs", "R")])];
    let exact = [
        JoinMatch::new("Nope", "K"),
        JoinMatch::new("K", "Gone"),
        JoinMatch::new("X", "Y"),
        JoinMatch::new("Id", "S"),
        JoinMatch::new("Lo", "N"),
        JoinMatch::same("K"),
    ];
    let error = refusal(&left(), &right(), &exact, "Lo <= P <= Hi", &aggregations);
    let issues = vec![
        "left table has no column \"Nope\"".to_string(),
        "right table has no column \"Gone\"".to_string(),
        "left table has no column \"X\"".to_string(),
        "right table has no column \"Y\"".to_string(),
        "left table column \"Id\" (data type Int), is incompatible with right table column \
         \"S\" (data type String)"
            .to_string(),
        "left table column \"Lo\" (data type Int), is incompatible with right table column \
         \"N\" (data type Long)"
            .to_string(),
    ];
    assert_eq!(
        error.to_string(),
        format!("Invalid exact matches: {}", issues.join(", "))
    );
    assert_eq!(error, RangeJoinError::InvalidExactMatches(issues));
    // Before the range match and the aggregations.
    let error = refusal(
        &left(),
        &right(),
        &on(&["Nope"]),
        "X <= Y <= Z",
        &[RangeAggregation::Other("sum".into())],
    );
    assert!(matches!(error, RangeJoinError::InvalidExactMatches(_)));
}

#[test]
fn the_range_match_must_name_columns_of_one_type_and_every_fault_is_listed() {
    let aggregations = [group(&[("Rs", "R")])];
    let issues = |range: &str| match refusal(&left(), &right(), &on(&["K"]), range, &aggregations) {
        RangeJoinError::InvalidRangeMatch(issues) => issues,
        other => panic!("{range}: {other:?}"),
    };
    assert_eq!(
        issues("X <= Y <= Z"),
        [
            "left start column X is missing",
            "right range column Y is missing",
            // Java says "start" of the end column too.
            "left start column Z is missing",
        ]
    );
    assert_eq!(
        issues("Nope <= P <= Hi"),
        ["left start column Nope is missing"]
    );
    assert_eq!(
        issues("Lo <= Nope <= Hi"),
        ["right range column Nope is missing"]
    );
    assert_eq!(
        issues("Lo <= P <= Nope"),
        ["left start column Nope is missing"]
    );
    let differs = |left: &str, left_type: &str, right: &str, right_type: &str| {
        format!(
            "left table column \"{left}\" (data type {left_type}), is incompatible with right \
             table column \"{right}\" (data type {right_type})"
        )
    };
    assert_eq!(
        issues("Lo <= N <= Hi"),
        [
            differs("Lo", "Int", "N", "Long"),
            differs("Hi", "Int", "N", "Long")
        ]
    );
    assert_eq!(issues("F <= P <= Hi"), [differs("F", "Double", "P", "Int")]);
    assert_eq!(issues("Lo <= P <= F"), [differs("F", "Double", "P", "Int")]);
    // A missing end and a start of another type together.
    assert_eq!(
        issues("F <= P <= Nope"),
        [
            "left start column Nope is missing".to_string(),
            differs("F", "Double", "P", "Int")
        ]
    );
    let error = refusal(&left(), &right(), &[], "Lo <= Nope <= Hi", &aggregations);
    assert_eq!(
        error.to_string(),
        "Invalid range match: right range column Nope is missing"
    );
    // Before the aggregations and the order of the right table.
    let error = refusal(
        &left(),
        &descending_right(),
        &on(&["K"]),
        "Lo <= Nope <= Hi",
        &[RangeAggregation::Other("sum".into())],
    );
    assert!(matches!(error, RangeJoinError::InvalidRangeMatch(_)));
}

#[test]
fn only_the_group_aggregation_is_let_through() {
    let aggregations = [
        RangeAggregation::Other("D aggregated with sum".into()),
        group(&[("Rs", "R")]),
        RangeAggregation::Other("count".into()),
    ];
    let error = refusal(
        &left(),
        &right(),
        &on(&["K"]),
        "Lo <= P <= Hi",
        &aggregations,
    );
    assert_eq!(
        error,
        RangeJoinError::UnsupportedAggregations(vec![
            "D aggregated with sum".to_string(),
            "count".to_string()
        ])
    );
    assert_eq!(
        error.to_string(),
        "rangeJoin only supports the \"group\" aggregation at this time - unsupported \
         aggregations were requested: [D aggregated with sum, count]"
    );
    // Before the order of the right table and the inputs of the group aggregations.
    let aggregations = [
        group(&[("Rs", "Nope")]),
        RangeAggregation::Other("sum".into()),
    ];
    let error = refusal(
        &left(),
        &descending_right(),
        &on(&["K"]),
        "Lo <= P <= Hi",
        &aggregations,
    );
    assert!(matches!(error, RangeJoinError::UnsupportedAggregations(_)));
}

#[test]
fn right_rows_out_of_order_in_a_bucket_are_refused_by_row_key() {
    // Bucket a is 4, null, 5, 2 at the row keys 10, 20, 30, 50; b is in order.
    let right = table_at(
        [10, 20, 30, 40, 50, 60],
        &right_columns(&[
            ("a", Some(4)),
            ("a", None),
            ("a", Some(5)),
            ("b", Some(9)),
            ("a", Some(2)),
            ("b", Some(9)),
        ]),
    );
    let aggregations = [group(&[("Rs", "R")])];
    let error = refusal(&left(), &right, &on(&["K"]), "Lo <= P <= Hi", &aggregations);
    assert_eq!(error, RangeJoinError::OutOfOrder(50));
    assert_eq!(
        error.to_string(),
        "Encountered out of order data in right table at row key 50"
    );
    // Without exact matches the whole table is one bucket: 4, 5, 9, 2.
    let error = refusal(&left(), &right, &[], "Lo <= P <= Hi", &aggregations);
    assert_eq!(error, RangeJoinError::OutOfOrder(50));
    // Before the inputs of the aggregations, and also without aggregations.
    let missing = [group(&[("Rs", "Nope")])];
    let error = refusal(&left(), &right, &on(&["K"]), "Lo <= P <= Hi", &missing);
    assert_eq!(error, RangeJoinError::OutOfOrder(50));
    let error = refusal(&left(), &right, &on(&["K"]), "Lo <= P <= Hi", &[]);
    assert_eq!(error, RangeJoinError::OutOfOrder(50));
}

#[test]
fn disorder_is_not_looked_for_in_buckets_no_left_row_has() {
    let only_b = static_table(&left_columns(&[("b", Some(0), Some(9))]));
    let joined = join(
        &only_b,
        &descending_right(),
        &on(&["K"]),
        &inclusive(),
        &[group(&[("Rs", "R")])],
    )
    .unwrap();
    assert_eq!(arrays(&joined, "Rs", None), [some(&["3"])]);
    // Nor anywhere when there is no left row.
    let nobody = static_table(&left_columns(&[]));
    for exact in [on(&["K"]), on(&[])] {
        let joined = join(
            &nobody,
            &descending_right(),
            &exact,
            &inclusive(),
            &[group(&[("Rs", "R")])],
        )
        .unwrap();
        assert_eq!(arrays(&joined, "Rs", None), []);
    }
}

#[test]
fn a_group_input_must_be_a_column_of_the_right_table() {
    // `Id` is a column of the left table only.
    for input in ["Nope", "Id"] {
        let aggregations = [group(&[("Rs", "R"), ("X", input), ("Y", "AlsoNot")])];
        let error = refusal(
            &left(),
            &right(),
            &on(&["K"]),
            "Lo <= P <= Hi",
            &aggregations,
        );
        assert_eq!(
            error,
            RangeJoinError::NoSuchColumn {
                missing: input.to_string(),
                present: ["K", "P", "R", "N", "S"].map(String::from).to_vec(),
            }
        );
        assert_eq!(
            error.to_string(),
            format!("Unknown column names [{input}], available column names are [K, P, R, N, S]")
        );
    }
}

#[test]
fn the_result_has_the_left_columns_then_one_array_column_per_pair() {
    let aggregations = [group(&[("Rs", "R"), ("N", "N")]), group(&[("S", "S")])];
    let joined = join(&left(), &right(), &on(&["K"]), &inclusive(), &aggregations).unwrap();
    assert_eq!(names(&joined), ["Id", "K", "Lo", "Hi", "F", "Rs", "N", "S"]);
    for (index, (_, column)) in joined.columns.iter().enumerate().take(5) {
        assert!(matches!(column, Column::Left(left) if *left == index));
    }
    assert_eq!(
        arrays(&joined, "Rs", None),
        [some(&["1", "2", "3"]), some(&["2"]), some(&["4"])]
    );
    assert_eq!(
        arrays(&joined, "N", None),
        [some(&["1", "3", "5"]), some(&["3"]), some(&["2"])]
    );
    assert_eq!(
        arrays(&joined, "S", None),
        [
            some(&["Some(1)", "Some(3)", "Some(5)"]),
            some(&["Some(3)"]),
            some(&["Some(2)"])
        ]
    );
}

#[test]
fn an_output_takes_the_place_of_a_column_of_its_name() {
    // A left column, the exact match column itself, and an earlier output.
    let aggregations = [
        group(&[("Lo", "R"), ("K", "K"), ("X", "R"), ("Y", "S")]),
        group(&[("X", "N")]),
    ];
    let joined = join(&left(), &right(), &on(&["K"]), &inclusive(), &aggregations).unwrap();
    assert_eq!(names(&joined), ["Id", "K", "Lo", "Hi", "F", "X", "Y"]);
    // The ranges are still those of the left table's own Lo.
    assert_eq!(
        arrays(&joined, "Lo", None),
        [some(&["1", "2", "3"]), some(&["2"]), some(&["4"])]
    );
    assert_eq!(
        arrays(&joined, "K", None),
        [some(&["a", "a", "a"]), some(&["a"]), some(&["b"])]
    );
    // The later pair's input, where the earlier pair's column stood.
    assert_eq!(
        arrays(&joined, "X", None),
        [some(&["1", "3", "5"]), some(&["3"]), some(&["2"])]
    );
}

#[test]
fn a_row_without_a_range_is_null_and_an_empty_range_is_empty() {
    let left = static_table(&left_columns(&[
        ("a", Some(5), Some(1)),
        ("a", Some(2), Some(2)),
        ("a", None, None),
        ("z", Some(1), Some(5)),
        ("z", Some(5), Some(1)),
    ]));
    let right = static_table(&right_columns(&[
        ("a", Some(1)),
        ("a", None),
        ("a", Some(3)),
    ]));
    let aggregations = [group(&[("Rs", "R"), ("N", "N")])];
    let joined = join(&left, &right, &on(&["K"]), &inclusive(), &aggregations).unwrap();
    let expected = [None, some(&[]), some(&["1", "3"]), some(&[]), None];
    assert_eq!(arrays(&joined, "Rs", None), expected);
    // The right row with a null range value is in no range, whatever the bounds.
    assert_eq!(arrays(&joined, "N", None)[2], some(&["1", "3"]));
}

#[test]
fn arrays_are_read_by_the_left_table_s_row_keys() {
    let left = table_at(
        [5, 9, 20],
        &left_columns(&[
            ("a", Some(1), Some(5)),
            ("a", Some(3), Some(3)),
            ("b", Some(0), Some(9)),
        ]),
    );
    let right = table_at(
        [3, 7, 100, 101],
        &right_columns(&[
            ("a", Some(1)),
            ("b", Some(2)),
            ("a", Some(3)),
            ("a", Some(5)),
        ]),
    );
    let aggregations = [group(&[("Rs", "R")])];
    let joined = join(&left, &right, &on(&["K"]), &inclusive(), &aggregations).unwrap();
    assert_eq!(
        arrays(&joined, "Rs", None),
        [some(&["1", "3", "4"]), some(&["3"]), some(&["2"])]
    );
    let some_rows = RowSet::from_keys([9, 20]);
    assert_eq!(
        arrays(&joined, "Rs", Some(&some_rows)),
        [some(&["3"]), some(&["2"])]
    );
    assert_eq!(arrays(&joined, "Rs", Some(&RowSet::empty())), []);
    let Some((_, Column::Group(group))) = joined.columns.last() else {
        panic!("a group column");
    };
    let error = group.chunk(&RowSet::from_keys([9, 10])).unwrap_err();
    assert_eq!(error.message, "row key 10 is not in the table");
}

#[test]
fn char_range_columns_are_searched_as_code_units() {
    let chars = |values: &[Option<char>]| -> ArrayRef {
        let units = values.iter().map(|value| value.map(|c| c as u16));
        Arc::new(units.collect::<UInt16Array>())
    };
    let left = static_table(&[
        (
            "Lo",
            ColumnType::Char,
            chars(&[Some('b'), None, Some('z'), Some('c'), Some('\u{e000}')]),
        ),
        (
            "Hi",
            ColumnType::Char,
            chars(&[Some('d'), Some('b'), Some('a'), Some('c'), None]),
        ),
    ]);
    let right = static_table(&[
        (
            "P",
            ColumnType::Char,
            chars(&[
                Some('a'),
                Some('b'),
                None,
                Some('c'),
                Some('c'),
                Some('\u{ffee}'),
            ]),
        ),
        ("R", ColumnType::Long, longs(1..=6)),
    ]);
    let aggregations = [group(&[("Rs", "R")])];
    let joined = join(&left, &right, &[], &inclusive(), &aggregations).unwrap();
    assert_eq!(
        arrays(&joined, "Rs", None),
        [
            some(&["2", "4", "5"]),
            some(&["1", "2"]),
            None,
            some(&["4", "5"]),
            some(&["6"])
        ]
    );
}

#[test]
fn without_aggregations_the_result_is_the_left_table_s_columns_as_a_static_table() {
    let left = table_at(
        [5, 9, 20],
        &left_columns(&[
            ("a", Some(1), Some(5)),
            ("a", Some(3), Some(3)),
            ("b", Some(0), Some(9)),
        ]),
    );
    for aggregations in [vec![], vec![group(&[])]] {
        let result = range_join(&left, &right(), &on(&["K"]), &inclusive(), &aggregations).unwrap();
        assert_eq!(result.definition(), left.definition());
        assert_eq!(*result.row_set(), RowSet::from_keys([5, 9, 20]));
        assert!(!result.is_refreshing());
        assert!(!result.is_flat());
        assert_eq!(result.attributes(), Default::default());
        let (ours, theirs) = (
            TableSnapshot::current(&*result).unwrap(),
            TableSnapshot::current(&*left).unwrap(),
        );
        assert_eq!(ours.columns, theirs.columns);
    }

    // The result is a table of its own over the left table's rows: it is not flat even
    // where the left table is.
    let columns = left_columns(&[("a", Some(1), Some(5)), ("b", Some(0), Some(9))]);
    let fields: Vec<Field> = columns
        .iter()
        .map(|(name, column_type, _)| Field::new(*name, column_type.arrow_type(), true))
        .collect();
    let names: Vec<(&str, ColumnType)> = columns
        .iter()
        .map(|(name, column_type, _)| (*name, column_type.clone()))
        .collect();
    let arrays = columns.iter().map(|(_, _, values)| values.clone());
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays.collect()).unwrap();
    let flat: Arc<dyn Table> = in_memory_table(definition(&names), &[batch]).unwrap();
    assert!(flat.is_flat());
    let result = range_join(&flat, &right(), &on(&["K"]), &inclusive(), &[]).unwrap();
    assert!(!result.is_flat());
    assert_eq!(*result.row_set(), RowSet::flat(2));
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.impl.sources.aggregate.AggregateColumnSource#forRangeJoin: column Rs holds arrays of Int"
)]
fn a_result_with_an_array_column_is_not_a_table_yet() {
    let aggregations = [group(&[("Rs", "R")])];
    let _ = range_join(&left(), &right(), &on(&["K"]), &inclusive(), &aggregations);
}
