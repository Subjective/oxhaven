use super::*;
use crate::ops::sort::testing::{graph, scripted, static_table};
use crate::ops::sort::{sort, SortColumn};
use arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray,
    TimestampNanosecondArray, UInt16Array,
};
use oxhaven_contracts::graph::locked;
use oxhaven_testkit::{list_of, TableChange};
use std::sync::Arc;

const JAVA_ANSWERS: &str = include_str!("java_seek.txt");
const T0: i64 = 1_767_225_600_000_000_000;

/// The first `size` rows of the table Java's answers were recorded for: 60 rows whose
/// values go through `0..20` three times, in steps of 7.
fn columns(size: i64) -> Vec<(&'static str, ColumnType, ArrayRef)> {
    let v = |i: i64| (i * 7) % 20;
    let rows = 0..size;
    let text = |i: i64| {
        let stem = if i % 2 == 0 { "Abc" } else { "abd" };
        (i % 9 != 4).then(|| format!("{stem}{}", (i * 7) % 5))
    };
    let times: TimestampNanosecondArray = rows
        .clone()
        .map(|i| Some(T0 + v(i) * 1_000_000_000))
        .collect();
    let longs: Int64Array = rows.clone().map(|i| Some(v(i))).collect();
    let ints: Int32Array = rows.clone().map(|i| Some(v(i) as i32)).collect();
    let doubles: Float64Array = rows.clone().map(|i| Some(v(i) as f64 / 2.0)).collect();
    let floats: Float32Array = rows.clone().map(|i| Some(v(i) as f32 / 2.0)).collect();
    let strings: StringArray = rows.clone().map(text).collect();
    let chars: UInt16Array = rows.clone().map(|i| Some(65 + v(i) as u16)).collect();
    let booleans: BooleanArray = rows
        .clone()
        .map(|i| (i % 3 != 0).then_some(i % 3 == 1))
        .collect();
    let nullable: Int32Array = rows.map(|i| (i % 4 != 0).then_some(v(i) as i32)).collect();
    vec![
        ("L", ColumnType::Long, Arc::new(longs)),
        ("I", ColumnType::Int, Arc::new(ints)),
        ("D", ColumnType::Double, Arc::new(doubles)),
        ("F", ColumnType::Float, Arc::new(floats)),
        ("S", ColumnType::String, Arc::new(strings)),
        ("C", ColumnType::Char, Arc::new(chars)),
        ("B", ColumnType::Boolean, Arc::new(booleans)),
        (
            "T",
            ColumnType::Instant,
            Arc::new(times.with_timezone("UTC")),
        ),
        ("N", ColumnType::Int, Arc::new(nullable)),
    ]
}

/// The table a line of Java's answers names, for a search of `column`.
fn table(name: &str, column: &str) -> Arc<dyn Table> {
    let plain = static_table(&columns(60));
    match name {
        "plain" => plain,
        "one" => static_table(&columns(1)),
        "empty" => static_table(&columns(0)),
        "asc" => sort(&plain, &[SortColumn::ascending(column)]).unwrap(),
        "desc" => sort(&plain, &[SortColumn::descending(column)]).unwrap(),
        "ascL" => sort(&plain, &[SortColumn::ascending("L")]).unwrap(),
        "descL" => sort(&plain, &[SortColumn::descending("L")]).unwrap(),
        _ => panic!("no table {name}"),
    }
}

/// The value the server makes of a literal for a column of `column_type`, for the
/// literals it does not refuse.
fn value(literal: &str, column_type: &ColumnType) -> SeekValue {
    let (kind, text) = literal.split_once(':').unwrap();
    match (kind, column_type) {
        ("s", _) => SeekValue::String(text.to_string()),
        ("b", _) => SeekValue::Boolean(text.parse().unwrap()),
        ("n", _) => SeekValue::Instant(text.parse().unwrap()),
        ("l", ColumnType::Long) => SeekValue::Long(text.parse().unwrap()),
        ("l", ColumnType::Int) => SeekValue::Int(text.parse().unwrap()),
        ("l", ColumnType::Double) => SeekValue::Double(text.parse().unwrap()),
        ("l", ColumnType::Float) => SeekValue::Float(text.parse().unwrap()),
        ("d", ColumnType::Long) => SeekValue::Long(text.parse::<f64>().unwrap() as i64),
        ("d", ColumnType::Int) => SeekValue::Int(text.parse::<f64>().unwrap() as i32),
        ("d", ColumnType::Double) => SeekValue::Double(text.parse().unwrap()),
        ("d", ColumnType::Float) => SeekValue::Float(text.parse().unwrap()),
        _ => panic!("no value for {literal} in a column of {column_type:?}"),
    }
}

#[test]
fn the_row_found_is_the_row_java_finds() {
    let mut checked = 0;
    for line in JAVA_ANSWERS.lines().filter(|line| !line.starts_with('#')) {
        let (case, expected) = line.split_once(" = ").unwrap();
        let fields: Vec<&str> = case.split(' ').collect();
        let [name, column, literal, start, flags] = fields[..] else {
            panic!("{line}");
        };
        let table = table(name, column);
        let definition = table.definition();
        let column_type = &definition.columns()[definition.index_of(column).unwrap()].column_type;
        let seek = Seek {
            starting_row: start.parse().unwrap(),
            column: column.to_string(),
            value: value(literal, column_type),
            insensitive: flags.contains('i'),
            contains: flags.contains('c'),
            backward: flags.contains('b'),
        };
        let found = match seek_row(&*table, &seek) {
            Ok(row) => row.to_string(),
            Err(failure) => {
                assert!(
                    failure.message.starts_with("java.lang."),
                    "{line}: {failure}"
                );
                "error".to_string()
            }
        };
        assert_eq!(found, expected, "{line}");
        checked += 1;
    }
    assert_eq!(checked, 511);
}

fn seek_long(column: &str, value: i64, starting_row: i64) -> Seek {
    Seek {
        starting_row,
        column: column.to_string(),
        value: SeekValue::Long(value),
        insensitive: false,
        contains: false,
        backward: false,
    }
}

#[test]
fn a_search_row_by_row_wraps_around() {
    let table = static_table(&columns(60));
    // L is 7 in the rows 1, 21 and 41
    let found = |start: i64, backward: bool| {
        let mut seek = seek_long("L", 7, start);
        seek.backward = backward;
        seek_row(&*table, &seek).unwrap()
    };
    assert_eq!(found(0, false), 1);
    assert_eq!(found(1, false), 21);
    assert_eq!(found(41, false), 1);
    assert_eq!(found(21, true), 1);
    // backward from the first of them the last is found, and at last the row itself
    assert_eq!(found(1, true), 41);
    assert_eq!(found(-5, false), 1);
    assert_eq!(seek_row(&*table, &seek_long("L", 20, 0)).unwrap(), -1);
}

#[test]
fn equal_is_the_equals_of_the_boxed_values() {
    let doubles: Float64Array = [Some(0.0), Some(f64::NAN), None, Some(-0.0)]
        .into_iter()
        .collect();
    let table = static_table(&[("D", ColumnType::Double, Arc::new(doubles))]);
    let found = |value: SeekValue| {
        let mut seek = seek_long("D", 0, 3);
        seek.value = value;
        seek_row(&*table, &seek).unwrap()
    };
    assert_eq!(found(SeekValue::Double(0.0)), 0);
    assert_eq!(found(SeekValue::Double(-0.0)), 3);
    assert_eq!(found(SeekValue::Double(f64::NAN)), 1);
    // a value of another class equals nothing
    assert_eq!(found(SeekValue::Long(0)), -1);
    assert_eq!(found(SeekValue::Float(0.0)), -1);
}

#[test]
fn a_column_that_does_not_exist_is_javas_null_pointer() {
    let table = static_table(&columns(60));
    let failure = seek_row(&*table, &seek_long("Q", 7, 0)).unwrap_err();
    assert_eq!(failure.message, "java.lang.NullPointerException");
}

#[test]
fn what_java_casts_to_an_int_is_not_ported() {
    const SEEK: &str = "not ported: io.deephaven.clientsupport.gotorow.SeekRow#seek: ";
    let table = static_table(&columns(60));
    let failure = seek_row(&*table, &seek_long("L", 7, i64::from(i32::MAX))).unwrap_err();
    assert_eq!(
        failure.message,
        format!("{SEEK}a starting row or a size that is not an int")
    );
    assert_eq!(
        seek_row(&*table, &seek_long("L", 7, i64::from(i32::MAX) - 1)).unwrap(),
        1
    );

    // a sorted table whose rows are at keys beyond an int
    let far = scripted(&graph(), (0..60).map(|k| k + (1 << 31)), &columns(60));
    let far: Arc<dyn Table> = far.table();
    let seek = seek_long("L", 7, 0);
    assert_eq!(seek_row(&*far, &seek).unwrap(), 1);
    let sorted = sort(&far, &[SortColumn::ascending("L")]).unwrap();
    let near = sort(&static_table(&columns(60)), &[SortColumn::ascending("L")]).unwrap();
    assert_eq!(seek_row(&*near, &seek).unwrap(), 21);
    // the result of a sort has the row keys `0..size`, so only the size could be too much
    assert_eq!(seek_row(&*sorted, &seek).unwrap(), 21);
}

#[test]
fn the_text_of_a_value_that_is_no_text_is_not_ported() {
    let lists = list_of(
        &ColumnType::Vector(Box::new(ColumnType::Int)),
        [Some(1), None, Some(0)],
        Arc::new(Int32Array::from(vec![1])),
    );
    let longs: Int64Array = (0..3).map(Some).collect();
    let table = static_table(&[
        ("V", ColumnType::Vector(Box::new(ColumnType::Int)), lists),
        ("L", ColumnType::Long, Arc::new(longs)),
    ]);
    let text = |column: &str| Seek {
        value: SeekValue::String("1".to_string()),
        ..seek_long(column, 0, 2)
    };
    for column in ["V", "L"] {
        let failure = seek_row(&*table, &text(column)).unwrap_err();
        assert_eq!(
            failure.message,
            "not ported: io.deephaven.clientsupport.gotorow.SeekRow#seek: the text of a value \
             that is not a String or a char"
        );
    }
    // a number equals no vector, and with `contains` the vector is cast to a String
    assert_eq!(seek_row(&*table, &seek_long("V", 1, 0)).unwrap(), -1);
    let contains = Seek {
        contains: true,
        ..seek_long("V", 1, 0)
    };
    let failure = seek_row(&*table, &contains).unwrap_err();
    assert_eq!(failure.message, "java.lang.ClassCastException");
}

#[test]
fn a_refreshing_table_is_searched_as_it_is_now() {
    let graph = graph();
    let script = scripted(&graph, 0..60, &columns(60));
    let table: Arc<dyn Table> = script.table();
    let found = || {
        locked(graph.as_ref(), || {
            seek_row(&*table, &seek_long("L", 7, 0)).unwrap()
        })
    };
    assert_eq!(found(), 1);
    script.push(TableChange::new().remove(1..=20));
    graph.run_cycle();
    assert_eq!(found(), 1);
    script.push(TableChange::new().remove([21]));
    graph.run_cycle();
    assert_eq!(found(), 20);
}
