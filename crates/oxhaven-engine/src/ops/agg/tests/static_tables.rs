//! Aggregation of static tables: every aggregation, type and key shape.

use super::super::AggregateError;
use super::*;
use arrow::array::{
    BooleanArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array, Int8Array,
    StringArray, TimestampNanosecondArray, UInt16Array,
};

fn strs(values: &[Option<&str>]) -> ArrayRef {
    Arc::new(values.iter().copied().collect::<StringArray>())
}

fn int_opts(values: &[Option<i32>]) -> ArrayRef {
    Arc::new(values.iter().copied().collect::<Int32Array>())
}

fn double_opts(values: &[Option<f64>]) -> ArrayRef {
    Arc::new(values.iter().copied().collect::<Float64Array>())
}

/// A column of each type with the values `[a, b, a, null]`, as `(name, type, values)`.
fn one_of_each_type() -> Vec<(&'static str, ColumnType, ArrayRef)> {
    fn pattern<T: Copy>(a: T, b: T) -> [Option<T>; 4] {
        [Some(a), Some(b), Some(a), None]
    }
    vec![
        (
            "Bool",
            ColumnType::Boolean,
            Arc::new(BooleanArray::from(pattern(true, false).to_vec())) as ArrayRef,
        ),
        (
            "Byte",
            ColumnType::Byte,
            Arc::new(Int8Array::from(pattern(3, -2).to_vec())),
        ),
        (
            "Char",
            ColumnType::Char,
            Arc::new(UInt16Array::from(pattern(0xFFFE, 0x41).to_vec())),
        ),
        (
            "Short",
            ColumnType::Short,
            Arc::new(Int16Array::from(pattern(3, -2).to_vec())),
        ),
        (
            "Int",
            ColumnType::Int,
            Arc::new(Int32Array::from(pattern(3, -2).to_vec())),
        ),
        (
            "Long",
            ColumnType::Long,
            Arc::new(Int64Array::from(pattern(3, -2).to_vec())),
        ),
        (
            "Float",
            ColumnType::Float,
            Arc::new(Float32Array::from(pattern(1.5, -0.25).to_vec())),
        ),
        (
            "Double",
            ColumnType::Double,
            Arc::new(Float64Array::from(pattern(1.5, -0.25).to_vec())),
        ),
        (
            "Str",
            ColumnType::String,
            Arc::new(StringArray::from(pattern("x", "w").to_vec())),
        ),
        (
            "Time",
            ColumnType::Instant,
            Arc::new(
                TimestampNanosecondArray::from(pattern(2_000, 1_000).to_vec()).with_timezone("UTC"),
            ),
        ),
    ]
}

#[test]
fn every_aggregation_by_one_key() {
    let parent = static_table(&[
        (
            "K",
            ColumnType::String,
            strs(&[
                Some("a"),
                Some("b"),
                Some("a"),
                None,
                Some("b"),
                Some("a"),
                Some("c"),
                Some("d"),
                Some("c"),
            ]),
        ),
        (
            "I",
            ColumnType::Int,
            int_opts(&[
                Some(1),
                Some(10),
                None,
                Some(7),
                Some(-4),
                Some(5),
                None,
                None,
                Some(3),
            ]),
        ),
    ]);
    let result = agg_by(&parent, &all_of("I"), &["K"]);
    assert_eq!(
        schema(&*result),
        [
            ("K".to_string(), ColumnType::String),
            ("Sum".to_string(), ColumnType::Long),
            ("Avg".to_string(), ColumnType::Double),
            ("Min".to_string(), ColumnType::Int),
            ("Max".to_string(), ColumnType::Int),
            ("First".to_string(), ColumnType::Int),
            ("Last".to_string(), ColumnType::Int),
            ("N".to_string(), ColumnType::Long),
        ]
    );
    // Groups in the order first seen; nulls are skipped by sum, avg, min and max, counted
    // by count, and are values to first and last.
    assert_eq!(
        rows(&*result),
        [
            "a|6|3.0|1|5|1|5|3",
            "b|6|3.0|-4|10|10|-4|2",
            "null|7|7.0|7|7|7|7|1",
            "c|3|3.0|3|3|null|3|2",
            "d|null|null|null|null|null|null|1",
        ]
    );
    assert!(!result.is_refreshing());
    assert!(result.is_flat());
}

#[test]
fn result_types_follow_java() {
    let columns = one_of_each_type();
    let parent = static_table(&columns);
    let type_of = |spec: AggSpec, input: &str| {
        let result = aggregate(
            &parent,
            &[agg(spec, &[&format!("Out = {input}")])],
            false,
            &[],
        );
        result.map(|table| schema(&*table)[0].1.clone())
    };
    for (name, column_type, _) in &columns {
        // min, max, first and last have the type of their input, whatever it is.
        for spec in [AggSpec::Min, AggSpec::Max, AggSpec::First, AggSpec::Last] {
            assert_eq!(
                type_of(spec.clone(), name),
                Ok(column_type.clone()),
                "{spec:?} of {name}"
            );
        }
        let unsupported = |spec| {
            Err(AggregateError::UnsupportedType {
                spec,
                column: name.to_string(),
                column_type: column_type.clone(),
            })
        };
        let (sum, avg) = match column_type {
            ColumnType::Byte
            | ColumnType::Char
            | ColumnType::Short
            | ColumnType::Int
            | ColumnType::Long => (Ok(ColumnType::Long), Ok(ColumnType::Double)),
            ColumnType::Float => (Ok(ColumnType::Float), Ok(ColumnType::Double)),
            ColumnType::Double => (Ok(ColumnType::Double), Ok(ColumnType::Double)),
            ColumnType::Boolean => (Ok(ColumnType::Long), unsupported(AggSpec::Avg)),
            _ => (unsupported(AggSpec::Sum), unsupported(AggSpec::Avg)),
        };
        assert_eq!(type_of(AggSpec::Sum, name), sum, "sum of {name}");
        assert_eq!(type_of(AggSpec::Avg, name), avg, "avg of {name}");
    }
}

#[test]
fn every_aggregation_of_every_type() {
    let parent = static_table(&one_of_each_type());
    let one = |spec: AggSpec, input: &str| {
        let result = agg_by(&parent, &[agg(spec, &[input])], &[]);
        rows(&*result).join(";")
    };
    // The values are [a, b, a, null]; a char is its code unit, so 0xFFFE is above 0x41.
    assert_eq!(one(AggSpec::Sum, "Bool"), "2");
    assert_eq!(one(AggSpec::Sum, "Byte"), "4");
    assert_eq!(one(AggSpec::Sum, "Char"), (2 * 0xFFFE + 0x41).to_string());
    assert_eq!(one(AggSpec::Sum, "Short"), "4");
    assert_eq!(one(AggSpec::Sum, "Int"), "4");
    assert_eq!(one(AggSpec::Sum, "Long"), "4");
    assert_eq!(one(AggSpec::Sum, "Float"), "2.75");
    assert_eq!(one(AggSpec::Sum, "Double"), "2.75");
    for input in ["Byte", "Short", "Int", "Long"] {
        assert_eq!(
            one(AggSpec::Avg, input),
            format!("{:?}", 4.0 / 3.0),
            "{input}"
        );
    }
    assert_eq!(
        one(AggSpec::Avg, "Char"),
        format!("{:?}", f64::from(2 * 0xFFFE + 0x41) / 3.0)
    );
    assert_eq!(one(AggSpec::Avg, "Float"), format!("{:?}", 2.75 / 3.0));
    assert_eq!(one(AggSpec::Avg, "Double"), format!("{:?}", 2.75 / 3.0));

    let all = |spec: AggSpec| {
        let pairs: Vec<&str> = one_of_each_type()
            .iter()
            .map(|(name, _, _)| *name)
            .collect();
        let result = agg_by(&parent, &[agg(spec, &pairs)], &[]);
        rows(&*result).join(";")
    };
    assert_eq!(all(AggSpec::Min), "false|-2|65|-2|-2|-2|-0.25|-0.25|w|1000");
    assert_eq!(all(AggSpec::Max), "true|3|65534|3|3|3|1.5|1.5|x|2000");
    assert_eq!(all(AggSpec::First), "true|3|65534|3|3|3|1.5|1.5|x|2000");
    assert_eq!(
        all(AggSpec::Last),
        "null|null|null|null|null|null|null|null|null|null"
    );
}

#[test]
fn integer_sums_widen_and_wrap_and_the_null_sentinel_reads_as_null() {
    let k = strs(&[
        Some("wrap"),
        Some("wrap"),
        Some("past"),
        Some("past"),
        Some("wide"),
        Some("wide"),
    ]);
    let l: ArrayRef = Arc::new(Int64Array::from(vec![i64::MAX, 1, i64::MAX, 2, 5, 6]));
    let i: ArrayRef = Arc::new(Int32Array::from(vec![1, 1, 1, 1, i32::MAX, i32::MAX]));
    let parent = static_table(&[
        ("K", ColumnType::String, k),
        ("L", ColumnType::Long, l),
        ("I", ColumnType::Int, i),
    ]);
    let result = agg_by(
        &parent,
        &[
            agg(AggSpec::Sum, &["L", "I"]),
            agg(AggSpec::Avg, &["AvgL = L"]),
        ],
        &["K"],
    );
    assert_eq!(
        rows(&*result),
        [
            // Long.MAX_VALUE + 1 wraps to Long.MIN_VALUE, which is the null long. The
            // average divides the wrapped sum.
            format!("wrap|null|2|{:?}", i64::MIN as f64 / 2.0),
            format!("past|{}|2|{:?}", i64::MIN + 1, (i64::MIN + 1) as f64 / 2.0),
            format!("wide|11|{}|5.5", 2 * i64::from(i32::MAX)),
        ]
    );
}

#[test]
fn nan_and_infinities_decide_floating_point_sums_and_averages() {
    let groups: [(&str, &[Option<f64>]); 7] = [
        ("nan", &[Some(1.5), Some(f64::NAN)]),
        ("inf", &[Some(f64::INFINITY), Some(1.0)]),
        ("both", &[Some(f64::INFINITY), Some(f64::NEG_INFINITY)]),
        ("nulls", &[None, None]),
        ("neg", &[Some(f64::NEG_INFINITY), Some(2.0)]),
        ("plain", &[Some(0.25), Some(0.5), None]),
        ("zeros", &[Some(-0.0), Some(-0.0)]),
    ];
    let k: Vec<Option<&str>> = groups
        .iter()
        .flat_map(|(name, values)| values.iter().map(|_| Some(*name)))
        .collect();
    let d: Vec<Option<f64>> = groups
        .iter()
        .flat_map(|(_, values)| values.iter().copied())
        .collect();
    let f: ArrayRef = Arc::new(
        d.iter()
            .map(|v| v.map(|v| v as f32))
            .collect::<Float32Array>(),
    );
    let parent = static_table(&[
        ("K", ColumnType::String, strs(&k)),
        ("D", ColumnType::Double, double_opts(&d)),
        ("F", ColumnType::Float, f),
    ]);
    let result = agg_by(
        &parent,
        &[
            agg(AggSpec::Sum, &["SumD = D", "SumF = F"]),
            agg(AggSpec::Avg, &["AvgD = D", "AvgF = F"]),
            agg(AggSpec::Min, &["MinD = D"]),
            agg(AggSpec::Max, &["MaxD = D"]),
        ],
        &["K"],
    );
    assert_eq!(
        schema(&*result)[1..3],
        [
            ("SumD".to_string(), ColumnType::Double),
            ("SumF".to_string(), ColumnType::Float)
        ]
    );
    assert_eq!(
        rows(&*result),
        [
            // A NaN makes the sum and average NaN, and both the minimum and the maximum.
            "nan|NaN|NaN|NaN|NaN|NaN|NaN",
            "inf|inf|inf|inf|inf|1.0|inf",
            "both|NaN|NaN|NaN|NaN|-inf|inf",
            "nulls|null|null|null|null|null|null",
            "neg|-inf|-inf|-inf|-inf|-inf|2.0",
            "plain|0.75|0.75|0.375|0.375|0.25|0.5",
            // Behavior: SumDoubleChunk#sumDoubleChunk starts from 0.0, so negative zeros
            // sum to 0.0.
            "zeros|0.0|0.0|0.0|0.0|-0.0|-0.0",
        ]
    );
}

#[test]
fn without_keys_there_is_one_row_unless_the_table_is_empty() {
    let parent = static_table(&[("I", ColumnType::Int, int_opts(&[Some(4), None, Some(-1)]))]);
    let result = agg_by(&parent, &all_of("I"), &[]);
    assert_eq!(rows(&*result), ["3|1.5|-1|4|4|-1|3"]);
    assert!(result.is_flat());

    // Behavior: ChunkedOperatorAggregationHelper#noKeyAggregation gives a result of size 0
    // for an empty table unless empty groups are preserved.
    let empty = static_table(&[("I", ColumnType::Int, int_opts(&[]))]);
    let result = agg_by(&empty, &all_of("I"), &[]);
    assert_eq!(rows(&*result), [""; 0]);
    assert!(result.is_flat());

    let preserved = aggregate(&empty, &all_of("I"), true, &[]).expect("an aggregation");
    assert_eq!(rows(&*preserved), ["null|null|null|null|null|null|0"]);

    let keyed = agg_by(&empty, &[count("N")], &["I"]);
    assert_eq!(rows(&*keyed), [""; 0]);
    assert!(keyed.is_flat());
}

#[test]
fn every_type_is_a_key_and_null_is_a_key() {
    let columns = one_of_each_type();
    let parent = static_table(&columns);
    for (name, column_type, values) in &columns {
        let result = agg_by(&parent, &[count("N")], &[name]);
        assert_eq!(schema(&*result)[0], (name.to_string(), column_type.clone()));
        let first = cell(values, 0);
        let second = cell(values, 1);
        assert_eq!(
            rows(&*result),
            [
                format!("{first}|2"),
                format!("{second}|1"),
                "null|1".to_string()
            ],
            "by {name}"
        );
    }
}

#[test]
fn floating_point_keys_group_nans_and_zeros_and_show_the_first_seen() {
    let other_nan = f64::from_bits(f64::NAN.to_bits() | 1);
    assert!(other_nan.is_nan());
    let d = double_opts(&[
        Some(f64::NAN),
        Some(0.0),
        Some(-0.0),
        Some(other_nan),
        None,
        Some(1.0),
        None,
        Some(-0.0),
    ]);
    let f: ArrayRef = Arc::new(Float32Array::from(vec![
        -0.0,
        0.0,
        f32::NAN,
        -f32::NAN,
        1.0,
        1.0,
        0.0,
        -0.0,
    ]));
    let parent = static_table(&[("D", ColumnType::Double, d), ("F", ColumnType::Float, f)]);
    let by_d = agg_by(&parent, &[count("N")], &["D"]);
    assert_eq!(rows(&*by_d), ["NaN|2", "0.0|3", "null|2", "1.0|1"]);
    // The zero shown is the one of the first row seen with the key.
    let by_f = agg_by(&parent, &[count("N")], &["F"]);
    assert_eq!(rows(&*by_f), ["-0.0|4", "NaN|2", "1.0|2"]);
}

#[test]
fn several_key_columns_group_by_their_combination() {
    let parent = static_table(&[
        (
            "A",
            ColumnType::String,
            strs(&[Some("x"), Some("x"), None, Some("x"), None, Some("y")]),
        ),
        (
            "B",
            ColumnType::Int,
            int_opts(&[Some(1), None, Some(1), Some(1), Some(1), Some(1)]),
        ),
        (
            "V",
            ColumnType::Int,
            int_opts(&[Some(1), Some(2), Some(4), Some(8), Some(16), Some(32)]),
        ),
    ]);
    let result = agg_by(&parent, &[agg(AggSpec::Sum, &["V"])], &["A", "B"]);
    assert_eq!(rows(&*result), ["x|1|9", "x|null|2", "null|1|20", "y|1|32"]);
    // Behavior: a group-by column named twice counts once.
    let twice = agg_by(&parent, &[agg(AggSpec::Sum, &["V"])], &["B", "B"]);
    assert_eq!(rows(&*twice), ["1|61", "null|2"]);
    assert_eq!(schema(&*twice).len(), 2);
}

#[test]
fn outputs_are_in_the_order_given_after_the_keys() {
    let parent = static_table(&[
        ("V", ColumnType::Int, int_opts(&[Some(1), Some(2)])),
        ("K", ColumnType::Int, int_opts(&[Some(7), Some(7)])),
    ]);
    let result = agg_by(
        &parent,
        &[
            agg(AggSpec::Max, &["Hi = V"]),
            count("N"),
            agg(AggSpec::Sum, &["V", "Again = V"]),
            agg(AggSpec::Min, &["Lo = V"]),
        ],
        &["K"],
    );
    let names: Vec<String> = schema(&*result).into_iter().map(|(name, _)| name).collect();
    assert_eq!(names, ["K", "Hi", "N", "V", "Again", "Lo"]);
    assert_eq!(rows(&*result), ["7|2|2|3|3|1"]);
}

#[test]
fn requests_java_refuses_are_errors() {
    let parent = static_table(&[
        ("K", ColumnType::String, strs(&[Some("a")])),
        ("I", ColumnType::Int, int_opts(&[Some(1)])),
    ]);
    let error = |aggregations: &[Aggregation], by: &[&str]| {
        aggregate(&parent, aggregations, false, by).err()
    };
    assert_eq!(error(&[], &["K"]), Some(AggregateError::NoAggregations));
    assert_eq!(
        error(&[count("N")], &["Nope"]),
        Some(AggregateError::UnknownColumn("Nope".to_string()))
    );
    assert_eq!(
        error(&[agg(AggSpec::Sum, &["S = Nope"])], &["K"]),
        Some(AggregateError::UnknownColumn("Nope".to_string()))
    );
    assert_eq!(
        error(
            &[agg(AggSpec::Sum, &["S = I"]), agg(AggSpec::Max, &["S = I"])],
            &[]
        ),
        Some(AggregateError::DuplicateOutput("S".to_string()))
    );
    assert_eq!(
        error(&[agg(AggSpec::Sum, &["N = I"]), count("N")], &[]),
        Some(AggregateError::DuplicateOutput("N".to_string()))
    );
    assert_eq!(
        error(&[agg(AggSpec::Sum, &["K"])], &[]),
        Some(AggregateError::UnsupportedType {
            spec: AggSpec::Sum,
            column: "K".to_string(),
            column_type: ColumnType::String,
        })
    );
    assert_eq!(error(&[count("N")], &["K"]), None);
}

#[test]
fn an_output_named_like_a_key_takes_the_place_of_the_key_column() {
    // Recorded from Java in `fixtures/rest_static.json` and `fixtures/rest_ticking.json`.
    let parent = static_table(&[
        ("K", ColumnType::String, strs(&[Some("a"), Some("b"), Some("a")])),
        ("I", ColumnType::Int, int_opts(&[Some(1), Some(2), Some(3)])),
    ]);
    let result = agg_by(&parent, &[agg(AggSpec::Sum, &["K = I"])], &["K"]);
    assert_eq!(
        schema(&*result),
        [("K".to_string(), ColumnType::Long)],
        "the groups are still those of the key"
    );
    assert_eq!(rows(&*result), ["4", "2"]);

    let both = [count("N"), agg(AggSpec::Sum, &["K = I"])];
    let result = agg_by(&parent, &both, &["K"]);
    let names: Vec<String> = schema(&*result).into_iter().map(|(name, _)| name).collect();
    assert_eq!(names, ["K", "N"]);
    assert_eq!(rows(&*result), ["4|2", "2|1"]);
}

#[test]
fn distinct_is_a_sorted_vector_and_null_for_a_group_without_values() {
    let parent = static_table(&[
        (
            "K",
            ColumnType::String,
            strs(&[Some("a"), Some("b"), Some("a"), Some("a"), Some("a")]),
        ),
        (
            "I",
            ColumnType::Int,
            int_opts(&[Some(3), None, Some(1), None, Some(3)]),
        ),
    ]);
    let specs = [
        agg(
            AggSpec::Distinct {
                include_nulls: false,
            },
            &["D = I"],
        ),
        agg(
            AggSpec::Distinct {
                include_nulls: true,
            },
            &["N = I"],
        ),
    ];
    let result = agg_by(&parent, &specs, &["K"]);
    let vector = ColumnType::vector(ColumnType::Int);
    assert_eq!(schema(&*result)[1..], [("D".into(), vector.clone()), ("N".into(), vector)]);
    assert_eq!(rows(&*result), ["a|[1, 3]|[null, 1, 3]", "b|null|[null]"]);
    // Without keys the group without values is an empty vector.
    let nulls = static_table(&[("I", ColumnType::Int, int_opts(&[None, None]))]);
    assert_eq!(rows(&*agg_by(&nulls, &specs[..1], &[])), ["[]"]);
}

#[test]
fn a_non_unique_sentinel_shows_for_several_values_and_must_fit_the_column() {
    let parent = static_table(&[
        ("K", ColumnType::String, strs(&[Some("a"), Some("b"), Some("a")])),
        ("I", ColumnType::Int, int_opts(&[Some(1), Some(2), Some(3)])),
    ]);
    let unique = |non_unique: Sentinel| {
        let spec = AggSpec::Unique {
            include_nulls: false,
            non_unique,
        };
        aggregate(&parent, &[agg(spec, &["U = I"])], false, &["K"])
    };
    assert_eq!(rows(&*unique(Sentinel::Int(-1)).unwrap()), ["a|-1", "b|2"]);
    assert_eq!(rows(&*unique(Sentinel::Long(7)).unwrap()), ["a|7", "b|2"]);
    // The null value of the column's type is a null.
    assert_eq!(
        rows(&*unique(Sentinel::Int(i32::MIN)).unwrap()),
        ["a|null", "b|2"]
    );
    for refused in [
        Sentinel::Long(i64::from(i32::MAX) + 1),
        Sentinel::Double(1.5),
        Sentinel::Double(f64::NAN),
        Sentinel::String("many".to_string()),
        Sentinel::Boolean(true),
    ] {
        assert!(
            matches!(
                unique(refused.clone()),
                Err(AggregateError::InvalidSentinel(_))
            ),
            "{refused:?}"
        );
    }
}

#[test]
fn count_where_counts_the_rows_that_pass_every_filter() {
    let parent = static_table(&[
        ("K", ColumnType::String, strs(&[Some("a"), Some("b"), Some("a")])),
        ("I", ColumnType::Int, int_opts(&[Some(1), Some(2), Some(3)])),
    ]);
    let where_ = |filters: &[&str]| Aggregation::count_where("N", filters.iter().copied());
    let result = agg_by(&parent, &[where_(&["I > 1"]), count("C")], &["K"]);
    assert_eq!(rows(&*result), ["a|1|2", "b|1|1"]);
    let result = agg_by(&parent, &[where_(&["I > 1", "I < 3"])], &[]);
    assert_eq!(rows(&*result), ["1"]);
    for refused in ["i > 1", "I_[0] > 1", "Nope > 1", "I >"] {
        assert!(
            matches!(
                aggregate(&parent, &[where_(&[refused])], false, &["K"]),
                Err(AggregateError::InvalidFilter(_))
            ),
            "{refused}"
        );
    }
    // Without a compiler there is no filter to make.
    assert!(matches!(
        super::super::aggregate(&parent, &[where_(&["I > 1"])], false, &["K"]),
        Err(AggregateError::InvalidFilter(_))
    ));
}

#[test]
fn groups_span_chunks() {
    // Three chunks of rows: groups are found again in later chunks, and first and last
    // look across them.
    let size = 10_000;
    let k: ArrayRef = Arc::new(Int32Array::from_iter_values((0..size).map(|i| i % 3)));
    let v: ArrayRef = Arc::new(Int32Array::from_iter_values(0..size));
    let parent = static_table(&[("K", ColumnType::Int, k), ("V", ColumnType::Int, v)]);
    let result = agg_by(&parent, &all_of("V"), &["K"]);
    let expected: Vec<String> = (0..3i64)
        .map(|key| {
            let values: Vec<i64> = (0..i64::from(size)).filter(|i| i % 3 == key).collect();
            let sum: i64 = values.iter().sum();
            let (first, last) = (values[0], values[values.len() - 1]);
            let avg = sum as f64 / values.len() as f64;
            format!(
                "{key}|{sum}|{avg:?}|{first}|{last}|{first}|{last}|{}",
                values.len()
            )
        })
        .collect();
    assert_eq!(rows(&*result), expected);
}

#[test]
fn minimum_and_maximum_use_java_ordering() {
    // In UTF-16 code units, as String#compareTo orders, U+1F600 (a surrogate pair starting
    // 0xD83D) is below U+FF5E; by code point or UTF-8 byte it is above.
    let parent = static_table(&[
        (
            "S",
            ColumnType::String,
            strs(&[Some("\u{FF5E}"), Some("\u{1F600}"), None, Some("a")]),
        ),
        (
            "D",
            ColumnType::Double,
            double_opts(&[Some(0.0), Some(-0.0), None, Some(0.0)]),
        ),
    ]);
    let result = agg_by(
        &parent,
        &[
            agg(AggSpec::Min, &["MinS = S", "MinD = D"]),
            agg(AggSpec::Max, &["MaxS = S", "MaxD = D"]),
        ],
        &[],
    );
    // The zeros compare equal, so the first one given is kept.
    assert_eq!(rows(&*result), ["a|0.0|\u{FF5E}|0.0"]);
}
