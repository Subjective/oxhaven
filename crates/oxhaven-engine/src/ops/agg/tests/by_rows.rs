//! Added rows taken in row order ([`AggOperator::add_rows`]) against the same rows brought
//! together by group ([`AggOperator::add_runs`]): the operators that take rows in order
//! must leave the same results and the same marks, chunk after chunk, and a static
//! aggregation that takes that path must give what a look at every row gives.

use super::super::operator::{AggOperator, FlatGroups, Marks, Rows, MODIFIED, RECOMPUTE};
use super::super::values::{Runs, Values};
use super::super::{Count, FloatAvg, FloatSum, LongAvg, LongSum, MinMax};
use super::*;
use arrow::array::{Float64Array, Int32Array, Int64Array, ListArray};
use oxhaven_contracts::rowset::{RowKey, RowSet};
use oxhaven_testkit::test_seeds;
use std::ops::Range;

/// A small deterministic generator.
struct Random(u64);

impl Random {
    fn below(&mut self, n: usize) -> usize {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % n as u64) as usize
    }
}

/// Values chosen so that a group of a chunk often has a NaN, an infinity, both zeros, a
/// sum that cancels, or a `long` sum that lands on the null sentinel (two of `-2^62`).
const LONGS: [Option<i64>; 8] = [
    Some(-(1 << 62)),
    Some(1),
    Some(-1),
    Some(7),
    None,
    Some(i64::MAX),
    Some(0),
    Some(-(1 << 62)),
];
const DOUBLES: [Option<f64>; 12] = [
    Some(1.5),
    Some(-0.0),
    Some(0.0),
    Some(0.1),
    Some(1e300),
    Some(-1e300),
    None,
    Some(0.2),
    Some(-7.25),
    Some(f64::NAN),
    Some(f64::INFINITY),
    Some(f64::NEG_INFINITY),
];

/// The positions of a chunk by slot and then increasing, with each slot's range of them.
fn grouped(slots: &[usize]) -> (Vec<usize>, Vec<(usize, Range<usize>)>) {
    let mut order: Vec<usize> = (0..slots.len()).collect();
    order.sort_by_key(|&position| slots[position]);
    let mut runs: Vec<(usize, Range<usize>)> = Vec::new();
    for (index, &position) in order.iter().enumerate() {
        match runs.last_mut() {
            Some((slot, range)) if *slot == slots[position] => range.end = index + 1,
            _ => runs.push((slots[position], index..index + 1)),
        }
    }
    (order, runs)
}

/// Each slot of a chunk once in the order of its first row, and where each row's slot
/// is among them.
fn distinct(slots: &[usize]) -> (Vec<usize>, Vec<u32>) {
    let mut found: Vec<usize> = Vec::new();
    let groups = slots
        .iter()
        .map(|slot| match found.iter().position(|seen| seen == slot) {
            Some(index) => index as u32,
            None => {
                found.push(*slot);
                found.len() as u32 - 1
            }
        })
        .collect();
    (found, groups)
}

/// Gives the same random chunks to two operators made by `new`, one by groups and one by
/// rows, and compares their results and marks after every chunk.
fn rows_against_groups(name: &str, floating: bool, new: &dyn Fn() -> Box<dyn AggOperator>) {
    for seed in test_seeds(0..40) {
        let mut random = Random(seed * 7919 + 17);
        // Fewer groups than a chunk has rows, about as many, and many more.
        let slot_count = [1, 3, 40, 2000][seed as usize % 4];
        // Some seeds have no NaN and no infinity, so that their counts are never made.
        let choices = if seed % 3 == 0 {
            DOUBLES.len() - 3
        } else {
            DOUBLES.len()
        };
        let (mut by_groups, mut by_rows) = (new(), new());
        assert!(by_rows.adds_rows(), "{name}");
        by_groups.ensure_slots(slot_count);
        by_rows.ensure_slots(slot_count);
        let all: Vec<usize> = (0..slot_count).collect();
        let mut next_row_key = 0;
        for chunk in 0..12 {
            let rows = [1, 5, 64, 300][random.below(4)];
            let slots: Vec<usize> = (0..rows).map(|_| random.below(slot_count)).collect();
            let row_keys: Vec<RowKey> = (0..rows)
                .map(|_| {
                    next_row_key += 1 + random.below(3) as RowKey;
                    next_row_key
                })
                .collect();
            let values = if floating {
                Values::Doubles(Float64Array::from_iter(
                    (0..rows).map(|_| DOUBLES[random.below(choices)]),
                ))
            } else {
                Values::Longs(Int64Array::from_iter(
                    (0..rows).map(|_| LONGS[random.below(LONGS.len())]),
                ))
            };
            let (order, runs) = grouped(&slots);
            let (found, groups) = distinct(&slots);
            let (mut expected, mut marks) = (Marks::default(), Marks::default());
            let mut effects = Vec::new();
            by_groups.add_runs(
                &Runs::new(Some(&values), None, &row_keys, &order, &runs),
                &mut effects,
            );
            assert_eq!(effects.len(), runs.len());
            for ((slot, _), effect) in runs.iter().zip(effects) {
                expected.note(*slot, effect);
            }
            by_rows.add_rows(
                &Rows {
                    slots: &slots,
                    distinct: &found,
                    groups: &groups,
                    row_keys: &row_keys,
                    values: Some(&values),
                },
                &mut marks,
            );
            let context = format!("{name}, seed {seed}, chunk {chunk}");
            for bit in [MODIFIED, RECOMPUTE] {
                assert_eq!(marks.marked(bit), expected.marked(bit), "{context}");
            }
            assert_eq!(
                text(&by_rows.results(&all)),
                text(&by_groups.results(&all)),
                "{context}"
            );
        }
    }
}

/// The values of `array` as text, in which a NaN is a NaN and the zeros differ.
fn text(array: &ArrayRef) -> Vec<String> {
    (0..array.len())
        .map(|row| {
            if array.is_null(row) {
                "null".to_string()
            } else {
                array_value_to_string(array, row).expect("a value")
            }
        })
        .collect()
}

#[test]
fn sums_averages_and_counts_take_rows_as_they_take_groups() {
    rows_against_groups("count", false, &|| Box::<Count>::default());
    rows_against_groups("sum of longs", false, &|| Box::new(LongSum::new(false)));
    rows_against_groups("absolute sum of longs", false, &|| {
        Box::new(LongSum::new(true))
    });
    rows_against_groups("average of longs", false, &|| Box::<LongAvg>::default());
    rows_against_groups("sum of doubles", true, &|| {
        Box::new(FloatSum::new(false, false))
    });
    rows_against_groups("sum of floats", true, &|| {
        Box::new(FloatSum::new(true, false))
    });
    rows_against_groups("absolute sum of doubles", true, &|| {
        Box::new(FloatSum::new(false, true))
    });
    rows_against_groups("average of doubles", true, &|| Box::<FloatAvg>::default());
}

#[test]
fn minimum_and_maximum_take_rows_as_they_take_groups() {
    for minimum in [true, false] {
        rows_against_groups("extreme of longs", false, &|| {
            Box::new(MinMax::new(ColumnType::Long, minimum, false))
        });
        rows_against_groups("extreme of ints", false, &|| {
            Box::new(MinMax::new(ColumnType::Int, minimum, false))
        });
        rows_against_groups("extreme of doubles", true, &|| {
            Box::new(MinMax::new(ColumnType::Double, minimum, false))
        });
    }
    // The other states take groups only.
    assert!(!MinMax::new(ColumnType::Long, true, true).adds_rows());
    assert!(!MinMax::new(ColumnType::String, true, false).adds_rows());
}

/// The typed states of a minimum and a maximum answer as Java's add-only operators do:
/// the first zero given stays, a NaN wins for good, a null is no value.
#[test]
fn typed_extremes_keep_the_first_zero_and_any_nan() {
    let run = |minimum: bool, values: &[Option<f64>]| -> String {
        let mut op = MinMax::new(ColumnType::Double, minimum, false);
        op.ensure_slots(1);
        let values = Values::Doubles(Float64Array::from(values.to_vec()));
        let slots = vec![0; values.len()];
        let row_keys: Vec<RowKey> = (0..values.len() as RowKey).collect();
        let (found, groups) = distinct(&slots);
        op.add_rows(
            &Rows {
                slots: &slots,
                distinct: &found,
                groups: &groups,
                row_keys: &row_keys,
                values: Some(&values),
            },
            &mut Marks::default(),
        );
        text(&op.results(&[0])).remove(0)
    };
    for minimum in [true, false] {
        assert_eq!(run(minimum, &[Some(-0.0), Some(0.0)]), "-0.0");
        assert_eq!(run(minimum, &[Some(0.0), Some(-0.0)]), "0.0");
        assert_eq!(
            run(minimum, &[Some(1.0), Some(f64::NAN), Some(-5.0)]),
            "NaN"
        );
        assert_eq!(run(minimum, &[None, None]), "null");
    }
    assert_eq!(run(true, &[Some(2.0), None, Some(-3.5), Some(9.0)]), "-3.5");
    assert_eq!(run(false, &[Some(2.0), None, Some(-3.5), Some(9.0)]), "9.0");
}

#[test]
fn rows_are_brought_together_by_group_in_order() {
    // Rows 10..=14 and 20..=22 in groups 2, 0, 2, 3, 0 and 0, 2, 2; group 1 has none.
    let rows = RowSet::from_keys([10, 11, 12, 13, 14, 20, 21, 22]);
    let slots = [2u32, 0, 2, 3, 0, 0, 2, 2];
    let flat = FlatGroups::new(&[3, 0, 4, 1], &slots, &rows);
    assert_eq!(flat.of(0), [11, 14, 20]);
    assert_eq!(flat.of(1), [] as [RowKey; 0]);
    assert_eq!(flat.of(2), [10, 12, 21, 22]);
    assert_eq!(flat.of(3), [13]);
}

/// A static table of `rows` rows whose keys are `key(row)`, with an int and a double.
fn keyed_table(rows: usize, key: impl Fn(usize) -> i32) -> Arc<dyn Table> {
    let definition = TableDefinition::new(vec![
        ColumnDefinition::new("K", ColumnType::Int),
        ColumnDefinition::new("A", ColumnType::Int),
        ColumnDefinition::new("B", ColumnType::Double),
    ])
    .expect("a definition");
    let fields: Vec<Field> = definition
        .columns()
        .iter()
        .map(|column| Field::new(&column.name, column.column_type.arrow_type(), true))
        .collect();
    let columns: Vec<ArrayRef> = vec![
        Arc::new(Int32Array::from_iter_values((0..rows).map(key))),
        Arc::new(Int32Array::from_iter((0..rows).map(|row| {
            (row % 11 != 4).then_some(((row * 31_337) % 1003) as i32 - 500)
        }))),
        Arc::new(Float64Array::from_iter((0..rows).map(|row| {
            (row % 7 != 2).then_some(((row * 4_871) % 997) as f64 / 8.0 - 60.0)
        }))),
    ];
    let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).expect("a batch");
    in_memory_table(definition, &[batch]).expect("a table")
}

/// A static aggregation whose operators all take rows in order, and a `group_by`, against
/// a look at every row: one group, a few, about as many groups as a chunk has rows (4,096,
/// one fewer and one more), and a group per row, over rows that span several chunks.
///
/// The sums here are exact (eighths and small integers), so they do not depend on the
/// order of the additions.
#[test]
fn static_aggregations_by_rows_are_a_look_at_every_row() {
    let rows = 3 * 4096 + 77;
    for groups in [1, 10, 4095, 4096, 4097, rows] {
        let key = |row: usize| ((row * 7919) % groups) as i32;
        let table = keyed_table(rows, key);
        let one = |spec: AggSpec, out: &str, input: &str| {
            Aggregation::columns(spec, [Pair::new(out, input)])
        };
        let aggregations = [
            one(AggSpec::Sum, "SumA", "A"),
            one(AggSpec::Avg, "AvgB", "B"),
            one(AggSpec::Min, "MinA", "A"),
            one(AggSpec::Max, "MaxB", "B"),
            one(AggSpec::Last, "LastA", "A"),
            one(AggSpec::First, "FirstB", "B"),
            one(AggSpec::Last, "LastB", "B"),
            Aggregation::count("N"),
        ];
        let result = aggregate(&table, &aggregations, false, &["K"]).expect("an aggregation");
        let grouped = super::super::group_by(&table, &["K"]).expect("a group_by");
        let read = |table: &Arc<dyn Table>, column: usize| -> ArrayRef {
            table
                .column(column)
                .chunk(&table.row_set())
                .expect("a column")
        };
        let source: Vec<ArrayRef> = (0..3).map(|column| read(&table, column)).collect();
        let a = source[1].as_primitive::<arrow::datatypes::Int32Type>();
        let b = source[2].as_primitive::<Float64Type>();
        // The groups in the order their keys are first seen, each with its rows.
        let mut order: Vec<i32> = Vec::new();
        let mut members: HashMap<i32, Vec<usize>> = HashMap::new();
        for row in 0..rows {
            members
                .entry(key(row))
                .or_insert_with(|| {
                    order.push(key(row));
                    Vec::new()
                })
                .push(row);
        }
        assert_eq!(*result.row_set(), RowSet::flat(order.len() as u64));
        assert_eq!(*grouped.row_set(), RowSet::flat(order.len() as u64));
        let opt = |value: Option<String>| value.unwrap_or_else(|| "null".to_string());
        let mut expected: Vec<Vec<String>> = vec![Vec::new(); 9];
        let mut expected_lists: Vec<Vec<Option<i32>>> = Vec::new();
        for group in &order {
            let members = &members[group];
            let ints: Vec<i64> = members
                .iter()
                .filter(|&&row| a.is_valid(row))
                .map(|&row| i64::from(a.value(row)))
                .collect();
            let doubles: Vec<f64> = members
                .iter()
                .filter(|&&row| b.is_valid(row))
                .map(|&row| b.value(row))
                .collect();
            let (first, last) = (members[0], members[members.len() - 1]);
            let cells = [
                group.to_string(),
                opt((!ints.is_empty()).then(|| ints.iter().sum::<i64>().to_string())),
                opt((!doubles.is_empty())
                    .then(|| format!("{:?}", doubles.iter().sum::<f64>() / doubles.len() as f64))),
                opt(ints.iter().min().map(i64::to_string)),
                opt(doubles
                    .iter()
                    .copied()
                    .reduce(f64::max)
                    .map(|v| format!("{v:?}"))),
                opt(a.is_valid(last).then(|| a.value(last).to_string())),
                opt(b.is_valid(first).then(|| format!("{:?}", b.value(first)))),
                opt(b.is_valid(last).then(|| format!("{:?}", b.value(last)))),
                members.len().to_string(),
            ];
            for (column, cell) in expected.iter_mut().zip(cells) {
                column.push(cell);
            }
            expected_lists.push(
                members
                    .iter()
                    .map(|&row| a.is_valid(row).then(|| a.value(row)))
                    .collect(),
            );
        }
        for (column, expected) in expected.iter().enumerate() {
            let found = read(&result, column);
            let found: Vec<String> = (0..found.len())
                .map(|row| {
                    if found.is_null(row) {
                        "null".to_string()
                    } else if let Some(doubles) = found.as_primitive_opt::<Float64Type>() {
                        format!("{:?}", doubles.value(row))
                    } else {
                        array_value_to_string(&found, row).expect("a value")
                    }
                })
                .collect();
            assert_eq!(&found, expected, "{groups} groups, column {column}");
        }
        let lists = read(&grouped, 1);
        let lists = lists.as_any().downcast_ref::<ListArray>().expect("vectors");
        let found_lists: Vec<Vec<Option<i32>>> = (0..lists.len())
            .map(|row| {
                lists
                    .value(row)
                    .as_primitive::<arrow::datatypes::Int32Type>()
                    .iter()
                    .collect()
            })
            .collect();
        assert_eq!(found_lists, expected_lists, "{groups} groups");
    }
}
