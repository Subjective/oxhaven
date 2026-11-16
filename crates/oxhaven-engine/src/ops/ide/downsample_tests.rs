//! The expected rows of the tests that name a line of `java_downsample.txt` are what Java
//! 42.9 answered for the same table and the same changes.

use super::*;
use crate::ops::sort::testing::{graph, keys_of, scripted, static_table};
use crate::ops::sort::{sort, SortColumn};
use arrow::array::{
    BooleanArray, Float64Array, Int32Array, Int64Array, StringArray, TimestampNanosecondArray,
    UInt16Array,
};
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::table::TableAttributes;
use oxhaven_testkit::{
    at_least_for, list_of, test_seeds, ConsistencyChecker, Recorder, ScriptedTable, TableChange,
};

const JAVA_ANSWERS: &str = include_str!("java_downsample.txt");
const T0: i64 = 1_700_000_000_000_000_000;
/// A zoom range within the second of the three clusters of rows, for the column `T`.
const ZOOM: (i64, i64) = (T0 + 1_000_005_000, T0 + 1_000_012_000);

/// One row of the test table. The rows come in clusters of twenty X values a million
/// apart, so that with a pixel count of 10 a cluster is one bucket.
#[derive(Clone)]
struct Row {
    k: i64,
    x: i64,
    y: Option<f64>,
    z: Option<i32>,
    s: Option<String>,
    b: Option<bool>,
    c: u16,
    n: Option<i32>,
}

/// The row with the key `k`, as `col` of the script that recorded Java's answers makes it.
fn row(k: i64) -> Row {
    let (cluster, j) = (k / 20, k % 20);
    Row {
        k,
        x: cluster * 1_000_000 + j,
        y: match k {
            _ if k % 9 == 4 => None,
            _ if k % 13 == 6 => Some(f64::NAN),
            _ => Some(((k * 37) % 11 - 5) as f64),
        },
        z: (!((5..=8).contains(&j) || k % 10 == 0)).then_some(((k * 7) % 5) as i32),
        s: (k % 6 != 1).then(|| ["b", "a", "d", "c"][((k * 3) % 4) as usize].to_string()),
        b: (k % 3 != 0).then_some(k % 3 == 1),
        c: 97 + ((k * 5) % 7) as u16,
        n: (cluster != 1).then_some(((k * 3) % 4) as i32),
    }
}

const NAMES: [&str; 9] = ["K", "X", "T", "Y", "Z", "S", "B", "C", "N"];
const Y: usize = 3;

fn types() -> [ColumnType; 9] {
    [
        ColumnType::Long,
        ColumnType::Long,
        ColumnType::Instant,
        ColumnType::Double,
        ColumnType::Int,
        ColumnType::String,
        ColumnType::Boolean,
        ColumnType::Char,
        ColumnType::Int,
    ]
}

/// One array per column of `NAMES`. `T` is `X` microseconds after `T0`.
fn arrays(rows: &[Row]) -> Vec<ArrayRef> {
    fn of<A, V>(rows: &[Row], value: impl Fn(&Row) -> V) -> ArrayRef
    where
        A: arrow::array::Array + FromIterator<V> + 'static,
    {
        Arc::new(rows.iter().map(value).collect::<A>())
    }
    let times: TimestampNanosecondArray = rows.iter().map(|r| Some(T0 + r.x * 1000)).collect();
    vec![
        of::<Int64Array, _>(rows, |r| Some(r.k)),
        of::<Int64Array, _>(rows, |r| Some(r.x)),
        Arc::new(times.with_timezone("UTC")),
        of::<Float64Array, _>(rows, |r| r.y),
        of::<Int32Array, _>(rows, |r| r.z),
        of::<StringArray, _>(rows, |r| r.s.clone()),
        of::<BooleanArray, _>(rows, |r| r.b),
        of::<UInt16Array, _>(rows, |r| Some(r.c)),
        of::<Int32Array, _>(rows, |r| r.n),
    ]
}

fn columns(rows: &[Row]) -> Vec<(&'static str, ColumnType, ArrayRef)> {
    let named = NAMES.into_iter().zip(types()).zip(arrays(rows));
    named
        .map(|((name, kind), values)| (name, kind, values))
        .collect()
}

fn rows(keys: impl IntoIterator<Item = i64>) -> Vec<Row> {
    keys.into_iter().map(row).collect()
}

fn request(x: &str, ys: &[&str], zoom: Option<(i64, i64)>) -> Downsample {
    Downsample {
        pixel_count: 10,
        zoom_range: zoom,
        x_column: x.to_string(),
        y_columns: ys.iter().map(|y| y.to_string()).collect(),
    }
}

/// The values of `K` that Java answered for `label`.
fn java(label: &str) -> Vec<RowKey> {
    let line = JAVA_ANSWERS
        .lines()
        .find_map(|line| line.strip_prefix(label)?.strip_prefix(" ="));
    let Some(line) = line else {
        panic!("java_downsample.txt has no line {label}");
    };
    line.split_whitespace()
        .map(|k| k.parse().unwrap())
        .collect()
}

/// A downsampled scripted table whose row keys are the values of `K`.
struct Rig {
    graph: Arc<dyn UpdateGraph>,
    script: Arc<ScriptedTable>,
    result: Arc<dyn Table>,
    recorder: Arc<Recorder>,
    checker: Arc<ConsistencyChecker>,
}

impl Rig {
    fn new(initial: &[Row], request: &Downsample) -> Rig {
        let graph = graph();
        let script = scripted(&graph, initial.iter().map(|r| r.k), &columns(initial));
        let source: Arc<dyn Table> = script.table();
        let result = run_chart_downsample(&source, request).unwrap();
        let recorder = Recorder::attach(result.clone());
        let checker = ConsistencyChecker::attach(result.clone());
        checker.assert_consistent();
        Rig {
            graph,
            script,
            result,
            recorder,
            checker,
        }
    }

    /// Applies `change` in a cycle of its own. An error is the failure of the result.
    fn step(&self, change: TableChange) -> Result<(), TableFailure> {
        let before = self.recorder.events().len();
        self.script.push(change);
        self.graph.run_cycle();
        let mut events = self.recorder.events().split_off(before);
        assert!(events.len() <= 1, "more than one update in a cycle");
        if let Some(Err(failure)) = events.pop() {
            assert!(self.result.is_failed());
            return Err(failure);
        }
        self.checker.assert_consistent();
        Ok(())
    }

    fn keys(&self) -> Vec<RowKey> {
        keys_of(&*self.result)
    }
}

fn add(rows: &[Row]) -> TableChange {
    TableChange::new().add(rows.iter().map(|r| r.k), arrays(rows))
}

/// The rows `keys` with `edit` applied, as a modify of every column: what adding a row
/// with a key it already has is to a keyed input table.
fn modify(keys: impl IntoIterator<Item = i64>, edit: impl Fn(&mut Row)) -> TableChange {
    let mut rows = rows(keys);
    rows.iter_mut().for_each(edit);
    TableChange::new().modify(
        rows.iter().map(|r| r.k),
        arrays(&rows).into_iter().enumerate(),
    )
}

fn remove(keys: impl IntoIterator<Item = i64>) -> TableChange {
    TableChange::new().remove(keys)
}

/// Checks the result against Java's answers for `label` before the first and after each
/// of `steps`.
fn check_steps(label: &str, request: &Downsample, steps: Vec<TableChange>) {
    let rig = Rig::new(&rows(0..60), request);
    assert_eq!(rig.keys(), java(&format!("{label} 0")), "{label} 0");
    for (step, change) in steps.into_iter().enumerate() {
        let step = step + 1;
        if let Err(failure) = rig.step(change) {
            panic!("{label} {step}: {failure}");
        }
        assert_eq!(
            rig.keys(),
            java(&format!("{label} {step}")),
            "{label} {step}"
        );
    }
}

#[test]
fn a_static_table_has_the_rows_java_picks() {
    let table = static_table(&columns(&rows(0..60)));
    let zooms = [
        None,
        Some(ZOOM),
        Some((T0 - 5, T0 + 3000)),
        Some((-50, 1_000_004)),
    ];
    let cases: [(&str, &[&str], usize); 15] = [
        ("X", &["Y"], 0),
        ("X", &["Z"], 0),
        ("X", &["S"], 0),
        ("X", &["B"], 0),
        ("X", &["C"], 0),
        ("X", &["N"], 0),
        ("X", &["Y", "Z", "S"], 0),
        ("T", &["Y"], 0),
        ("T", &["Z", "N"], 0),
        ("T", &["N", "Y"], 1),
        ("T", &["S", "B"], 2),
        ("X", &["S"], 3),
        ("T", &["Z"], 1),
        ("X", &["T"], 0),
        ("X", &["X"], 0),
    ];
    for (x, ys, zoom) in cases {
        let zoom = zooms[zoom];
        let range = zoom.map_or("-".to_string(), |(first, last)| format!("{first}..{last}"));
        let label = format!("static {x} {} {range}", ys.join(","));
        let result = run_chart_downsample(&table, &request(x, ys, zoom)).unwrap();
        assert_eq!(keys_of(&*result), java(&label), "{label}");
        assert!(!result.is_refreshing());
        assert_eq!(result.definition(), table.definition());
    }
}

#[test]
fn the_result_has_no_attributes_and_ticks_with_its_parent() {
    let rig = Rig::new(&rows(0..60), &request("X", &["Y"], None));
    assert!(rig.result.is_refreshing());
    assert_eq!(rig.result.attributes(), TableAttributes::default());
    assert!(!rig.result.is_flat());
}

#[test]
fn modified_values_move_the_extremes_and_the_runs_of_nulls() {
    let steps = vec![
        // the last bucket grows
        add(&rows(60..64)),
        // a new bucket
        add(&rows([80])),
        // a new maximum
        modify([3], |r| r.y = Some(100.0)),
        // the maximum becomes the minimum: the bucket is read again
        modify([3], |r| r.y = Some(-100.0)),
        // a tie with the minimum: the first stays
        modify([7], |r| r.y = Some(-100.0)),
        // the minimum becomes null
        modify([3], |r| r.y = None),
        // a run of nulls grows at its start
        modify([21, 22, 23], |r| r.z = None),
        // a run of nulls goes
        modify([5, 6, 7, 8], |r| r.z = Some(9)),
        // a bucket of nulls
        modify(40..60, |r| (r.y, r.z) = (None, None)),
        // and one value in it
        modify([45], |r| (r.y, r.z) = (Some(1.0), Some(1))),
    ];
    check_steps("modifies", &request("X", &["Y", "Z"], None), steps);
}

#[test]
fn a_string_made_null_stays_the_extreme_it_was() {
    let steps = vec![
        modify([2], |r| r.s = Some("z".to_string())),
        modify([2], |r| r.s = None),
        modify([9], |r| r.s = Some("y".to_string())),
        add(&rows(60..62)),
    ];
    check_steps("strings", &request("X", &["S"], None), steps);
    // the row 9 with its `y` is no maximum, because the `z` of the row 2 still is
    assert!(!java("strings 3").contains(&9));
    // a number that is made null is given up, and the next largest value is the maximum
    let rig = Rig::new(&rows(0..60), &request("X", &["Z"], None));
    rig.step(modify([2], |r| r.z = Some(50))).unwrap();
    rig.step(modify([2], |r| r.z = None)).unwrap();
    assert!(!rig.keys().contains(&13));
    rig.step(modify([13], |r| r.z = Some(40))).unwrap();
    assert!(rig.keys().contains(&13));
}

#[test]
fn removed_rows_that_are_no_extreme_leave_their_bucket() {
    let steps = vec![
        add(&rows(60..63)),
        remove([10]),
        remove([0]),
        add(&rows(63..64)),
    ];
    check_steps("removes", &request("X", &["Z"], None), steps);
}

#[test]
fn rows_outside_a_zoom_range_are_two_buckets() {
    let steps = vec![
        add(&rows(60..64)),
        modify([2], |r| r.z = Some(50)),
        modify([25, 26, 27], |r| r.z = None),
        modify([31], |r| r.z = Some(-3)),
        modify([2], |r| r.z = None),
        modify([50], |r| r.z = Some(77)),
    ];
    check_steps("zoom", &request("T", &["Z"], Some(ZOOM)), steps);
}

#[test]
fn the_result_of_an_empty_table_fails_when_a_row_arrives() {
    let rig = Rig::new(&[], &request("X", &["Y", "Z"], None));
    assert_eq!(rig.keys(), java("empty 0"));
    let failure = rig.step(add(&rows(0..3))).unwrap_err();
    assert_eq!(failure.message, "Interval is not positive: 0");
    assert_eq!(rig.keys(), java("empty 1"));
}

#[test]
fn rows_of_one_x_value_are_refused() {
    let mut same = rows(0..5);
    same.iter_mut().for_each(|r| r.x = 7);
    let table = static_table(&columns(&same));
    let failure = run_chart_downsample(&table, &request("X", &["Y"], None))
        .err()
        .unwrap();
    assert_eq!(failure.message, "Interval is not positive: 0");
    // a zoom range gives the buckets a width
    let zoomed = run_chart_downsample(&table, &request("X", &["Y"], Some((0, 1000)))).unwrap();
    assert!(!keys_of(&*zoomed).is_empty());
}

/// 3000 rows with the X values `0..3000` and a Y value that rises: of each bucket the
/// first row and the last.
fn rising(pixel_count: i32) -> Result<usize, TableFailure> {
    let numbers: ArrayRef = Arc::new(Int64Array::from_iter_values(0..3000));
    let table = static_table(&[
        ("X", ColumnType::Long, numbers.clone()),
        ("Y", ColumnType::Long, numbers),
    ]);
    let mut request = request("X", &["Y"], None);
    request.pixel_count = pixel_count;
    Ok(keys_of(&*run_chart_downsample(&table, &request)?).len())
}

#[test]
fn the_pixel_count_is_rounded_up_to_a_number_of_buckets() {
    // 500 buckets of (long) (1.1 * 2999 / 500) = 6 X values each
    assert_eq!(rising(1).unwrap(), 1000);
    assert_eq!(rising(500).unwrap(), 1000);
    // 1000 buckets of 3
    assert_eq!(rising(501).unwrap(), 2000);
    // 4000 buckets of (long) 0.82 = 0
    let failure = rising(2001).unwrap_err();
    assert_eq!(failure.message, "Interval is not positive: 0");
    // and more than 4000 as they are
    assert_eq!(rising(5000).unwrap_err(), failure);
}

#[test]
fn a_request_java_refuses_is_refused() {
    let lists: ArrayRef = list_of(
        &ColumnType::Vector(Box::new(ColumnType::Int)),
        vec![Some(1); 60],
        Arc::new(Int32Array::from(vec![1; 60])),
    );
    let mut all = columns(&rows(0..60));
    all.push(("V", ColumnType::Vector(Box::new(ColumnType::Int)), lists));
    let table = static_table(&all);
    let refused = |change: &dyn Fn(&mut Downsample)| {
        let mut request = request("X", &["Y"], None);
        change(&mut request);
        run_chart_downsample(&table, &request)
            .err()
            .unwrap()
            .message
    };
    assert_eq!(
        refused(&|r| r.pixel_count = 0),
        "Assertion failed: asserted pxCount > 0, instead pxCount == 0."
    );
    assert_eq!(
        refused(&|r| r.y_columns.clear()),
        "Assertion failed: asserted yColumnNames.length > 0, instead yColumnNames.length == 0."
    );
    assert_eq!(
        refused(&|r| r.zoom_range = Some((5, 5))),
        "Assertion failed: asserted zoomRange[0] < zoomRange[1], instead zoomRange[0] == 5, \
         zoomRange[1] == 5."
    );
    assert_eq!(
        refused(&|r| r.x_column = "Q".to_string()),
        "Unknown column names [Q], available column names are [K, X, T, Y, Z, S, B, C, N, V]"
    );
    assert!(refused(&|r| r.y_columns.push("Q".to_string())).starts_with("Unknown column names [Q]"));
    assert_eq!(
        refused(&|r| r.x_column = "Y".to_string()),
        "Cannot use non-Instant, non-long x column Y in downsample"
    );
    assert_eq!(
        refused(&|r| r.y_columns.push("V".to_string())),
        "Comparable.class.isAssignableFrom(columnSource.getType())"
    );
}

/// The test table with its last row first, whose columns read the parent's through a
/// redirection.
fn redirected() -> Arc<dyn Table> {
    let table = static_table(&columns(&rows(0..60)));
    sort(&table, &[SortColumn::descending("K")]).unwrap()
}

#[test]
#[should_panic(
    expected = "unported: io.deephaven.engine.table.ColumnSource#allowsReinterpret: downsampling by the long column X"
)]
fn a_long_x_column_that_is_not_held_in_memory_is_not_ported() {
    let _ = run_chart_downsample(&redirected(), &request("X", &["Y"], None));
}

#[test]
fn an_instant_x_column_need_not_be_held_in_memory() {
    let result = run_chart_downsample(&redirected(), &request("T", &["Y"], Some(ZOOM))).unwrap();
    // the rows 32 to 25 of the first order, which the range shows, and the two ends
    let keys = keys_of(&*result);
    assert!(
        [0, 26, 27, 34, 35, 59].iter().all(|k| keys.contains(k)),
        "{keys:?}"
    );
    assert!(keys.len() < 20);
}

#[test]
fn a_cycle_that_only_removes_an_extreme_fails_the_result() {
    // 2 has the largest Z of its bucket: 4
    let rig = Rig::new(&rows(0..60), &request("X", &["Z"], None));
    assert!(rig.keys().contains(&2));
    let failure = rig.step(remove([2])).unwrap_err();
    assert_eq!(
        failure.message,
        "Assertion failed: asserted yContexts.get(yColIndex) != null, instead \
         yContexts.get(yColIndex) == null."
    );

    // with a row added in the same cycle the bucket is read again: 9 is the first 4 now
    let rig = Rig::new(&rows(0..60), &request("X", &["Z"], None));
    assert!(!rig.keys().contains(&12));
    rig.step(remove([2]).add([60], arrays(&rows([60]))))
        .unwrap();
    assert_eq!(rig.keys()[..6], [0, 1, 4, 5, 8, 9]);
    assert!(rig.keys().contains(&12));
}

#[test]
fn a_modified_row_that_changes_its_bucket_is_not_ported() {
    let rig = Rig::new(&rows(0..60), &request("X", &["Y"], None));
    // within its bucket a row may move
    let before = rig.keys();
    rig.step(modify([10], |r| r.x = 19)).unwrap();
    assert_eq!(rig.keys(), before);
    let failure = rig.step(modify([10], |r| r.x = 1_000_000)).unwrap_err();
    assert_eq!(
        failure.message,
        "not ported: io.deephaven.clientsupport.plotdownsampling.RunChartDownsample.\
         DownsamplerListener#handleModified: a modified row whose X value moves it to another \
         bucket"
    );
}

/// A row below the others, alone in its bucket, and five rows in the bucket of the bin 0.
fn low_row_and_bin_zero() -> Rig {
    let mut rows = rows(0..7);
    rows[0].x = -10_000;
    rows[6].x = 1_000_000;
    for (row, y) in rows.iter_mut().zip([0.0, 5.0, 1.0, 9.0, 5.0, 5.0, 5.0]) {
        row.y = Some(y);
    }
    let rig = Rig::new(&rows, &request("X", &["Y"], None));
    assert_eq!(rig.keys(), [0, 1, 2, 3, 5, 6]);
    rig
}

#[test]
fn a_removed_row_of_the_bin_zero_is_taken_from_the_bucket_of_the_row_before_it() {
    // alone, the first row of the bin 0 leaves its bucket
    let rig = low_row_and_bin_zero();
    rig.step(remove([1])).unwrap();
    assert_eq!(rig.keys(), [0, 2, 3, 5, 6]);
    // after a row of another bucket it stays in its own, which then has a row that the
    // table has not
    let rig = low_row_and_bin_zero();
    let failure = rig.step(remove([0, 1])).unwrap_err();
    assert_eq!(
        failure.message,
        "not ported: io.deephaven.clientsupport.plotdownsampling.RunChartDownsample.\
         DownsamplerListener#notifyResultTable: a result with rows its parent does not have"
    );
}

#[test]
fn shifted_rows_are_followed_where_their_bucket_changes_too() {
    // the last cluster moves up and gets a row: its bucket forgets what it reported
    let rig = Rig::new(&rows(0..60), &request("X", &["Z"], None));
    let before = rig.keys();
    let mut added = row(59);
    added.k = 170;
    let change = TableChange::new()
        .shift(40, 59, 100)
        .add([170], arrays(&[added]));
    rig.step(change).unwrap();
    // 59 was there as the last row of its bucket only
    let moved = |k: &RowKey| if *k >= 40 { k + 100 } else { *k };
    let kept = before.iter().filter(|k| **k != 59);
    let mut expected: Vec<RowKey> = kept.map(moved).collect();
    expected.push(170);
    assert_eq!(rig.keys(), expected);

    // without the row the bucket reports the rows it had, at the keys they had
    let rig = Rig::new(&rows(0..60), &request("X", &["Z"], None));
    let failure = rig.step(TableChange::new().shift(40, 59, 100)).unwrap_err();
    assert_eq!(
        failure.message,
        "not ported: io.deephaven.clientsupport.plotdownsampling.RunChartDownsample.\
         DownsamplerListener#notifyResultTable: a result with rows its parent does not have"
    );
}

#[test]
fn a_failure_of_the_parent_fails_the_result() {
    let rig = Rig::new(&rows(0..60), &request("X", &["Y"], None));
    rig.script.push_failure("no more");
    rig.graph.run_cycle();
    let failure = rig.recorder.events().pop().unwrap().unwrap_err();
    assert_eq!(failure.message, "no more");
    assert!(rig.result.is_failed());
}

#[test]
fn the_buckets_outside_a_zoom_range_are_never_read_again() {
    // 2 has the largest Z below the range
    let rig = Rig::new(&rows(0..60), &request("T", &["Z"], Some(ZOOM)));
    assert_eq!(rig.keys()[..3], [0, 2, 15]);
    let failure = rig
        .step(remove([2]).add([60], arrays(&rows([60]))))
        .unwrap_err();
    assert_eq!(
        failure.message,
        "not ported: io.deephaven.clientsupport.plotdownsampling.RunChartDownsample.\
         DownsamplerListener#notifyResultTable: a result with rows its parent does not have"
    );
    // a row that was no extreme may go
    let rig = Rig::new(&rows(0..60), &request("T", &["Z"], Some(ZOOM)));
    rig.step(remove([3]).add([60], arrays(&rows([60]))))
        .unwrap();
    assert_eq!(rig.keys()[..3], [0, 2, 15]);
}

/// A small generator of the same numbers for a seed everywhere.
struct Numbers(u64);

impl Numbers {
    fn below(&mut self, bound: i64) -> i64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 % bound as u64) as i64
    }

    fn row(&mut self, k: i64) -> Row {
        let mut row = row(0);
        row.k = k;
        row.x = self.below(1_100_000);
        row.y = (self.below(10) != 0).then(|| (self.below(200) - 100) as f64);
        row.z = (self.below(3) != 0).then(|| self.below(50) as i32);
        row
    }
}

/// The largest and the smallest of the values of column `column` that are not null.
fn extremes(table: &dyn Table, column: usize) -> Option<(f64, f64)> {
    let values = table.column(column).chunk(&table.row_set()).unwrap();
    let values: Vec<f64> = match table.column(column).column_type() {
        ColumnType::Double => values
            .as_primitive::<Float64Type>()
            .iter()
            .flatten()
            .collect(),
        _ => {
            let values = values.as_primitive::<Int32Type>();
            values.iter().flatten().map(f64::from).collect()
        }
    };
    let largest = values.iter().copied().reduce(f64::max)?;
    Some((largest, values.into_iter().reduce(f64::min)?))
}

/// Random adds, modifies of the Y columns, and removes together with adds, which are the
/// changes Java follows without failing. What the result must then hold whatever the
/// order of the changes: rows of the parent, with its first and its last row and, of each
/// Y column, a largest and a smallest value. With a zoom range, which every other seed
/// has, no rows are removed and the values are not looked at: the buckets outside the
/// range do not keep their extremes.
#[test]
fn random_changes_keep_the_ends_and_the_extremes() {
    let mut cycles = 0;
    for seed in test_seeds(0..40) {
        let mut numbers = Numbers(seed * 0x9e37_79b9 + 1);
        let mut initial: Vec<Row> = (0..600).map(|k| numbers.row(2 * k)).collect();
        // the width of the buckets is that of the X values of the first and the last row
        (initial[0].x, initial[599].x) = (0, 1_000_000);
        let zoom = (seed % 2 == 1).then_some((T0 + 300_000_000, T0 + 600_000_000));
        let rig = Rig::new(&initial, &request("T", &["Y", "Z"], zoom));
        let mut live: Vec<i64> = initial.iter().map(|r| r.k).collect();
        let mut next = 1200;
        for step in 0..20 {
            let mut added = Vec::new();
            for _ in 0..1 + numbers.below(20) {
                added.push(numbers.row(next));
                next += 1 + numbers.below(3);
            }
            let kinds = if zoom.is_some() { 2 } else { 3 };
            let change = match numbers.below(kinds) {
                0 => add(&added),
                1 => {
                    let mut keys: Vec<i64> = (0..1 + numbers.below(20))
                        .map(|_| live[numbers.below(live.len() as i64) as usize])
                        .collect();
                    keys.sort_unstable();
                    keys.dedup();
                    let rows: Vec<Row> = keys.iter().map(|&k| numbers.row(k)).collect();
                    let values = arrays(&rows);
                    let ys = [Y, Y + 1].map(|column| (column, values[column].clone()));
                    TableChange::new().modify(keys, ys)
                }
                _ => {
                    let mut removed: Vec<i64> = (0..1 + numbers.below(5))
                        .map(|_| live[numbers.below(live.len() as i64) as usize])
                        .collect();
                    removed.sort_unstable();
                    removed.dedup();
                    live.retain(|k| !removed.contains(k));
                    live.extend(added.iter().map(|r| r.k));
                    add(&added).remove(removed)
                }
            };
            if change.update.removed.is_empty() && !change.update.added.is_empty() {
                live.extend(added.iter().map(|r| r.k));
            }
            if let Err(failure) = rig.step(change) {
                panic!("seed {seed}, step {step}: {failure}");
            }
            cycles += 1;
            let parent: Arc<dyn Table> = rig.script.table();
            let (rows, all) = (rig.result.row_set(), parent.row_set());
            assert!(rows.is_subset_of(&all), "seed {seed}, step {step}");
            assert_eq!(
                rows.first_row_key(),
                all.first_row_key(),
                "seed {seed}, step {step}"
            );
            assert_eq!(
                rows.last_row_key(),
                all.last_row_key(),
                "seed {seed}, step {step}"
            );
            assert!(rows.size() < all.size());
            for column in [Y, Y + 1].into_iter().filter(|_| zoom.is_none()) {
                assert_eq!(
                    extremes(&*rig.result, column),
                    extremes(&*parent, column),
                    "seed {seed}, step {step}, column {column}"
                );
            }
        }
    }
    assert!(cycles >= at_least_for(800, 0..40));
}
