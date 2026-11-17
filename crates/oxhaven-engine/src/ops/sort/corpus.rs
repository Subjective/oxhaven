//! The oracle's scenarios, as far as they can be built from `sort`, `reverse`, `head`,
//! `tail`, `slice`, `merge` and `flatten`, checked against the results recorded from the
//! Java server by the replay of `ops/oracle.rs`.

use super::{head, reverse, slice, sort, tail, SortColumn, SortError};
use crate::ops::merge::{flatten, merge, MergeError};
use crate::ops::oracle::{list, replay_all, text, Built};
use oxhaven_contracts::table::Table;
use serde_json::Value;
use std::path::PathBuf;
use std::sync::Arc;

/// The updates of a sort, not only its rows: which modified rows it reports as modified
/// and which as removed and added. Recorded from the Java server by
/// `ops/snapshot/fixtures/record.py`; the oracle's corpus holds no updates.
#[test]
fn the_updates_for_modified_rows_match_java() {
    use crate::ops::snapshot::testing::{replay, Built};
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("src/ops/sort/fixtures/sort_modified_rows.json");
    let define =
        |operation: &Value, _: &[Arc<dyn Table>]| -> Built { panic!("no operation {operation}") };
    // 2 results at 14 steps, and the updates of both during 13 steps.
    assert_eq!(replay(&path, &define), 2 * 14 + 2 * 13);
}

/// The operations this test builds.
const OURS: [&str; 7] = [
    "sort", "reverse", "head", "tail", "slice", "merge", "flatten",
];

/// The gRPC status code the Java server answers with for each refusal, as recorded in
/// `error_unknown_column` (a sort by a column the table does not have), `sort_absolute`
/// (the absolute value of a string), `slice_positions` (a slice that ends before it
/// starts) and `error_types_and_matches` (a merge of tables with different columns). A
/// refusal without a recorded code gets a text that equals no code.
fn sort_code(error: &SortError) -> &'static str {
    match error {
        SortError::NoSuchColumn(_) => "FAILED_PRECONDITION",
        SortError::AbsoluteNotSupported { .. } | SortError::IllegalArgument(_) => {
            "INVALID_ARGUMENT"
        }
        SortError::Unsupported(_) => "an unsupported operation, which has no recorded code",
        SortError::NotSortable(_) => "a restricted sort, which has no recorded code",
        SortError::NotSortableColumn(_) => "an unsortable column, which has no recorded code",
        SortError::Table(_) => "a failed table, which has no recorded code",
    }
}

fn merge_code(error: &MergeError) -> &'static str {
    match error {
        MergeError::IncompatibleDefinitions(_) => "INVALID_ARGUMENT",
        MergeError::IllegalArgument(_) => "an illegal argument, which has no recorded code",
        MergeError::Unsupported(_)
        | MergeError::NoSuchColumn(_)
        | MergeError::Runtime(_)
        | MergeError::NullKey(_) => "a refused sorted merge, which has no recorded code",
        MergeError::Table(_) => "a failed table, which has no recorded code",
    }
}

fn sorted_or_refused(result: Result<Arc<dyn Table>, SortError>) -> Built {
    match result {
        Ok(table) => Built::Table(table),
        Err(error) => Built::Refused(sort_code(&error)),
    }
}

/// Builds a table of one of this test's operations.
fn define(operation: &Value, parents: &[Arc<dyn Table>]) -> Built {
    let number = |key: &str| operation[key].as_i64().expect("an integer");
    match text(&operation["op"]) {
        "sort" => {
            let columns: Vec<SortColumn> = list(&operation["by"])
                .iter()
                .map(|by| {
                    let column = match text(&by["direction"]) {
                        "ascending" => SortColumn::ascending(text(&by["column"])),
                        "descending" => SortColumn::descending(text(&by["column"])),
                        other => panic!("no direction {other}"),
                    };
                    if by["absolute"] == true {
                        column.absolute()
                    } else {
                        column
                    }
                })
                .collect();
            sorted_or_refused(sort(&parents[0], &columns))
        }
        "reverse" => sorted_or_refused(reverse(&parents[0])),
        "head" => sorted_or_refused(head(&parents[0], number("rows"))),
        "tail" => sorted_or_refused(tail(&parents[0], number("rows"))),
        "slice" => sorted_or_refused(slice(&parents[0], number("start"), number("stop"))),
        "flatten" => sorted_or_refused(flatten(&parents[0]).map_err(SortError::Table)),
        "merge" => match merge(parents) {
            Ok(table) => Built::Table(table),
            Err(error) => Built::Refused(merge_code(&error)),
        },
        other => panic!("no operation {other}"),
    }
}

/// Results that differ from what Java recorded, each with the first difference found.
const KNOWN_DIFFERENCES: [&str; 0] = [];

#[test]
fn results_match_the_recorded_java_results() {
    let tally = replay_all(&OURS, &define);
    assert_eq!(
        tally.differences, KNOWN_DIFFERENCES,
        "differences from Java"
    );

    // Every result table of the corpus that is a sort, reverse, head, tail, slice, merge
    // or flatten of tables that are themselves inputs or such results, and agrees with
    // Java at every step.
    let expected = [
        "empty_table_ops/h",
        "empty_table_ops/m",
        "empty_table_ops/s",
        "empty_table_ops/tl",
        "error_types_and_matches/merge_different",
        "error_unknown_column/bad_sort",
        "flatten_reverse/flat",
        "flatten_reverse/flat_empty",
        "flatten_reverse/flat_rev",
        "flatten_reverse/rev",
        "flatten_reverse/rev_empty",
        "flatten_reverse/rev_head",
        "flatten_reverse/rev_rev",
        "flatten_reverse/rev_sorted",
        "flatten_reverse/rev_tail",
        "flatten_reverse/sorted_s",
        "head_tail/h0",
        "head_tail/h3",
        "head_tail/h5_t2",
        "head_tail/h7",
        "head_tail/h99",
        "head_tail/t0",
        "head_tail/t2",
        "head_tail/t7",
        "head_tail/t99",
        "head_tail_zero/head_all",
        "head_tail_zero/head_empty",
        "head_tail_zero/head_more",
        "head_tail_zero/head_of_tail",
        "head_tail_zero/head_zero",
        "head_tail_zero/sorted_head_zero",
        "head_tail_zero/tail_all",
        "head_tail_zero/tail_empty_zero",
        "head_tail_zero/tail_more",
        "head_tail_zero/tail_of_head_zero",
        "head_tail_zero/tail_zero",
        "merge_column_order/fewer_columns",
        "merge_column_order/one_table",
        "merge_column_order/only_empty",
        "merge_column_order/other_type",
        "merge_column_order/reordered",
        "merge_column_order/reordered_first",
        "merge_column_order/with_empty",
        "merge_column_order/with_itself",
        "merge_tables/aa",
        "merge_tables/ab",
        "merge_tables/abab",
        "merge_tables/aeb",
        "merge_tables/ba",
        "merge_tables/ee",
        "slice_positions/all",
        "slice_positions/backwards",
        "slice_positions/before_start",
        "slice_positions/beyond",
        "slice_positions/end_then_start",
        "slice_positions/from_end",
        "slice_positions/last_two",
        "slice_positions/middle",
        "slice_positions/none",
        "slice_positions/of_empty",
        "slice_positions/of_empty_from_end",
        "slice_positions/past_end",
        "slice_positions/same",
        "slice_positions/slice_of_slice",
        "slice_positions/start_to_from_end",
        "slice_positions/start_to_from_end_empty",
        "sort_absolute/abs_byte_down",
        "sort_absolute/abs_double",
        "sort_absolute/abs_double_down",
        "sort_absolute/abs_float",
        "sort_absolute/abs_float_down",
        "sort_absolute/abs_int",
        "sort_absolute/abs_int_down",
        "sort_absolute/abs_long",
        "sort_absolute/abs_string",
        "sort_absolute/abs_then_plain",
        "sort_absolute/plain_then_abs",
        "sort_descending_ties/bool_down",
        "sort_descending_ties/double_down",
        "sort_descending_ties/down_of_down",
        "sort_descending_ties/down_then_up",
        "sort_descending_ties/float_down",
        "sort_descending_ties/int_down",
        "sort_descending_ties/reverse_of_down",
        "sort_descending_ties/string_down",
        "sort_descending_ties/time_down",
        "sort_descending_ties/two_down",
        "sort_descending_ties/up_then_down",
        "sort_float_nan/d_down",
        "sort_float_nan/d_up",
        "sort_float_nan/f_down",
        "sort_float_nan/f_up",
        "sort_int_nulls/down",
        "sort_int_nulls/up",
        "sort_multi_column/by_bool",
        "sort_multi_column/by_bool_down",
        "sort_multi_column/s_down_l_up",
        "sort_multi_column/s_then_i",
        "ticking_blink_sort_head_tail/bottom_one",
        "ticking_blink_sort_head_tail/first_two",
        "ticking_blink_sort_head_tail/last_two",
        "ticking_blink_sort_head_tail/none",
        "ticking_blink_sort_head_tail/rev",
        "ticking_blink_sort_head_tail/sorted_k",
        "ticking_blink_sort_head_tail/top_two",
        "ticking_empty_start/s",
        "ticking_head_tail/bottom",
        "ticking_head_tail/head_many",
        "ticking_head_tail/head_of_tail",
        "ticking_head_tail/head_two",
        "ticking_head_tail/head_zero",
        "ticking_head_tail/log_head",
        "ticking_head_tail/log_tail",
        "ticking_head_tail/log_tail_zero",
        "ticking_head_tail/sorted_k",
        "ticking_head_tail/tail_many",
        "ticking_head_tail/tail_of_head",
        "ticking_head_tail/tail_two",
        "ticking_head_tail/tail_zero",
        "ticking_head_tail/top",
        "ticking_join_merge/m",
        "ticking_join_merge/m_sorted",
        "ticking_merge_refreshing/ab",
        "ticking_merge_refreshing/all",
        "ticking_merge_refreshing/b_sorted",
        "ticking_merge_refreshing/ba",
        "ticking_merge_refreshing/merge_of_merge",
        "ticking_merge_refreshing/merged_sorted",
        "ticking_merge_refreshing/merged_tail",
        "ticking_merge_refreshing/twice",
        "ticking_slice_flatten_reverse/first_three",
        "ticking_slice_flatten_reverse/flat",
        "ticking_slice_flatten_reverse/flat_sorted",
        "ticking_slice_flatten_reverse/inner",
        "ticking_slice_flatten_reverse/last_two",
        "ticking_slice_flatten_reverse/middle",
        "ticking_slice_flatten_reverse/rev",
        "ticking_slice_flatten_reverse/rev_head",
        "ticking_slice_flatten_reverse/rev_slice",
        "ticking_slice_flatten_reverse/rev_sorted",
        "ticking_slice_flatten_reverse/sorted_k",
        "ticking_slice_flatten_reverse/sorted_middle",
        "ticking_sort_absolute_descending/abs_double",
        "ticking_sort_absolute_descending/abs_double_down",
        "ticking_sort_absolute_descending/abs_int",
        "ticking_sort_absolute_descending/abs_int_down",
        "ticking_sort_absolute_descending/abs_then_down",
        "ticking_sort_absolute_descending/double_down",
        "ticking_sort_absolute_descending/int_down",
        "ticking_sort_absolute_descending/string_down_int_up",
        "ticking_time_table/blinking_none",
        "ticking_time_table/first_half_days",
        "ticking_time_table/first_hours",
        "ticking_time_table/some_hours",
        "ticking_update_by_append/first3",
        "ticking_where_sort/by_s",
        "ticking_where_sort/by_v",
        "ticking_where_sort/first2",
        "ticking_where_sort/last2",
        "timestamps/down",
        "timestamps/twice",
        "timestamps/up",
    ];
    assert_eq!(tally.compared, expected);
    // 101 tables of static scenarios at one step, and the ticking ones at every step:
    // 4 at 3 steps, 8 at 5 steps, 1 at 6 steps, 22 at 8 steps, 26 at 9 steps.
    assert_eq!(tally.comparisons, 101 + 4 * 3 + 8 * 5 + 6 + 22 * 8 + 26 * 9);
    // Results of this test's operations that need an operation of another task, or an
    // input this test cannot make.
    let not_run = [
        "empty_table_and_meta/five_tail: update",
        "error_unknown_column/after_bad_where: where",
        "flatten_reverse/flat_big: where",
        "flatten_reverse/rev_big: where",
        "lazy_update_update_view/lazy_then_sort: lazy_update",
        "merge_column_order/after_view: view",
        "pipeline/ranked: agg_by",
        "pipeline/top: agg_by",
        "slice_positions/of_filtered: where",
        "ticking_head_tail/big_head: where",
        "ticking_head_tail/big_tail: where",
        "ticking_lazy_update_update_view/lazy_sorted: lazy_update",
        "ticking_merge_refreshing/derived: where",
        "ticking_slice_flatten_reverse/flat_big: where",
        "ticking_slice_flatten_reverse/rev_big: where",
        "ticking_update_by_append/last3: update_by",
    ];
    assert_eq!(tally.not_run, not_run, "not run");
    // The results of every other operation, none of which is built here.
    let others: Vec<(&str, usize)> = tally
        .others
        .iter()
        .map(|(operation, results)| (operation.as_str(), *results))
        .collect();
    let expected = [
        ("agg_all_by", 43),
        ("agg_by", 74),
        ("aj", 12),
        ("column_statistics", 19),
        ("drop_columns", 8),
        ("empty_table", 2),
        ("exact_join", 5),
        ("filter", 113),
        ("group_by", 13),
        ("head_by", 14),
        ("join", 7),
        ("keyed_transpose", 5),
        ("lazy_update", 13),
        ("left_join", 2),
        ("meta_table", 9),
        ("multi_join", 17),
        ("natural_join", 14),
        ("partition_get", 9),
        ("partition_merge", 5),
        ("raj", 12),
        ("range_join", 22),
        ("ring_table", 6),
        ("select", 6),
        ("select_distinct", 18),
        ("snapshot", 4),
        ("snapshot_when", 20),
        ("tail_by", 10),
        ("time_table", 2),
        ("ungroup", 15),
        ("update", 39),
        ("update_by", 42),
        ("update_view", 16),
        ("view", 40),
        ("where", 53),
        ("where_in", 20),
    ];
    assert_eq!(others, expected, "results of other operations");
}
