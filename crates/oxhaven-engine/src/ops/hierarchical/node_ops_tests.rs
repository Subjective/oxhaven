//! Update views and formats of the nodes of a rollup (task 7.8.2). What the cases expect
//! is what Java 42.9 answers for the same operations on a rollup
//! (`oracle/scenarios/python/rollup_update_view*.py`, `rollup_formatted*.py`).

use super::tests::{aggregations, filtered, keys, text, trades, EXPAND_ALL};
use super::*;
use crate::ops::sort::SortColumn;
use oxhaven_expr::Compiler;

/// What `ColorUtil.toLong(RED)` is.
const RED: &str = "143833713130660064";

fn trades_rollup() -> RollupTable {
    rollup(&trades(), &aggregations(), true, &["Sym"], None).unwrap()
}

fn all_rows(rollup: RollupTable) -> Vec<String> {
    let view = View::new(
        Arc::new(rollup),
        Some(keys(&[(0, "", "", EXPAND_ALL)])),
        Some("Action".to_string()),
    );
    text(&view.snapshot(None, 0, 100).unwrap())
}

fn names(rollup: &RollupTable) -> Vec<&str> {
    rollup
        .columns()
        .iter()
        .map(|column| column.name.as_str())
        .collect()
}

fn strings(texts: &[&str]) -> Vec<String> {
    texts.iter().map(|text| text.to_string()).collect()
}

fn illegal(message: &str) -> HierarchicalError {
    HierarchicalError::IllegalArgument(message.to_string())
}

const INCOMPATIBLE: &str = "Incompatible operation recorders; compatible recorders must be \
                            created from the same table, with the same node type";

#[test]
fn an_update_view_adds_a_column_to_the_aggregated_nodes() {
    let rollup = trades_rollup();
    let views = rollup
        .node_operations(false)
        .unwrap()
        .update_view(
            &strings(&["Twice = Qty * 2", "Label = Sym + `!`"]),
            &Compiler,
        )
        .unwrap();
    let viewed = rollup.with_node_operations(false, &views).unwrap();
    assert_eq!(
        names(&viewed),
        [
            "__DEPTH__",
            "__EXPANDED__",
            "Sym",
            "Qty",
            "Rows",
            "Twice",
            "Label",
            "Sym",
            "Ex",
            "Qty"
        ]
    );
    let rows = all_rows(viewed);
    assert_eq!(
        rows[..4],
        [
            // the grand total has the columns too, of a null key
            "1|true|-|210|6|420|null!|-|-|-",
            "2|true|AAPL|70|3|140|AAPL!|-|-|-",
            "3|-|-|-|-|-|-|AAPL|N|10",
            "3|-|-|-|-|-|-|AAPL|Q|20",
        ]
    );
    // the rollup itself is as it was
    assert_eq!(all_rows(rollup)[1], "2|true|AAPL|70|3|-|-|-");
}

#[test]
fn the_rows_of_the_source_have_their_own_columns() {
    let rollup = trades_rollup();
    let operations = rollup
        .node_operations(true)
        .unwrap()
        .update_view(&strings(&["Neg = -Qty"]), &Compiler)
        .unwrap()
        .format_columns(&strings(&["Qty = RED"]), &Compiler)
        .unwrap();
    let viewed = rollup.with_node_operations(true, &operations).unwrap();
    assert_eq!(
        names(&viewed)[5..],
        ["Sym", "Ex", "Qty", "Neg", "Qty__TABLE_STYLE_FORMAT"]
    );
    let rows = all_rows(viewed.clone());
    assert_eq!(rows[1], "2|true|AAPL|70|3|-|-|-|-|-");
    assert_eq!(rows[2], format!("3|-|-|-|-|AAPL|N|10|-10|{RED}"));
    // a rollup without constituents has no such nodes
    let plain = super::rollup(&trades(), &aggregations(), false, &["Sym"], None).unwrap();
    assert_eq!(
        plain.node_operations(true).err(),
        Some(illegal("Rollup does not have constituent nodes"))
    );
    // a filter keeps the columns
    let kept = filtered(&viewed, &["Ex != `N`"]).unwrap();
    assert_eq!(names(&kept), names(&viewed));
    assert_eq!(
        all_rows(kept)[..3],
        [
            "1|true|-|130|3|-|-|-|-|-".to_string(),
            "2|true|AAPL|20|1|-|-|-|-|-".to_string(),
            format!("3|-|-|-|-|AAPL|Q|20|-20|{RED}"),
        ]
    );
}

#[test]
fn a_format_is_a_hidden_column_after_the_update_views() {
    let rollup = trades_rollup();
    let operations = rollup
        .node_operations(false)
        .unwrap()
        .format_columns(
            &strings(&["Qty = Qty > 100 ? RED : NO_FORMATTING"]),
            &Compiler,
        )
        .unwrap()
        // recorded later, computed first: a format can read it
        .update_view(&strings(&["Twice = Qty * 2"]), &Compiler)
        .unwrap()
        .format_columns(&strings(&["Twice = RED"]), &Compiler)
        .unwrap();
    let formatted = rollup.with_node_operations(false, &operations).unwrap();
    assert_eq!(
        names(&formatted)[2..7],
        ["Sym", "Qty", "Rows", "Qty__TABLE_STYLE_FORMAT", "Twice",]
    );
    assert_eq!(names(&formatted)[7], "Twice__TABLE_STYLE_FORMAT");
    let rows = all_rows(formatted);
    assert_eq!(rows[0], format!("1|true|-|210|6|{RED}|420|{RED}|-|-|-"));
    assert_eq!(rows[1], format!("2|true|AAPL|70|3|0|140|{RED}|-|-|-"));
}

#[test]
fn what_a_recorder_refuses() {
    let rollup = trades_rollup();
    let recorder = rollup.node_operations(false).unwrap();
    let refusal = |result: Result<NodeOperations, HierarchicalError>| result.err().unwrap();
    assert_eq!(
        refusal(recorder.update_view(&strings(&["Qty = 1"]), &Compiler)),
        illegal("Column Qty already exists")
    );
    assert_eq!(
        refusal(recorder.update_view(&strings(&["R = i"]), &Compiler)),
        illegal("updateView does not support virtual row variables. Invalid columns: [R=i]")
    );
    assert_eq!(
        refusal(recorder.format_columns(&strings(&["Qty RED"]), &Compiler)),
        illegal("Illegal format specification: Qty RED")
    );
    assert!(matches!(
        refusal(recorder.format_columns(&strings(&["Nope = RED"]), &Compiler)),
        HierarchicalError::IllegalArgument(message) if message.starts_with("Unknown columns: [Nope]")
    ));
    assert!(matches!(
        refusal(recorder.update_view(&strings(&["A = Nope + 1"]), &Compiler)),
        HierarchicalError::Formula(_)
    ));
    // the column of the source is no column of an aggregated node
    assert!(matches!(
        refusal(recorder.update_view(&strings(&["A = Ex"]), &Compiler)),
        HierarchicalError::Formula(_)
    ));
    // the same name twice in one call is the first
    let twice = recorder
        .update_view(&strings(&["B = Qty", "B = Qty + 1"]), &Compiler)
        .unwrap();
    let rows = all_rows(rollup.with_node_operations(false, &twice).unwrap());
    assert_eq!(rows[0], "1|true|-|210|6|210|-|-|-");
    // and in a second call refused
    assert_eq!(
        refusal(twice.update_view(&strings(&["B = Qty"]), &Compiler)),
        illegal("Column B already exists")
    );
}

#[test]
fn nodes_with_operations_take_no_operations_that_add_a_column() {
    let rollup = trades_rollup();
    let view = |rollup: &RollupTable, name: &str| {
        rollup
            .node_operations(false)
            .unwrap()
            .update_view(&[format!("{name} = Qty * 2")], &Compiler)
            .unwrap()
    };
    let viewed = rollup
        .with_node_operations(false, &view(&rollup, "Twice"))
        .unwrap();
    assert_eq!(
        viewed
            .with_node_operations(false, &view(&viewed, "Again"))
            .err(),
        Some(illegal(INCOMPATIBLE))
    );
    // nor those of a recorder of the rollup before
    assert_eq!(
        viewed
            .with_node_operations(false, &view(&rollup, "Again"))
            .err(),
        Some(illegal(INCOMPATIBLE))
    );
    // a sort is an operation too
    let sorted = rollup.with_sorts(&[SortColumn::ascending("Qty")]);
    assert_eq!(
        sorted
            .with_node_operations(false, &view(&sorted, "Twice"))
            .err(),
        Some(illegal(INCOMPATIBLE))
    );
    // the nodes of the other type are free
    let other = viewed
        .node_operations(true)
        .unwrap()
        .update_view(&strings(&["Neg = -Qty"]), &Compiler)
        .unwrap();
    let both = viewed.with_node_operations(true, &other).unwrap();
    assert_eq!(names(&both)[5..], ["Twice", "Sym", "Ex", "Qty", "Neg"]);
    // nothing recorded is the rollup
    let nothing = viewed.node_operations(false).unwrap();
    assert_eq!(
        names(&viewed.with_node_operations(false, &nothing).unwrap()),
        names(&viewed)
    );
}

#[test]
fn a_column_of_an_update_view_can_be_sorted_by_and_a_format_cannot() {
    let rollup = trades_rollup();
    let recorder = rollup
        .node_operations(false)
        .unwrap()
        .update_view(&strings(&["Neg = -Qty"]), &Compiler)
        .unwrap()
        .format_columns(&strings(&["Qty = RED"]), &Compiler)
        .unwrap();
    let by_neg = [SortColumn::ascending("Neg")];
    assert_eq!(
        rollup.check_recorded_sorts(false, &recorder, &by_neg),
        Ok(())
    );
    assert_eq!(
        rollup.check_recorded_sorts(
            false,
            &recorder,
            &[SortColumn::ascending("Qty__TABLE_STYLE_FORMAT")]
        ),
        Err(illegal(
            "Unknown column names [Qty__TABLE_STYLE_FORMAT], available column names are \
             [Sym, Qty, Rows, Neg]"
        ))
    );
    // in the same recorder
    let sorted = rollup
        .with_node_operations(false, &recorder.sort(&by_neg))
        .unwrap();
    let rows = all_rows(sorted);
    let groups: Vec<&str> = rows
        .iter()
        .filter(|row| row.starts_with("2|"))
        .map(|row| row.split('|').nth(2).unwrap())
        .collect();
    assert_eq!(groups, ["MSFT", "AAPL", "GOOG"]);
    // and in a recorder of the rollup that has the column
    let viewed = rollup.with_node_operations(false, &recorder).unwrap();
    let later = viewed.node_operations(false).unwrap();
    assert_eq!(viewed.check_recorded_sorts(false, &later, &by_neg), Ok(()));
    let sorted = viewed
        .with_node_operations(false, &later.sort(&by_neg))
        .unwrap();
    assert_eq!(all_rows(sorted), rows);
    // the rollup without the column does not know it
    let plain = rollup.node_operations(false).unwrap();
    assert!(rollup.check_recorded_sorts(false, &plain, &by_neg).is_err());
}

#[test]
fn a_recorder_of_a_formatted_rollup_formats_the_column_again() {
    let rollup = trades_rollup();
    let format = |rollup: &RollupTable, format: &str| {
        rollup
            .node_operations(false)
            .unwrap()
            .format_columns(&strings(&[format]), &Compiler)
            .unwrap()
    };
    let red = rollup
        .with_node_operations(false, &format(&rollup, "Qty = RED"))
        .unwrap();
    assert_eq!(
        all_rows(red.clone())[0],
        format!("1|true|-|210|6|{RED}|-|-|-")
    );
    let none = red
        .with_node_operations(false, &format(&red, "Qty = NO_FORMATTING"))
        .unwrap();
    assert_eq!(names(&none), names(&red));
    assert_eq!(all_rows(none)[0], "1|true|-|210|6|0|-|-|-");
}
