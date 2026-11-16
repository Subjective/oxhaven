//! Table formats: hidden columns that say how a client paints the cells of a column.
//!
//! A format `Column = format` is an update view of a hidden column. A style (a colour,
//! or a background and a foreground) is a `long` in `Column__TABLE_STYLE_FORMAT`, and for
//! `* = format`, the style of a whole row, in `__ROW__TABLE_STYLE_FORMAT`.
//! `Column = Decimal(pattern)` is the pattern, a `String`, in
//! `Column__TABLE_NUMBER_FORMAT`, and `Column = Date(pattern)` in
//! `Column__TABLE_DATE_FORMAT`. The schema a client receives links each hidden column to
//! its column.

use super::{update_view, SelectError};
use oxhaven_contracts::expr::FormulaCompiler;
use oxhaven_contracts::table::Table;
use std::sync::Arc;

const STYLE_SUFFIX: &str = "__TABLE_STYLE_FORMAT";
const NUMBER_SUFFIX: &str = "__TABLE_NUMBER_FORMAT";
const DATE_SUFFIX: &str = "__TABLE_DATE_FORMAT";
const ROW_NAME: &str = "__ROW";
const ROW_WILDCARD: &str = "*";
/// The method that makes the `long` of a style: of a colour, of the name of one, or of a
/// `long`, which it keeps.
const TO_LONG: &str = "io.deephaven.engine.util.ColorUtil.toLong";

/// Behavior: io.deephaven.engine.util.ColumnFormatting#isFormattingColumn
pub fn is_formatting_column(name: &str) -> bool {
    name.ends_with(STYLE_SUFFIX) || name.ends_with(NUMBER_SUFFIX) || name.ends_with(DATE_SUFFIX)
}

/// The column a formatting column formats, `*` for a row, or `None` for a column that
/// is no formatting column.
///
/// Behavior: ColumnFormatting#getFormatBaseColumn
fn base_column(name: &str) -> Option<&str> {
    if name.starts_with(ROW_NAME) {
        return Some(ROW_WILDCARD);
    }
    [STYLE_SUFFIX, NUMBER_SUFFIX, DATE_SUFFIX]
        .iter()
        .find_map(|suffix| name.rfind(suffix))
        .map(|index| &name[..index])
}

/// What is inside `name( .. )` when `text` is such a call and nothing else, with
/// something in the parentheses that does not end in white space.
fn inside<'a>(text: &'a str, name: &str) -> Option<&'a str> {
    let inner = text
        .strip_prefix(name)?
        .strip_prefix('(')?
        .strip_suffix(')')?;
    inner
        .chars()
        .last()
        .is_some_and(|last| !last.is_whitespace())
        .then_some(inner)
}

/// The hidden column and the formula of the format `format`, as an assignment.
///
/// Behavior: io.deephaven.engine.table.impl.select.SelectColumnFactory#getFormatExpression
fn format_assignment(format: &str) -> Result<String, SelectError> {
    let illegal =
        || SelectError::IllegalArgument(format!("Illegal format specification: {format}"));
    let text = format.trim();
    // Java's pattern does not match across a line.
    if text.contains(['\n', '\r']) {
        return Err(illegal());
    }
    let (column, rest) = text.split_once('=').ok_or_else(illegal)?;
    let (column, value) = (column.trim_end(), rest.trim_start());
    let identifier = column.chars().enumerate().all(|(index, c)| {
        c == '_' || c == '$' || c.is_ascii_alphabetic() || (index > 0 && c.is_ascii_digit())
    });
    if column.is_empty() || value.is_empty() || !(identifier || column == ROW_WILDCARD) {
        return Err(illegal());
    }
    Ok(if let Some(pattern) = inside(value, "Decimal") {
        format!("{column}{NUMBER_SUFFIX} = {pattern}")
    } else if let Some(pattern) = inside(value, "Date") {
        format!("{column}{DATE_SUFFIX} = {pattern}")
    } else {
        let hidden = if column == ROW_WILDCARD {
            format!("{ROW_NAME}{STYLE_SUFFIX}")
        } else {
            format!("{column}{STYLE_SUFFIX}")
        };
        let style = inside(value, "Color").unwrap_or(value);
        format!("{hidden} = {TO_LONG}({style})")
    })
}

/// The assignments of the hidden columns of `formats`, for a table whose columns that
/// are no formatting columns are `columns`. A format of another column is refused.
///
/// Behavior: io.deephaven.engine.table.impl.TableDefaults#formatColumns
pub fn format_assignments<S: AsRef<str>>(
    formats: &[S],
    columns: &[&str],
) -> Result<Vec<String>, SelectError> {
    let assignments = formats
        .iter()
        .map(|format| format_assignment(format.as_ref()))
        .collect::<Result<Vec<String>, SelectError>>()?;
    let unknown: Vec<&str> = assignments
        .iter()
        .filter_map(|assignment| {
            let hidden = assignment.split_once(" = ").map_or("", |(name, _)| name);
            base_column(hidden)
        })
        .filter(|base| *base != ROW_WILDCARD && !columns.contains(base))
        .collect();
    if !unknown.is_empty() {
        return Err(SelectError::IllegalArgument(format!(
            "Unknown columns: [{}], available columns = [{}]",
            unknown.join(", "),
            columns.join(", ")
        )));
    }
    Ok(assignments)
}

/// The format of `column` that is `format` in the rows where `condition` holds and none
/// in the others.
///
/// Behavior: TableDefaults#formatColumnWhere; #formatRowWhere is the same of `*`
pub fn conditional_format(column: &str, condition: &str, format: &str) -> String {
    format!("{column} = ({condition}) ? {TO_LONG}({format}) : {TO_LONG}(NO_FORMATTING)")
}

/// `parent` with the formats `formats`: its columns and the hidden columns, each computed
/// when it is read.
///
/// Behavior: TableDefaults#formatColumns
pub fn format_columns<S: AsRef<str>>(
    parent: &Arc<dyn Table>,
    formats: &[S],
    compiler: &dyn FormulaCompiler,
) -> Result<Arc<dyn Table>, SelectError> {
    let definition = parent.definition();
    let columns: Vec<&str> = definition
        .columns()
        .iter()
        .map(|column| column.name.as_str())
        .filter(|name| !is_formatting_column(name))
        .collect();
    let assignments = format_assignments(formats, &columns)?;
    update_view(parent, &assignments, compiler)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_format_is_an_assignment_of_its_hidden_column() {
        let columns = ["X", "Y"];
        let one =
            |format: &str| format_assignments(&[format], &columns).map(|mut all| all.remove(0));
        assert_eq!(
            one("X = RED").unwrap(),
            "X__TABLE_STYLE_FORMAT = io.deephaven.engine.util.ColorUtil.toLong(RED)"
        );
        assert_eq!(
            one(" * =bg(`#102030`) ").unwrap(),
            "__ROW__TABLE_STYLE_FORMAT = io.deephaven.engine.util.ColorUtil.toLong(bg(`#102030`))"
        );
        assert_eq!(
            one("X = Color(`blue`)").unwrap(),
            "X__TABLE_STYLE_FORMAT = io.deephaven.engine.util.ColorUtil.toLong(`blue`)"
        );
        assert_eq!(
            one("Y = Decimal(`0.00`)").unwrap(),
            "Y__TABLE_NUMBER_FORMAT = `0.00`"
        );
        assert_eq!(
            one("Y=Date(`yyyy`)").unwrap(),
            "Y__TABLE_DATE_FORMAT = `yyyy`"
        );
        // a comparison is not split at its `==`
        assert_eq!(
            one("X = X == 1 ? RED : BLUE").unwrap(),
            "X__TABLE_STYLE_FORMAT = io.deephaven.engine.util.ColorUtil.toLong(X == 1 ? RED : BLUE)"
        );
    }

    #[test]
    fn what_java_refuses_is_refused() {
        let columns = ["X", "Y"];
        let refusal = |format: &str| match format_assignments(&[format], &columns) {
            Err(SelectError::IllegalArgument(message)) => message,
            other => panic!("{format}: {other:?}"),
        };
        assert_eq!(refusal("X RED"), "Illegal format specification: X RED");
        assert_eq!(refusal("X ="), "Illegal format specification: X =");
        assert_eq!(
            refusal("1X = RED"),
            "Illegal format specification: 1X = RED"
        );
        assert_eq!(
            refusal("Nope = RED"),
            "Unknown columns: [Nope], available columns = [X, Y]"
        );
        // the format of a formatting column is the format of no column
        assert_eq!(
            refusal("X__TABLE_STYLE_FORMAT = RED"),
            "Unknown columns: [X__TABLE_STYLE_FORMAT], available columns = [X, Y]"
        );
    }
}
