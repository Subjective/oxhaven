//! Formulas that read a column at a fixed distance from the current row: `Col_[i - 1]`.
//!
//! Java does not evaluate such a formula as written. It adds a column to the table that
//! holds, in each row, the value of `Col` so many rows before or after, rewrites the formula
//! to read that column in the current row, and drops the column afterwards. The rewritten
//! formula reads no other row, so it is accepted on a refreshing table, and the added
//! column reports as modified the rows whose neighbour changed.
//!
//! | Here | Java |
//! | --- | --- |
//! | [`constant_array_accesses`] | `ShiftedColumnsFactory#getShiftedColumnDefinitions` |
//! | [`add_shifted_columns`] | `ShiftedColumnOperation#addShiftedColumns` |
//! | [`with_shifted_column`] | `SelectAndViewAnalyzer.AnalyzerContext#applyShiftsAndRemainingColumns` |
//!
//! `where` does the same with every such read of its filters at once
//! (`ShiftedColumnsFactory#where`); see the filter module.
//!
//! Every other use of a column array (`Col_[3]`, `Col_[i - Other]`, `Col_.size()`,
//! `sum(Col_)`) is evaluated by the formula itself, which is given the table's rows; see
//! the expression crate. Java refuses those on a refreshing table that is not a blink
//! table, and `plan::validate_safe_for_refresh` does.
//!
//! An offset that `Long#parseLong` cannot read (`Col_[i - 0x1]`, `Col_[i - 1_0]`) fails the
//! formula, as Java's parser does.
//!
//! Not ported, and refused by name until the owner decides (questions 6 and 8 of
//! `tasks/10.2-unported-updateby-select-rollup.md` and `tasks/10.5-unported-select-arrays.md`):
//!
//! - shifts of two different distances, neither of them zero, on a refreshing table
//!   (`Col_[i - 1] + Col_[i + 1]`, `Col_[i - 1] - Col_[i - 2]`). Java computes the rows such a
//!   pair makes dirty by feeding row positions to `RowSet#invert` as if they were row keys
//!   (`ShiftedColumnOperation#computeDirtyModifiedRowSetInPositionSpace`), so a change does
//!   not reach the rows that read it. Seen on Java 42.9: `update("M = X_[i-1] + X_[i+1]")`
//!   of a keyed input table keeps the old `M` in the rows next to a modified, added or
//!   removed row. [`shifted_table`] can serve them with the values the same formula has
//!   when it is made again, which `update_view` also shows.
//! - `select` with a shifted column on a static table whose result gets new row keys. Java
//!   then reads the parent's columns by the new keys
//!   (`SelectAndViewAnalyzer.AnalyzerContext#getPublishedColumnSources`). Seen on Java 42.9:
//!   `empty_table(6).update("X = i * 10").where("X != 20").select("Y = X_[i-1]")` has the
//!   20 of the row that `where` dropped.
//! - a table that already has a column named like a shifted column (`__X_Shifted_Minus_1__`).

pub(crate) use super::plan::Shift;
use super::{drop_columns_as, failure, Flavor, SelectError};
use crate::ops::merge::flatten;
use crate::table::{
    forget_reads_through, is_append_only, register_reads_through, QueryTable, TableSpec,
};
use crate::update::ModifiedColumnTransformer;
use arrow::array::{new_null_array, ArrayRef};
use oxhaven_contracts::expr::{
    ColumnAssignment, Expr, Formula, FormulaCompiler, FormulaError, RowVariable,
};
use oxhaven_contracts::rowset::{RowKey, RowRange, RowSet};
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table, TableAttributes, TableFailure};
use oxhaven_contracts::unported;
use oxhaven_contracts::update::{ModifiedColumnSet, TableUpdate};
use std::sync::Arc;

impl Shift {
    /// The name of the column that holds the shifted values.
    ///
    /// Behavior: io.deephaven.engine.table.impl.select.ShiftedColumnDefinition#getResultColumnName
    pub(crate) fn result_name(&self) -> String {
        format!(
            "__{}_Shifted_{}_{}__",
            self.column,
            if self.amount < 0 { "Minus" } else { "Plus" },
            self.amount.unsigned_abs()
        )
    }
}

/// A formula in which every `Col_[i ± n]` that Java rewrites has been replaced by the
/// column that holds the shifted values.
pub(crate) struct Rewritten {
    pub(crate) expr: Expr,
    /// The shifted columns the rewritten formula reads, in order of first use.
    pub(crate) shifts: Vec<Shift>,
}

/// A column array as the text of a formula writes it.
#[derive(Debug, PartialEq)]
struct Written<'a> {
    /// The column, without the `_`.
    column: &'a str,
    /// The text between the brackets of `Name_[...]`; `None` for `(Name_)[...]`, which
    /// Java does not read as a column at a distance.
    index: Option<&'a str>,
}

/// The column arrays of `text` that are indexed, in the order their names are written;
/// `None` when the text cannot be read that way.
///
/// The expression tree keeps neither parentheses nor how an index was written, and Java
/// tells `X_[i - 1]` from `X_[(i - 1)]` and from `(X_)[i - 1]`: only the first is a shift.
/// Names are read as the expression crate's lexer reads them, outside ASCII too.
fn written_indexes(text: &str) -> Option<Vec<Written<'_>>> {
    let chars: Vec<(usize, char)> = text.char_indices().collect();
    let byte = |at: usize| chars.get(at).map_or(text.len(), |(byte, _)| *byte);
    let char_at = |at: usize| chars.get(at).map(|(_, c)| *c);
    let is_name = |c: char| c == '_' || c == '$' || c.is_alphanumeric();
    let skip_space = |mut at: usize| {
        while char_at(at).is_some_and(char::is_whitespace) {
            at += 1;
        }
        at
    };
    // Where the last character before `at` that is not white space is.
    let back_space = |mut at: usize| {
        while at > 0 && chars[at - 1].1.is_whitespace() {
            at -= 1;
        }
        at.checked_sub(1)
    };
    // The end of the quoted literal that starts at `at`.
    let skip_quoted = |at: usize| -> Option<usize> {
        let quote = chars[at].1;
        let mut at = at + 1;
        while at < chars.len() {
            match chars[at].1 {
                '\\' => at += 2,
                c if c == quote => return Some(at + 1),
                _ => at += 1,
            }
        }
        None
    };
    let mut found = Vec::new();
    let mut at = 0;
    while at < chars.len() {
        match chars[at].1 {
            '"' | '\'' | '`' => at = skip_quoted(at)?,
            first if is_name(first) => {
                let start = at;
                // A qualified name, `Foo.X_`, is one name to the parser too.
                let dotted = |at: usize| {
                    !first.is_numeric()
                        && char_at(at) == Some('.')
                        && char_at(at + 1).is_some_and(|next| is_name(next) && !next.is_numeric())
                };
                while char_at(at).is_some_and(is_name) || dotted(at) {
                    at += 1;
                }
                let name = &text[byte(start)..byte(at)];
                let column = match name.strip_suffix('_') {
                    Some(column) if !column.is_empty() && !first.is_numeric() => column,
                    _ => continue,
                };
                let open = skip_space(at);
                if char_at(open) != Some('[') {
                    // `(Name_)[...]`: as many parentheses close right after the name as
                    // open right before it, and they are not those of a call.
                    let mut closes = 0;
                    let mut after = open;
                    while char_at(after) == Some(')') {
                        closes += 1;
                        after = skip_space(after + 1);
                    }
                    if closes == 0 || char_at(after) != Some('[') {
                        continue;
                    }
                    let (mut opens, mut outer) = (0, start);
                    while opens < closes {
                        match back_space(outer) {
                            Some(before) if chars[before].1 == '(' => {
                                opens += 1;
                                outer = before;
                            }
                            _ => break,
                        }
                    }
                    let called = back_space(outer).is_some_and(|before| is_name(chars[before].1));
                    if opens == closes && !called {
                        found.push(Written {
                            column,
                            index: None,
                        });
                    }
                    continue;
                }
                // The matching bracket.
                let mut depth = 0_usize;
                let mut close = open;
                loop {
                    match char_at(close)? {
                        '"' | '\'' | '`' => {
                            close = skip_quoted(close)?;
                            continue;
                        }
                        '[' => depth += 1,
                        ']' => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                    close += 1;
                }
                found.push(Written {
                    column,
                    index: Some(&text[byte(open + 1)..byte(close)]),
                });
                // An index may hold further indexed names.
                at = open + 1;
            }
            _ => at += 1,
        }
    }
    Some(found)
}

/// Whether `literal` is what Java's parser reads as an `IntegerLiteralExpr`: an `int`
/// literal in any base, with or without `_`, and not a `long` or floating point one. The
/// formula has been parsed, so a literal is a valid one.
fn is_int_literal(literal: &str) -> bool {
    let lower = literal.to_ascii_lowercase();
    let integer = if lower.starts_with("0x") {
        !lower.contains('p')
    } else {
        lower.starts_with("0b") || !(lower.contains('e') || lower.ends_with(['f', 'd']))
    };
    literal.starts_with(|c: char| c.is_ascii_digit())
        && literal
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !lower.ends_with('l')
        && integer
}

/// The distance of an index that is `i`, `ii`, or one of them plus or minus an `int`
/// literal; `None` for any other index.
///
/// # Errors
/// Java's `NumberFormatException` for a literal that `Long#parseLong` does not read: one
/// in another base than ten, with `_`, or beyond the `long` range (`i + 0x10`).
///
/// Behavior: io.deephaven.engine.table.impl.ShiftedColumnsFactory#parseForConstantArrayAccessAttributes,
/// which takes a name, or a binary `+` or `-` of a name and an `IntegerLiteralExpr`, and
/// reads the literal's text with `Long#parseLong`.
fn constant_distance(index: &str) -> Result<Option<i64>, FormulaError> {
    let index = index.trim();
    let Some(rest) = index
        .strip_prefix("ii")
        .or_else(|| index.strip_prefix('i'))
        .map(str::trim_start)
    else {
        return Ok(None);
    };
    let negative = match rest.chars().next() {
        None => return Ok(Some(0)),
        Some('+') => false,
        Some('-') => true,
        Some(_) => return Ok(None),
    };
    let literal = rest[1..].trim_start();
    if !is_int_literal(literal) {
        return Ok(None);
    }
    // `Long#parseLong` reads an optional sign and decimal digits, and nothing else.
    let distance: i64 = literal
        .bytes()
        .all(|byte| byte.is_ascii_digit())
        .then(|| literal.parse().ok())
        .flatten()
        .ok_or_else(|| FormulaError::Syntax(format!("For input string: \"{literal}\"")))?;
    Ok(Some(if negative { -distance } else { distance }))
}

/// Rewrites the `Col_[i ± n]` of a formula that Java turns into shifted columns; `None`
/// when the formula has none. `text` is the formula as written, or any text that holds it
/// and no other index.
///
/// Java looks for them from the top of the expression through binary operators,
/// parentheses and the branches and condition of `?:` only. An index inside a call, a
/// cast, a unary operator or another index stays an array access: `isNull(X_[i - 1])`.
/// So does a column array in parentheses, `(X_)[i - 1]`.
///
/// # Errors
/// Java's `NumberFormatException` for an offset that `Long#parseLong` cannot read; see
/// [`constant_distance`].
///
/// # Panics
/// With `unported!` for a text whose indexed column arrays this function cannot match with
/// those of the expression. No such text is known: the check guards against a text that
/// is not the expression's.
///
/// Behavior: io.deephaven.engine.table.impl.ShiftedColumnsFactory#getShiftedColumnDefinitions
/// and the `visit` methods of that class
pub(crate) fn constant_array_accesses(
    text: &str,
    expr: &Expr,
) -> Result<Option<Rewritten>, FormulaError> {
    // The array accesses of the tree, in the order their names are written.
    fn accesses<'a>(expr: &'a Expr, out: &mut Vec<&'a str>) {
        match expr {
            Expr::Column(_) | Expr::Literal(_) | Expr::RowVariable(_) | Expr::ColumnVector(_) => {}
            Expr::Unary { operand, .. } | Expr::Cast { operand, .. } => accesses(operand, out),
            Expr::Binary { left, right, .. } => {
                accesses(left, out);
                accesses(right, out);
            }
            Expr::Ternary {
                condition,
                if_true,
                if_false,
            } => {
                accesses(condition, out);
                accesses(if_true, out);
                accesses(if_false, out);
            }
            Expr::Call { arguments, .. } => arguments.iter().for_each(|a| accesses(a, out)),
            Expr::MethodCall {
                receiver,
                arguments,
                ..
            } => {
                accesses(receiver, out);
                arguments.iter().for_each(|a| accesses(a, out));
            }
            Expr::ColumnArrayAccess { column, position } => {
                out.push(column);
                accesses(position, out);
            }
        }
    }
    let mut in_tree = Vec::new();
    accesses(expr, &mut in_tree);
    if in_tree.is_empty() {
        return Ok(None);
    }
    let written = written_indexes(text);
    let agree = written.as_ref().is_some_and(|written| {
        written.len() == in_tree.len()
            && written
                .iter()
                .zip(&in_tree)
                .all(|(written, column)| written.column == *column)
    });
    let Some(written) = written.filter(|_| agree) else {
        unported!(
            "io.deephaven.engine.table.impl.ShiftedColumnsFactory#getShiftedColumnDefinitions",
            "a column array in a text this server cannot match with its expression: {text}"
        );
    };

    struct Rewriter<'a> {
        written: Vec<Written<'a>>,
        /// How many array accesses of the tree have been passed.
        seen: usize,
        shifts: Vec<Shift>,
    }
    impl Rewriter<'_> {
        /// Counts the accesses of a part Java does not look into.
        fn pass(&mut self, expr: &Expr) {
            let mut inside = Vec::new();
            accesses(expr, &mut inside);
            self.seen += inside.len();
        }

        fn rewrite(&mut self, expr: &Expr) -> Result<Expr, FormulaError> {
            Ok(match expr {
                Expr::Binary { op, left, right } => Expr::Binary {
                    op: *op,
                    left: Box::new(self.rewrite(left)?),
                    right: Box::new(self.rewrite(right)?),
                },
                Expr::Ternary {
                    condition,
                    if_true,
                    if_false,
                } => Expr::Ternary {
                    condition: Box::new(self.rewrite(condition)?),
                    if_true: Box::new(self.rewrite(if_true)?),
                    if_false: Box::new(self.rewrite(if_false)?),
                },
                Expr::ColumnArrayAccess { column, .. } => {
                    let distance = match self.written[self.seen].index {
                        Some(index) => constant_distance(index)?,
                        None => None,
                    };
                    self.pass(expr);
                    match distance {
                        Some(amount) => {
                            let shift = Shift {
                                column: column.clone(),
                                amount,
                            };
                            let name = shift.result_name();
                            if !self.shifts.contains(&shift) {
                                self.shifts.push(shift);
                            }
                            Expr::Column(name)
                        }
                        None => expr.clone(),
                    }
                }
                other => {
                    self.pass(other);
                    other.clone()
                }
            })
        }
    }
    let mut rewriter = Rewriter {
        written,
        seen: 0,
        shifts: Vec::new(),
    };
    let rewritten = rewriter.rewrite(expr)?;
    if rewriter.shifts.is_empty() {
        return Ok(None);
    }
    Ok(Some(Rewritten {
        expr: rewritten,
        shifts: rewriter.shifts,
    }))
}

/// A column of a table at a fixed distance in rows: the value in a row is the inner
/// column's value `shift` rows later, or earlier for a negative `shift`, and null where
/// there is no such row. It reads through the inner column and holds nothing.
///
/// Behavior: io.deephaven.engine.table.impl.sources.ShiftedColumnSource
struct ShiftedSource {
    /// The table whose rows give the positions, now and on the previous cycle.
    table: Arc<dyn Table>,
    inner: Arc<dyn ColumnSource>,
    shift: i64,
}

impl ShiftedSource {
    fn read(&self, rows: &RowSet, use_prev: bool) -> Result<ArrayRef, TableFailure> {
        let all = if use_prev {
            self.table.prev_row_set()
        } else {
            self.table.row_set()
        };
        let positions = all
            .invert(rows)
            .map_err(|_| failure("a shifted column was asked for rows its table lacks"))?;
        // The positions whose shifted position is a row: all but a run at one end.
        let last = all.size() as i64 - 1;
        let with_source = positions.intersect(&RowSet::from_ranges(
            (0.max(-self.shift) <= last.min(last - self.shift))
                .then(|| RowRange::new(0.max(-self.shift), last.min(last - self.shift))),
        ));
        let sources = with_source
            .shift(self.shift)
            .map_err(|error| failure(format!("a shifted column could not shift: {error}")))?;
        let keys = all.sub_set_for_positions(&sources, false);
        let values = if use_prev {
            self.inner.prev_chunk(&keys)?
        } else {
            self.inner.chunk(&keys)?
        };
        let missing = (positions.size() - with_source.size()) as usize;
        if missing == 0 {
            return Ok(values);
        }
        let nulls = new_null_array(values.data_type(), missing);
        let parts: [&dyn arrow::array::Array; 2] = if self.shift < 0 {
            [nulls.as_ref(), values.as_ref()]
        } else {
            [values.as_ref(), nulls.as_ref()]
        };
        arrow::compute::concat(&parts).map_err(|error| failure(error.to_string()))
    }
}

impl Drop for ShiftedSource {
    fn drop(&mut self) {
        forget_reads_through(self);
    }
}

impl ColumnSource for ShiftedSource {
    fn column_type(&self) -> &ColumnType {
        self.inner.column_type()
    }

    fn chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.read(rows, false)
    }

    fn prev_chunk(&self, rows: &RowSet) -> Result<ArrayRef, TableFailure> {
        self.read(rows, true)
    }
}

/// The positions in `source` of the rows whose shifted column changes because the rows
/// `updated` were added, removed or modified, without the positions of `updated` itself.
/// Both sets are in one key space. Positions past the end may be named.
///
/// Behavior: io.deephaven.engine.table.impl.ShiftedColumnOperation#computeDirtyModifiedRowSetInPositionSpace(RowSet, RowSet, long)
fn dirty_positions(source: &RowSet, updated: &RowSet, shift: i64) -> Result<RowSet, TableFailure> {
    let positions = source
        .invert(updated)
        .map_err(|_| failure("a table changed rows it does not have"))?;
    let mut dirty = Vec::new();
    let mut push = |first: RowKey, last: RowKey| {
        if first <= last {
            dirty.push(RowRange::new(first, last));
        }
    };
    if shift > 0 {
        // The rows before each run, up to the run before it.
        let mut lowest = 0;
        for range in positions.ranges() {
            push(lowest.max(range.first() - shift), range.first() - 1);
            lowest = range.last() + 1;
        }
    } else {
        // The rows after each run, up to the run after it.
        let mut ranges = positions.ranges();
        let mut run_end = ranges.next().map(|range| range.last());
        for next in ranges {
            if let Some(end) = run_end {
                push(end + 1, (next.first() - 1).min(end - shift));
            }
            run_end = Some(next.last());
        }
        if let Some(end) = run_end {
            if ((end + 1) as u64) < source.size() {
                push(end + 1, end - shift);
            }
        }
    }
    Ok(RowSet::from_ranges(dirty))
}

/// How far the shifted columns read from the current row: the farthest distance before it
/// (negative) and the farthest after it (positive), each zero when no shift reads that side.
fn reach(shifts: &[Shift]) -> (i64, i64) {
    let amounts = || shifts.iter().map(|shift| shift.amount);
    (
        amounts().min().unwrap_or(0).min(0),
        amounts().max().unwrap_or(0).max(0),
    )
}

/// The positions in `source` of the rows whose shifted columns change because the rows
/// `updated` changed, for shifts that read at most `behind` rows before the current row
/// and `ahead` rows after it: the rows on either side of a changed row within that reach.
/// For one distance, or one distance and zero, this is Java's answer; see
/// [`refuse_two_distances`] for the others.
///
/// Behavior: io.deephaven.engine.table.impl.ShiftedColumnOperation#computeDirtyModifiedRowSetInPositionSpace(RowSet, RowSet, long, long)
fn dirty_positions_within(
    source: &RowSet,
    updated: &RowSet,
    (behind, ahead): (i64, i64),
) -> Result<RowSet, TableFailure> {
    let mut dirty = RowSet::empty();
    for shift in [behind, ahead] {
        if shift != 0 {
            dirty.insert(&dirty_positions(source, updated, shift)?);
        }
    }
    Ok(dirty)
}

/// Refuses shifts at two different distances that are both not zero on a refreshing
/// table: owner question 8 of `tasks/10.5-unported-select-arrays.md`.
///
/// Java runs the one-distance computation twice, the second time over the positions the
/// first returned as if they were row keys, so a change does not reach the rows that read
/// it. Seen on Java 42.9 for `update` of `X_[i - 1] + X_[i + 1]`, `X_[i - 1] + X_[i - 2]`
/// and `X_[i - 1] * 1000 + X_[i + 2]` over a keyed input table: a modified, added or
/// removed row leaves the old values in its neighbours, which `update_view` and the same
/// `update` made again both show changed.
///
/// Behavior: io.deephaven.engine.table.impl.ShiftedColumnOperation#computeDirtyModifiedRowSetInPositionSpace(RowSet, RowSet, long, long)
fn refuse_two_distances(shifts: &[Shift]) {
    let amounts = || shifts.iter().map(|shift| shift.amount);
    let lowest = amounts().min().unwrap_or(0);
    let highest = amounts().max().unwrap_or(0);
    if lowest != highest && lowest != 0 && highest != 0 {
        unported!(
            "io.deephaven.engine.table.impl.ShiftedColumnOperation#computeDirtyModifiedRowSetInPositionSpace",
            "column arrays at two different distances from the current row, {lowest} and \
             {highest}, on a refreshing table: Java leaves the old values in the rows next \
             to a change"
        );
    }
}

/// `parent` with one more column for each shift: the values of the shift's column at its
/// distance. The result has the parent's rows and none of its attributes, and is not
/// marked flat.
///
/// When a row is added, removed, or modified in a shifted column's source, the rows within
/// the distance of it, on the side that reads it, are reported as modified in the shifted
/// columns.
///
/// # Panics
/// With `unported!` for a refreshing parent and shifts at two different distances that are
/// both not zero, and for a parent that has a column with the name of a shifted column.
///
/// Behavior: io.deephaven.engine.table.impl.ShiftedColumnOperation#addShiftedColumns and
/// the listener its `getShiftedColumns` makes
pub(crate) fn add_shifted_columns(
    parent: &Arc<dyn Table>,
    shifts: &[Shift],
) -> Result<Arc<dyn Table>, TableFailure> {
    if parent.is_refreshing() {
        refuse_two_distances(shifts);
    }
    shifted_table(parent, shifts)
}

/// [`add_shifted_columns`] without the refusal of two distances: a change marks the rows
/// on both sides of it within the farthest distance on each side.
pub(super) fn shifted_table(
    parent: &Arc<dyn Table>,
    shifts: &[Shift],
) -> Result<Arc<dyn Table>, TableFailure> {
    let declared = parent.definition().columns();
    let mut definitions: Vec<ColumnDefinition> = declared.to_vec();
    // The parent column each shifted column reads, by index.
    let mut reads = Vec::with_capacity(shifts.len());
    for shift in shifts {
        let index = parent.definition().index_of(&shift.column).ok_or_else(|| {
            failure(format!(
                "Column \"{}\" doesn't exist in this table",
                shift.column
            ))
        })?;
        reads.push(index);
        let name = shift.result_name();
        if parent.definition().index_of(&name).is_some() {
            // Java 42.9 computes the formula from its own shifted column and then drops
            // the table's column of that name from the result.
            unported!(
                "io.deephaven.engine.table.impl.ShiftedColumnOperation#addShiftedColumns",
                "a column array at a fixed distance on a table that has a column named {name}"
            );
        }
        definitions.push(ColumnDefinition::new(
            shift.result_name(),
            declared[index].column_type.clone(),
        ));
    }
    let definition = TableDefinition::new(definitions).map_err(|error| {
        failure(format!(
            "a shifted column has the name of a column: {error}"
        ))
    })?;
    let reach = Some(reach(shifts)).filter(|reach| parent.is_refreshing() && *reach != (0, 0));

    let columns = declared.len();
    let shifted_columns: Vec<usize> = (columns..columns + shifts.len()).collect();
    let transformer = ModifiedColumnTransformer::new((0..columns).map(|index| {
        let mut changes = vec![index];
        changes.extend(
            reads
                .iter()
                .enumerate()
                .filter(|(_, read)| **read == index)
                .map(|(shifted, _)| columns + shifted),
        );
        (index, changes)
    }));
    let source_columns = reads.clone();
    let operator_parent = parent.clone();

    let table = QueryTable::derive(parent, || {
        let mut sources: Vec<Arc<dyn ColumnSource>> =
            (0..columns).map(|index| parent.column(index)).collect();
        for (shift, &index) in shifts.iter().zip(&reads) {
            sources.push(register_reads_through(ShiftedSource {
                table: parent.clone(),
                inner: parent.column(index),
                shift: shift.amount,
            }));
        }
        let definition = super::inferred(parent, definition, &sources);
        let spec =
            TableSpec::new(definition, (*parent.row_set()).clone(), sources).keeping_partitioning();
        Ok((spec, move |_result: &QueryTable, upstream: &TableUpdate| {
            let parent = &operator_parent;
            let mut modified_columns = transformer.transform(&upstream.modified_columns);
            let Some(reach) = reach else {
                return Ok(TableUpdate {
                    modified_columns,
                    ..upstream.clone()
                });
            };
            let mut dirty: Option<RowSet> = None;
            let mut dirty_columns = false;

            if !upstream.removed.is_empty() {
                let before = parent.prev_row_set();
                let positions = dirty_positions_within(&before, &upstream.removed, reach)?;
                if !positions.is_empty() {
                    // The neighbours of the removed rows, by the keys they have now.
                    let rows = before.sub_set_for_positions(&positions, false);
                    dirty = Some(upstream.shifted.apply(&rows).map_err(|error| {
                        failure(format!("a shifted column could not follow shifts: {error}"))
                    })?);
                    dirty_columns = true;
                }
            }

            let adds = !upstream.added.is_empty();
            let modifies = !upstream.modified.is_empty()
                && upstream.modified_columns.contains_any(&source_columns);
            if adds || modifies {
                let now = parent.row_set();
                let mut positions = RowSet::empty();
                if adds {
                    positions = dirty_positions_within(&now, &upstream.added, reach)?;
                    dirty_columns |= !positions.is_empty();
                }
                if modifies {
                    // The transformer reports the shifted columns for these.
                    positions.insert(&dirty_positions_within(&now, &upstream.modified, reach)?);
                }
                if !positions.is_empty() {
                    let mut rows = now.sub_set_for_positions(&positions, false);
                    if adds && modifies {
                        // A modification may make an added row dirty.
                        rows.remove(&upstream.added);
                    }
                    if !rows.is_empty() {
                        match &mut dirty {
                            Some(dirty) => dirty.insert(&rows),
                            None => dirty = Some(rows),
                        }
                    }
                }
            }

            if dirty_columns {
                modified_columns =
                    modified_columns.union(&ModifiedColumnSet::of(shifted_columns.clone()));
            }
            let modified = match dirty {
                Some(mut dirty) => {
                    dirty.insert(&upstream.modified);
                    dirty
                }
                None => upstream.modified.clone(),
            };
            Ok(TableUpdate {
                modified,
                modified_columns,
                ..upstream.clone()
            })
        }))
    })?;
    Ok(table)
}

/// A compiler that reads the texts it is given as the rewritten expressions they come
/// with, and every other text as `compiler` does.
pub(crate) struct Replacing<'a> {
    compiler: &'a dyn FormulaCompiler,
    expressions: Vec<(String, Expr)>,
}

impl<'a> Replacing<'a> {
    pub(crate) fn expressions(
        compiler: &'a dyn FormulaCompiler,
        expressions: Vec<(String, Expr)>,
    ) -> Self {
        Self {
            compiler,
            expressions,
        }
    }
}

impl FormulaCompiler for Replacing<'_> {
    fn parse(&self, text: &str) -> Result<Expr, FormulaError> {
        match self.expressions.iter().find(|(known, _)| known == text) {
            Some((_, expr)) => Ok(expr.clone()),
            None => self.compiler.parse(text),
        }
    }

    fn parse_assignment(&self, text: &str) -> Result<ColumnAssignment, FormulaError> {
        self.compiler.parse_assignment(text)
    }

    fn compile(
        &self,
        expr: &Expr,
        definition: &TableDefinition,
    ) -> Result<Arc<dyn Formula>, FormulaError> {
        self.compiler.compile(expr, definition)
    }
}

/// The column of `columns` at which [`plan::analyze`](super::plan::analyze) stopped: the
/// first whose formula reads a column at a fixed distance.
pub(super) struct ShiftedColumn {
    /// Its index in the column list.
    pub index: usize,
    pub name: String,
    pub rewritten: Rewritten,
    /// Whether the columns before it leave a static `select` free to give its result new
    /// row keys.
    pub renumbers: bool,
}

fn run<S: AsRef<str>>(
    parent: &Arc<dyn Table>,
    columns: &[S],
    compiler: &dyn FormulaCompiler,
    flavor: Flavor,
) -> Result<Arc<dyn Table>, SelectError> {
    match flavor {
        Flavor::View => super::view(parent, columns, compiler),
        Flavor::UpdateView => super::update_view(parent, columns, compiler),
        Flavor::LazyUpdate => super::lazy_update(parent, columns, compiler),
        Flavor::Select => super::select(parent, columns, compiler),
        Flavor::Update => super::update(parent, columns, compiler),
    }
}

/// Whether `columns`, the shifted column of a static `select` and the columns after it, read
/// nothing of the parent: no column but those that the formulas of `before` and of
/// `columns` themselves define, and no row key.
///
/// Java computes the columns before the shifted one into storage with new row keys, and
/// then reads the parent's columns by those keys as well. Seen on Java 42.9 with
/// `t = empty_table(6).update(["X = i * 10", "U = i"]).where("X != 20")`:
/// `t.select(["W = X + 1", "Y = W_[i-1]", "Z = W * 2"])` is right, and so are a later
/// column that reads `i`, one that shifts `W` or `Y` again, and one that redefines `X`
/// before it is read; `Y = W_[i-1] + X`, a later `Z = X * 2`, `Z = X_[i-1]` and
/// `sum(X_)` have the values of other rows, and `k` is the new row key.
fn reads_nothing_of_the_parent(
    parent: &TableDefinition,
    before: &[&str],
    columns: &[&str],
    compiler: &dyn FormulaCompiler,
) -> Result<bool, SelectError> {
    let parse = |text: &str| {
        compiler
            .parse_assignment(text)
            .map_err(|error| SelectError::Formula {
                column: text.trim().to_string(),
                error,
            })
    };
    let mut defined = Vec::with_capacity(before.len() + columns.len());
    for text in before {
        defined.push(parse(text)?.name);
    }
    for text in columns {
        let assignment = parse(text)?;
        // `W_` is a name like any other until it is compiled: the whole of the column `W`,
        // unless the table has a column of that name.
        let is_defined = |name: &String| {
            defined.contains(name)
                || name.strip_suffix('_').is_some_and(|column| {
                    defined.iter().any(|defined| defined == column)
                        && parent.index_of(name).is_none()
                })
        };
        let mut nothing = true;
        super::plan::visit(&assignment.expr, &mut |expr| match expr {
            Expr::Column(name)
            | Expr::ColumnVector(name)
            | Expr::ColumnArrayAccess { column: name, .. } => nothing &= is_defined(name),
            Expr::RowVariable(RowVariable::K) => nothing = false,
            _ => {}
        });
        if !nothing {
            return Ok(false);
        }
        defined.push(assignment.name);
    }
    Ok(true)
}

/// The result of a formula operation whose column list has a column that reads another
/// column at a fixed distance. The columns before it are computed first, with the parent's
/// columns kept; then the shifted columns are added, the formula is computed over them,
/// and they are dropped; then the operation is applied to what is left of the list, which
/// may have such a column again.
///
/// A static `select` that gives its result new row keys keeps none of the parent's columns
/// with the columns before the shifted one: what follows is computed over those columns
/// alone, with the new row keys, and reads nothing else.
///
/// The result is append-only when the parent is and no distance is positive, a blink
/// table when the parent is, and never add-only.
///
/// With `unported!` for a static `select` that gets new row keys and whose shifted column,
/// or a column after it, reads a column of the parent or `k`: Java reads the parent's
/// columns by the new row keys there.
///
/// Behavior: the first pass of
/// io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer#createContext,
/// `AnalyzerContext#applyShiftsAndRemainingColumns` and
/// io.deephaven.engine.table.impl.ShiftedColumnsFactory#getShiftedColumnsTable
pub(super) fn with_shifted_column<S: AsRef<str>>(
    parent: &Arc<dyn Table>,
    columns: &[S],
    compiler: &dyn FormulaCompiler,
    flavor: Flavor,
    shifted: ShiftedColumn,
) -> Result<Arc<dyn Table>, SelectError> {
    let texts: Vec<&str> = columns.iter().map(AsRef::as_ref).collect();
    let mut renumbered = false;
    if flavor == Flavor::Select && !parent.is_refreshing() && !parent.is_flat() {
        // Behavior: io.deephaven.engine.table.impl.QueryTable#selectInternal
        if super::materialize::sparse_storage_wasteful(&parent.row_set()) {
            let flat = flatten(parent)?;
            return super::select(&flat, &texts, compiler);
        }
        if shifted.renumbers {
            let (before, rest) = texts.split_at(shifted.index);
            if !reads_nothing_of_the_parent(parent.definition(), before, rest, compiler)? {
                unported!(
                    "io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer.AnalyzerContext#getPublishedColumnSources",
                    "select on a static table whose result gets new row keys, with a \
                     column array at a fixed distance and, in that column or after it, a \
                     column of the table or k: {}",
                    shifted.name
                );
            }
            renumbered = true;
        }
    }
    let (before, text, after) = (
        &texts[..shifted.index],
        texts[shifted.index],
        &texts[shifted.index + 1..],
    );

    // The columns before it: with the parent's, or alone when they get new row keys.
    let keeping = match flavor {
        Flavor::Select if renumbered => Flavor::Select,
        Flavor::Select => Flavor::Update,
        Flavor::View => Flavor::UpdateView,
        other => other,
    };
    let so_far = if before.is_empty() {
        parent.clone()
    } else {
        run(parent, before, compiler, keeping)?
    };

    // Java makes the shifted column of a column of `ObjectVector`s of `Boolean` or `Instant`
    // one without an element type, which a client receives as strings: seen on Java 42.9
    // for `update("X = T_[i - 1]")` and `update("X = T_[i - 1][0]")` after a `group_by`.
    for shift in &shifted.rewritten.shifts {
        let declared = so_far.definition();
        let objects = declared.index_of(&shift.column).is_some_and(|index| {
            matches!(
                &declared.columns()[index].column_type,
                ColumnType::Vector(element)
                    if matches!(**element, ColumnType::Boolean | ColumnType::Instant)
            )
        });
        if objects {
            return Err(SelectError::Formula {
                column: text.trim().to_string(),
                error: FormulaError::Unsupported(format!(
                    "column array {}_ of a column of vectors of booleans or instants, whose \
                     elements Java sends as strings",
                    shift.column
                )),
            });
        }
    }
    let with_shifts = add_shifted_columns(&so_far, &shifted.rewritten.shifts)?;
    let dropped: Vec<String> = shifted
        .rewritten
        .shifts
        .iter()
        .map(Shift::result_name)
        .collect();
    let rewritten = ColumnAssignment {
        name: shifted.name.clone(),
        expr: shifted.rewritten.expr,
    };
    let computed = match flavor {
        Flavor::Select | Flavor::Update => super::materialize::stored(
            &with_shifts,
            &[text],
            compiler,
            Flavor::Update,
            Some(rewritten),
        ),
        Flavor::View | Flavor::UpdateView => super::computed_on_read(
            &with_shifts,
            &[text],
            compiler,
            Flavor::UpdateView,
            Some(rewritten),
        ),
        Flavor::LazyUpdate => super::computed_on_read(
            &with_shifts,
            &[text],
            compiler,
            Flavor::LazyUpdate,
            Some(rewritten),
        ),
    }?;

    let mut attributes = TableAttributes::default();
    if parent.is_refreshing() {
        let forward = shifted
            .rewritten
            .shifts
            .iter()
            .any(|shift| shift.amount > 0);
        attributes.append_only = !forward && is_append_only(&**parent);
        attributes.blink = parent.attributes().blink;
        // Java then applies the columns after the shifted one with the operation asked for,
        // also when there are none (`remainingCols` is an empty list, never null, once a
        // shifted column was found). `update` and `lazyUpdate` of no columns still make a
        // table, which is add-only because its append-only parent is
        // (`BaseTable#isAddOnly`); `updateView` of no columns returns its parent.
        attributes.add_only = attributes.append_only
            && after.is_empty()
            && matches!(flavor, Flavor::Update | Flavor::LazyUpdate);
    }
    let result = drop_columns_as(&computed, &dropped, Some(attributes))?;

    match flavor {
        Flavor::Select | Flavor::View => {
            let mut listed: Vec<String> = Vec::with_capacity(texts.len());
            for text in before {
                let assignment =
                    compiler
                        .parse_assignment(text)
                        .map_err(|error| SelectError::Formula {
                            column: text.trim().to_string(),
                            error,
                        })?;
                listed.push(assignment.name);
            }
            listed.push(shifted.name);
            listed.extend(after.iter().map(|text| text.to_string()));
            run(&result, &listed, compiler, flavor)
        }
        _ if after.is_empty() => Ok(result),
        _ => run(&result, after, compiler, flavor),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(set: &RowSet) -> Vec<RowKey> {
        set.iter().collect()
    }

    fn indexed<'a>(column: &'a str, index: &'a str) -> Written<'a> {
        Written {
            column,
            index: Some(index),
        }
    }

    fn wrapped(column: &str) -> Written<'_> {
        Written {
            column,
            index: None,
        }
    }

    #[test]
    fn indexes_are_read_as_written() {
        assert_eq!(
            written_indexes("Y = X_[i - 1] + Z_ [ (i+1) ] + `a_[` + 'b_[' + \"c_[\" + Q[2]"),
            Some(vec![indexed("X", "i - 1"), indexed("Z", " (i+1) ")])
        );
        assert_eq!(
            written_indexes("X_[Y_[i] + 1]"),
            Some(vec![indexed("X", "Y_[i] + 1"), indexed("Y", "i")])
        );
        assert_eq!(written_indexes("X_[i"), None);
        // Names outside ASCII, as the lexer reads them.
        assert_eq!(
            written_indexes("Ä_[i - 1] + Xé_[i]"),
            Some(vec![indexed("Ä", "i - 1"), indexed("Xé", "i")])
        );
        // A column array in parentheses, and parentheses that are not around one.
        assert_eq!(
            written_indexes("(X_)[i - 1] + ( (Y_) ) [i] + ((Z_)[0]) + (int)(W_)[1]"),
            Some(vec![wrapped("X"), wrapped("Y"), wrapped("Z"), wrapped("W")])
        );
        assert_eq!(
            written_indexes("f(X_)[0] + (A + X_)[1] + g((X_))[2] + (X_) + 1_[2] + X_.size()"),
            Some(vec![])
        );
        // A qualified name is one name, as the parser reads it.
        assert_eq!(
            written_indexes("Foo.X_[i - 1] + 1.5 + Y_.get(0)"),
            Some(vec![indexed("Foo.X", "i - 1")])
        );
    }

    /// Behavior: `ShiftedColumnsFactory#parseForConstantArrayAccessAttributes`. Seen on
    /// Java 42.9: `i + 2147483648` and `i - 9223372036854775807` are distances, and the
    /// offsets that are not are refused with `NumberFormatException`.
    #[test]
    fn only_a_row_variable_and_an_int_literal_is_a_distance() {
        for (index, distance) in [
            ("i", Some(0)),
            ("ii", Some(0)),
            (" i - 1 ", Some(-1)),
            ("ii+12", Some(12)),
            ("i + 010", Some(10)),
            ("i + 2147483648", Some(2_147_483_648)),
            ("i - 9223372036854775807", Some(-i64::MAX)),
            ("(i - 1)", None),
            ("i - 1L", None),
            ("i - 0x1L", None),
            ("i - 1.0", None),
            ("i - 1e1", None),
            ("i - 1f", None),
            ("i - 1D", None),
            ("i - 0x1p1", None),
            ("1 + i", None),
            ("i + -1", None),
            ("i * 2", None),
            ("i - 1 - 1", None),
            ("k - 1", None),
            ("iii", None),
            ("i -", None),
        ] {
            assert_eq!(constant_distance(index), Ok(distance), "{index}");
        }
        for (index, literal) in [
            ("i + 0x10", "0x10"),
            ("i - 0X1f", "0X1f"),
            ("i - 0b1", "0b1"),
            ("i - 1_0", "1_0"),
            ("ii + 0_7", "0_7"),
            ("i - 9223372036854775808", "9223372036854775808"),
            ("i + 99999999999999999999", "99999999999999999999"),
        ] {
            assert_eq!(
                constant_distance(index),
                Err(FormulaError::Syntax(format!(
                    "For input string: \"{literal}\""
                ))),
                "{index}"
            );
        }
    }

    /// Behavior: ShiftedColumnOperation#computeDirtyModifiedRowSetInPositionSpace. The rows
    /// that read a changed row through a negative shift are the ones after it, and through
    /// a positive shift the ones before it; the changed rows themselves are left out.
    #[test]
    fn dirty_positions_are_the_neighbours_on_the_reading_side() {
        let source = RowSet::from_keys([10, 20, 30, 40, 50, 60, 70, 80]);
        let updated = RowSet::from_keys([20, 30, 60]);
        // Positions 1, 2 and 5 changed.
        assert_eq!(
            keys(&dirty_positions(&source, &updated, -1).unwrap()),
            [3, 6]
        );
        assert_eq!(
            keys(&dirty_positions(&source, &updated, -3).unwrap()),
            [3, 4, 6, 7, 8]
        );
        assert_eq!(
            keys(&dirty_positions(&source, &updated, 1).unwrap()),
            [0, 4]
        );
        assert_eq!(
            keys(&dirty_positions(&source, &updated, 3).unwrap()),
            [0, 3, 4]
        );
        // The last row has no row after it, and the first none before it.
        let last = RowSet::from_keys([80]);
        assert!(dirty_positions(&source, &last, -2).unwrap().is_empty());
        let first = RowSet::from_keys([10]);
        assert!(dirty_positions(&source, &first, 2).unwrap().is_empty());
        assert_eq!(keys(&dirty_positions(&source, &first, -2).unwrap()), [1, 2]);
    }
}
