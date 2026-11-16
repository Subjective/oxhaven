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
//! Not ported, and refused by name:
//!
//! - shifts of two different distances, neither of them zero, on a refreshing table
//!   (`Col_[i - 1] + Col_[i + 1]`, `Col_[i - 1] - Col_[i - 2]`). Java computes the rows such a
//!   pair makes dirty by feeding row positions to `RowSet#invert` as if they were row keys
//!   (`ShiftedColumnOperation#computeDirtyModifiedRowSetInPositionSpace`), so which rows an
//!   `update` recomputes depends on how its row sets happen to be stored. Seen on Java
//!   42.9: `update("M = X_[i-1] + X_[i+1]")` of a keyed input table keeps the old `M` in
//!   the rows next to a modified row.
//! - `select` with a shifted column on a static table whose result gets new row keys. Java
//!   then reads the parent's columns by the new keys
//!   (`SelectAndViewAnalyzer.AnalyzerContext#getPublishedColumnSources`). Seen on Java 42.9:
//!   `empty_table(6).update("X = i * 10").where("X != 20").select("Y = X_[i-1]")` has the
//!   20 of the row that `where` dropped.
//! - an offset that is not written in decimal digits (`Col_[i - 0x1]`).

pub(crate) use super::plan::Shift;
use super::{drop_columns_as, failure, Flavor, SelectError};
use crate::ops::merge::flatten;
use crate::table::{
    forget_reads_through, is_append_only, register_reads_through, QueryTable, TableSpec,
};
use crate::update::ModifiedColumnTransformer;
use arrow::array::{new_null_array, ArrayRef};
use oxhaven_contracts::expr::{ColumnAssignment, Expr, Formula, FormulaCompiler, FormulaError};
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

/// The index of each `Name_[...]` in `text`, as written, in the order the names appear;
/// `None` when the text cannot be read that way.
///
/// The expression tree does not keep parentheses, and Java tells `X_[i - 1]` from
/// `X_[(i - 1)]`: only the first is a shift.
fn written_indexes(text: &str) -> Option<Vec<(&str, &str)>> {
    let bytes = text.as_bytes();
    let is_name = |byte: u8| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'$';
    // The end of the quoted literal that starts at `at`.
    let skip_quoted = |at: usize| -> Option<usize> {
        let quote = bytes[at];
        let mut at = at + 1;
        while at < bytes.len() {
            match bytes[at] {
                b'\\' => at += 2,
                byte if byte == quote => return Some(at + 1),
                _ => at += 1,
            }
        }
        None
    };
    let mut found = Vec::new();
    let mut at = 0;
    while at < bytes.len() {
        match bytes[at] {
            b'"' | b'\'' | b'`' => at = skip_quoted(at)?,
            byte if is_name(byte) => {
                let start = at;
                while at < bytes.len() && is_name(bytes[at]) {
                    at += 1;
                }
                let name = &text[start..at];
                let mut open = at;
                while open < bytes.len() && bytes[open].is_ascii_whitespace() {
                    open += 1;
                }
                let indexed = bytes.get(open) == Some(&b'[')
                    && name.len() > 1
                    && name.ends_with('_')
                    && !bytes[start].is_ascii_digit();
                if !indexed {
                    continue;
                }
                // The matching bracket.
                let mut depth = 0_usize;
                let mut close = open;
                loop {
                    match bytes.get(close)? {
                        b'"' | b'\'' | b'`' => {
                            close = skip_quoted(close)?;
                            continue;
                        }
                        b'[' => depth += 1,
                        b']' => {
                            depth -= 1;
                            if depth == 0 {
                                break;
                            }
                        }
                        _ => {}
                    }
                    close += 1;
                }
                found.push((&name[..name.len() - 1], &text[open + 1..close]));
                // An index may hold further indexed names.
                at = open + 1;
            }
            _ => at += 1,
        }
    }
    Some(found)
}

/// The distance of an index that is `i`, `ii`, or one of them plus or minus a whole number
/// written in decimal digits; `None` for any other index.
///
/// Behavior: io.deephaven.engine.table.impl.ShiftedColumnsFactory#parseForConstantArrayAccessAttributes,
/// which takes a name, or a binary `+` or `-` of a name and an `IntegerLiteralExpr`, and
/// reads the literal with `Long#parseLong`.
fn constant_distance(index: &str, formula: &str) -> Option<i64> {
    let index = index.trim();
    let rest = index
        .strip_prefix("ii")
        .or_else(|| index.strip_prefix('i'))?
        .trim_start();
    if rest.is_empty() {
        return Some(0);
    }
    let negative = match rest.as_bytes()[0] {
        b'+' => false,
        b'-' => true,
        _ => return None,
    };
    let digits = rest[1..].trim_start();
    if digits.is_empty() {
        return None;
    }
    if !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        // An `int` literal in another base or with `_` separators is an
        // `IntegerLiteralExpr` too, and `Long#parseLong` does not read it as Java does.
        let literal = digits
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() || matches!(byte, b'x' | b'X' | b'_'));
        if literal && digits.as_bytes()[0].is_ascii_digit() {
            unported!(
                "io.deephaven.engine.table.impl.ShiftedColumnsFactory#parseForConstantArrayAccessAttributes",
                "a column array offset that is not written in decimal digits: {formula}"
            );
        }
        return None;
    }
    let distance: i64 = match digits.parse() {
        Ok(distance) => distance,
        Err(_) => unported!(
            "io.deephaven.engine.table.impl.ShiftedColumnsFactory#parseForConstantArrayAccessAttributes",
            "a column array offset beyond the long range: {formula}"
        ),
    };
    Some(if negative { -distance } else { distance })
}

/// Rewrites the `Col_[i ± n]` of a formula that Java turns into shifted columns; `None`
/// when the formula has none. `text` is the formula as written, or any text that holds it
/// and no other index.
///
/// Java looks for them from the top of the expression through binary operators,
/// parentheses and the branches and condition of `?:` only. An index inside a call, a
/// cast, a unary operator or another index stays an array access: `isNull(X_[i - 1])`.
///
/// Behavior: io.deephaven.engine.table.impl.ShiftedColumnsFactory#getShiftedColumnDefinitions
/// and the `visit` methods of that class
pub(crate) fn constant_array_accesses(text: &str, expr: &Expr) -> Option<Rewritten> {
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
        return None;
    }
    let written = written_indexes(text);
    let agree = written.as_ref().is_some_and(|written| {
        written.len() == in_tree.len()
            && written
                .iter()
                .zip(&in_tree)
                .all(|((name, _), column)| name == column)
    });
    let Some(written) = written.filter(|_| agree) else {
        unported!(
            "io.deephaven.engine.table.impl.ShiftedColumnsFactory#getShiftedColumnDefinitions",
            "a column array that is indexed through parentheses or in a text this server \
             cannot match with its expression: {text}"
        );
    };

    struct Rewriter<'a> {
        text: &'a str,
        written: Vec<(&'a str, &'a str)>,
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

        fn rewrite(&mut self, expr: &Expr) -> Expr {
            match expr {
                Expr::Binary { op, left, right } => Expr::Binary {
                    op: *op,
                    left: Box::new(self.rewrite(left)),
                    right: Box::new(self.rewrite(right)),
                },
                Expr::Ternary {
                    condition,
                    if_true,
                    if_false,
                } => Expr::Ternary {
                    condition: Box::new(self.rewrite(condition)),
                    if_true: Box::new(self.rewrite(if_true)),
                    if_false: Box::new(self.rewrite(if_false)),
                },
                Expr::ColumnArrayAccess { column, .. } => {
                    let (_, index) = self.written[self.seen];
                    match constant_distance(index, self.text) {
                        Some(amount) => {
                            self.pass(expr);
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
                        None => {
                            self.pass(expr);
                            expr.clone()
                        }
                    }
                }
                other => {
                    self.pass(other);
                    other.clone()
                }
            }
        }
    }
    let mut rewriter = Rewriter {
        text,
        written,
        seen: 0,
        shifts: Vec::new(),
    };
    let rewritten = rewriter.rewrite(expr);
    if rewriter.shifts.is_empty() {
        return None;
    }
    Some(Rewritten {
        expr: rewritten,
        shifts: rewriter.shifts,
    })
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
        let ranges = positions.ranges();
        for pair in ranges.windows(2) {
            push(
                pair[0].last() + 1,
                (pair[1].first() - 1).min(pair[0].last() - shift),
            );
        }
        if let Some(end) = ranges.last().map(|range| range.last()) {
            if ((end + 1) as u64) < source.size() {
                push(end + 1, end - shift);
            }
        }
    }
    Ok(RowSet::from_ranges(dirty))
}

/// The one distance that decides which rows a change makes dirty; `None` when the shifted
/// columns are all at distance zero.
///
/// Behavior: io.deephaven.engine.table.impl.ShiftedColumnOperation#computeDirtyModifiedRowSetInPositionSpace(RowSet, RowSet, long, long).
/// For two distances that differ and are both not zero Java runs the one-distance
/// computation twice, the second time over the positions the first returned as if they
/// were row keys; that case is refused before a table is made.
fn deciding_shift(shifts: &[Shift]) -> Option<i64> {
    let lowest = shifts.iter().map(|shift| shift.amount).min().unwrap_or(0);
    let highest = shifts.iter().map(|shift| shift.amount).max().unwrap_or(0);
    if lowest == 0 && highest == 0 {
        None
    } else if lowest == highest || highest == 0 {
        Some(lowest)
    } else if lowest == 0 {
        Some(highest)
    } else {
        unported!(
            "io.deephaven.engine.table.impl.ShiftedColumnOperation#computeDirtyModifiedRowSetInPositionSpace",
            "column arrays at two different distances from the current row, {lowest} and \
             {highest}, on a refreshing table"
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
    let deciding = if parent.is_refreshing() {
        deciding_shift(shifts)
    } else {
        None
    };

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
            let Some(shift) = deciding else {
                return Ok(TableUpdate {
                    modified_columns,
                    ..upstream.clone()
                });
            };
            let mut dirty: Option<RowSet> = None;
            let mut dirty_columns = false;

            if !upstream.removed.is_empty() {
                let before = parent.prev_row_set();
                let positions = dirty_positions(&before, &upstream.removed, shift)?;
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
                    positions = dirty_positions(&now, &upstream.added, shift)?;
                    dirty_columns |= !positions.is_empty();
                }
                if modifies {
                    // The transformer reports the shifted columns for these.
                    positions.insert(&dirty_positions(&now, &upstream.modified, shift)?);
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

/// The result of a formula operation whose column list has a column that reads another
/// column at a fixed distance. The columns before it are computed first, with the parent's
/// columns kept; then the shifted columns are added, the formula is computed over them,
/// and they are dropped; then the operation is applied to what is left of the list, which
/// may have such a column again.
///
/// The result is append-only when the parent is and no distance is positive, a blink
/// table when the parent is, and never add-only.
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
    if flavor == Flavor::Select && !parent.is_refreshing() && !parent.is_flat() {
        // Behavior: io.deephaven.engine.table.impl.QueryTable#selectInternal
        if super::materialize::sparse_storage_wasteful(&parent.row_set()) {
            let flat = flatten(parent)?;
            return super::select(&flat, &texts, compiler);
        }
        if shifted.renumbers {
            unported!(
                "io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer.AnalyzerContext#getPublishedColumnSources",
                "select with a column array at a fixed distance on a static table whose \
                 result gets new row keys: {}",
                shifted.name
            );
        }
    }
    let (before, text, after) = (
        &texts[..shifted.index],
        texts[shifted.index],
        &texts[shifted.index + 1..],
    );

    // The columns before it, with the parent's.
    let keeping = match flavor {
        Flavor::Select => Flavor::Update,
        Flavor::View => Flavor::UpdateView,
        other => other,
    };
    let so_far = if before.is_empty() {
        parent.clone()
    } else {
        run(parent, before, compiler, keeping)?
    };

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

    #[test]
    fn indexes_are_read_as_written() {
        assert_eq!(
            written_indexes("Y = X_[i - 1] + Z_ [ (i+1) ] + `a_[` + 'b_[' + \"c_[\" + Q[2]"),
            Some(vec![("X", "i - 1"), ("Z", " (i+1) ")])
        );
        assert_eq!(
            written_indexes("X_[Y_[i] + 1]"),
            Some(vec![("X", "Y_[i] + 1"), ("Y", "i")])
        );
        assert_eq!(written_indexes("X_[i"), None);
    }

    #[test]
    fn only_a_row_variable_and_a_decimal_offset_is_a_distance() {
        for (index, distance) in [
            ("i", Some(0)),
            ("ii", Some(0)),
            (" i - 1 ", Some(-1)),
            ("ii+12", Some(12)),
            ("i + 010", Some(10)),
            ("(i - 1)", None),
            ("i - 1L", None),
            ("i - 1.0", None),
            ("1 + i", None),
            ("i + -1", None),
            ("i * 2", None),
            ("i - 1 - 1", None),
            ("k - 1", None),
            ("iii", None),
            ("i -", None),
        ] {
            assert_eq!(constant_distance(index, "f"), distance, "{index}");
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
