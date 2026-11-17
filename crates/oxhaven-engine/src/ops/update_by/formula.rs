//! The rolling formula: a formula evaluated once per row over the vectors of the values
//! in the row's window.
//!
//! `RollingFormulaSpec` has two forms. With a parameter token the formula is an expression
//! of that token, which stands for the vector of one input column, and the operation makes
//! one result per column pair. Without one the formula is `Result = expression` over the
//! columns of the table, each a vector of its values in the window, except the bucket
//! columns, which are the scalars of the row's bucket; the operation makes that one result,
//! whatever its column pairs.
//!
//! Both are computed the same way: a vector per input and row, as the rolling group makes
//! them (`group::windows`), handed to an [`ArrayFormula`].

use super::group;
use super::rolling::Slide;
use super::window::WindowReads;
use super::{failure, invalid, UpdateByError};
use crate::ops::select::ArrayFormula;
use arrow::array::{ArrayRef, Int32Array};
use oxhaven_contracts::expr::{Expr, FormulaCompiler, FormulaError};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_contracts::table::{Table, TableFailure};
use oxhaven_contracts::unported;
use std::sync::Arc;

/// The name the token of a formula is replaced with.
///
/// Behavior: `BaseRollingFormulaOperator#PARAM_COLUMN_NAME`
const PARAM_COLUMN_NAME: &str = "__PARAM_COLUMN__";

/// The input a formula that reads no column is compiled with, so that it has a value per
/// row.
const ROWS_COLUMN_NAME: &str = "__ROLLING_FORMULA_ROWS__";

/// One input of a formula.
struct Input {
    /// Index of the column in the parent; `None` for the input that only counts the rows.
    column: Option<usize>,
    column_type: ColumnType,
    /// Whether the formula reads the value of the row's bucket and not a vector.
    key: bool,
}

/// The formula of one result column and where its inputs come from.
pub(super) struct RollingFormula {
    formula: ArrayFormula,
    inputs: Vec<Input>,
    /// What Java names the buffer of the window in the assertion that fails when a window
    /// holds no row at all; `None` where it fails without that assertion.
    buffer: Option<&'static str>,
    /// What Java throws whenever it computes a row with this formula, if it does.
    fails: Option<&'static str>,
    /// The formula's reads of a window by position; the formula then also reads the
    /// inputs they add, after `inputs`.
    reads: Option<WindowReads>,
}

/// What Java throws for a window of a `Boolean` or `Instant` column where it expects the
/// objects and is handed the bytes or longs they are stored as.
const CHUNK_CAST: &str = "class io.deephaven.chunk.ResettableWritableLongChunk cannot be cast \
                          to class io.deephaven.chunk.ObjectChunk";

/// What Java throws for `len` of a window of objects: the window gives the function an
/// `Object[]` where the formula casts to an array of the element type.
const ARRAY_CAST: &str = "class [Ljava.lang.Object; cannot be cast to class [Ljava.lang.String;";

/// Looks through `expression` for what Java does differently with the vector of a window
/// (an `io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper`)
/// than with a vector column, other than reads by position, which [`WindowReads`] checks
/// row by row. `objects` tells whether a name is a window of a type that is not
/// primitive. Gives what Java throws for every row, if the formula has such a call.
///
/// With `unported!` for `len` of anything that is not a column or the result of a
/// function.
fn window_calls(expression: &Expr, objects: &dyn Fn(&str) -> bool) -> Option<&'static str> {
    let all = |parts: &[&Expr]| parts.iter().find_map(|part| window_calls(part, objects));
    match expression {
        // A column array is refused before this, by `ArrayFormula::compile`.
        Expr::Column(_)
        | Expr::Literal(_)
        | Expr::RowVariable(_)
        | Expr::ColumnVector(_)
        | Expr::ColumnArrayAccess { .. } => None,
        Expr::Unary { operand, .. } | Expr::Cast { operand, .. } => all(&[operand]),
        Expr::Binary { left, right, .. } => all(&[left, right]),
        Expr::Ternary {
            condition,
            if_true,
            if_false,
        } => all(&[condition, if_true, if_false]),
        Expr::Call {
            function,
            arguments,
        } => {
            let inner = arguments
                .iter()
                .find_map(|part| window_calls(part, objects));
            match (function.as_str(), arguments.as_slice()) {
                ("len", [Expr::Column(name)]) if objects(name) => Some(ARRAY_CAST),
                ("len", [Expr::Column(_) | Expr::Call { .. }]) => inner,
                ("len", [_]) => unported!(
                    "io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper.ObjectRingBufferVectorWrapper#toArray",
                    "len of an expression in a rolling formula, which Java fails for a window of objects"
                ),
                _ => inner,
            }
        }
        Expr::MethodCall {
            receiver,
            arguments,
            ..
        } => window_calls(receiver, objects).or_else(|| {
            arguments
                .iter()
                .find_map(|part| window_calls(part, objects))
        }),
    }
}

/// Refuses a result that is a vector or an array of objects other than strings: Java makes
/// a column of `ObjectVector` that no longer knows its element type, and a client
/// receives the elements as strings.
fn check_result(result: &ColumnType) {
    if let Some(element) = result.list_element() {
        if !primitive(element) && *element != ColumnType::String {
            unported!(
                "io.deephaven.engine.table.impl.updateby.rollingformula.BaseRollingFormulaOperator#initializeSources",
                "a rolling formula whose result is of type {result:?}"
            );
        }
    }
}

/// Whether `expression` uses `i`, `ii` or `k`.
fn row_variables(expression: &Expr) -> bool {
    let any = |parts: &[&Expr]| parts.iter().any(|part| row_variables(part));
    match expression {
        Expr::RowVariable(_) => true,
        Expr::Column(_) | Expr::Literal(_) | Expr::ColumnVector(_) => false,
        Expr::Unary { operand, .. } | Expr::Cast { operand, .. } => any(&[operand]),
        Expr::Binary { left, right, .. } => any(&[left, right]),
        Expr::Ternary {
            condition,
            if_true,
            if_false,
        } => any(&[condition, if_true, if_false]),
        Expr::ColumnArrayAccess { position, .. } => any(&[position]),
        Expr::Call { arguments, .. } => arguments.iter().any(row_variables),
        Expr::MethodCall {
            receiver,
            arguments,
            ..
        } => row_variables(receiver) || arguments.iter().any(row_variables),
    }
}

/// Whether Java has a typed vector, chunk and ring buffer for the type.
fn primitive(column_type: &ColumnType) -> bool {
    use ColumnType::*;
    matches!(
        column_type,
        Byte | Char | Short | Int | Long | Float | Double
    )
}

fn refused(error: FormulaError) -> UpdateByError {
    invalid(error.to_string())
}

/// `formula` with every `source` that stands as a token of its own replaced by `dest`: one
/// that neither follows a letter or `_` nor is followed by a letter, a digit or `_`.
///
/// Java tests UTF-16 units with `Character.isLetter` and `Character.isDigit`; outside
/// ASCII this uses Rust's alphabetic and numeric classes for characters of the basic
/// plane, which differ from Java's for a few letter-like and digit-like characters.
///
/// Behavior: io.deephaven.engine.table.impl.select.FormulaUtil#replaceFormulaTokens
fn replace_formula_tokens(formula: &str, source: &str, dest: &str) -> String {
    let letter = |c: char| (c as u32) < 0x1_0000 && c.is_alphabetic();
    let digit = |c: char| {
        (c as u32) < 0x1_0000 && (c.is_ascii_digit() || (!c.is_ascii() && c.is_numeric()))
    };
    let mut text: Vec<char> = formula.chars().collect();
    let source: Vec<char> = source.chars().collect();
    let dest: Vec<char> = dest.chars().collect();
    let mut at = 0;
    while at + source.len() <= text.len() {
        let Some(found) = (at..=text.len() - source.len())
            .find(|&start| text[start..start + source.len()] == source[..])
        else {
            break;
        };
        at = found;
        let after = at + source.len();
        let joined_before = at > 0 && (letter(text[at - 1]) || text[at - 1] == '_');
        let joined_after =
            after < text.len() && (letter(text[after]) || text[after] == '_' || digit(text[after]));
        if joined_before || joined_after {
            at += 1;
            continue;
        }
        text.splice(at..after, dest.iter().copied());
        at += dest.len();
    }
    text.into_iter().collect()
}

/// Behavior: the name each typed operator of
/// io.deephaven.engine.table.impl.updateby.rollingformula gives `Assert.geq` in its `pop`;
/// `ObjectRollingFormulaOperator` names its buffer `charWindowValues` there.
fn buffer_name(input: &ColumnType) -> &'static str {
    match input {
        ColumnType::Boolean => "windowValues",
        ColumnType::Byte => "byteWindowValues",
        ColumnType::Short => "shortWindowValues",
        ColumnType::Int => "intWindowValues",
        ColumnType::Long => "longWindowValues",
        ColumnType::Float => "floatWindowValues",
        ColumnType::Double => "doubleWindowValues",
        _ => "charWindowValues",
    }
}

/// The formula of the form with a parameter token, for the input column `input` of
/// `definition`: `formula` with `token` standing for the vector of the column's values in
/// the window.
///
/// Behavior: the constructor of
/// io.deephaven.engine.table.impl.updateby.rollingformula.BaseRollingFormulaOperator. Java
/// makes one formula per type of input column and operation; here each column has its own,
/// which is the same formula.
pub(super) fn of_token(
    compiler: Option<&dyn FormulaCompiler>,
    definition: &TableDefinition,
    formula: &str,
    token: &str,
    input: usize,
) -> Result<RollingFormula, UpdateByError> {
    let compiler = compiler_of(compiler)?;
    if token.is_empty() {
        return Err(invalid("sourceToken cannot be empty"));
    }
    let column_type = definition.columns()[input].column_type.clone();
    if column_type.list_element().is_some() {
        unported!(
            "io.deephaven.engine.table.impl.updateby.rollingformula.ObjectRollingFormulaOperator",
            "a rolling formula over a column of type {column_type:?}"
        );
    }
    let expression = replace_formula_tokens(formula, token, PARAM_COLUMN_NAME);
    let parsed = compiler.parse(&expression).map_err(refused)?;
    // Java evaluates the formula over a table of one row, and does not refuse `i`, `ii`
    // and `k` as it does in the form without a token: they are 0 for every row.
    if row_variables(&parsed) {
        unported!(
            "io.deephaven.engine.table.impl.updateby.rollingformula.BaseRollingFormulaOperator",
            "i, ii or k in a rolling formula with a parameter token, which are 0 in Java"
        );
    }
    let vector = ColumnType::vector(column_type.clone());
    let formula = ArrayFormula::compile(compiler, &expression, &[(PARAM_COLUMN_NAME, vector)])
        .map_err(refused)?;
    // Behavior: ObjectRollingFormulaOperator.Context#setValueChunks, which takes the chunk
    // of an `Instant` column, a chunk of longs, for one of objects; and the `getByte` of
    // BaseRollingFormulaOperator#getChunkSetter on the formula's own column, which
    // ObjectRollingFormulaOperator and BooleanRollingFormulaOperator do not reinterpret.
    let fails = if column_type == ColumnType::Instant {
        Some(CHUNK_CAST)
    } else if !primitive(&column_type) && *formula.result_type() == ColumnType::Boolean {
        Some("class java.lang.Boolean cannot be cast to class java.lang.Byte")
    } else {
        window_calls(&parsed, &|_| !primitive(&column_type))
    };
    let (formula, reads) = match fails {
        Some(_) => (formula, None),
        None => {
            check_result(formula.result_type());
            let inputs = [(PARAM_COLUMN_NAME, ColumnType::vector(column_type.clone()))];
            with_reads(compiler, &parsed, formula, &inputs, &|name| {
                name == PARAM_COLUMN_NAME
            })?
        }
    };
    Ok(RollingFormula {
        formula,
        fails,
        reads,
        buffer: Some(buffer_name(&column_type)),
        inputs: vec![Input {
            column: Some(input),
            column_type,
            key: false,
        }],
    })
}

/// The result column and the formula of the form without a token: `formula` is
/// `Result = expression`, or the name of a column, which is then the result too. The
/// expression reads the columns of `definition`: those named in `by` as they are, every
/// other as the vector of its values in the window.
///
/// Behavior: `io.deephaven.api.Selectable#parse` and
/// UpdateByOperatorFactory.OperationVisitor#makeRollingFormulaMultiColumnOperator
pub(super) fn of_columns(
    compiler: Option<&dyn FormulaCompiler>,
    definition: &TableDefinition,
    formula: &str,
    by: &[&str],
) -> Result<(String, RollingFormula), UpdateByError> {
    let compiler = compiler_of(compiler)?;
    let (name, expression) = match formula.split_once('=') {
        Some((_, expression)) if expression.is_empty() || expression.starts_with('=') => {
            return Err(invalid(format!(
                "Unable to parse formula '{formula}', expected form '<newColumn>=<expression>'"
            )))
        }
        Some((name, expression)) => (name.trim(), expression),
        None => (formula.trim(), formula.trim()),
    };
    let mut chars = name.chars();
    let valid = chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_' || c == '$')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$');
    if !valid {
        return Err(invalid(format!("Invalid column name \"{name}\"")));
    }
    let input_of = |index: usize| {
        let column = &definition.columns()[index];
        let key = by.contains(&column.name.as_str());
        let column_type = column.column_type.clone();
        let seen = if key {
            column_type.clone()
        } else {
            ColumnType::vector(column_type.clone())
        };
        let input = Input {
            column: Some(index),
            column_type,
            key,
        };
        (column.name.as_str(), seen, input)
    };
    // Compiled against every column to learn which the formula reads, then against those.
    let all: Vec<(&str, ColumnType)> = (0..definition.columns().len())
        .map(|index| {
            let (name, seen, _) = input_of(index);
            (name, seen)
        })
        .collect();
    let read: Vec<usize> = ArrayFormula::compile(compiler, expression, &all)
        .map_err(refused)?
        .inputs_read()
        .iter()
        .filter_map(|name| definition.index_of(name))
        .collect();
    let mut seen: Vec<(&str, ColumnType)> = Vec::new();
    let mut inputs: Vec<Input> = Vec::new();
    let mut fails = None;
    for index in read {
        let (name, column_type, mut input) = input_of(index);
        if !input.key && input.column_type.list_element().is_some() {
            unported!(
                "io.deephaven.engine.table.impl.updateby.rollingformulamulticolumn.RollingFormulaMultiColumnOperator",
                "a rolling formula over a column of type {:?}",
                input.column_type
            );
        }
        // Behavior: RingBufferWindowConsumer#create makes an
        // ObjectRingBufferWindowConsumer for a `Boolean` or `Instant` column, whose
        // `setInputChunk` takes the chunk of bytes or longs for one of objects.
        if !input.key && matches!(input.column_type, ColumnType::Boolean | ColumnType::Instant) {
            fails = Some(CHUNK_CAST);
        }
        if input.key {
            // Behavior: RollingFormulaMultiColumnOperator.Context#setBucketKeyValues, which
            // gives the formula's bucket columns, in the order it found them in the
            // formula, the values of the first bucket columns of the operation: a
            // formula that reads one bucket column reads the first one's value.
            if inputs.iter().any(|input| input.key) {
                unported!(
                    "io.deephaven.engine.table.impl.updateby.rollingformulamulticolumn.RollingFormulaMultiColumnOperator.Context#setBucketKeyValues",
                    "a rolling formula that reads two bucket columns, which Java hands the \
                     values of the first bucket columns in the order its parser found them"
                );
            }
            let first = by.first().and_then(|name| definition.index_of(name));
            let first_type = first.map(|first| &definition.columns()[first].column_type);
            if first != Some(index) {
                if first_type == Some(&input.column_type) {
                    input.column = first;
                } else if primitive(&input.column_type) || input.column_type == ColumnType::Boolean
                {
                    fails = Some("java.lang.ClassCastException");
                } else {
                    unported!(
                        "io.deephaven.engine.table.impl.updateby.rollingformulamulticolumn.RollingFormulaMultiColumnOperator.Context#setBucketKeyValues",
                        "a rolling formula that reads a bucket column of objects that is \
                         not the first: Java hands it the value of the first, of another type"
                    );
                }
            }
        }
        seen.push((name, column_type));
        inputs.push(input);
    }
    let parsed = compiler.parse(expression).map_err(refused)?;
    let objects = |name: &str| {
        inputs.iter().any(|input| {
            !input.key
                && !primitive(&input.column_type)
                && input
                    .column
                    .is_some_and(|column| definition.columns()[column].name == name)
        })
    };
    let fails = fails.or(window_calls(&parsed, &objects));
    if inputs.is_empty() {
        seen.push((ROWS_COLUMN_NAME, ColumnType::Int));
        inputs.push(Input {
            column: None,
            column_type: ColumnType::Int,
            key: true,
        });
    }
    let formula = ArrayFormula::compile(compiler, expression, &seen).map_err(refused)?;
    let (formula, reads) = match fails {
        Some(_) => (formula, None),
        None => {
            check_result(formula.result_type());
            let windows: Vec<&str> = seen
                .iter()
                .zip(&inputs)
                .filter(|(_, input)| !input.key)
                .map(|((name, _), _)| *name)
                .collect();
            with_reads(compiler, &parsed, formula, &seen, &|name| {
                windows.contains(&name)
            })?
        }
    };
    Ok((
        name.to_owned(),
        RollingFormula {
            formula,
            inputs,
            buffer: None,
            fails,
            reads,
        },
    ))
}

/// `formula`, compiled from `parsed` over `inputs`, and its reads of the windows that
/// `windows` names by position, if it has any: the formula is then compiled again to read
/// the positions those reads add.
fn with_reads(
    compiler: &dyn FormulaCompiler,
    parsed: &Expr,
    formula: ArrayFormula,
    inputs: &[(&str, ColumnType)],
    windows: &dyn Fn(&str) -> bool,
) -> Result<(ArrayFormula, Option<WindowReads>), UpdateByError> {
    let Some((reads, rewritten, all)) =
        WindowReads::find(compiler, parsed, inputs, windows).map_err(refused)?
    else {
        return Ok((formula, None));
    };
    let all: Vec<(&str, ColumnType)> = all
        .iter()
        .map(|(name, column_type)| (name.as_str(), column_type.clone()))
        .collect();
    let formula = ArrayFormula::compile_parsed(compiler, &rewritten, &all).map_err(refused)?;
    Ok((formula, Some(reads)))
}

fn compiler_of(
    compiler: Option<&dyn FormulaCompiler>,
) -> Result<&dyn FormulaCompiler, UpdateByError> {
    compiler
        .ok_or_else(|| invalid("a rolling formula needs a formula compiler: use update_by_with"))
}

impl RollingFormula {
    pub(super) fn result_type(&self) -> &ColumnType {
        self.formula.result_type()
    }

    /// The parent columns whose modification changes the result: the inputs that are not
    /// bucket columns.
    ///
    /// Behavior: the `affectingColumns` of
    /// UpdateByOperatorFactory.OperationVisitor#makeRollingFormulaOperator and
    /// #makeRollingFormulaMultiColumnOperator
    pub(super) fn reads(&self) -> Vec<usize> {
        self.inputs
            .iter()
            .filter(|input| !input.key)
            .filter_map(|input| input.column)
            .collect()
    }

    /// The formula's value for each of `rows`, the rows computed, in order; `slides` holds
    /// the slide of the window to each of them over the rows `influencers`. A window that
    /// holds no row is an empty vector, and the formula is evaluated for it like any other.
    /// A window whose two sides add up to no rows at all slides its head ahead of its
    /// tail, and fails as in Java, unless the formula reads no vector.
    ///
    /// Behavior: `accumulateRolling`, `push` and `pop` of
    /// io.deephaven.engine.table.impl.updateby.rollingformula.IntRollingFormulaOperator.Context
    /// and its siblings, and of
    /// io.deephaven.engine.table.impl.updateby.rollingformulamulticolumn.RollingFormulaMultiColumnOperator.Context,
    /// whose buffers (`io.deephaven.base.ringbuffer.IntRingBuffer#remove`) throw a
    /// `NoSuchElementException` when they are empty
    pub(super) fn evaluate(
        &self,
        table: &dyn Table,
        rows: &RowSet,
        influencers: &RowSet,
        slides: &[Slide],
    ) -> Result<ArrayRef, TableFailure> {
        if let Some(thrown) = self.fails {
            return Err(failure(thrown));
        }
        if self.inputs.iter().any(|input| !input.key) {
            let mut held = 0;
            for slide in slides {
                if slide.pop > held {
                    return Err(failure(match self.buffer {
                        Some(buffer) => format!(
                            "Assertion failed: asserted {buffer}.size() >= {}, instead {buffer}.size() == {held}.",
                            slide.pop
                        ),
                        None => "java.util.NoSuchElementException".to_owned(),
                    }));
                }
                held = held - slide.pop + slide.push;
            }
        }
        let mut values = self
            .inputs
            .iter()
            .map(|input| match input.column {
                None => Ok(Arc::new(Int32Array::from(vec![0; slides.len()])) as ArrayRef),
                Some(column) if input.key => table.column(column).chunk(rows),
                Some(column) => {
                    let values = table.column(column).chunk(influencers)?;
                    group::windows(&input.column_type, &values, slides)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(reads) = &self.reads {
            let added = reads.check(&values)?;
            values.extend(added);
        }
        self.formula.evaluate(&values)
    }
}

#[cfg(test)]
mod tests {
    use super::replace_formula_tokens;

    /// Behavior: io.deephaven.engine.table.impl.select.FormulaUtil#replaceFormulaTokens
    #[test]
    fn a_token_is_replaced_where_it_stands_alone() {
        let replace = |formula| replace_formula_tokens(formula, "x", "__P__");
        assert_eq!(replace("sum(x)"), "sum(__P__)");
        assert_eq!(replace("x+x*x"), "__P__+__P__*__P__");
        assert_eq!(
            replace("max(x) + xx + x_ + _x + x1 + ax"),
            "max(__P__) + xx + x_ + _x + x1 + ax"
        );
        // A digit before the token does not join it.
        assert_eq!(replace("1x + 2"), "1__P__ + 2");
        assert_eq!(replace(""), "");
        assert_eq!(replace_formula_tokens("sum(sum)", "sum", "p"), "p(p)");
        // The replacement is not searched again.
        assert_eq!(replace_formula_tokens("a + b", "a", "a + a"), "a + a + b");
    }
}
