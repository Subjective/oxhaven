//! Turns the column strings of one call into steps: what each result column is, what it can
//! read, and which parent columns change it.

use super::arrays::{self, ShiftedColumn};
use super::{Flavor, SelectError};
use crate::table::holds_values;
use crate::update::ModifiedColumnTransformer;
use oxhaven_contracts::expr::{
    ColumnAssignment, Expr, Formula, FormulaCompiler, Literal, RowVariable,
};
use oxhaven_contracts::schema::{ColumnDefinition, ColumnType, TableDefinition};
use oxhaven_contracts::table::{ColumnSource, Table};
use std::collections::BTreeSet;
use std::sync::Arc;

/// Where the values of a name come from.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(super) enum Origin {
    /// The parent's column at this index.
    Parent(usize),
    /// The step at this index of the same call.
    Step(usize),
}

/// A name a formula can read, or a column of the result.
#[derive(Clone, Debug)]
pub(super) struct Named {
    pub name: String,
    pub column_type: ColumnType,
    pub origin: Origin,
}

pub(super) enum Kind {
    /// The column is another column under this name: a pass-through or a rename.
    ///
    /// Behavior: io.deephaven.engine.table.impl.select.SourceColumn
    Source(Named),
    /// A formula made of literals only, evaluated once.
    ///
    /// Behavior: io.deephaven.engine.table.impl.select.DhFormulaColumn#hasConstantValue
    Constant(Arc<dyn Formula>),
    Formula(Arc<dyn Formula>),
}

/// One entry of the column list.
pub(super) struct Step {
    pub name: String,
    pub column_type: ColumnType,
    pub kind: Kind,
    /// Every name the step's expression could read, in definition order: the parent's
    /// columns and the steps before this one, a later step of a name replacing an earlier
    /// column of that name in place.
    pub scope: Vec<Named>,
    /// The parent columns whose change changes this column, directly or through earlier
    /// steps.
    ///
    /// Behavior: io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer.AnalyzerContext#populateParentDependenciesMCS
    pub parent_columns: BTreeSet<usize>,
    /// Whether the step reads a column made by an earlier step.
    pub reads_steps: bool,
    /// Which of `i`, `ii` and `k` the step's expression uses.
    pub row_variables: RowVariables,
}

/// The row variables an expression uses: `i` and `ii`, which are the row's position, and
/// `k`, its row key.
#[derive(Clone, Copy, Default, PartialEq, Eq, Debug)]
pub(super) struct RowVariables {
    pub position: bool,
    pub key: bool,
}

impl RowVariables {
    pub(super) fn of(expr: &Expr) -> RowVariables {
        let mut found = RowVariables::default();
        visit(expr, &mut |node| match node {
            Expr::RowVariable(RowVariable::I | RowVariable::Ii) => found.position = true,
            Expr::RowVariable(RowVariable::K) => found.key = true,
            _ => {}
        });
        found
    }
}

impl Step {
    /// The definition the step's formula was compiled against.
    pub(super) fn scope_definition(&self) -> TableDefinition {
        definition_of(&self.scope)
    }
}

/// A column read at a fixed distance in rows from the current row: `Col_[i + amount]`.
///
/// Behavior: io.deephaven.engine.table.impl.select.ShiftedColumnDefinition
#[derive(Clone, PartialEq, Eq, Debug)]
pub(crate) struct Shift {
    /// The column read.
    pub column: String,
    /// How many rows after the current row, negative for rows before it.
    pub amount: i64,
}

pub(super) struct Plan {
    /// The steps of the listed columns, up to the first that reads a column at a fixed
    /// distance.
    pub steps: Vec<Step>,
    /// The first listed column that reads a column at a fixed distance, which the steps
    /// stop before.
    pub shifted: Option<ShiftedColumn>,
    /// The result's columns, in order.
    pub published: Vec<Named>,
    /// Whether no step passes through a parent column that holds its values, which is what
    /// lets a static `select` give its result new row keys.
    pub may_flatten: bool,
    /// Whether every listed column is a name alone, without an `=`.
    pub only_names: bool,
}

impl Plan {
    /// Whether Java memoizes the result: every listed column is the name of a column and
    /// nothing else. A list without columns is left as it is.
    ///
    /// `X = K` is not memoized, though it is another column under another name: Java's
    /// server reads a text with an `=` as a `SwitchColumn`, which decides what it is when
    /// the operation runs, and only a `SourceColumn` or a `ReinterpretedColumn` is
    /// memoized. Seen on Java 42.9: `view(K)` of a blink input table, which is a copy, is
    /// not a blink table, and `view(X = K)` of it is.
    ///
    /// Behavior: io.deephaven.engine.table.impl.MemoizedOperationKey#isMemoizable and
    /// io.deephaven.engine.table.impl.select.SelectColumnFactory#getExpressions
    pub(super) fn is_memoized(&self) -> bool {
        !self.steps.is_empty()
            && self.only_names
            && self
                .steps
                .iter()
                .all(|step| matches!(step.kind, Kind::Source(_)))
    }

    pub(super) fn definition(&self) -> TableDefinition {
        definition_of(&self.published)
    }

    /// For each parent column, the result columns it changes.
    ///
    /// Behavior: io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer.AnalyzerContext#calcEffects
    pub(super) fn transformer(&self) -> ModifiedColumnTransformer {
        let mut effects = Vec::new();
        for (index, column) in self.published.iter().enumerate() {
            match column.origin {
                Origin::Parent(parent) => effects.push((parent, [index])),
                Origin::Step(step) => effects.extend(
                    self.steps[step]
                        .parent_columns
                        .iter()
                        .map(|&parent| (parent, [index])),
                ),
            }
        }
        ModifiedColumnTransformer::new(effects)
    }
}

fn definition_of(columns: &[Named]) -> TableDefinition {
    let columns = columns
        .iter()
        .map(|column| ColumnDefinition::new(column.name.clone(), column.column_type.clone()))
        .collect();
    match TableDefinition::new(columns) {
        Ok(definition) => definition,
        Err(e) => panic!("a later column of a name must replace the earlier one: {e}"),
    }
}

/// Puts `column` where a column of its name is, or at the end.
///
/// Behavior: `java.util.LinkedHashMap#put`, which keeps a replaced key at its position, as
/// used for the published sources of io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer.AnalyzerContext#addLayer
fn put(columns: &mut Vec<Named>, column: Named) {
    match columns.iter_mut().find(|c| c.name == column.name) {
        Some(existing) => *existing = column,
        None => columns.push(column),
    }
}

/// Behavior: io.deephaven.api.util.NameValidator#validateColumnName. The shape of an
/// identifier is checked by the formula parser; left are the names of the formula language's
/// own variables and what `javax.lang.model.SourceVersion#isKeyword` refuses: the keywords
/// and the boolean and null literals of the Java Language Specification, section 3.9.
pub(super) fn validate_name(name: &str) -> Result<(), SelectError> {
    const RESERVED: &[&str] = &[
        "in",
        "not",
        "i",
        "ii",
        "k",
        "abstract",
        "assert",
        "boolean",
        "break",
        "byte",
        "case",
        "catch",
        "char",
        "class",
        "const",
        "continue",
        "default",
        "do",
        "double",
        "else",
        "enum",
        "extends",
        "final",
        "finally",
        "float",
        "for",
        "goto",
        "if",
        "implements",
        "import",
        "instanceof",
        "int",
        "interface",
        "long",
        "native",
        "new",
        "package",
        "private",
        "protected",
        "public",
        "return",
        "short",
        "static",
        "strictfp",
        "super",
        "switch",
        "synchronized",
        "this",
        "throw",
        "throws",
        "transient",
        "try",
        "void",
        "volatile",
        "while",
        "_",
        "true",
        "false",
        "null",
    ];
    if RESERVED.contains(&name) {
        return Err(SelectError::InvalidName(format!(
            "Invalid column name \"{name}\": \"{name}\" is a reserved keyword"
        )));
    }
    Ok(())
}

pub(super) fn visit(expr: &Expr, f: &mut impl FnMut(&Expr)) {
    f(expr);
    match expr {
        Expr::Column(_) | Expr::Literal(_) | Expr::RowVariable(_) | Expr::ColumnVector(_) => {}
        Expr::Unary { operand, .. } | Expr::Cast { operand, .. } => visit(operand, f),
        Expr::Binary { left, right, .. } => {
            visit(left, f);
            visit(right, f);
        }
        Expr::Ternary {
            condition,
            if_true,
            if_false,
        } => {
            visit(condition, f);
            visit(if_true, f);
            visit(if_false, f);
        }
        Expr::Call { arguments, .. } => arguments.iter().for_each(|a| visit(a, f)),
        Expr::MethodCall {
            receiver,
            arguments,
            ..
        } => {
            visit(receiver, f);
            arguments.iter().for_each(|a| visit(a, f));
        }
        Expr::ColumnArrayAccess { position, .. } => visit(position, f),
    }
}

/// Whether an expression is made of literals and binary operators only. A time literal is
/// not one: Java replaces it with a variable before it looks.
///
/// Behavior: io.deephaven.engine.table.impl.lang.JavaExpressionParser#isConstantValueExpression
fn is_constant(expr: &Expr) -> bool {
    match expr {
        Expr::Literal(Literal::Instant(_)) => false,
        Expr::Literal(_) => true,
        Expr::Binary { left, right, .. } => is_constant(left) && is_constant(right),
        _ => false,
    }
}

/// What a refreshing parent promises about its changes.
struct Changes {
    add_only: bool,
    append_only: bool,
    blink: bool,
}

impl Changes {
    /// Behavior: io.deephaven.engine.table.impl.BaseTable#isAddOnly, #isAppendOnly and
    /// io.deephaven.engine.table.impl.BlinkTableTools#isBlink
    fn of(parent: &dyn Table) -> Self {
        let attributes = parent.attributes();
        let refreshing = parent.is_refreshing();
        Self {
            add_only: !refreshing || attributes.add_only || attributes.append_only,
            append_only: !refreshing
                || attributes.append_only
                || (attributes.add_only && parent.is_flat()),
            blink: refreshing && attributes.blink,
        }
    }
}

/// Refuses a formula whose result for a row can change without the row changing, on a
/// parent whose rows can move.
///
/// Behavior: io.deephaven.engine.table.impl.select.AbstractFormulaColumn#validateSafeForRefresh
fn validate_safe_for_refresh(
    text: &str,
    expr: &Expr,
    formula: &dyn Formula,
    parent: &dyn Table,
) -> Result<(), SelectError> {
    if !parent.is_refreshing() {
        return Ok(());
    }
    let RowVariables { position, key } = RowVariables::of(expr);
    let changes = Changes::of(parent);
    let unsafe_to_refresh = (position && !changes.append_only && !changes.blink)
        || (key && !changes.add_only && !changes.blink)
        || (!formula.column_arrays().is_empty() && !changes.blink);
    if unsafe_to_refresh {
        return Err(SelectError::UnsafeForRefresh(format!(
            "Formula '{}' uses i, ii, k, or column array variables, and is not safe to refresh",
            text.trim()
        )));
    }
    Ok(())
}

/// Reads the column list of one call against `parent`. With `first`, the first column is
/// that assignment in place of what its text says: a formula whose reads at a fixed
/// distance have been replaced by shifted columns of `parent`, which is not looked
/// through for such reads again.
///
/// Behavior: io.deephaven.api.Selectable#parse, the two forms of
/// io.deephaven.engine.table.impl.select.SelectColumnFactory, the choice between a source
/// column and a formula of io.deephaven.engine.table.impl.select.SwitchColumn#initDef, and
/// both passes of io.deephaven.engine.table.impl.select.analyzers.SelectAndViewAnalyzer#createContext
pub(super) fn analyze<S: AsRef<str>>(
    parent: &Arc<dyn Table>,
    columns: &[S],
    compiler: &dyn FormulaCompiler,
    flavor: Flavor,
    mut first: Option<ColumnAssignment>,
) -> Result<Plan, SelectError> {
    let mut scope: Vec<Named> = parent
        .definition()
        .columns()
        .iter()
        .enumerate()
        .map(|(index, column)| Named {
            name: column.name.clone(),
            column_type: column.column_type.clone(),
            origin: Origin::Parent(index),
        })
        .collect();
    let mut published = if flavor.keeps_parent_columns() {
        scope.clone()
    } else {
        Vec::new()
    };
    let mut steps: Vec<Step> = Vec::with_capacity(columns.len());
    let mut may_flatten = true;
    let mut only_names = true;
    let mut shifted = None;

    for (index, text) in columns.iter().enumerate() {
        let text = text.as_ref();
        let formula_error = |error| SelectError::Formula {
            column: text.trim().to_string(),
            error,
        };
        let replaced = first.is_some();
        let assignment = match first.take() {
            Some(assignment) => assignment,
            None => compiler.parse_assignment(text).map_err(formula_error)?,
        };
        validate_name(&assignment.name)?;

        // `Name` alone always names a column. `Name = Other` names one when the text after
        // the `=` is exactly the name of a column readable here, and is a formula otherwise.
        let written = text
            .split_once('=')
            .map(|(_, expression)| expression.trim());
        only_names &= written.is_none();
        let named = match (&assignment.expr, written) {
            (Expr::Column(source), None) => Some(source),
            (Expr::Column(source), Some(written)) if written == source => Some(source),
            _ => None,
        };
        let source = named.and_then(|source| scope.iter().find(|n| n.name == *source));
        if let (Some(missing), None, None) = (named, source, written) {
            let mut available: Vec<&str> = scope.iter().map(|n| n.name.as_str()).collect();
            available.sort_unstable();
            return Err(SelectError::NoSuchColumn(format!(
                "Unknown column names [{missing}], available column names are [{}]",
                available.join(", ")
            )));
        }

        let depends_on = |names: &mut dyn Iterator<Item = &String>| {
            let mut parent_columns = BTreeSet::new();
            let mut reads_steps = false;
            for name in names {
                match scope.iter().find(|n| n.name == *name).map(|n| n.origin) {
                    Some(Origin::Parent(index)) => {
                        parent_columns.insert(index);
                    }
                    Some(Origin::Step(index)) => {
                        reads_steps = true;
                        parent_columns.extend(steps[index].parent_columns.iter().copied());
                    }
                    None => panic!("a compiled formula reads a column {name} it was not given"),
                }
            }
            (parent_columns, reads_steps)
        };

        let row_variables = RowVariables::of(&assignment.expr);
        let (column_type, kind, (parent_columns, reads_steps)) = match source {
            Some(source) => {
                if let Origin::Parent(index) = source.origin {
                    may_flatten &= !holds_values(&parent.column(index));
                }
                (
                    source.column_type.clone(),
                    Kind::Source(source.clone()),
                    depends_on(&mut std::iter::once(&source.name)),
                )
            }
            None => {
                let compiled = compiler.compile(&assignment.expr, &definition_of(&scope));
                // A shifted column needs no more of its column than that it exists, so
                // the formula is judged once the shifted columns stand in for what they
                // can: a column of vectors has no column array here, and can be shifted.
                // Java reads the offsets while it parses the formula, before it looks
                // at a column.
                let rewritten = match replaced {
                    true => None,
                    false => arrays::constant_array_accesses(text, &assignment.expr)
                        .map_err(formula_error)?,
                };
                let rewritten = rewritten.filter(|rewritten| {
                    compiled.is_ok()
                        || rewritten
                            .shifts
                            .iter()
                            .all(|shift| scope.iter().any(|named| named.name == shift.column))
                });
                if let Some(rewritten) = rewritten {
                    shifted = Some(ShiftedColumn {
                        index,
                        name: assignment.name,
                        rewritten,
                        renumbers: may_flatten,
                    });
                    break;
                }
                let formula = compiled.map_err(formula_error)?;
                validate_safe_for_refresh(text, &assignment.expr, &*formula, &**parent)?;
                let depends =
                    depends_on(&mut formula.columns().iter().chain(formula.column_arrays()));
                let kind = if is_constant(&assignment.expr) {
                    Kind::Constant(formula.clone())
                } else {
                    Kind::Formula(formula.clone())
                };
                (formula.result_type().clone(), kind, depends)
            }
        };

        let named = Named {
            name: assignment.name.clone(),
            column_type: column_type.clone(),
            origin: Origin::Step(steps.len()),
        };
        steps.push(Step {
            name: assignment.name,
            column_type,
            kind,
            scope: scope.clone(),
            parent_columns,
            reads_steps,
            row_variables,
        });
        put(&mut scope, named.clone());
        put(&mut published, named);
    }

    Ok(Plan {
        steps,
        shifted,
        published,
        may_flatten,
        only_names,
    })
}

/// The sources of the columns a call has made so far, and of its parent.
pub(super) struct Sources<'a> {
    parent: &'a Arc<dyn Table>,
    steps: Vec<Arc<dyn ColumnSource>>,
}

impl<'a> Sources<'a> {
    pub(super) fn new(parent: &'a Arc<dyn Table>) -> Self {
        Self {
            parent,
            steps: Vec::new(),
        }
    }

    /// Records the source of the next step.
    pub(super) fn push(&mut self, source: Arc<dyn ColumnSource>) {
        self.steps.push(source);
    }

    pub(super) fn of(&self, origin: Origin) -> Arc<dyn ColumnSource> {
        match origin {
            Origin::Parent(index) => self.parent.column(index),
            Origin::Step(index) => self.steps[index].clone(),
        }
    }

    /// One source per name of `names`, in order.
    pub(super) fn all(&self, names: &[Named]) -> Vec<Arc<dyn ColumnSource>> {
        names.iter().map(|named| self.of(named.origin)).collect()
    }

    /// As [`all`](Self::all), with each parent column passed through `parent_column`.
    pub(super) fn all_with(
        &self,
        names: &[Named],
        parent_column: impl Fn(Arc<dyn ColumnSource>) -> Arc<dyn ColumnSource>,
    ) -> Vec<Arc<dyn ColumnSource>> {
        names
            .iter()
            .map(|named| match named.origin {
                Origin::Parent(_) => parent_column(self.of(named.origin)),
                Origin::Step(_) => self.of(named.origin),
            })
            .collect()
    }
}
