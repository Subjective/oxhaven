//! Reads of a rolling formula's window by position.
//!
//! Java hands a rolling formula each window as a vector over its ring buffer
//! (`IntRingBufferVectorWrapper` and its siblings), whose `get` throws
//! `NoSuchElementException` for a position outside the window (`IntRingBuffer#front`),
//! where the vector of a column gives null. A slice of a window (`IntVectorSlice`) reads a
//! position outside the window through that same `get`, so it throws too, while its
//! iterator gives nulls there as for a column. The library functions that read a vector
//! by position (`first`, `nth`, `diff`, `rank` and the like) therefore throw for some
//! windows and slices where a vector column gives a result.
//!
//! The formula itself is evaluated over list arrays, as for a vector column. Before that,
//! each read by position that it makes is evaluated on its own, over the rows that reach
//! it (both sides of `?:`, `&&` and `||` are evaluated lazily in Java), and a read that
//! Java fails fails the computation.

use super::failure;
use crate::ops::select::ArrayFormula;
use arrow::array::{Array, ArrayRef, AsArray, BooleanArray, Int32Array};
use arrow::compute::filter;
use arrow::datatypes::{DataType, Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type};
use oxhaven_contracts::expr::{BinaryOp, Expr, FormulaCompiler, FormulaError};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use oxhaven_contracts::unported;
use std::sync::Arc;

const NO_SUCH_ELEMENT: &str = "java.util.NoSuchElementException";

/// The name of the `n`th input a read adds to the formula.
fn added_name(n: usize) -> String {
    format!("__WINDOW_POSITION_{n}__")
}

/// The vector a read reads.
#[derive(Clone, Copy)]
enum View {
    /// The window of the formula's input at this index.
    Window(usize),
    /// The slice the probe at this index makes.
    Slice(usize),
}

/// Which positions a library function reads with `get`.
///
/// Behavior: `first`, `last`, `nth`, `countDistinct`, `distinct` and the `Obj` forms of
/// io.deephaven.function.Basic, `rank` and `rankDescending` of
/// io.deephaven.function.Sort (a sort of all positions by a comparator that calls `get`)
#[derive(Clone, Copy)]
enum Reads {
    First,
    Last,
    /// The position its first formula gives.
    Nth,
    /// The first position of a vector of one element, unless its first formula, the
    /// `countNull` or `includeNull` argument, is true.
    Single,
    /// Every position of a vector of two or more.
    All,
}

enum Kind {
    /// A condition: which side of a `?:`, `&&` or `||` Java evaluates.
    Condition,
    /// `get` or `[]` of the view at the position its formula gives. For a window, the
    /// position Java reads is handed to the formula as the input added at this index.
    Get { view: View, added: Option<usize> },
    /// `subVector(from, to)` of the view.
    SubVector { view: View },
    /// `diff(stride, view)`.
    Diff { view: View },
    /// A library function that reads the view by position.
    Reads { view: View, reads: Reads },
}

struct Probe {
    formulas: Vec<ArrayFormula>,
    /// The number of inputs the formulas were compiled with.
    inputs: usize,
    /// The conditions, by probe, and the side of each, under which Java evaluates this.
    gate: Vec<(usize, bool)>,
    kind: Kind,
}

/// The reads of a formula, the formula rewritten to read the inputs they add, and its
/// inputs with those added.
pub(super) type Found = (WindowReads, Expr, Vec<(String, ColumnType)>);

/// The reads by position of a rolling formula, found when it is compiled.
pub(super) struct WindowReads {
    probes: Vec<Probe>,
    /// The `int` inputs the reads add after the formula's own.
    added: usize,
}

/// A slice of a window: Java's `IntVectorSlice` with the window as its inner vector.
#[derive(Clone, Copy)]
struct Slice {
    offset: i64,
    length: i64,
    /// The positions of the window it may read, `from` inclusive, `to` exclusive.
    from: i64,
    to: i64,
}

/// Behavior: io.deephaven.base.ClampUtil#clampLong
fn clamp(min: i64, max: i64, value: i64) -> i64 {
    value.max(min).min(max)
}

struct Builder<'a> {
    compiler: &'a dyn FormulaCompiler,
    inputs: Vec<(String, ColumnType)>,
    windows: &'a dyn Fn(&str) -> bool,
    probes: Vec<Probe>,
    added: usize,
}

/// Whether `expression` holds a read this module checks, so that the side of a condition
/// it stands in needs the condition evaluated.
fn reads_by_position(expression: &Expr) -> bool {
    match expression {
        Expr::Column(_) | Expr::Literal(_) | Expr::RowVariable(_) | Expr::ColumnVector(_) => false,
        // `w_[position]` of a window named with a final `_`.
        Expr::ColumnArrayAccess { .. } => true,
        Expr::Unary { operand, .. } | Expr::Cast { operand, .. } => reads_by_position(operand),
        Expr::Binary { left, right, .. } => reads_by_position(left) || reads_by_position(right),
        Expr::Ternary {
            condition,
            if_true,
            if_false,
        } => [condition, if_true, if_false]
            .iter()
            .any(|part| reads_by_position(part)),
        Expr::Call {
            function,
            arguments,
        } => reads_of(function).is_some() || arguments.iter().any(reads_by_position),
        Expr::MethodCall {
            receiver,
            method,
            arguments,
        } => {
            matches!(method.as_str(), "get" | "[]" | "subVector")
                || reads_by_position(receiver)
                || arguments.iter().any(reads_by_position)
        }
    }
}

/// The positions a library function of a vector reads with `get`, and where its vector
/// and position arguments are.
fn reads_of(function: &str) -> Option<Reads> {
    Some(match function {
        "first" | "firstObj" => Reads::First,
        "last" | "lastObj" => Reads::Last,
        "nth" | "nthObj" => Reads::Nth,
        "countDistinct" | "countDistinctObj" | "distinct" | "distinctObj" => Reads::Single,
        "rank" | "rankDescending" | "rankObj" | "rankDescendingObj" => Reads::All,
        _ => return None,
    })
}

impl Builder<'_> {
    fn window(&self, name: &str) -> Option<usize> {
        if !(self.windows)(name) {
            return None;
        }
        self.inputs.iter().position(|(input, _)| input == name)
    }

    /// Whether `expression` may be a window or a slice of one that this module does not
    /// follow: a side of a condition or a cast.
    fn may_be_window(&self, expression: &Expr) -> bool {
        match expression {
            Expr::Column(name) => self.window(name).is_some(),
            Expr::Ternary {
                if_true, if_false, ..
            } => self.may_be_window(if_true) || self.may_be_window(if_false),
            Expr::Cast { operand, .. } => self.may_be_window(operand),
            Expr::MethodCall {
                receiver, method, ..
            } if method == "subVector" => self.may_be_window(receiver),
            _ => false,
        }
    }

    /// The type of `expression` if it is a window, a slice of one (`subVector` with two
    /// arguments), or a condition each of whose sides is one of these, all of one type.
    fn views_of_type(&self, expression: &Expr) -> Option<&ColumnType> {
        match expression {
            Expr::Column(name) => self.window(name).map(|input| &self.inputs[input].1),
            Expr::MethodCall {
                receiver,
                method,
                arguments,
            } if method == "subVector" && arguments.len() == 2 => self.views_of_type(receiver),
            Expr::Ternary {
                if_true, if_false, ..
            } => {
                let column_type = self.views_of_type(if_true)?;
                (self.views_of_type(if_false)? == column_type).then_some(column_type)
            }
            _ => None,
        }
    }

    /// `expression`, a condition between windows and slices of one type or a slice of
    /// such a condition, as a condition between windows and slices: `(c ? x : y)
    /// .subVector(a, b)` is `c ? x.subVector(a, b) : y.subVector(a, b)`. `None` for any
    /// other expression.
    ///
    /// Java evaluates the condition, then the arguments of what reads the vector it chose,
    /// then reads it; and every window of a formula holds the same rows. So a read of a
    /// condition reads as the condition between the reads of its sides, with the same
    /// order of evaluation and the same failures.
    fn hoisted(&self, expression: &Expr) -> Option<Expr> {
        match expression {
            Expr::Ternary { .. } => self
                .views_of_type(expression)
                .is_some()
                .then(|| expression.clone()),
            Expr::MethodCall {
                receiver,
                method,
                arguments,
            } if method == "subVector" && arguments.len() == 2 => {
                let condition = self.hoisted(receiver)?;
                Some(on_sides(&condition, &|side| Expr::MethodCall {
                    receiver: Box::new(side.clone()),
                    method: method.clone(),
                    arguments: arguments.clone(),
                }))
            }
            _ => None,
        }
    }

    /// Whether the result of `expression` may be a slice of a window. Java copies a
    /// result that is the window itself, and stores any other vector as it is.
    ///
    /// Behavior: io.deephaven.engine.table.impl.updateby.rollingformula.BaseRollingFormulaOperator#getChunkSetter
    fn slice_result(&self, expression: &Expr) -> bool {
        match expression {
            Expr::Ternary {
                if_true, if_false, ..
            } => self.slice_result(if_true) || self.slice_result(if_false),
            Expr::Cast { operand, .. } => self.slice_result(operand),
            Expr::MethodCall {
                receiver, method, ..
            } if method == "subVector" => self.may_be_window(receiver),
            _ => false,
        }
    }

    fn probe(
        &mut self,
        formulas: &[&Expr],
        gate: &[(usize, bool)],
        kind: Kind,
    ) -> Result<usize, FormulaError> {
        let inputs: Vec<(&str, ColumnType)> = self
            .inputs
            .iter()
            .map(|(name, column_type)| (name.as_str(), column_type.clone()))
            .collect();
        let formulas = formulas
            .iter()
            .map(|formula| ArrayFormula::compile_parsed(self.compiler, formula, &inputs))
            .collect::<Result<Vec<_>, _>>()?;
        self.probes.push(Probe {
            formulas,
            inputs: inputs.len(),
            gate: gate.to_vec(),
            kind,
        });
        Ok(self.probes.len() - 1)
    }

    /// The view `expression` is, with the expression rewritten, if it is a window or a
    /// slice of one.
    fn view(
        &mut self,
        expression: &Expr,
        gate: &[(usize, bool)],
    ) -> Result<Option<(Expr, View)>, FormulaError> {
        match expression {
            Expr::Column(name) => Ok(self
                .window(name)
                .map(|input| (expression.clone(), View::Window(input)))),
            Expr::MethodCall {
                receiver,
                method,
                arguments,
            } if method == "subVector" && arguments.len() == 2 => {
                let Some((receiver, view)) = self.view(receiver, gate)? else {
                    return Ok(None);
                };
                let from = self.walk(&arguments[0], gate)?;
                let to = self.walk(&arguments[1], gate)?;
                let probe = self.probe(&[&from, &to], gate, Kind::SubVector { view })?;
                let rewritten = Expr::MethodCall {
                    receiver: Box::new(receiver),
                    method: method.clone(),
                    arguments: vec![from, to],
                };
                Ok(Some((rewritten, View::Slice(probe))))
            }
            _ => Ok(None),
        }
    }

    /// `side` walked under `gate` and, when it holds a read, the condition `condition`
    /// being `wanted`.
    fn walk_side(
        &mut self,
        side: &Expr,
        gate: &[(usize, bool)],
        condition: &mut Option<usize>,
        guard: &Expr,
        wanted: bool,
    ) -> Result<Expr, FormulaError> {
        if !reads_by_position(side) {
            return self.walk(side, gate);
        }
        let probe = match *condition {
            Some(probe) => probe,
            None => {
                let probe = self.probe(&[guard], gate, Kind::Condition)?;
                *condition = Some(probe);
                probe
            }
        };
        let mut gated = gate.to_vec();
        gated.push((probe, wanted));
        self.walk(side, &gated)
    }

    /// `expression` with the position of each `get` of a window replaced by an added
    /// input, after a probe is made for each read by position it holds.
    fn walk(&mut self, expression: &Expr, gate: &[(usize, bool)]) -> Result<Expr, FormulaError> {
        let boxed = |expression: Expr| Box::new(expression);
        Ok(match expression {
            // `w_[position]` of a window `w_`, such as the token's `__PARAM_COLUMN__`: an
            // index into the window, as the type checker reads it. Any other column array
            // is refused before this, by `ArrayFormula::compile`.
            Expr::ColumnArrayAccess { column, position }
                if self.window(&format!("{column}_")).is_some() =>
            {
                let index = Expr::MethodCall {
                    receiver: Box::new(Expr::Column(format!("{column}_"))),
                    method: "[]".to_owned(),
                    arguments: vec![(**position).clone()],
                };
                self.walk(&index, gate)?
            }
            Expr::Column(_)
            | Expr::Literal(_)
            | Expr::RowVariable(_)
            | Expr::ColumnVector(_)
            | Expr::ColumnArrayAccess { .. } => expression.clone(),
            Expr::Unary { op, operand } => Expr::Unary {
                op: *op,
                operand: boxed(self.walk(operand, gate)?),
            },
            Expr::Cast { to, operand } => Expr::Cast {
                to: to.clone(),
                operand: boxed(self.walk(operand, gate)?),
            },
            Expr::Binary { op, left, right } if matches!(op, BinaryOp::And | BinaryOp::Or) => {
                let left = self.walk(left, gate)?;
                let mut condition = None;
                let right =
                    self.walk_side(right, gate, &mut condition, &left, *op == BinaryOp::And)?;
                Expr::Binary {
                    op: *op,
                    left: boxed(left),
                    right: boxed(right),
                }
            }
            Expr::Binary { op, left, right } => Expr::Binary {
                op: *op,
                left: boxed(self.walk(left, gate)?),
                right: boxed(self.walk(right, gate)?),
            },
            Expr::Ternary {
                condition,
                if_true,
                if_false,
            } => {
                let guard = self.walk(condition, gate)?;
                let mut probe = None;
                let if_true = self.walk_side(if_true, gate, &mut probe, &guard, true)?;
                let if_false = self.walk_side(if_false, gate, &mut probe, &guard, false)?;
                Expr::Ternary {
                    condition: boxed(guard),
                    if_true: boxed(if_true),
                    if_false: boxed(if_false),
                }
            }
            Expr::MethodCall {
                receiver,
                method,
                arguments,
            } if matches!(method.as_str(), "get" | "[]") && arguments.len() == 1 => {
                // Java evaluates the condition, then the position, then reads the window
                // or slice the condition chose: see `hoisted`.
                if let Some(condition) = self.hoisted(receiver) {
                    let read = on_sides(&condition, &|side| Expr::MethodCall {
                        receiver: Box::new(side.clone()),
                        method: method.clone(),
                        arguments: arguments.clone(),
                    });
                    return self.walk(&read, gate);
                }
                let Some((receiver, view)) = self.view(receiver, gate)? else {
                    if self.may_be_window(receiver) {
                        unported!(
                            "io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper.IntRingBufferVectorWrapper#get",
                            "{method} of a window chosen by a cast, or by a condition with a \
                             side that is not a window or a slice of one, in a rolling \
                             formula: outside a window Java throws NoSuchElementException"
                        );
                    }
                    return Ok(Expr::MethodCall {
                        receiver: boxed(self.walk(receiver, gate)?),
                        method: method.clone(),
                        arguments: vec![self.walk(&arguments[0], gate)?],
                    });
                };
                let position = self.walk(&arguments[0], gate)?;
                let added = matches!(view, View::Window(_)).then(|| {
                    self.added += 1;
                    self.added - 1
                });
                self.probe(&[&position], gate, Kind::Get { view, added })?;
                let position = match added {
                    Some(added) => {
                        self.inputs.push((added_name(added), ColumnType::Int));
                        Expr::Column(added_name(added))
                    }
                    None => position,
                };
                Expr::MethodCall {
                    receiver: boxed(receiver),
                    method: method.clone(),
                    arguments: vec![position],
                }
            }
            Expr::MethodCall {
                receiver,
                method,
                arguments,
            } => {
                if let Some((rewritten, _)) = self.view(expression, gate)? {
                    return Ok(rewritten);
                }
                if let Some(condition) = self.hoisted(expression) {
                    return self.walk(&condition, gate);
                }
                if method == "subVector" && self.may_be_window(receiver) {
                    unported!(
                        "io.deephaven.vector.IntVectorSlice#get",
                        "subVector of a window chosen by a cast, or by a condition with a side \
                         that is not a window or a slice of one, in a rolling formula"
                    );
                }
                Expr::MethodCall {
                    receiver: boxed(self.walk(receiver, gate)?),
                    method: method.clone(),
                    arguments: arguments
                        .iter()
                        .map(|argument| self.walk(argument, gate))
                        .collect::<Result<_, _>>()?,
                }
            }
            Expr::Call {
                function,
                arguments,
            } => self.call(function, arguments, gate)?,
        })
    }

    fn call(
        &mut self,
        function: &str,
        arguments: &[Expr],
        gate: &[(usize, bool)],
    ) -> Result<Expr, FormulaError> {
        // The argument that is the vector the function reads by position.
        let vector = match (function, arguments.len()) {
            ("diff", 2) | ("nth" | "nthObj", 2) => Some(1),
            ("binSearchIndex" | "rawBinSearchIndex", 3) => Some(0),
            ("countDistinct" | "countDistinctObj" | "distinct" | "distinctObj", 1 | 2) => Some(0),
            (_, 1) if reads_of(function).is_some() => Some(0),
            _ => None,
        };
        let walk_all = |builder: &mut Self, arguments: &[Expr]| {
            arguments
                .iter()
                .map(|argument| builder.walk(argument, gate))
                .collect::<Result<Vec<_>, _>>()
        };
        let Some(at) = vector else {
            return Ok(Expr::Call {
                function: function.to_owned(),
                arguments: walk_all(self, arguments)?,
            });
        };
        // A function of a condition between windows or slices: see `hoisted`. Java
        // evaluates the arguments before the vector's first (`nth`, `diff`) before the
        // condition; a failure of either fails the operation all the same.
        if let Some(condition) = self.hoisted(&arguments[at]) {
            let call = on_sides(&condition, &|side| {
                let mut arguments = arguments.to_vec();
                arguments[at] = side.clone();
                Expr::Call {
                    function: function.to_owned(),
                    arguments,
                }
            });
            return self.walk(&call, gate);
        }
        let Some((rewritten, view)) = self.view(&arguments[at], gate)? else {
            if self.may_be_window(&arguments[at]) {
                unported!(
                    "io.deephaven.engine.table.impl.updateby.rollingformula.ringbuffervectorwrapper.IntRingBufferVectorWrapper#get",
                    "{function} of a window chosen by a cast, or by a condition with a side \
                     that is not a window or a slice of one, in a rolling formula"
                );
            }
            return Ok(Expr::Call {
                function: function.to_owned(),
                arguments: walk_all(self, arguments)?,
            });
        };
        let mut walked = Vec::with_capacity(arguments.len());
        for (index, argument) in arguments.iter().enumerate() {
            walked.push(if index == at {
                rewritten.clone()
            } else {
                self.walk(argument, gate)?
            });
        }
        let others: Vec<&Expr> = walked
            .iter()
            .enumerate()
            .filter(|(index, _)| *index != at)
            .map(|(_, argument)| argument)
            .collect();
        match (function, view) {
            ("diff", _) => {
                self.probe(&others, gate, Kind::Diff { view })?;
            }
            ("binSearchIndex" | "rawBinSearchIndex", View::Slice(_)) => unported!(
                "io.deephaven.function.BinSearch#binSearchIndex",
                "{function} of a slice of a window in a rolling formula, which reads \
                 positions outside the window with get"
            ),
            // A library function reads only positions inside a window, which it has the
            // size of.
            (_, View::Window(_)) => {}
            (_, View::Slice(_)) => {
                if let Some(reads) = reads_of(function) {
                    self.probe(&others, gate, Kind::Reads { view, reads })?;
                }
            }
        }
        Ok(Expr::Call {
            function: function.to_owned(),
            arguments: walked,
        })
    }
}

/// `condition` with `f` applied to each of the sides of the conditions it is made of:
/// with `f` reading at `i`, `c ? x : y` is `c ? x[i] : y[i]`.
fn on_sides(condition: &Expr, f: &dyn Fn(&Expr) -> Expr) -> Expr {
    match condition {
        Expr::Ternary {
            condition,
            if_true,
            if_false,
        } => Expr::Ternary {
            condition: condition.clone(),
            if_true: Box::new(on_sides(if_true, f)),
            if_false: Box::new(on_sides(if_false, f)),
        },
        side => f(side),
    }
}

impl WindowReads {
    /// The reads by position in `expression`, a formula over `inputs`, of which those that
    /// `windows` names are windows; and `expression` rewritten to read a window's elements
    /// at the positions the reads add as inputs, after `inputs`. `None` when the formula
    /// reads no window by position.
    pub(super) fn find(
        compiler: &dyn FormulaCompiler,
        expression: &Expr,
        inputs: &[(&str, ColumnType)],
        windows: &dyn Fn(&str) -> bool,
    ) -> Result<Option<Found>, FormulaError> {
        let mut builder = Builder {
            compiler,
            inputs: inputs
                .iter()
                .map(|(name, column_type)| (name.to_string(), column_type.clone()))
                .collect(),
            windows,
            probes: Vec::new(),
            added: 0,
        };
        if builder.slice_result(expression) {
            unported!(
                "io.deephaven.engine.table.impl.updateby.rollingformula.BaseRollingFormulaOperator#getChunkSetter",
                "a rolling formula whose result is a slice of its window: Java stores the \
                 slice, which reads the window's buffer as it is when the result is read"
            );
        }
        let rewritten = builder.walk(expression, &[])?;
        if builder
            .probes
            .iter()
            .all(|probe| matches!(probe.kind, Kind::Condition))
        {
            return Ok(None);
        }
        let all = builder.inputs;
        Ok(Some((
            WindowReads {
                probes: builder.probes,
                added: builder.added,
            },
            rewritten,
            all,
        )))
    }

    /// Checks every read of the formula over `values`, the formula's inputs for each row
    /// computed, and gives the inputs the reads add.
    ///
    /// Behavior: `IntRingBufferVectorWrapper#get` and `#subVector`, `IntRingBuffer#front`
    /// (throws `NoSuchElementException` outside the buffer), the constructors, `get` and
    /// `subVector` of io.deephaven.vector.IntVectorSlice and `Vector#clampIndex`, and
    /// io.deephaven.function.Numeric#diff
    pub(super) fn check(&self, values: &[ArrayRef]) -> Result<Vec<ArrayRef>, TableFailure> {
        let rows = values.first().map_or(0, |values| values.len());
        let mut inputs: Vec<ArrayRef> = values.to_vec();
        let mut added: Vec<Vec<i32>> = vec![vec![0; rows]; self.added];
        let added_from = values.len();
        let mut conditions: Vec<Vec<Option<bool>>> = Vec::with_capacity(self.probes.len());
        let mut slices: Vec<Vec<Option<Slice>>> = Vec::with_capacity(self.probes.len());
        for probe in &self.probes {
            // Inputs added by earlier reads, for formulas that read them.
            while inputs.len() < probe.inputs {
                let index = inputs.len() - added_from;
                inputs.push(Arc::new(Int32Array::from(added[index].clone())));
            }
            let view_of = |view: View, row: usize| -> Option<Slice> {
                match view {
                    View::Window(input) => {
                        let length = inputs[input].as_list::<i32>().value_length(row) as i64;
                        Some(Slice {
                            offset: 0,
                            length,
                            from: 0,
                            to: length,
                        })
                    }
                    View::Slice(probe) => slices[probe][row],
                }
            };
            let viewed = |row: usize| match probe.kind {
                Kind::Condition => true,
                Kind::Get { view, .. }
                | Kind::SubVector { view }
                | Kind::Diff { view }
                | Kind::Reads { view, .. } => view_of(view, row).is_some(),
            };
            let active: Vec<bool> = (0..rows)
                .map(|row| {
                    probe
                        .gate
                        .iter()
                        .all(|&(condition, side)| conditions[condition][row] == Some(side))
                        && viewed(row)
                })
                .collect();
            let results = probe
                .formulas
                .iter()
                .map(|formula| evaluate_on(formula, &inputs[..probe.inputs], &active))
                .collect::<Result<Vec<_>, _>>()?;
            let mut condition = vec![None; rows];
            let mut slice = vec![None; rows];
            let mut at = 0;
            for row in 0..rows {
                if !active[row] {
                    continue;
                }
                let value = |formula: usize| java_long(results[formula].as_ref(), at);
                let fails = match probe.kind {
                    Kind::Condition => {
                        let result = results[0].as_boolean();
                        condition[row] = result.is_valid(at).then(|| result.value(at));
                        false
                    }
                    Kind::Get { view, added: into } => {
                        let position = value(0)?;
                        let view = view_of(view, row).expect("an active row has its view");
                        match into {
                            // Behavior: IntRingBufferVectorWrapper#get, which reads
                            // `front((int) index)`.
                            Some(into) => {
                                let position = position as i32;
                                added[into][row] = position;
                                !(0..view.length).contains(&(position as i64))
                            }
                            None => !view.reads(position),
                        }
                    }
                    Kind::SubVector { view } => {
                        let (from, to) = (value(0)?, value(1)?);
                        slice[row] = match view {
                            View::Window(_) => view_of(view, row)
                                .expect("an active row has its view")
                                .window_slice(from, to),
                            View::Slice(_) => view_of(view, row)
                                .expect("an active row has its view")
                                .slice(from, to),
                        };
                        false
                    }
                    Kind::Diff { view } => {
                        let stride = value(0)? as i32;
                        let view = view_of(view, row).expect("an active row has its view");
                        (0..view.length).any(|position| {
                            let other = (position as i32).wrapping_add(stride) as i64;
                            !view.reads(position) || !view.reads(other)
                        })
                    }
                    Kind::Reads { view, reads } => {
                        let view = view_of(view, row).expect("an active row has its view");
                        let length = view.length;
                        match reads {
                            Reads::First => length > 0 && !view.reads(0),
                            Reads::Last => length > 0 && !view.reads(length - 1),
                            Reads::Nth => {
                                let position = value(0)?;
                                (0..length).contains(&position) && !view.reads(position)
                            }
                            Reads::Single => {
                                let skips = match results.first() {
                                    None => Some(false),
                                    Some(flag) => {
                                        let flag = flag.as_boolean();
                                        flag.is_valid(at).then(|| flag.value(at))
                                    }
                                };
                                length == 1 && skips == Some(false) && !view.reads(0)
                            }
                            Reads::All => length >= 2 && (0..length).any(|p| !view.reads(p)),
                        }
                    }
                };
                if fails {
                    return Err(failure(NO_SUCH_ELEMENT));
                }
                at += 1;
            }
            conditions.push(condition);
            slices.push(slice);
        }
        Ok(added
            .into_iter()
            .map(|positions| Arc::new(Int32Array::from(positions)) as ArrayRef)
            .collect())
    }
}

impl Slice {
    /// Whether `get(position)` of this view reads inside the window. A window's own view
    /// casts the position to `int` first; that is handled where it applies.
    ///
    /// Behavior: IntVectorSlice#get and `Vector#clampIndex`, whose -1 for a position
    /// outside reaches `IntRingBuffer#front`
    fn reads(&self, position: i64) -> bool {
        let at = position.wrapping_add(self.offset);
        (self.from..self.to).contains(&at)
    }

    /// `subVector(from, to)` of a window of `self.length` elements; `None` where Java's
    /// assertion fails, which the formula's own evaluation reports.
    ///
    /// Behavior: IntRingBufferVectorWrapper#subVector and the public constructor of
    /// IntVectorSlice
    fn window_slice(&self, from: i64, to: i64) -> Option<Slice> {
        let length = to.wrapping_sub(from);
        (length >= 0).then(|| Slice {
            offset: from,
            length,
            from: clamp(0, self.length, from),
            to: clamp(0, self.length, from.wrapping_add(length)),
        })
    }

    /// `subVector(from, to)` of this slice; `None` where Java's `Require.leq` fails.
    ///
    /// Behavior: IntVectorSlice#subVector
    fn slice(&self, from: i64, to: i64) -> Option<Slice> {
        if from > to {
            return None;
        }
        let length = to.wrapping_sub(from);
        let offset = self.offset.wrapping_add(from);
        Some(Slice {
            offset,
            length,
            from: clamp(self.from, self.to, offset),
            to: clamp(self.from, self.to, offset.wrapping_add(length)),
        })
    }
}

/// `formula` over the rows of `inputs` that `active` selects, one result per such row.
fn evaluate_on(
    formula: &ArrayFormula,
    inputs: &[ArrayRef],
    active: &[bool],
) -> Result<ArrayRef, TableFailure> {
    let mask = BooleanArray::from(active.to_vec());
    let selected = inputs
        .iter()
        .map(|input| filter(input.as_ref(), &mask))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| failure(error.to_string()))?;
    formula.evaluate(&selected)
}

/// The value at `at` of an integer result as Java holds it, with a null as its type's
/// null constant (`QueryConstants`), widened to `long`.
fn java_long(values: &dyn Array, at: usize) -> Result<i64, TableFailure> {
    let null = values.is_null(at);
    Ok(match values.data_type() {
        DataType::Int8 if null => i8::MIN as i64,
        DataType::Int8 => values.as_primitive::<Int8Type>().value(at) as i64,
        DataType::Int16 if null => i16::MIN as i64,
        DataType::Int16 => values.as_primitive::<Int16Type>().value(at) as i64,
        DataType::Int32 if null => i32::MIN as i64,
        DataType::Int32 => values.as_primitive::<Int32Type>().value(at) as i64,
        DataType::Int64 if null => i64::MIN,
        DataType::Int64 => values.as_primitive::<Int64Type>().value(at),
        DataType::UInt16 if null => u16::MAX as i64,
        DataType::UInt16 => values.as_primitive::<UInt16Type>().value(at) as i64,
        other => {
            return Err(failure(format!(
                "a position in a rolling formula of type {other}"
            )))
        }
    })
}
