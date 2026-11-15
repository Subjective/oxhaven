//! The query scope: named values, the tickets `s/<name>` and Flight paths `scope/<name>`
//! that name them, and the variables a formula may name.
//!
//! A server with a script session has one holder of the variables, the session: a
//! [`ScopeHolder`] the [`Scope`] asks for everything. A server without one keeps the values
//! that clients publish in a map of its own.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};

use oxhaven_contracts::expr::{
    BinaryOp, ColumnAssignment, Expr, Formula, FormulaCompiler, FormulaError, Literal, UnaryOp,
};
use oxhaven_contracts::schema::{ColumnType, TableDefinition};
use oxhaven_proto::flight::flight_descriptor::DescriptorType;
use oxhaven_proto::flight::FlightDescriptor;
use tonic::Status;

use crate::session::{
    descriptor_log_name, DescribedFlight, Export, ExportBuilder, ExportError, ExportValue, Session,
    TicketResolver,
};

/// The first byte of every scope ticket.
///
/// Behavior: `io.deephaven.proto.util.ScopeTicketHelper#TICKET_PREFIX`
pub const SCOPE_TICKET_ROUTE: u8 = b's';

/// The first path element of the Flight descriptor of a scope variable.
///
/// Behavior: `io.deephaven.proto.util.ScopeTicketHelper#FLIGHT_DESCRIPTOR_ROUTE`
pub const SCOPE_DESCRIPTOR_ROUTE: &str = "scope";

/// The ticket of the scope variable `name`.
///
/// Behavior: `io.deephaven.proto.util.ScopeTicketHelper#nameToBytes`
pub fn scope_ticket(name: &str) -> Vec<u8> {
    format!("s/{name}").into_bytes()
}

/// The value of a scope variable as a formula sees it. Java gives a formula the Java
/// object its script session converts the variable to, so the kinds are the classes that
/// conversion produces.
///
/// Behavior: `io.deephaven.engine.util.PythonScopeJpyImpl#convert`, and jpy's conversion of
/// a Python `int` to the smallest of `Byte`, `Short`, `Integer` and `Long` that holds it
#[derive(Clone, PartialEq, Debug)]
pub enum FormulaValue {
    Boolean(bool),
    Byte(i8),
    Short(i16),
    Int(i32),
    Long(i64),
    Double(f64),
    String(String),
    /// A value of a kind formulas cannot hold here, with words for it, such as
    /// `a Python list`.
    Other(String),
}

/// The one holder of the variables of a server that has a script session.
///
/// Behavior: `io.deephaven.engine.util.AbstractScriptSession.ScriptSessionQueryScope`
pub trait ScopeHolder: Send + Sync {
    /// The value of the variable `name` as clients may open it; `None` if there is no such
    /// variable or it holds the language's null.
    ///
    /// Behavior: `QueryScope#readParamValue(String)` followed by `QueryScope#unwrapObject`
    fn get(&self, name: &str) -> Option<ExportValue>;

    /// Sets the variable `name`.
    ///
    /// Behavior: `QueryScope#putParam`
    fn put(&self, name: &str, value: ExportValue) -> Result<(), String>;

    /// The names of the variables, sorted.
    ///
    /// Behavior: `QueryScope#getParamNames`
    fn names(&self) -> Vec<String>;

    /// The values of the variables `names` for a formula compiled now on this thread;
    /// `None` for a name that is no variable. Fails if the variables cannot be read at
    /// all, as Java's do not when one of them has no Java counterpart.
    ///
    /// Behavior: `QueryScope#toMap`, as `FormulaAnalyzer` and `ConditionFilter` read it
    fn formula_values(&self, names: &[&str]) -> Result<Vec<Option<FormulaValue>>, String>;

    /// Why no formula and no filter can be made now on this thread, if that is so: Java
    /// has no query scope where the code runs, as in the function of a partitioned
    /// table's transform.
    ///
    /// Behavior: `io.deephaven.engine.context.PoisonedQueryScope`, whose every use fails
    /// with `ExecutionContextRegistrationException`
    fn refuses_formulas(&self) -> Option<String> {
        None
    }
}

/// The variables of the server, by name. In Java these are the variables of the script
/// session, and so they are here when the server has one ([`Scope::hold`]). Without one,
/// the process and the clients that publish to a scope ticket put them.
///
/// Behavior: `io.deephaven.engine.context.QueryScope`
#[derive(Default)]
pub struct Scope {
    variables: Mutex<BTreeMap<String, ExportValue>>,
    holder: RwLock<Option<Arc<dyn ScopeHolder>>>,
}

impl Scope {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn lock(&self) -> MutexGuard<'_, BTreeMap<String, ExportValue>> {
        self.variables
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    fn holder(&self) -> Option<Arc<dyn ScopeHolder>> {
        self.holder
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Makes `holder` the holder of the variables: from now on every variable is read from
    /// it and written to it. The variables put before are handed to it; one it refuses
    /// stays unreadable and is logged.
    pub fn hold(&self, holder: Arc<dyn ScopeHolder>) {
        let before = std::mem::take(&mut *self.lock());
        for (name, value) in before {
            if let Err(problem) = holder.put(&name, value) {
                tracing::error!("the scope variable {name} was lost: {problem}");
            }
        }
        *self.holder.write().unwrap_or_else(PoisonError::into_inner) = Some(holder);
    }

    /// Sets the variable `name`, replacing the value it had.
    ///
    /// Behavior: `QueryScope#putParam`
    pub fn put(&self, name: impl Into<String>, value: ExportValue) -> Result<(), String> {
        let name = name.into();
        match self.holder() {
            Some(holder) => holder.put(&name, value),
            None => {
                self.lock().insert(name, value);
                Ok(())
            }
        }
    }

    /// The value of the variable `name`.
    ///
    /// Behavior: `QueryScope#readParamValue(String)`
    pub fn get(&self, name: &str) -> Option<ExportValue> {
        match self.holder() {
            Some(holder) => holder.get(name),
            None => self.lock().get(name).cloned(),
        }
    }

    /// Removes the variable `name` of a server without a script session and returns the
    /// value it had. The variables of a script session are removed by its code.
    pub fn remove(&self, name: &str) -> Option<ExportValue> {
        self.lock().remove(name)
    }

    /// The names of the variables, sorted.
    ///
    /// Behavior: `QueryScope#getParamNames`
    pub fn names(&self) -> Vec<String> {
        match self.holder() {
            Some(holder) => holder.names(),
            None => self.lock().keys().cloned().collect(),
        }
    }

    /// The values of `names` for a formula; see [`ScopeHolder::formula_values`]. A value a
    /// client published to a server without a script session is a table or an object, which
    /// no formula can hold here.
    fn refuses_formulas(&self) -> Option<String> {
        self.holder()?.refuses_formulas()
    }

    fn formula_values(&self, names: &[&str]) -> Result<Vec<Option<FormulaValue>>, String> {
        match self.holder() {
            Some(holder) => holder.formula_values(names),
            None => {
                let variables = self.lock();
                Ok(names
                    .iter()
                    .map(|name| {
                        variables.get(*name).map(|value| {
                            FormulaValue::Other(match value {
                                ExportValue::Table(_) => "a table".to_string(),
                                ExportValue::Object(_) => "an object".to_string(),
                            })
                        })
                    })
                    .collect())
            }
        }
    }
}

/// Resolves scope tickets against the [`Scope`] of the server, and puts published values
/// into it.
///
/// Behavior: `io.deephaven.server.console.ScopeTicketResolver`
pub struct ScopeTicketResolver {
    scope: Arc<Scope>,
}

impl ScopeTicketResolver {
    pub fn new(scope: Arc<Scope>) -> Self {
        Self { scope }
    }

    /// Behavior: `ScopeTicketResolver#publish(SessionState, String, String, Runnable)`
    fn publish_to(
        &self,
        session: &Arc<Session>,
        name: String,
        on_publish: Option<Box<dyn FnOnce() + Send>>,
    ) -> Result<ExportBuilder, Status> {
        let result = session.non_export()?;
        let scope = self.scope.clone();
        session
            .non_export()?
            .requires_serial_queue()
            .require([result.export().clone()])
            .submit(move |mut values| {
                scope
                    .put(name, values.remove(0))
                    .map_err(ExportError::Internal)?;
                if let Some(on_publish) = on_publish {
                    on_publish();
                }
                Ok(ExportValue::unit())
            })?;
        Ok(result)
    }
}

/// The variable a ticket names: what follows `s/`. Bytes that are not UTF-8 read as U+FFFD.
///
/// Behavior: `ScopeTicketResolver#nameForTicket`
fn name_for_ticket(ticket: &[u8], log_id: &str) -> Result<String, Status> {
    if ticket.is_empty() {
        return Err(Status::failed_precondition(format!(
            "Could not resolve '{log_id}': no ticket supplied"
        )));
    }
    match ticket {
        [SCOPE_TICKET_ROUTE, b'/', name @ ..] if !name.is_empty() => {
            Ok(String::from_utf8_lossy(name).into_owned())
        }
        _ => {
            let hex: String = ticket.iter().map(|byte| format!("{byte:02x}")).collect();
            // the quote that is never opened is Java's
            Err(Status::failed_precondition(format!(
                "Could not resolve '{log_id}': found 0x{hex}' (hex)"
            )))
        }
    }
}

/// The variable a path descriptor names: the second element of `["scope", name]`.
///
/// Behavior: `ScopeTicketResolver#nameForDescriptor`
fn name_for_descriptor(descriptor: &FlightDescriptor, log_id: &str) -> Result<String, Status> {
    if descriptor.r#type != DescriptorType::Path as i32 {
        return Err(Status::failed_precondition(format!(
            "Could not resolve descriptor '{log_id}': only paths are supported"
        )));
    }
    if descriptor.path.len() != 2 {
        return Err(Status::failed_precondition(format!(
            "Could not resolve descriptor '{log_id}': unexpected path length (found: {}, \
             expected: 2)",
            descriptor_log_name(descriptor)
        )));
    }
    if descriptor.path[0] != SCOPE_DESCRIPTOR_ROUTE {
        return Err(Status::failed_precondition(format!(
            "Could not resolve descriptor '{log_id}': unexpected path (found: {}, expected: \
             {SCOPE_DESCRIPTOR_ROUTE})",
            descriptor_log_name(descriptor)
        )));
    }
    Ok(descriptor.path[1].clone())
}

/// Behavior: `ScopeTicketResolver#newNotFoundSRE`
fn not_found(log_id: &str, name: &str) -> Status {
    // the quote that is never closed is Java's
    Status::not_found(format!(
        "Could not resolve '{log_id}: variable '{name}' not found"
    ))
}

impl TicketResolver for ScopeTicketResolver {
    fn route(&self) -> u8 {
        SCOPE_TICKET_ROUTE
    }

    /// Behavior: `ScopeTicketResolver#getLogNameFor`
    fn log_name(&self, ticket: &[u8], log_id: &str) -> Result<String, Status> {
        Ok(format!(
            "{SCOPE_DESCRIPTOR_ROUTE}/{}",
            name_for_ticket(ticket, log_id)?
        ))
    }

    /// The value of the variable now, whatever it is: a table as a table, anything else as
    /// an object. There is no waiting for a variable to appear.
    ///
    /// Behavior: `ScopeTicketResolver#resolve(SessionState, ByteBuffer, String)`
    fn resolve(
        &self,
        _session: Option<&Arc<Session>>,
        ticket: &[u8],
        log_id: &str,
    ) -> Result<Export, Status> {
        let name = name_for_ticket(ticket, log_id)?;
        let value = self.scope.get(&name);
        value
            .map(Export::ready)
            .ok_or_else(|| not_found(log_id, &name))
    }

    /// The value the caller defines becomes the variable, and `on_publish` runs after that.
    ///
    /// Behavior: `ScopeTicketResolver#publish(SessionState, ByteBuffer, String, Runnable)`
    fn publish(
        &self,
        session: &Arc<Session>,
        ticket: &[u8],
        log_id: &str,
        on_publish: Option<Box<dyn FnOnce() + Send>>,
    ) -> Result<ExportBuilder, Status> {
        self.publish_to(session, name_for_ticket(ticket, log_id)?, on_publish)
    }

    fn descriptor_route(&self) -> Option<&'static str> {
        Some(SCOPE_DESCRIPTOR_ROUTE)
    }

    /// The flight of a variable that holds a table now; any other variable is not found.
    /// As in Java, the call needs no session.
    ///
    /// Behavior: `ScopeTicketResolver#flightInfoFor`
    fn flight_for(
        &self,
        _session: Option<&Arc<Session>>,
        descriptor: &FlightDescriptor,
        log_id: &str,
    ) -> Result<DescribedFlight, Status> {
        let name = name_for_descriptor(descriptor, log_id)?;
        let value = self.scope.get(&name);
        let table = value
            .filter(|value| value.as_table().is_some())
            .ok_or_else(|| not_found(log_id, &name))?;
        Ok(DescribedFlight {
            export: Export::ready(table),
            ticket: Ok(scope_ticket(&name)),
            not_a_table: not_found(log_id, &name),
        })
    }

    /// Behavior: `ScopeTicketResolver#publish(SessionState, Flight.FlightDescriptor, String,
    /// Runnable)`
    fn publish_descriptor(
        &self,
        session: &Arc<Session>,
        descriptor: &FlightDescriptor,
        log_id: &str,
    ) -> Result<ExportBuilder, Status> {
        self.publish_to(session, name_for_descriptor(descriptor, log_id)?, None)
    }
}

/// A formula compiler whose formulas may name the variables of a [`Scope`]: a name that is
/// no column of the table is looked up there, and the value the variable has at that
/// moment is compiled into the formula.
///
/// Java gives its formula parser the variables as Java objects, a number as a boxed
/// `Byte`, `Short`, `Integer`, `Long` or `Double`. Here a variable becomes a literal of the
/// primitive type, which computes the same wherever Java unboxes. Where Java treats the
/// boxed value differently, the formula is refused: as Java refuses it (the negation of a
/// variable, a cast that narrows, a conditional whose other side has another type), or as
/// unsupported (a method of a boxed number, a variable that holds the null value of its
/// type, a comparison a filter would make after converting the variable to the column's
/// type). Variables of other kinds (lists, functions, tables, `None`) are unsupported.
///
/// Behavior: `io.deephaven.engine.table.impl.select.codegen.FormulaAnalyzer#parseFormula`
/// ("columns and column arrays take precedence over parameters"), and
/// `io.deephaven.engine.table.impl.lang.QueryLanguageParser` for the refusals
pub struct ScopedCompiler {
    inner: Arc<dyn FormulaCompiler>,
    scope: Arc<Scope>,
}

impl ScopedCompiler {
    pub fn new(inner: Arc<dyn FormulaCompiler>, scope: Arc<Scope>) -> Self {
        Self { inner, scope }
    }
}

fn unsupported(message: String) -> FormulaError {
    FormulaError::Unsupported(message)
}

/// The names in `expr` that could be variables: read like a column, without the `.` of a
/// qualified name.
fn candidate_names<'a>(expr: &'a Expr, names: &mut Vec<&'a str>) {
    match expr {
        Expr::Column(name) => {
            if !name.contains('.') && !names.contains(&name.as_str()) {
                names.push(name);
            }
        }
        Expr::Literal(_)
        | Expr::RowVariable(_)
        | Expr::ColumnVector(_)
        | Expr::ColumnArrayAccess { .. } => {}
        Expr::Unary { operand, .. } | Expr::Cast { operand, .. } => candidate_names(operand, names),
        Expr::Binary { left, right, .. } => {
            candidate_names(left, names);
            candidate_names(right, names);
        }
        Expr::Ternary {
            condition,
            if_true,
            if_false,
        } => {
            candidate_names(condition, names);
            candidate_names(if_true, names);
            candidate_names(if_false, names);
        }
        Expr::Call { arguments, .. } => {
            for argument in arguments {
                candidate_names(argument, names);
            }
        }
        Expr::MethodCall {
            receiver,
            arguments,
            ..
        } => {
            candidate_names(receiver, names);
            for argument in arguments {
                candidate_names(argument, names);
            }
        }
    }
}

impl FormulaValue {
    /// The primitive type Java unboxes the value to, for a boxed number.
    fn number_type(&self) -> Option<ColumnType> {
        match self {
            FormulaValue::Byte(_) => Some(ColumnType::Byte),
            FormulaValue::Short(_) => Some(ColumnType::Short),
            FormulaValue::Int(_) => Some(ColumnType::Int),
            FormulaValue::Long(_) => Some(ColumnType::Long),
            FormulaValue::Double(_) => Some(ColumnType::Double),
            FormulaValue::Boolean(_) | FormulaValue::String(_) | FormulaValue::Other(_) => None,
        }
    }

    /// The Java class of the value, for messages.
    fn class_name(&self) -> &str {
        match self {
            FormulaValue::Boolean(_) => "java.lang.Boolean",
            FormulaValue::Byte(_) => "java.lang.Byte",
            FormulaValue::Short(_) => "java.lang.Short",
            FormulaValue::Int(_) => "java.lang.Integer",
            FormulaValue::Long(_) => "java.lang.Long",
            FormulaValue::Double(_) => "java.lang.Double",
            FormulaValue::String(_) => "java.lang.String",
            FormulaValue::Other(what) => what,
        }
    }

    /// Whether the value is the one Deephaven reads as the null of its primitive type.
    fn is_null_sentinel(&self) -> bool {
        match self {
            FormulaValue::Byte(value) => *value == i8::MIN,
            FormulaValue::Short(value) => *value == i16::MIN,
            FormulaValue::Int(value) => *value == i32::MIN,
            FormulaValue::Long(value) => *value == i64::MIN,
            FormulaValue::Double(value) => *value == -f64::MAX,
            FormulaValue::Boolean(_) | FormulaValue::String(_) | FormulaValue::Other(_) => false,
        }
    }
}

/// The rank of a primitive numeric type in Java's widening order, without `char`.
fn width(column_type: &ColumnType) -> Option<u8> {
    Some(match column_type {
        ColumnType::Byte => 0,
        ColumnType::Short => 1,
        ColumnType::Int => 2,
        ColumnType::Long => 3,
        ColumnType::Float => 4,
        ColumnType::Double => 5,
        _ => return None,
    })
}

/// Replaces the variables in one expression by their values.
struct Substitution<'a> {
    inner: &'a dyn FormulaCompiler,
    definition: &'a TableDefinition,
    values: BTreeMap<&'a str, FormulaValue>,
    /// The type a `Byte` variable is given where it is an argument of a call.
    byte_argument: ColumnType,
    /// Whether a `Byte` variable was an argument of a call.
    byte_arguments: Cell<bool>,
}

impl Substitution<'_> {
    /// The variable `expr` is, if it is nothing but one.
    fn variable<'e>(&self, expr: &'e Expr) -> Option<(&'e str, &FormulaValue)> {
        match expr {
            Expr::Column(name) => self.values.get(name.as_str()).map(|v| (name.as_str(), v)),
            _ => None,
        }
    }

    /// The literal a variable is compiled as.
    fn literal(&self, name: &str, value: &FormulaValue) -> Result<Expr, FormulaError> {
        if value.is_null_sentinel() {
            // Java's results for such a variable follow no rule that could be kept: a
            // `Byte` of -128 is a null cell as a column and -127 after `+ 1`, and
            // `isNull` of it is false
            return Err(unsupported(format!(
                "query scope variable {name}, whose value is the null value of {}",
                value.class_name()
            )));
        }
        let narrow = |to: ColumnType, value: i32| Expr::Cast {
            to,
            operand: Box::new(Expr::Literal(Literal::Int(value))),
        };
        Ok(match value {
            FormulaValue::Boolean(value) => Expr::Literal(Literal::Boolean(*value)),
            FormulaValue::Byte(value) => narrow(ColumnType::Byte, (*value).into()),
            FormulaValue::Short(value) => narrow(ColumnType::Short, (*value).into()),
            FormulaValue::Int(value) => Expr::Literal(Literal::Int(*value)),
            FormulaValue::Long(value) => Expr::Literal(Literal::Long(*value)),
            FormulaValue::Double(value) => Expr::Literal(Literal::Double(*value)),
            FormulaValue::String(value) => Expr::Literal(Literal::String(value.clone())),
            FormulaValue::Other(what) => {
                return Err(unsupported(format!(
                    "query scope variable {name}, which holds {what}"
                )))
            }
        })
    }

    /// The arguments of a call. A `Byte` variable among them is given the type
    /// [`Substitution::byte_argument`]; see [`ScopedCompiler::compile`].
    fn arguments(&self, arguments: &[Expr]) -> Result<Vec<Expr>, FormulaError> {
        arguments
            .iter()
            .map(|argument| match self.variable(argument) {
                Some((_, FormulaValue::Byte(value))) if *value != i8::MIN => {
                    self.byte_arguments.set(true);
                    Ok(Expr::Cast {
                        to: self.byte_argument.clone(),
                        operand: Box::new(Expr::Literal(Literal::Int((*value).into()))),
                    })
                }
                _ => self.rewrite(argument),
            })
            .collect()
    }

    fn rewrite(&self, expr: &Expr) -> Result<Expr, FormulaError> {
        let rewrite = |expr: &Expr| self.rewrite(expr).map(Box::new);
        Ok(match expr {
            Expr::Column(name) => match self.values.get(name.as_str()) {
                Some(value) => self.literal(name, value)?,
                None => expr.clone(),
            },
            Expr::Literal(_)
            | Expr::RowVariable(_)
            | Expr::ColumnVector(_)
            | Expr::ColumnArrayAccess { .. } => expr.clone(),
            Expr::Unary { op, operand } => {
                if let (UnaryOp::Negate, Some((name, value))) = (op, self.variable(operand)) {
                    if let Some(primitive) = value.number_type() {
                        // Behavior: `QueryLanguageParser#visit(UnaryExpr)`, whose check of
                        // the result type fails for a boxed operand
                        return Err(FormulaError::Type(format!(
                            "the negation of the query scope variable {name}: asserted \
                             ret.equals(result), instead ret == class {}, result == {}",
                            value.class_name(),
                            java_primitive_name(&primitive)
                        )));
                    }
                }
                Expr::Unary {
                    op: *op,
                    operand: rewrite(operand)?,
                }
            }
            Expr::Cast { to, operand } => {
                if let Some((_, value)) = self.variable(operand) {
                    if let (Some(from), Some(target)) =
                        (value.number_type().as_ref().and_then(width), width(to))
                    {
                        // Behavior: `QueryLanguageParser#visit(CastExpr)`: a boxed number
                        // is cast to its primitive type and to the types that widens to
                        if target < from {
                            return Err(FormulaError::Type(format!(
                                "Incompatible types; {} cannot be converted to {}",
                                value.class_name(),
                                java_primitive_name(to)
                            )));
                        }
                    } else if value.number_type().is_some() && *to == ColumnType::Char {
                        return Err(FormulaError::Type(format!(
                            "Incompatible types; {} cannot be converted to char",
                            value.class_name()
                        )));
                    }
                }
                Expr::Cast {
                    to: to.clone(),
                    operand: rewrite(operand)?,
                }
            }
            Expr::Binary { op, left, right } => Expr::Binary {
                op: *op,
                left: rewrite(left)?,
                right: rewrite(right)?,
            },
            Expr::Ternary {
                condition,
                if_true,
                if_false,
            } => {
                self.check_branches(if_true, if_false)?;
                self.check_branches(if_false, if_true)?;
                Expr::Ternary {
                    condition: rewrite(condition)?,
                    if_true: rewrite(if_true)?,
                    if_false: rewrite(if_false)?,
                }
            }
            Expr::Call {
                function,
                arguments,
            } => Expr::Call {
                function: function.clone(),
                arguments: self.arguments(arguments)?,
            },
            Expr::MethodCall {
                receiver,
                method,
                arguments,
            } => {
                if let Some((name, value)) = self.variable(receiver) {
                    if !matches!(value, FormulaValue::String(_) | FormulaValue::Other(_)) {
                        return Err(unsupported(format!(
                            "method {method} of the query scope variable {name}, a {}",
                            value.class_name()
                        )));
                    }
                }
                Expr::MethodCall {
                    receiver: rewrite(receiver)?,
                    method: method.clone(),
                    arguments: self.arguments(arguments)?,
                }
            }
        })
    }

    /// Refuses a conditional one of whose sides is a boxed number and the other neither
    /// null nor of the type the number unboxes to.
    ///
    /// Behavior: `QueryLanguageParser#visit(ConditionalExpr)`: "Incompatible types in
    /// condition operation not supported"
    fn check_branches(&self, branch: &Expr, other: &Expr) -> Result<(), FormulaError> {
        let Some((_, value)) = self.variable(branch) else {
            return Ok(());
        };
        let Some(primitive) = value.number_type() else {
            return Ok(());
        };
        if matches!(other, Expr::Literal(Literal::Null)) {
            return Ok(());
        }
        let other_type = match self.variable(other) {
            Some((_, other_value)) => match other_value.number_type() {
                Some(other_primitive) if other_primitive == primitive => return Ok(()),
                _ => other_value.class_name().to_string(),
            },
            None => {
                let compiled = self.inner.compile(&self.rewrite(other)?, self.definition)?;
                if *compiled.result_type() == primitive {
                    return Ok(());
                }
                format!("{:?}", compiled.result_type())
            }
        };
        Err(FormulaError::Type(format!(
            "Incompatible types in condition operation not supported : class {} {other_type}",
            value.class_name()
        )))
    }

    /// Refuses a comparison of a column with a variable, as the whole expression, when a
    /// filter of that text does not compare the two as a formula does. Java makes such a
    /// filter a match or range filter, which converts the variable to the column's type
    /// first: `X >= v` with an `int` column and `v = 1.5` keeps the rows from 1 on. The
    /// comparisons where that conversion changes nothing are compiled as formulas.
    ///
    /// Behavior: `io.deephaven.engine.table.impl.select.MatchFilter#init`,
    /// `RangeFilter#init` and the conversions of their `ColumnTypeConvertor`s
    fn check_filter_comparison(&self, expr: &Expr) -> Result<(), FormulaError> {
        let Expr::Binary { op, left, right } = expr else {
            return Ok(());
        };
        let comparison = matches!(
            op,
            BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::Less
                | BinaryOp::LessOrEqual
                | BinaryOp::Greater
                | BinaryOp::GreaterOrEqual
        );
        if !comparison {
            return Ok(());
        }
        let column_of = |expr: &Expr| match expr {
            Expr::Column(name) => self
                .definition
                .index_of(name)
                .map(|index| &self.definition.columns()[index]),
            _ => None,
        };
        let (column, (name, value)) = match (
            column_of(left),
            self.variable(right),
            column_of(right),
            self.variable(left),
        ) {
            (Some(column), Some(variable), _, _) | (_, _, Some(column), Some(variable)) => {
                (column, variable)
            }
            _ => return Ok(()),
        };
        let integer = match value {
            FormulaValue::Byte(value) => Some(i64::from(*value)),
            FormulaValue::Short(value) => Some(i64::from(*value)),
            FormulaValue::Int(value) => Some(i64::from(*value)),
            FormulaValue::Long(value) => Some(*value),
            _ => None,
        };
        let fits = |low: i64, high: i64| integer.is_some_and(|value| (low..=high).contains(&value));
        let unchanged = match (&column.column_type, value) {
            (ColumnType::Byte, _) => fits(i8::MIN.into(), i8::MAX.into()),
            (ColumnType::Short, _) => fits(i16::MIN.into(), i16::MAX.into()),
            (ColumnType::Int, _) => fits(i32::MIN.into(), i32::MAX.into()),
            (ColumnType::Long, _) => integer.is_some(),
            // exact as a float and as a double
            (ColumnType::Float | ColumnType::Double, _) if integer.is_some() => {
                fits(-(1 << 24), 1 << 24)
            }
            (ColumnType::Double, FormulaValue::Double(value)) => !value.is_nan(),
            (ColumnType::Float, FormulaValue::Double(value)) => f64::from(*value as f32) == *value,
            (ColumnType::String, FormulaValue::String(_)) => true,
            (ColumnType::Boolean, FormulaValue::Boolean(_)) => true,
            _ => false,
        };
        if unchanged {
            return Ok(());
        }
        Err(unsupported(format!(
            "comparison of column {} ({:?}) with the query scope variable {name}, a {}: a \
             filter converts the variable to the column's type first",
            column.name,
            column.column_type,
            value.class_name()
        )))
    }
}

/// The name of a primitive type in Java.
fn java_primitive_name(column_type: &ColumnType) -> String {
    format!("{column_type:?}").to_lowercase()
}

impl FormulaCompiler for ScopedCompiler {
    fn parse(&self, text: &str) -> Result<Expr, FormulaError> {
        // Java makes a filter of the text with the query scope at hand, whatever the text
        if let Some(why) = self.scope.refuses_formulas() {
            return Err(FormulaError::Type(why));
        }
        self.inner.parse(text)
    }

    fn parse_assignment(&self, text: &str) -> Result<ColumnAssignment, FormulaError> {
        self.inner.parse_assignment(text)
    }

    fn compile(
        &self,
        expr: &Expr,
        definition: &TableDefinition,
    ) -> Result<Arc<dyn Formula>, FormulaError> {
        if let Some(why) = self.scope.refuses_formulas() {
            return Err(FormulaError::Type(why));
        }
        let mut names = Vec::new();
        candidate_names(expr, &mut names);
        // columns, and the arrays of columns, take precedence over variables
        names.retain(|name| {
            let array_of = name
                .strip_suffix('_')
                .is_some_and(|column| definition.index_of(column).is_some());
            definition.index_of(name).is_none() && !array_of
        });
        // Java reads every variable for every formula, so a variable it cannot read fails
        // a formula that does not name it
        let values = self.scope.formula_values(&names).map_err(|problem| {
            FormulaError::Type(format!("the query scope cannot be read: {problem}"))
        })?;
        let values: BTreeMap<&str, FormulaValue> = names
            .into_iter()
            .zip(values)
            .filter_map(|(name, value)| Some((name, value?)))
            .collect();
        if values.is_empty() {
            return self.inner.compile(expr, definition);
        }
        let mut substitution = Substitution {
            inner: self.inner.as_ref(),
            definition,
            values,
            byte_argument: ColumnType::Byte,
            byte_arguments: Cell::new(false),
        };
        substitution.check_filter_comparison(expr)?;
        let formula = self
            .inner
            .compile(&substitution.rewrite(expr)?, definition)?;
        if substitution.byte_arguments.get() {
            // Java chooses the overload of a function for a boxed `Byte` argument by rules
            // that are not those for a `byte`: `abs` of one is a `short`, `max` of two is a
            // `byte`. The formula is taken only if that choice cannot be seen.
            substitution.byte_argument = ColumnType::Short;
            let widened = self.inner.compile(&substitution.rewrite(expr)?, definition);
            if !widened.is_ok_and(|widened| widened.result_type() == formula.result_type()) {
                return Err(unsupported(
                    "call with a query scope variable that is a java.lang.Byte as an \
                     argument, where the type of the result depends on the overload Java \
                     chooses for it"
                        .to_string(),
                ));
            }
        }
        Ok(formula)
    }
}

#[cfg(test)]
mod tests {
    //! The expectations are what the Java reference answered for the same formulas with
    //! the same Python globals (Deephaven 42.9), except where a test says a formula is
    //! unsupported here.

    use super::*;
    use arrow::array::{Array, Float32Array, Float64Array, Int32Array, Int8Array, StringArray};
    use oxhaven_column::FlatSource;
    use oxhaven_contracts::rowset::RowSet;
    use oxhaven_contracts::schema::ColumnDefinition;
    use oxhaven_contracts::table::{ColumnSource, Table};
    use oxhaven_engine::table::{QueryTable, TableSpec};

    /// A holder with fixed variables; `unreadable` is Java's failure to convert one.
    #[derive(Default)]
    struct Fixed {
        values: BTreeMap<&'static str, FormulaValue>,
        unreadable: bool,
    }

    impl ScopeHolder for Fixed {
        fn get(&self, _name: &str) -> Option<ExportValue> {
            None
        }

        fn put(&self, _name: &str, _value: ExportValue) -> Result<(), String> {
            Err("fixed".to_string())
        }

        fn names(&self) -> Vec<String> {
            self.values.keys().map(|name| name.to_string()).collect()
        }

        fn formula_values(&self, names: &[&str]) -> Result<Vec<Option<FormulaValue>>, String> {
            if self.unreadable {
                return Err("OverflowError: int too big to convert".to_string());
            }
            Ok(names
                .iter()
                .map(|name| self.values.get(name).cloned())
                .collect())
        }
    }

    /// Three rows of `X` (`int`: 0, 1, 2), `B` (`byte`), `D` (`double`), `F` (`float`: 0.5,
    /// 1.5, 2.5) and `S` (`String`: `abc`).
    fn table() -> Arc<dyn Table> {
        let columns: Vec<(&str, ColumnType, arrow::array::ArrayRef)> = vec![
            (
                "X",
                ColumnType::Int,
                Arc::new(Int32Array::from(vec![0, 1, 2])),
            ),
            (
                "B",
                ColumnType::Byte,
                Arc::new(Int8Array::from(vec![0, 1, 2])),
            ),
            (
                "D",
                ColumnType::Double,
                Arc::new(Float64Array::from(vec![0.0, 1.0, 2.0])),
            ),
            (
                "F",
                ColumnType::Float,
                Arc::new(Float32Array::from(vec![0.5, 1.5, 2.5])),
            ),
            (
                "S",
                ColumnType::String,
                Arc::new(StringArray::from(vec!["abc", "abc", "abc"])),
            ),
        ];
        let mut definitions = Vec::new();
        let mut sources: Vec<Arc<dyn ColumnSource>> = Vec::new();
        for (name, column_type, values) in columns {
            definitions.push(ColumnDefinition::new(name, column_type.clone()));
            sources.push(Arc::new(FlatSource::new(column_type, values).unwrap()));
        }
        let definition = TableDefinition::new(definitions).unwrap();
        QueryTable::new_static(TableSpec::new(definition, RowSet::flat(3), sources))
    }

    fn compiler(holder: Fixed) -> ScopedCompiler {
        let scope = Scope::new();
        scope.hold(Arc::new(holder));
        ScopedCompiler::new(Arc::new(oxhaven_expr::Compiler), scope)
    }

    /// The variables of the Java session the expectations were recorded with.
    fn variables() -> Fixed {
        let values = [
            ("xi", FormulaValue::Byte(41)),
            ("xsh", FormulaValue::Short(300)),
            ("xint", FormulaValue::Int(100_000)),
            ("xl", FormulaValue::Long(1 << 40)),
            ("xf", FormulaValue::Double(1.5)),
            ("xhalf", FormulaValue::Double(0.5)),
            ("xtenth", FormulaValue::Double(0.1)),
            ("xs", FormulaValue::String("abc".to_string())),
            ("xb", FormulaValue::Boolean(true)),
            ("xbmin", FormulaValue::Byte(i8::MIN)),
            ("xmin", FormulaValue::Int(i32::MIN)),
            ("xdmin", FormulaValue::Double(-f64::MAX)),
            ("lst", FormulaValue::Other("a Python list".to_string())),
            // a variable named like a column, like a column array and like a constant
            ("X", FormulaValue::Byte(99)),
            ("X_", FormulaValue::Byte(98)),
            ("NULL_INT", FormulaValue::Byte(5)),
        ];
        Fixed {
            values: values.into_iter().collect(),
            unreadable: false,
        }
    }

    fn compile(compiler: &ScopedCompiler, text: &str) -> Result<Arc<dyn Formula>, FormulaError> {
        let table = table();
        compiler.compile(&compiler.parse(text)?, table.definition())
    }

    /// The type of `text` and its value in the second row, printed.
    fn value(compiler: &ScopedCompiler, text: &str) -> String {
        let table = table();
        let formula = compile(compiler, text).unwrap_or_else(|error| panic!("{text}: {error}"));
        let values = formula
            .evaluate(table.as_ref(), &RowSet::flat(3), false)
            .unwrap();
        let second = values.slice(1, 1);
        let printed = arrow::util::display::array_value_to_string(&second, 0).unwrap();
        format!("{:?} {printed}", formula.result_type())
    }

    #[test]
    fn a_variable_has_the_type_java_converts_it_to_and_computes_as_its_primitive() {
        let compiler = compiler(variables());
        for (text, expected) in [
            ("xi", "Byte 41"),
            ("xsh", "Short 300"),
            ("xint", "Int 100000"),
            ("xl", "Long 1099511627776"),
            ("xf", "Double 1.5"),
            ("xs", "String abc"),
            ("xb", "Boolean true"),
            ("xi + 1", "Int 42"),
            ("xi + xi", "Int 82"),
            ("xl + i", "Long 1099511627777"),
            ("xi / 2", "Double 20.5"),
            ("xint * xint * xint", "Int -1530494976"),
            ("xs + xi", "String abc41"),
            ("xs.length()", "Int 3"),
            ("-(xi + 1)", "Int -42"),
            ("xb ? xi : xi", "Byte 41"),
            ("xb ? xint : 2", "Int 100000"),
            ("xb ? xi : null", "Byte 41"),
            ("(long) xi", "Long 41"),
            ("(double) xl", "Double 1099511627776.0"),
            ("(byte) xi", "Byte 41"),
            ("max(xi, 2)", "Int 41"),
            ("X + xi", "Int 42"),
        ] {
            assert_eq!(value(&compiler, text), expected, "{text}");
        }
    }

    #[test]
    fn columns_and_column_arrays_come_before_variables_and_variables_before_constants() {
        let compiler = compiler(variables());
        assert_eq!(value(&compiler, "X"), "Int 1");
        assert_eq!(value(&compiler, "NULL_INT"), "Byte 5");
        // the array of column X is not the variable X_; column arrays are not ported
        let array = compile(&compiler, "X_").err();
        assert!(
            matches!(array, Some(FormulaError::Unsupported(_))),
            "{array:?}"
        );
        // without the variable the name is the constant
        let plain = self::compiler(Fixed::default());
        assert_eq!(
            *compile(&plain, "NULL_INT").unwrap().result_type(),
            ColumnType::Int
        );
    }

    #[test]
    fn what_java_refuses_for_a_boxed_variable_is_a_type_error() {
        let compiler = compiler(variables());
        for text in [
            "-xi",
            "-xint",
            "-xl",
            "-xf",
            "-xi + 1",
            "(byte) xl",
            "(int) xf",
            "(short) xint",
            "(float) xf",
            "(char) xi",
            "xb ? xi : 2",
            "xb ? xi : xl",
            "xb ? xi : (short) 2",
            "xb ? xi : i",
            "xb + 1",
        ] {
            let error = compile(&compiler, text).err();
            assert!(
                matches!(error, Some(FormulaError::Type(_))),
                "{text}: {error:?}"
            );
        }
    }

    #[test]
    fn what_java_computes_by_rules_that_are_not_ported_is_unsupported() {
        let compiler = compiler(variables());
        for text in [
            // values that are the null of their type
            "xbmin",
            "xmin + 1",
            "isNull(xdmin)",
            // other kinds of value
            "lst",
            // methods of boxed numbers
            "xi.intValue()",
            "xf.isNaN()",
            "xb.booleanValue()",
            // overloads chosen for a Byte: Java's abs is a short, max of two a byte
            "abs(xi)",
            "max(xi, xi)",
            "max(xi, xsh)",
            // what a filter of this text compares after converting the variable
            "X >= xf",
            "X == xf",
            "xf <= X",
            "X != xl",
            "B < xsh",
            "F > xtenth",
            "S == xi",
            "X == xb",
        ] {
            let error = compile(&compiler, text).err();
            assert!(
                matches!(error, Some(FormulaError::Unsupported(_))),
                "{text}: {error:?}"
            );
        }
        // the same comparisons where the conversion changes nothing, or inside a formula
        for (text, expected) in [
            ("X == xi", "Boolean false"),
            ("X < xi", "Boolean true"),
            ("xi > X", "Boolean true"),
            ("D >= xhalf", "Boolean true"),
            ("F == xf", "Boolean true"),
            ("S == xs", "Boolean true"),
            ("B < xi", "Boolean true"),
            ("X >= xf && true", "Boolean false"),
            ("X + 1 > xf", "Boolean true"),
        ] {
            assert_eq!(value(&compiler, text), expected, "{text}");
        }
    }

    #[test]
    fn a_scope_that_cannot_be_read_fails_every_formula() {
        let unreadable = compiler(Fixed {
            unreadable: true,
            ..Default::default()
        });
        let error = compile(&unreadable, "X + 1").err();
        assert!(matches!(error, Some(FormulaError::Type(_))), "{error:?}");
        // parsing, which is all Java does with a column that is copied, works
        assert!(unreadable.parse_assignment("Y = X").is_ok());
    }

    #[test]
    fn without_a_holder_a_published_table_is_no_formula_value() {
        let scope = Scope::new();
        scope.put("t", ExportValue::Table(table())).unwrap();
        let compiler = ScopedCompiler::new(Arc::new(oxhaven_expr::Compiler), scope);
        assert_eq!(value(&compiler, "X + 1"), "Int 2");
        let error = compile(&compiler, "t").err();
        assert!(
            matches!(error, Some(FormulaError::Unsupported(_))),
            "{error:?}"
        );
        let missing = compile(&compiler, "nosuch").err();
        assert!(
            matches!(missing, Some(FormulaError::Type(_))),
            "{missing:?}"
        );
    }
}
