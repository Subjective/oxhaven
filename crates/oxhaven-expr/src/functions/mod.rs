//! The functions and constants a formula can use by name.
//!
//! Java imports the static members of a list of classes into every formula
//! (`io.deephaven.engine.table.lang.impl.QueryLibraryImportsDefaults#statics`). This module
//! holds the ones Oxhaven evaluates: [`constant`] for the constants and [`resolve`] for the
//! functions, which chooses the overload as `QueryLanguageParser` does and says how each
//! argument is converted. [`eval`] computes them.
//!
//! A default import that is not implemented here is reported as unsupported, by name. A name
//! that is not a default import is a type error, as it is in Java.
//!
//! The overload rules were recorded from Deephaven 42.9 for every function here over every
//! argument type (`tests/data/java_functions.json`):
//!
//! - A function with one overload per primitive type and parameters of that one type
//!   (`T f(T, T)`) is called with the type all arguments share, and otherwise with the type
//!   binary numeric promotion gives them: `int`, `long`, `float` or `double`. So
//!   `replaceIfNull(B, B)` on two bytes is a `byte` and on a byte and a short is an `int`.
//! - A function that only has `float` and `double` overloads takes `double` for a `double`
//!   and `float` for everything else.
//! - An argument of another type is converted as a cast converts it: null stays null.
//! - The `null` literal is not an argument of any primitive overload.

mod classes;
pub(crate) mod eval;
mod names;
pub(crate) mod random;
pub(crate) mod range;
pub(crate) mod statics;
pub(crate) mod vectors;
pub(crate) mod volatile;

use crate::types::Numeric;
use oxhaven_contracts::expr::{BinaryOp, FormulaError, Literal, UnaryOp};
use oxhaven_contracts::schema::ColumnType;

fn type_error(message: impl Into<String>) -> FormulaError {
    FormulaError::Type(message.into())
}

fn unsupported(message: impl Into<String>) -> FormulaError {
    FormulaError::Unsupported(message.into())
}

/// The value and type of the default-imported constant `name`.
///
/// A `byte` or `short` constant is given as an `int` literal with its own type; a null
/// constant is the value Java reserves for null in its type.
///
/// Behavior: `io.deephaven.util.QueryConstants`, the unit constants of
/// `io.deephaven.time.DateTimeUtils`, `io.deephaven.function.Numeric#PI` and `#E`, and
/// `QueryLanguageFunctionUtils#DEFAULT_SCALE`. The `_BOXED` constants have the primitive
/// type in a formula, as recorded: `NULL_INT_BOXED + 1` is a null `int`.
pub(crate) fn constant(name: &str) -> Option<(ColumnType, Literal)> {
    const SECOND: i64 = 1_000_000_000;
    const DAY: i64 = 24 * 60 * 60 * SECOND;
    const YEAR_AVG: i64 = 31_556_952_000_000_000;
    let byte = |value: i8| (ColumnType::Byte, Literal::Int(value.into()));
    let short = |value: i16| (ColumnType::Short, Literal::Int(value.into()));
    let char = |value: u16| (ColumnType::Char, Literal::Char(value));
    let int = |value: i32| (ColumnType::Int, Literal::Int(value));
    let long = |value: i64| (ColumnType::Long, Literal::Long(value));
    let float = |value: f32| (ColumnType::Float, Literal::Float(value));
    let double = |value: f64| (ColumnType::Double, Literal::Double(value));
    Some(match name {
        "NULL_BOOLEAN" => (ColumnType::Boolean, Literal::Null),
        "NULL_CHAR" | "NULL_CHAR_BOXED" => char(u16::MAX),
        "MIN_CHAR" => char(0),
        "MAX_CHAR" => char(u16::MAX - 1),
        "NULL_BYTE" | "NULL_BYTE_BOXED" => byte(i8::MIN),
        "MIN_BYTE" => byte(i8::MIN + 1),
        "MAX_BYTE" => byte(i8::MAX),
        "NULL_SHORT" | "NULL_SHORT_BOXED" => short(i16::MIN),
        "MIN_SHORT" => short(i16::MIN + 1),
        "MAX_SHORT" => short(i16::MAX),
        "NULL_INT" | "NULL_INT_BOXED" => int(i32::MIN),
        "MIN_INT" => int(i32::MIN + 1),
        "MAX_INT" => int(i32::MAX),
        "NULL_LONG" | "NULL_LONG_BOXED" => long(i64::MIN),
        "MIN_LONG" => long(i64::MIN + 1),
        "MAX_LONG" => long(i64::MAX),
        "NULL_FLOAT" | "NULL_FLOAT_BOXED" => float(-f32::MAX),
        "NAN_FLOAT" => float(f32::NAN),
        "NEG_INFINITY_FLOAT" | "MIN_FLOAT" => float(f32::NEG_INFINITY),
        "POS_INFINITY_FLOAT" | "MAX_FLOAT" => float(f32::INFINITY),
        // The float below `-Float.MAX_VALUE`'s neighbor: `-0x1.fffffcp127f`.
        "MIN_FINITE_FLOAT" => float(-f32::from_bits(f32::MAX.to_bits() - 1)),
        "MAX_FINITE_FLOAT" => float(f32::MAX),
        "MIN_POS_FLOAT" => float(f32::from_bits(1)),
        "NULL_DOUBLE" | "NULL_DOUBLE_BOXED" => double(-f64::MAX),
        "NAN_DOUBLE" => double(f64::NAN),
        "NEG_INFINITY_DOUBLE" | "MIN_DOUBLE" => double(f64::NEG_INFINITY),
        "POS_INFINITY_DOUBLE" | "MAX_DOUBLE" => double(f64::INFINITY),
        // `-0x1.ffffffffffffep1023`.
        "MIN_FINITE_DOUBLE" => double(-f64::from_bits(f64::MAX.to_bits() - 1)),
        "MAX_FINITE_DOUBLE" => double(f64::MAX),
        "MIN_POS_DOUBLE" => double(f64::from_bits(1)),
        "MICRO" => long(1_000),
        "MILLI" => long(1_000_000),
        "SECOND" => long(SECOND),
        "MINUTE" => long(60 * SECOND),
        "HOUR" => long(60 * 60 * SECOND),
        "DAY" => long(DAY),
        "WEEK" => long(7 * DAY),
        "YEAR_365" => long(365 * DAY),
        "YEAR_AVG" => long(YEAR_AVG),
        "SECONDS_PER_NANO" => double(1.0 / SECOND as f64),
        "MINUTES_PER_NANO" => double(1.0 / (60 * SECOND) as f64),
        "HOURS_PER_NANO" => double(1.0 / (60 * 60 * SECOND) as f64),
        "DAYS_PER_NANO" => double(1.0 / DAY as f64),
        "YEARS_PER_NANO_365" => double(1.0 / (365 * DAY) as f64),
        "YEARS_PER_NANO_AVG" => double(1.0 / YEAR_AVG as f64),
        "PI" => double(std::f64::consts::PI),
        "E" => double(std::f64::consts::E),
        "DEFAULT_SCALE" => int(8),
        _ => return None,
    })
}

/// A class of the default static imports that a formula names as `name`: by its full name,
/// or for `DateTimeUtils` by its simple name too.
///
/// Behavior: `QueryLibraryImportsDefaults#classes`, which lists `DateTimeUtils` among the
/// classes a formula names without a package, and `QueryLanguageParser#visit(NameExpr)`
/// and `#visit(FieldAccessExpr)`, which find any class by its full name.
fn library_class(name: &str) -> Option<&'static classes::LibraryClass> {
    classes::CLASSES.iter().find(|class| {
        class.name == name || (class.simple && class.name.rsplit('.').next() == Some(name))
    })
}

/// What `class.function(...)` means when `class` is a class of the default static imports.
pub(crate) enum LibraryMember {
    /// The class is not one of them.
    NotLibrary,
    /// The class has no public static member of that name.
    Missing,
    /// The member is the default import of the same name, of this class alone.
    Import,
    /// The member is a default import that another class has too (`plus`, `lowerBin`).
    /// `time` says whether the class is `DateTimeUtils`.
    Shared { time: bool },
}

/// How a formula reaches the static method `class.function`.
pub(crate) fn library_function(class: &str, function: &str) -> LibraryMember {
    let Some(class) = library_class(class) else {
        return LibraryMember::NotLibrary;
    };
    if class.functions.binary_search(&function).is_err() {
        return LibraryMember::Missing;
    }
    let shared = classes::CLASSES
        .iter()
        .filter(|other| other.functions.binary_search(&function).is_ok())
        .count()
        > 1;
    if shared {
        LibraryMember::Shared {
            time: class.name == "io.deephaven.time.DateTimeUtils",
        }
    } else {
        LibraryMember::Import
    }
}

/// Whether `class.field` is a constant of a class of the default static imports, and so
/// the constant a formula names as `field` alone.
pub(crate) fn is_library_constant(class: &str, field: &str) -> bool {
    library_class(class).is_some_and(|class| class.constants.binary_search(&field).is_ok())
}

/// The value and type of the static field `class.field` of a class of `java.lang`, given
/// as [`constant`] gives a constant.
///
/// `java.lang` is a default-imported package, so a formula names these classes by their
/// simple names, and the field keeps its declared primitive type: `Long.MIN_VALUE` is a
/// `long`, the value Deephaven reserves for null, and so is null in a column, as recorded
/// for `FC/grid-function.jsonl: java.27`. `TYPE` is a `Class` and is not given, nor is
/// `PRECISION`, which exists only from JDK 19 on.
///
/// Behavior: the `public static final` fields of `java.lang.Byte`, `Short`, `Integer`,
/// `Long`, `Float`, `Double`, `Boolean`, `Math` and `StrictMath`, resolved by
/// `QueryLanguageParser#visit(FieldAccessExpr)` through
/// `QueryLibraryImportsDefaults#packages`, or by the class's full name.
pub(crate) fn static_field(class: &str, field: &str) -> Option<(ColumnType, Literal)> {
    let class = class.strip_prefix("java.lang.").unwrap_or(class);
    let byte = |value: i8| (ColumnType::Byte, Literal::Int(value.into()));
    let short = |value: i16| (ColumnType::Short, Literal::Int(value.into()));
    let int = |value: i32| (ColumnType::Int, Literal::Int(value));
    let long = |value: i64| (ColumnType::Long, Literal::Long(value));
    let float = |value: f32| (ColumnType::Float, Literal::Float(value));
    let double = |value: f64| (ColumnType::Double, Literal::Double(value));
    Some(match (class, field) {
        ("Math" | "StrictMath", "PI") => double(std::f64::consts::PI),
        ("Math" | "StrictMath", "E") => double(std::f64::consts::E),
        // `java.util` is imported: `java.util.Calendar#JANUARY`.
        ("Calendar" | "java.util.Calendar", "JANUARY") => int(0),
        ("Boolean", "TRUE") => (ColumnType::Boolean, Literal::Boolean(true)),
        ("Boolean", "FALSE") => (ColumnType::Boolean, Literal::Boolean(false)),
        ("Byte", "MIN_VALUE") => byte(i8::MIN),
        ("Byte", "MAX_VALUE") => byte(i8::MAX),
        ("Byte", "SIZE") => int(8),
        ("Byte", "BYTES") => int(1),
        ("Short", "MIN_VALUE") => short(i16::MIN),
        ("Short", "MAX_VALUE") => short(i16::MAX),
        ("Short", "SIZE") => int(16),
        ("Short", "BYTES") => int(2),
        ("Integer", "MIN_VALUE") => int(i32::MIN),
        ("Integer", "MAX_VALUE") => int(i32::MAX),
        ("Integer", "SIZE") => int(32),
        ("Integer", "BYTES") => int(4),
        ("Long", "MIN_VALUE") => long(i64::MIN),
        ("Long", "MAX_VALUE") => long(i64::MAX),
        ("Long", "SIZE") => int(64),
        ("Long", "BYTES") => int(8),
        ("Float", "POSITIVE_INFINITY") => float(f32::INFINITY),
        ("Float", "NEGATIVE_INFINITY") => float(f32::NEG_INFINITY),
        ("Float", "NaN") => float(f32::NAN),
        // 0x1.fffffeP+127f, 0x1.0p-126f and 0x0.000002P-126f.
        ("Float", "MAX_VALUE") => float(f32::MAX),
        ("Float", "MIN_NORMAL") => float(f32::MIN_POSITIVE),
        ("Float", "MIN_VALUE") => float(f32::from_bits(1)),
        ("Float", "MAX_EXPONENT") => int(127),
        ("Float", "MIN_EXPONENT") => int(-126),
        ("Float", "SIZE") => int(32),
        ("Float", "BYTES") => int(4),
        ("Double", "POSITIVE_INFINITY") => double(f64::INFINITY),
        ("Double", "NEGATIVE_INFINITY") => double(f64::NEG_INFINITY),
        ("Double", "NaN") => double(f64::NAN),
        // 0x1.fffffffffffffP+1023, 0x1.0p-1022 and 0x0.0000000000001P-1022.
        ("Double", "MAX_VALUE") => double(f64::MAX),
        ("Double", "MIN_NORMAL") => double(f64::MIN_POSITIVE),
        ("Double", "MIN_VALUE") => double(f64::from_bits(1)),
        ("Double", "MAX_EXPONENT") => int(1023),
        ("Double", "MIN_EXPONENT") => int(-1022),
        ("Double", "SIZE") => int(64),
        ("Double", "BYTES") => int(8),
        _ => return None,
    })
}

/// Whether `name` is a default-imported constant that [`constant`] does not give: one whose
/// type is not a column type here.
pub(crate) fn is_other_constant(name: &str) -> bool {
    [
        "BS_ANY",
        "BS_HIGHEST",
        "BS_LOWEST",
        "COLOR_SET_BIT",
        "FOREGROUND_SET_BIT",
        "ROUNDING_MODE",
        "SELECTION_OVERRIDE_BIT",
        "ZERO_LENGTH_INSTANT_ARRAY",
    ]
    .contains(&name)
}

/// A function of `double` to `double`, `java.lang.Math`'s of the same name.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Math {
    Acos,
    Asin,
    Atan,
    Ceil,
    Cos,
    Cosh,
    Exp,
    Expm1,
    Floor,
    Log,
    Log10,
    Log1p,
    Rint,
    Sin,
    Sinh,
    Sqrt,
    Tan,
    Tanh,
    ToDegrees,
    ToRadians,
}

impl Math {
    fn named(name: &str) -> Option<Math> {
        Some(match name {
            "acos" => Math::Acos,
            "asin" => Math::Asin,
            "atan" => Math::Atan,
            "ceil" => Math::Ceil,
            "cos" => Math::Cos,
            "cosh" => Math::Cosh,
            "exp" => Math::Exp,
            "expm1" => Math::Expm1,
            "floor" => Math::Floor,
            "log" => Math::Log,
            "log10" => Math::Log10,
            "log1p" => Math::Log1p,
            "rint" => Math::Rint,
            "sin" => Math::Sin,
            "sinh" => Math::Sinh,
            "sqrt" => Math::Sqrt,
            "tan" => Math::Tan,
            "tanh" => Math::Tanh,
            "toDegrees" => Math::ToDegrees,
            "toRadians" => Math::ToRadians,
            _ => return None,
        })
    }
}

/// The comparison a time predicate makes.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum TimeOrder {
    Before,
    BeforeOrEqual,
    After,
    AfterOrEqual,
}

/// What a parse function returns.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Parsed {
    Byte,
    Short,
    Int,
    Long,
    Boolean,
    Float,
    Double,
}

/// A method of `java.time.Instant` a formula calls on a value.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum InstantMethod {
    GetEpochSecond,
    ToEpochMilli,
    ToString,
}

impl InstantMethod {
    pub(crate) fn named(name: &str) -> Option<Self> {
        Some(match name {
            "getEpochSecond" => InstantMethod::GetEpochSecond,
            "toEpochMilli" => InstantMethod::ToEpochMilli,
            "toString" => InstantMethod::ToString,
            _ => return None,
        })
    }

    pub(crate) fn result_type(self) -> ColumnType {
        match self {
            InstantMethod::ToString => ColumnType::String,
            _ => ColumnType::Long,
        }
    }
}

/// A field of the date and time an instant is in a time zone.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum ZonedField {
    SecondOfDay,
    MinuteOfDay,
    HourOfDay,
    MillisOfDay,
    NanosOfDay,
    Year,
    YearOfCentury,
    MonthOfYear,
    DayOfYear,
    DayOfMonth,
    DayOfWeekValue,
    MinuteOfHour,
    SecondOfMinute,
    MillisOfSecond,
    MicrosOfSecond,
    NanosOfSecond,
}

impl ZonedField {
    pub(crate) fn named(name: &str) -> Option<Self> {
        Some(match name {
            "secondOfDay" => ZonedField::SecondOfDay,
            "minuteOfDay" => ZonedField::MinuteOfDay,
            "hourOfDay" => ZonedField::HourOfDay,
            "millisOfDay" => ZonedField::MillisOfDay,
            "nanosOfDay" => ZonedField::NanosOfDay,
            "year" => ZonedField::Year,
            "yearOfCentury" => ZonedField::YearOfCentury,
            "monthOfYear" => ZonedField::MonthOfYear,
            "dayOfYear" => ZonedField::DayOfYear,
            "dayOfMonth" => ZonedField::DayOfMonth,
            "dayOfWeekValue" => ZonedField::DayOfWeekValue,
            "minuteOfHour" => ZonedField::MinuteOfHour,
            "secondOfMinute" => ZonedField::SecondOfMinute,
            "millisOfSecond" => ZonedField::MillisOfSecond,
            "microsOfSecond" => ZonedField::MicrosOfSecond,
            "nanosOfSecond" => ZonedField::NanosOfSecond,
            _ => return None,
        })
    }

    /// Whether the function counts from the start of the day, and so takes the flag that
    /// says which start: midnight on the clock, or the instant the day began.
    pub(crate) fn of_day(self) -> bool {
        matches!(
            self,
            ZonedField::SecondOfDay
                | ZonedField::MinuteOfDay
                | ZonedField::HourOfDay
                | ZonedField::MillisOfDay
                | ZonedField::NanosOfDay
        )
    }

    pub(crate) fn result_type(self) -> ColumnType {
        match self {
            ZonedField::NanosOfDay | ZonedField::MicrosOfSecond | ZonedField::NanosOfSecond => {
                ColumnType::Long
            }
            _ => ColumnType::Int,
        }
    }
}

/// The unit of a count of time since the epoch.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum EpochUnit {
    Seconds,
    Millis,
    Micros,
}

/// A function as it is evaluated: the algorithm, for whatever types its arguments have.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Kernel {
    Math(Math),
    Abs,
    Signum,
    Round,
    IsNaN,
    IsInf,
    IsFinite,
    Pow,
    Atan2,
    Min,
    Max,
    Clamp,
    InRange,
    In,
    ReplaceIfNull,
    ReplaceIfNaN,
    ReplaceIfNullNaN,
    ReplaceIfNonFinite,
    LowerBin,
    UpperBin,
    IfElse,
    And,
    Or,
    Parse(Parsed),
    EpochNanos,
    EpochNanosToInstant,
    /// `plus(Instant, long)`.
    InstantPlus,
    /// `minus(Instant, long)`.
    InstantMinus,
    /// `minus(Instant, Instant)`; `diffNanos` is the same with the arguments exchanged.
    InstantDifference,
    TimeOrder(TimeOrder),
    InstantLowerBin,
    InstantUpperBin,
    /// A function or method of a vector or an array.
    Vector(vectors::VectorFn),
    /// `multiply(Duration, long)` and `multiply(Period, int)`, on nanoseconds.
    DurationMultiply,
    /// `divide(Duration, long)`, on nanoseconds.
    DurationDivide,
    /// `now()`. It takes no argument, so the evaluator supplies the number of rows.
    Now,
    /// A function of `io.deephaven.function.Random`; the evaluator supplies the number of
    /// rows, and the arguments are widened as Java widens them, without regard to null.
    Random(volatile::RandomFn),
    /// A static method of a class of `java.lang`, called through the class. The evaluator
    /// supplies the number of rows and makes null what holds the null value of its type.
    Static(&'static statics::Method),
    /// `new T[] { a, b }`: an array of the arguments, which all have the element type.
    ArrayOf,
    /// `epochSeconds(Instant)`, `epochMillis(Instant)` and `epochMicros(Instant)`.
    Epoch(EpochUnit),
    /// `epochSecondsToInstant(long)` and its two relatives.
    EpochToInstant(EpochUnit),
    /// `parseInstant(String)`, or `parseInstantQuiet(String)`, which is null where the
    /// other throws.
    ParseInstant {
        quiet: bool,
    },
    /// A method called on an `Instant`, which throws for null.
    InstantMethod(InstantMethod),
    /// `hourOfDay(Instant, ZoneId, boolean)`, `dayOfYear(Instant, ZoneId)` and their
    /// relatives, for a zone the formula names.
    Zoned(ZonedField, crate::parse::instant::Zone),
    /// `compareTo` of two numbers.
    CompareTo,
    /// The constructor or a method of `DiscretizedRangeEqual`; a method's first argument
    /// is the range.
    Range(range::RangeFn),
    /// `new Random(seed).nextInt(bound)` of `java.util.Random`, of the seed and the bound.
    RandomNextInt,
}

impl Kernel {
    /// Whether two calls with the same arguments may return different values.
    pub(crate) fn is_volatile(self) -> bool {
        match self {
            Kernel::Now | Kernel::Random(_) => true,
            Kernel::Static(method) => method.function == statics::StaticFn::Random,
            _ => false,
        }
    }

    /// Whether the kernel is a method of `DateTimeUtils` that shares its name with a
    /// method of another class of the default imports.
    pub(crate) fn is_shared_time(self) -> bool {
        matches!(
            self,
            Kernel::InstantPlus
                | Kernel::InstantMinus
                | Kernel::InstantDifference
                | Kernel::InstantLowerBin
                | Kernel::InstantUpperBin
                | Kernel::DurationMultiply
                | Kernel::DurationDivide
        )
    }
}

/// A call to an evaluated function.
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct Call {
    pub(crate) kernel: Kernel,
    /// The type each argument is converted to before the call, as a cast converts it.
    pub(crate) parameters: Vec<ColumnType>,
    pub(crate) result: ColumnType,
    /// The kernel takes the arguments in the opposite order.
    pub(crate) exchanged: bool,
}

/// What a call by name means.
#[derive(Clone, PartialEq, Debug)]
pub(crate) enum Resolved {
    Call(Call),
    /// The function is the one a binary operator compiles to.
    Binary(BinaryOp),
    /// The function is the one a unary operator compiles to.
    Unary(UnaryOp),
    /// `negate` of a `byte`, `short` or `char`, which returns `int`.
    NegateAsInt,
    /// The function is the one a cast compiles to.
    Cast(ColumnType),
}

fn is_numeric(column_type: &ColumnType) -> bool {
    Numeric::of(column_type).is_some()
}

fn is_integral(column_type: &ColumnType) -> bool {
    matches!(Numeric::of(column_type), Some(Numeric::Int | Numeric::Long))
}

/// The type of the overload `T f(T, ...)` that a call with these argument types reaches, or
/// `None` when an argument is not a number.
fn shared(types: &[ColumnType]) -> Option<ColumnType> {
    let first = types.first()?;
    let mut promoted = Numeric::of(first)?;
    for other in &types[1..] {
        promoted = promoted.max(Numeric::of(other)?);
    }
    Some(if types.iter().all(|other| other == first) {
        first.clone()
    } else {
        promoted.column_type()
    })
}

/// [`shared`] for a function that has `float` and `double` overloads only.
fn shared_floating(types: &[ColumnType]) -> Option<ColumnType> {
    Some(match shared(types)? {
        ColumnType::Double => ColumnType::Double,
        _ => ColumnType::Float,
    })
}

fn java_name(column_type: &ColumnType) -> String {
    match column_type {
        ColumnType::Boolean => "Boolean".to_string(),
        ColumnType::Byte => "byte".to_string(),
        ColumnType::Char => "char".to_string(),
        ColumnType::Short => "short".to_string(),
        ColumnType::Int => "int".to_string(),
        ColumnType::Long => "long".to_string(),
        ColumnType::Float => "float".to_string(),
        ColumnType::Double => "double".to_string(),
        ColumnType::String => "String".to_string(),
        ColumnType::Instant => "Instant".to_string(),
        other => format!("{other:?}"),
    }
}

/// The meaning of `name(arguments)`. An argument is its type, or `None` for the `null`
/// literal.
///
/// Behavior: `QueryLanguageParser#visit(MethodCallExpr)` and `#getMethod`, for the overloads
/// of `io.deephaven.function.Numeric`, `Basic`, `Logic` and `Parse`,
/// `QueryLanguageFunctionUtils` and `io.deephaven.time.DateTimeUtils`.
pub(crate) fn resolve(
    name: &str,
    arguments: &[Option<ColumnType>],
) -> Result<Resolved, FormulaError> {
    if names::NAMES.binary_search(&name).is_err() {
        return Err(type_error(format!("cannot find function {name}")));
    }
    let no_overload = || {
        let given: Vec<String> = arguments
            .iter()
            .map(|argument| argument.as_ref().map_or("null".to_string(), java_name))
            .collect();
        type_error(format!("cannot find {name}({})", given.join(", ")))
    };
    let Some(types) = arguments
        .iter()
        .cloned()
        .collect::<Option<Vec<ColumnType>>>()
    else {
        // Java takes null for an object parameter: the generic `replaceIfNull`, a boxed
        // `compare`, a cast, a `String` to parse, and a `Duration` or second `Instant`.
        let takes_object = match name {
            "replaceIfNull" | "compare" | "isNull" | "nullValueFor" => true,
            "byteCast" | "shortCast" | "charCast" | "intCast" | "longCast" | "floatCast"
            | "doubleCast" => true,
            "binaryAnd" | "binaryOr" | "xor" => arguments.contains(&Some(ColumnType::Boolean)),
            // The `Comparable` overloads, which order null first.
            "eq" | "less" | "lessEquals" | "greater" | "greaterEquals" | "compareTo" => {
                arguments.iter().flatten().any(|other| !is_numeric(other))
            }
            _ if name.starts_with("parse") => true,
            _ => arguments.contains(&Some(ColumnType::Instant)),
        };
        return Err(if takes_object {
            unsupported(format!("null literal as an argument of {name}()"))
        } else {
            no_overload()
        });
    };
    if types.iter().any(|t| t.list_element().is_some()) {
        return vectors::resolve(name, &types);
    }
    let call = |kernel: Kernel, parameters: Vec<ColumnType>, result: ColumnType| {
        Ok(Resolved::Call(Call {
            kernel,
            parameters,
            result,
            exchanged: false,
        }))
    };
    // `T f(T, ...)` over every primitive type.
    let same = |kernel: Kernel, result: Option<ColumnType>| match shared(&types) {
        Some(shared) => call(
            kernel,
            vec![shared.clone(); types.len()],
            result.unwrap_or(shared),
        ),
        None => Err(no_overload()),
    };
    let floating = |kernel: Kernel| match shared_floating(&types) {
        Some(shared) => call(kernel, vec![shared.clone(); types.len()], shared),
        None => Err(no_overload()),
    };
    let instants = |kernel: Kernel, result: ColumnType| {
        if types == [ColumnType::Instant, ColumnType::Instant] {
            call(kernel, types.clone(), result)
        } else {
            Err(no_overload())
        }
    };
    // `(Instant, long)` and, with `offset`, `(Instant, long, long)`.
    let instant_and_longs = |kernel: Kernel, most: usize| {
        let longs = &types[1..];
        if !longs.is_empty() && longs.len() <= most && longs.iter().all(is_integral) {
            let mut parameters = vec![ColumnType::Instant];
            parameters.resize(types.len(), ColumnType::Long);
            call(kernel, parameters, ColumnType::Instant)
        } else {
            Err(no_overload())
        }
    };
    let operator = |op: BinaryOp, numbers_only: bool| {
        if types.len() != 2 || (numbers_only && !types.iter().all(is_numeric)) {
            Err(no_overload())
        } else {
            Ok(Resolved::Binary(op))
        }
    };
    let starts_with_instant = types.first() == Some(&ColumnType::Instant);
    let unit = |name: &str| {
        if name.starts_with("epochSeconds") {
            EpochUnit::Seconds
        } else if name.starts_with("epochMillis") {
            EpochUnit::Millis
        } else {
            EpochUnit::Micros
        }
    };

    if let Some(math) = Math::named(name) {
        return match types.as_slice() {
            [argument] if is_numeric(argument) => call(
                Kernel::Math(math),
                vec![ColumnType::Double],
                ColumnType::Double,
            ),
            _ => Err(no_overload()),
        };
    }
    // `io.deephaven.function.Random`: one overload each, reached by widening.
    let random = |function: volatile::RandomFn, parameter: ColumnType| {
        let widens = |argument: &ColumnType| {
            let rank = |column_type: &ColumnType| match column_type {
                ColumnType::Byte | ColumnType::Short | ColumnType::Char | ColumnType::Int => 0,
                ColumnType::Long => 1,
                ColumnType::Float => 2,
                _ => 3,
            };
            is_numeric(argument) && rank(argument) <= rank(&parameter)
        };
        if types.iter().all(widens) {
            call(
                Kernel::Random(function),
                vec![parameter.clone(); types.len()],
                parameter.clone(),
            )
        } else {
            Err(no_overload())
        }
    };
    match (name, types.len()) {
        // `DateTimeUtils#now()` and `#nowSystem()`: the clock of `now` is the system clock.
        ("now" | "nowSystem", 0) => call(Kernel::Now, Vec::new(), ColumnType::Instant),
        ("random", 0) => call(
            Kernel::Random(volatile::RandomFn::Uniform),
            Vec::new(),
            ColumnType::Double,
        ),
        ("randomBool", 0) => call(
            Kernel::Random(volatile::RandomFn::Bool),
            Vec::new(),
            ColumnType::Boolean,
        ),
        ("randomInt", 2) => random(volatile::RandomFn::Int, ColumnType::Int),
        ("randomLong", 2) => random(volatile::RandomFn::Long, ColumnType::Long),
        ("randomFloat", 2) => random(volatile::RandomFn::Float, ColumnType::Float),
        ("randomDouble", 2) => random(volatile::RandomFn::Double, ColumnType::Double),
        ("randomGaussian", 2) => random(volatile::RandomFn::Gaussian, ColumnType::Double),
        ("abs", 1) => same(Kernel::Abs, None),
        ("signum", 1) => same(Kernel::Signum, Some(ColumnType::Int)),
        ("round", 1) => same(Kernel::Round, Some(ColumnType::Long)),
        ("isNaN", 1) => same(Kernel::IsNaN, Some(ColumnType::Boolean)),
        ("isInf", 1) => same(Kernel::IsInf, Some(ColumnType::Boolean)),
        ("isFinite", 1) => same(Kernel::IsFinite, Some(ColumnType::Boolean)),
        // `pow(T, U)` exists for every pair of types.
        ("pow", 2) if types.iter().all(is_numeric) => {
            call(Kernel::Pow, types.clone(), ColumnType::Double)
        }
        ("atan2", 2) => same(Kernel::Atan2, Some(ColumnType::Double)),
        ("min", 1..) => same(Kernel::Min, None),
        ("max", 1..) => same(Kernel::Max, None),
        ("clamp", 3) => same(Kernel::Clamp, None),
        ("inRange", 3) if types.iter().all(is_numeric) => {
            same(Kernel::InRange, Some(ColumnType::Boolean))
        }
        // `inRange(T extends Comparable, T, T)`.
        ("inRange", 3) => Err(unsupported(
            "function inRange() on values that are not numbers",
        )),
        ("in", 2..) => same(Kernel::In, Some(ColumnType::Boolean)),
        ("replaceIfNull", 2) if types.iter().all(is_numeric) => same(Kernel::ReplaceIfNull, None),
        // The generic `replaceIfNull(T, T)`.
        ("replaceIfNull", 2) if types[0] == types[1] => {
            call(Kernel::ReplaceIfNull, types.clone(), types[0].clone())
        }
        ("replaceIfNaN", 2) => floating(Kernel::ReplaceIfNaN),
        ("replaceIfNullNaN", 2) => floating(Kernel::ReplaceIfNullNaN),
        ("replaceIfNonFinite", 2) => floating(Kernel::ReplaceIfNonFinite),
        ("lowerBin", 2 | 3) if starts_with_instant => instant_and_longs(Kernel::InstantLowerBin, 2),
        ("upperBin", 2 | 3) if starts_with_instant => instant_and_longs(Kernel::InstantUpperBin, 2),
        ("lowerBin", 2 | 3) => same(Kernel::LowerBin, None),
        ("upperBin", 2 | 3) => same(Kernel::UpperBin, None),
        ("ifelse", 3) if types[0] == ColumnType::Boolean => match shared(&types[1..]) {
            Some(shared) => call(
                Kernel::IfElse,
                vec![ColumnType::Boolean, shared.clone(), shared.clone()],
                shared,
            ),
            None => Err(no_overload()),
        },
        ("and" | "or", 1..) if types.iter().all(|t| *t == ColumnType::Boolean) => call(
            if name == "and" {
                Kernel::And
            } else {
                Kernel::Or
            },
            types.clone(),
            ColumnType::Boolean,
        ),
        ("parseByte" | "parseShort" | "parseInt" | "parseLong" | "parseBoolean", 1)
            if types[0] == ColumnType::String =>
        {
            let (parsed, result) = match name {
                "parseByte" => (Parsed::Byte, ColumnType::Byte),
                "parseShort" => (Parsed::Short, ColumnType::Short),
                "parseInt" => (Parsed::Int, ColumnType::Int),
                "parseLong" => (Parsed::Long, ColumnType::Long),
                _ => (Parsed::Boolean, ColumnType::Boolean),
            };
            call(Kernel::Parse(parsed), types.clone(), result)
        }
        // `QueryLanguageFunctionUtils#compareTo`, one overload per pair of primitive
        // types. A `long` beside a floating point number has rules of its own.
        ("compareTo", 2) if types.iter().all(is_numeric) => {
            let has = |wanted: ColumnType| types.contains(&wanted);
            let floating = has(ColumnType::Float) || has(ColumnType::Double);
            let parameter = if !floating {
                ColumnType::Long
            } else if has(ColumnType::Long) {
                return Err(unsupported(
                    "function compareTo() of a long and a floating point number",
                ));
            } else if has(ColumnType::Double) {
                ColumnType::Double
            } else {
                ColumnType::Float
            };
            call(
                Kernel::CompareTo,
                vec![parameter.clone(), parameter],
                ColumnType::Int,
            )
        }
        ("parseFloat", 1) if types[0] == ColumnType::String => call(
            Kernel::Parse(Parsed::Float),
            types.clone(),
            ColumnType::Float,
        ),
        ("parseDouble", 1) if types[0] == ColumnType::String => call(
            Kernel::Parse(Parsed::Double),
            types.clone(),
            ColumnType::Double,
        ),
        ("parseInstant" | "parseInstantQuiet", 1) if types[0] == ColumnType::String => call(
            Kernel::ParseInstant {
                quiet: name == "parseInstantQuiet",
            },
            types.clone(),
            ColumnType::Instant,
        ),
        ("epochSeconds" | "epochMillis" | "epochMicros", 1) if starts_with_instant => {
            call(Kernel::Epoch(unit(name)), types.clone(), ColumnType::Long)
        }
        ("epochSecondsToInstant" | "epochMillisToInstant" | "epochMicrosToInstant", 1)
            if is_integral(&types[0]) =>
        {
            call(
                Kernel::EpochToInstant(unit(name)),
                vec![ColumnType::Long],
                ColumnType::Instant,
            )
        }
        ("epochNanos", 1) if starts_with_instant => {
            call(Kernel::EpochNanos, types.clone(), ColumnType::Long)
        }
        ("epochNanosToInstant", 1) if is_integral(&types[0]) => call(
            Kernel::EpochNanosToInstant,
            vec![ColumnType::Long],
            ColumnType::Instant,
        ),
        ("plus", 2) if starts_with_instant => instant_and_longs(Kernel::InstantPlus, 1),
        ("minus", 2) if types[1] == ColumnType::Instant => {
            instants(Kernel::InstantDifference, ColumnType::Long)
        }
        ("minus", 2) if starts_with_instant => instant_and_longs(Kernel::InstantMinus, 1),
        ("diffNanos", 2) => match instants(Kernel::InstantDifference, ColumnType::Long)? {
            Resolved::Call(call) => Ok(Resolved::Call(Call {
                exchanged: true,
                ..call
            })),
            other => Ok(other),
        },
        ("isBefore", 2) => instants(Kernel::TimeOrder(TimeOrder::Before), ColumnType::Boolean),
        ("isBeforeOrEqual", 2) => instants(
            Kernel::TimeOrder(TimeOrder::BeforeOrEqual),
            ColumnType::Boolean,
        ),
        ("isAfter", 2) => instants(Kernel::TimeOrder(TimeOrder::After), ColumnType::Boolean),
        ("isAfterOrEqual", 2) => instants(
            Kernel::TimeOrder(TimeOrder::AfterOrEqual),
            ColumnType::Boolean,
        ),

        // The functions the operators compile to, `QueryLanguageFunctionUtils`'s. Unlike
        // the operator `+`, `plus` has no overload for a `String`.
        ("plus", _) => operator(BinaryOp::Add, true),
        ("minus", _) => operator(BinaryOp::Subtract, true),
        ("multiply", _) => operator(BinaryOp::Multiply, true),
        ("divide", _) => operator(BinaryOp::Divide, true),
        ("remainder", _) => operator(BinaryOp::Remainder, true),
        ("eq", _) => operator(BinaryOp::Equal, false),
        ("less", _) => operator(BinaryOp::Less, false),
        ("lessEquals", _) => operator(BinaryOp::LessOrEqual, false),
        ("greater", _) => operator(BinaryOp::Greater, false),
        ("greaterEquals", _) => operator(BinaryOp::GreaterOrEqual, false),
        ("binaryAnd", _) => operator(BinaryOp::BitAnd, false),
        ("binaryOr", _) => operator(BinaryOp::BitOr, false),
        ("xor", _) => operator(BinaryOp::BitXor, false),
        ("negate", 1) => match &types[0] {
            ColumnType::Byte | ColumnType::Short | ColumnType::Char => Ok(Resolved::NegateAsInt),
            numeric if is_numeric(numeric) => Ok(Resolved::Unary(UnaryOp::Negate)),
            _ => Err(no_overload()),
        },
        ("not", 1) if types[0] == ColumnType::Boolean => Ok(Resolved::Unary(UnaryOp::Not)),
        ("byteCast", 1) => Ok(Resolved::Cast(ColumnType::Byte)),
        ("shortCast", 1) => Ok(Resolved::Cast(ColumnType::Short)),
        ("charCast", 1) => Ok(Resolved::Cast(ColumnType::Char)),
        ("intCast", 1) => Ok(Resolved::Cast(ColumnType::Int)),
        ("longCast", 1) => Ok(Resolved::Cast(ColumnType::Long)),
        ("floatCast", 1) => Ok(Resolved::Cast(ColumnType::Float)),
        ("doubleCast", 1) => Ok(Resolved::Cast(ColumnType::Double)),

        // Every overload of these that takes values of the column types is implemented
        // above, so no other argument list compiles, as recorded.
        (
            "abs"
            | "signum"
            | "round"
            | "isNaN"
            | "isInf"
            | "isFinite"
            | "pow"
            | "atan2"
            | "clamp"
            | "in"
            | "replaceIfNaN"
            | "replaceIfNullNaN"
            | "replaceIfNonFinite"
            | "ifelse"
            | "negate"
            | "not"
            | "and"
            | "or"
            | "min"
            | "max"
            | "replaceIfNull"
            | "epochNanos"
            | "epochNanosToInstant"
            | "diffNanos"
            | "isBefore"
            | "isBeforeOrEqual"
            | "isAfter"
            | "isAfterOrEqual",
            _,
        ) => Err(no_overload()),
        ("parseByte" | "parseShort" | "parseInt" | "parseLong" | "parseBoolean", 0 | 1) => {
            Err(no_overload())
        }
        _ => Err(unsupported(format!("function {name}()"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn call(name: &str, arguments: &[ColumnType]) -> Result<Resolved, FormulaError> {
        let arguments: Vec<Option<ColumnType>> = arguments.iter().cloned().map(Some).collect();
        resolve(name, &arguments)
    }

    fn result(name: &str, arguments: &[ColumnType]) -> ColumnType {
        match call(name, arguments) {
            Ok(Resolved::Call(call)) => call.result,
            other => panic!("{name}{arguments:?} gave {other:?}"),
        }
    }

    #[test]
    fn the_overload_is_the_shared_type_or_the_promoted_one() {
        use ColumnType::{Byte, Char, Double, Float, Int, Long, Short};
        assert_eq!(result("replaceIfNull", &[Byte, Byte]), Byte);
        assert_eq!(result("replaceIfNull", &[Byte, Short]), Int);
        assert_eq!(result("replaceIfNull", &[Char, Char]), Char);
        assert_eq!(result("replaceIfNull", &[Char, Int]), Int);
        assert_eq!(result("max", &[Int, Long, Float]), Float);
        assert_eq!(result("max", &[Byte, Int, Double]), Double);
        assert_eq!(result("clamp", &[Short, Short, Short]), Short);
        assert_eq!(result("lowerBin", &[Long, Int]), Long);
    }

    #[test]
    fn a_function_with_floating_overloads_only_takes_float_unless_given_a_double() {
        use ColumnType::{Byte, Double, Float, Int, Long};
        assert_eq!(result("replaceIfNaN", &[Int, Int]), Float);
        assert_eq!(result("replaceIfNaN", &[Long, Byte]), Float);
        assert_eq!(result("replaceIfNonFinite", &[Float, Double]), Double);
        assert_eq!(result("replaceIfNullNaN", &[Int, Double]), Double);
    }

    #[test]
    fn results_that_do_not_follow_the_argument_type() {
        use ColumnType::{Boolean, Byte, Double, Float, Instant, Int, Long, String};
        assert_eq!(result("sqrt", &[Byte]), Double);
        assert_eq!(result("signum", &[Double]), Int);
        assert_eq!(result("round", &[Float]), Long);
        assert_eq!(result("pow", &[Byte, Float]), Double);
        assert_eq!(result("isFinite", &[Int]), Boolean);
        assert_eq!(result("parseLong", &[String]), Long);
        assert_eq!(result("lowerBin", &[Instant, Int]), Instant);
        assert_eq!(result("minus", &[Instant, Instant]), Long);
        assert_eq!(result("minus", &[Instant, Long]), Instant);
    }

    #[test]
    fn an_import_that_is_not_implemented_is_unsupported_and_an_unknown_name_a_type_error() {
        use ColumnType::{Boolean, Int, String};
        assert_eq!(
            call("cbrt", &[Int]),
            Err(FormulaError::Unsupported("function cbrt()".into()))
        );
        assert!(matches!(
            call("noSuchFunction", &[Int]),
            Err(FormulaError::Type(_))
        ));
        assert!(matches!(call("abs", &[String]), Err(FormulaError::Type(_))));
        assert!(matches!(
            call("abs", &[Int, Int]),
            Err(FormulaError::Type(_))
        ));
        assert!(matches!(
            call("plus", &[String, String]),
            Err(FormulaError::Type(_))
        ));
        assert!(matches!(
            call("and", &[Boolean, Int]),
            Err(FormulaError::Type(_))
        ));
        assert!(matches!(
            resolve("abs", &[None]),
            Err(FormulaError::Type(_))
        ));
        assert!(matches!(
            resolve("replaceIfNull", &[Some(Int), None]),
            Err(FormulaError::Unsupported(_))
        ));
    }

    #[test]
    fn the_names_are_sorted() {
        assert!(names::NAMES.windows(2).all(|pair| pair[0] < pair[1]));
    }
}
