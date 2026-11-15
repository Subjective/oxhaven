//! Functions and methods of vectors and arrays.
//!
//! A vector or array value is one entry of an Arrow `ListArray`, as
//! `ColumnType::list_element` describes it. Every function here walks the list offsets and
//! reads the entry's elements from the child array in place: a reduction ([`VectorFn::Sum`],
//! [`VectorFn::Median`], ...) runs once per entry over a slice of the child's values in the
//! Rust type of the Java element type, and a function that gives a vector or an array
//! writes one child array for the whole batch.
//!
//! [`resolve`] and [`resolve_method`] choose the overload as `QueryLanguageParser` does for a
//! vector column (`io.deephaven.vector.IntVector` and its relatives) and for an array
//! column (`int[]`), which reach the same computation: the array overloads of
//! `io.deephaven.function` wrap the array in a vector.
//!
//! This is also what formula aggregation and the rolling formula of `update_by` evaluate
//! through: both hand a formula a column whose cell is the vector of a group's or a
//! window's values, which is a `ListArray` chunk like any other.

use super::eval::Number;
use super::{Call, Kernel, Resolved};
use crate::parse::{INDEX_METHOD, LENGTH_FIELD};
use crate::types::Numeric;
use arrow::array::{
    Array, ArrayRef, AsArray, BooleanArray, Float64Array, Int32Array, Int64Array, ListArray,
    PrimitiveArray, UInt64Array,
};
use arrow::buffer::{NullBuffer, OffsetBuffer};
use arrow::datatypes::{DataType, Field};
use oxhaven_contracts::expr::FormulaError;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::TableFailure;
use oxhaven_contracts::unported;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::sync::Arc;

fn failure(message: impl Into<String>) -> TableFailure {
    TableFailure {
        message: message.into(),
    }
}

fn arrow_failure(error: arrow::error::ArrowError) -> TableFailure {
    failure(error.to_string())
}

fn unsupported(message: impl Into<String>) -> FormulaError {
    FormulaError::Unsupported(message.into())
}

/// A function or method of a vector or an array. The first argument is the list.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum VectorFn {
    Sum,
    AbsSum,
    Product,
    Avg,
    Var,
    Std,
    Median,
    Min,
    Max,
    /// `count` and `countObj`: the elements that are not null.
    Count,
    CountDistinct,
    /// `first` and `firstObj`.
    First,
    /// `last` and `lastObj`.
    Last,
    /// `len`: null for a null list.
    Len,
    /// `Vector#size`: fails for a null vector.
    Size,
    /// `Vector#isEmpty`.
    IsEmpty,
    /// The `length` of an array, an `int`.
    ArrayLength,
    /// `Vector#get(long)`: null outside the vector.
    Get,
    /// `array[int]`: fails outside the array.
    ArrayGet,
    /// `Vector#subVector(long, long)`.
    SubVector,
    Sort,
    SortDescending,
    Distinct,
    Reverse,
    CumSum,
    CumProd,
    CumMin,
    CumMax,
    AbsAvg,
    /// `ste`: the standard error.
    Ste,
    TStat,
    CountPos,
    CountNeg,
    CountZero,
    IndexOfMax,
    IndexOfMin,
    /// `percentile(double, vector)`: the list is the second argument.
    Percentile,
    /// `diff(int, vector)`: the list is the second argument.
    Diff,
    /// `diff(int, array)`: fails for a null array.
    ArrayDiff,
    /// `wsum`, `wavg`, `cov` and `cor` of two lists.
    WSum,
    WAvg,
    Cov,
    Cor,
    /// `array(vector)`: the same cells as an array, null for null.
    Array,
    /// `vec(array)`: the same cells as a vector; fails for null.
    Vec,
}

/// Whether Java has a typed vector of this element: `ByteVector` to `DoubleVector`.
fn is_primitive(element: &ColumnType) -> bool {
    Numeric::of(element).is_some()
}

fn is_floating(element: &ColumnType) -> bool {
    matches!(element, ColumnType::Float | ColumnType::Double)
}

/// Whether a column of this type can be read by a formula: a vector or array of a type
/// that is not itself a vector or array.
pub(crate) fn is_supported_list(column_type: &ColumnType) -> bool {
    column_type
        .list_element()
        .is_some_and(|element| element.list_element().is_none())
}

fn call(function: VectorFn, parameters: Vec<ColumnType>, result: ColumnType) -> Resolved {
    Resolved::Call(Call {
        kernel: Kernel::Vector(function),
        parameters,
        result,
        exchanged: false,
    })
}

/// Refuses a call that gives an element of a `Boolean` vector or array.
///
/// In a formula column such a call has the type `Boolean`, but as a filter, or as an
/// operand of `&&` in one, Java fails to compile it, as recorded for `where firstObj(VB)`
/// and `where R != 2 && VB.get(0)`: the filter's class declares the vector without its
/// element type. A formula is compiled here without knowing which of the two it is for.
fn refuse_boolean_element(result: &ColumnType) -> Result<(), FormulaError> {
    if *result == ColumnType::Boolean {
        return Err(unsupported(
            "an element of a Boolean vector or array, which Java compiles in a formula \
             column and not in a filter",
        ));
    }
    Ok(())
}

/// The meaning of `name(arguments)` where an argument is a vector or an array.
///
/// Behavior: `QueryLanguageParser#visit(MethodCallExpr)` and `#getMethod` for the overloads
/// of `io.deephaven.function.Numeric`, `Basic` and `Sort` that take one `ByteVector`,
/// `CharVector`, `ShortVector`, `IntVector`, `LongVector`, `FloatVector`, `DoubleVector` or
/// `ObjectVector`, or one array of the same element. The result types are those of the
/// overloads: `long` for `sum`, `absSum` and `product` of integers and `double` for those
/// of `float` and `double`; `double` for `avg`, `var`, `std` and `median`; the element for
/// `min`, `max`, `first` and `last`; `long` for the counts and `len`; an array of the
/// element for `sort`, `sortDescending`, `distinct` and `reverse`; `long[]` or `double[]`
/// for `cumsum` and `cumprod`; `double` for `absAvg`, `ste` and `tstat`; `long` for
/// `countPos`, `countNeg`, `countZero`, `indexOfMax` and `indexOfMin`; an array of the
/// element for `cummin` and `cummax`.
///
/// Of the overloads with a second argument: `percentile(double, vector)` gives the
/// element; `diff(int, vector)`, which no `char` has, an array of the element;
/// `wsum(vector, vector)` a `long` when neither element is a `float` or `double` and a
/// `double` otherwise; `wavg`, `cov` and `cor` a `double`. Either of two lists is a vector
/// or an array.
pub(crate) fn resolve(name: &str, types: &[ColumnType]) -> Result<Resolved, FormulaError> {
    let refused = || {
        Err(unsupported(format!(
            "function {name}() with a vector or array argument"
        )))
    };
    let primitive_list = |column_type: &ColumnType| {
        column_type
            .list_element()
            .filter(|element| is_primitive(element))
            .cloned()
    };
    if let [first, second] = types {
        let resolved = match (name, primitive_list(first), primitive_list(second)) {
            // A `double` parameter takes any number but a `char`, which is refused here.
            ("percentile", None, Some(element))
                if Numeric::of(first).is_some() && *first != ColumnType::Char =>
            {
                call(
                    VectorFn::Percentile,
                    vec![ColumnType::Double, second.clone()],
                    element,
                )
            }
            ("diff", None, Some(element))
                if matches!(
                    first,
                    ColumnType::Byte | ColumnType::Short | ColumnType::Int
                ) && element != ColumnType::Char =>
            {
                let function = if matches!(second, ColumnType::Vector(_)) {
                    VectorFn::Diff
                } else {
                    VectorFn::ArrayDiff
                };
                call(
                    function,
                    vec![ColumnType::Int, second.clone()],
                    ColumnType::array(element),
                )
            }
            ("wsum" | "wavg" | "cov" | "cor", Some(values), Some(weights)) => {
                let (function, result) = match name {
                    "wsum" if is_floating(&values) || is_floating(&weights) => {
                        (VectorFn::WSum, ColumnType::Double)
                    }
                    "wsum" => (VectorFn::WSum, ColumnType::Long),
                    "wavg" => (VectorFn::WAvg, ColumnType::Double),
                    "cov" => (VectorFn::Cov, ColumnType::Double),
                    _ => (VectorFn::Cor, ColumnType::Double),
                };
                call(function, types.to_vec(), result)
            }
            _ => return refused(),
        };
        return Ok(resolved);
    }
    let [list] = types else {
        return refused();
    };
    let Some(element) = list.list_element() else {
        return refused();
    };
    let primitive = is_primitive(element);
    let floating = is_floating(element);
    let sum_type = if floating {
        ColumnType::Double
    } else {
        ColumnType::Long
    };
    let (function, result) = match name {
        "len" => (VectorFn::Len, ColumnType::Long),
        "countObj" if !primitive => (VectorFn::Count, ColumnType::Long),
        "firstObj" if !primitive => (VectorFn::First, element.clone()),
        "lastObj" if !primitive => (VectorFn::Last, element.clone()),
        _ if !primitive => return refused(),
        "sum" => (VectorFn::Sum, sum_type),
        "absSum" => (VectorFn::AbsSum, sum_type),
        "product" => (VectorFn::Product, sum_type),
        "avg" => (VectorFn::Avg, ColumnType::Double),
        "var" => (VectorFn::Var, ColumnType::Double),
        "std" => (VectorFn::Std, ColumnType::Double),
        "median" => (VectorFn::Median, ColumnType::Double),
        "min" => (VectorFn::Min, element.clone()),
        "max" => (VectorFn::Max, element.clone()),
        "count" => (VectorFn::Count, ColumnType::Long),
        "countDistinct" => (VectorFn::CountDistinct, ColumnType::Long),
        "first" => (VectorFn::First, element.clone()),
        "last" => (VectorFn::Last, element.clone()),
        "sort" => (VectorFn::Sort, ColumnType::array(element.clone())),
        "sortDescending" => (VectorFn::SortDescending, ColumnType::array(element.clone())),
        "distinct" => (VectorFn::Distinct, ColumnType::array(element.clone())),
        "reverse" => (VectorFn::Reverse, ColumnType::array(element.clone())),
        "cumsum" => (VectorFn::CumSum, ColumnType::array(sum_type)),
        "cumprod" => (VectorFn::CumProd, ColumnType::array(sum_type)),
        "cummin" => (VectorFn::CumMin, ColumnType::array(element.clone())),
        "cummax" => (VectorFn::CumMax, ColumnType::array(element.clone())),
        "absAvg" => (VectorFn::AbsAvg, ColumnType::Double),
        "ste" => (VectorFn::Ste, ColumnType::Double),
        "tstat" => (VectorFn::TStat, ColumnType::Double),
        "countPos" => (VectorFn::CountPos, ColumnType::Long),
        "countNeg" => (VectorFn::CountNeg, ColumnType::Long),
        "countZero" => (VectorFn::CountZero, ColumnType::Long),
        "indexOfMax" => (VectorFn::IndexOfMax, ColumnType::Long),
        "indexOfMin" => (VectorFn::IndexOfMin, ColumnType::Long),
        "array" if matches!(list, ColumnType::Vector(_)) => {
            (VectorFn::Array, ColumnType::array(element.clone()))
        }
        "vec" if matches!(list, ColumnType::Array(_)) => {
            (VectorFn::Vec, ColumnType::vector(element.clone()))
        }
        _ => return refused(),
    };
    if matches!(function, VectorFn::First | VectorFn::Last) {
        refuse_boolean_element(&result)?;
    }
    Ok(call(function, types.to_vec(), result))
}

/// The meaning of `receiver.method(arguments)` for a vector or array receiver; the
/// arguments of the call are the receiver and then `arguments`.
///
/// Behavior: `QueryLanguageParser#visit(MethodCallExpr)` for the methods of
/// `io.deephaven.vector.Vector` and its typed interfaces (`IntVector#get(long)`,
/// `#subVector(long, long)`, `Vector#size()`, `#isEmpty()`), and
/// `QueryLanguageParser#visit(ArrayAccessExpr)`, which prints an index into a Java array
/// as it is and turns an index into anything else into a call of `get`.
pub(crate) fn resolve_method(
    receiver: &ColumnType,
    method: &str,
    arguments: &[ColumnType],
) -> Result<Resolved, FormulaError> {
    let element = receiver.list_element().expect("a vector or an array");
    let vector = matches!(receiver, ColumnType::Vector(_));
    let kind = if vector { "a vector" } else { "an array" };
    let shown = if method == INDEX_METHOD {
        "an index".to_string()
    } else if method == LENGTH_FIELD {
        "field access .length".to_string()
    } else {
        format!("method call .{method}()")
    };
    let refused = || Err(unsupported(format!("{shown} on a value of {kind} type")));
    // A `long` parameter takes any integer; a Java array index takes what widens to `int`.
    let longs = arguments
        .iter()
        .all(|t| matches!(Numeric::of(t), Some(Numeric::Int | Numeric::Long)));
    let ints = arguments
        .iter()
        .all(|t| matches!(Numeric::of(t), Some(Numeric::Int)));
    let index_parameters = |index: ColumnType| {
        let mut parameters = vec![receiver.clone()];
        parameters.resize(arguments.len() + 1, index);
        parameters
    };
    let (function, parameters, result) = match (vector, method, arguments.len()) {
        (true, "size", 0) => (VectorFn::Size, vec![receiver.clone()], ColumnType::Long),
        (true, "isEmpty", 0) => (
            VectorFn::IsEmpty,
            vec![receiver.clone()],
            ColumnType::Boolean,
        ),
        (true, "get" | INDEX_METHOD, 1) if longs => (
            VectorFn::Get,
            index_parameters(ColumnType::Long),
            element.clone(),
        ),
        // `ObjectVector#subVector` returns an `ObjectVector` whose element type Java no
        // longer knows: the column it makes holds `Object`, as recorded for
        // `VB.subVector(1, 3)`, which a client receives as strings.
        (true, "subVector", 2) if longs && is_primitive(element) => (
            VectorFn::SubVector,
            index_parameters(ColumnType::Long),
            receiver.clone(),
        ),
        // Behavior: `QueryLanguageParser#visit(FieldAccessExpr)`: `length` of an array is an
        // `int`.
        (false, LENGTH_FIELD, 0) => (
            VectorFn::ArrayLength,
            vec![receiver.clone()],
            ColumnType::Int,
        ),
        (false, INDEX_METHOD, 1) if ints => (
            VectorFn::ArrayGet,
            index_parameters(ColumnType::Int),
            element.clone(),
        ),
        _ => return refused(),
    };
    if matches!(function, VectorFn::Get | VectorFn::ArrayGet) {
        refuse_boolean_element(&result)?;
    }
    Ok(call(function, parameters, result))
}

/// A Java primitive as a vector element.
pub(super) trait Element: Number + arrow::datatypes::ArrowNativeType {
    const FLOATING: bool;
    /// Java's widening to `double`.
    fn double(self) -> f64;
    /// Java's widening of an integer type to `long`.
    fn long(self) -> i64;
    /// `Math.abs(value)` widened to `long`: computed in `int` for the types narrower than
    /// `long`, and the value itself for a `char`.
    fn abs_long(self) -> i64;
    /// The order of `java.util.Arrays#sort`: for a floating type `-0.0` before `0.0` and
    /// NaN last.
    fn order(&self, other: &Self) -> Ordering;
    /// What makes two values one in a fastutil hash set: the value, and for a floating type
    /// its bits with every NaN alike (`Float#floatToIntBits`), so that `-0.0` and `0.0`
    /// are two values.
    fn identity(self) -> u64;
    /// `0.5 * (self + other)` as Java computes it for two operands of this type: the sum in
    /// `int` for the types narrower than `long`, in the type itself for the others.
    fn half_sum(self, other: Self) -> f64;
    /// Java's `Float.NaN` or `Double.NaN`.
    fn nan() -> Self;
    /// Where the type stands in Java's binary numeric promotion: 0 for the types that
    /// become `int`, then `long`, `float` and `double`.
    const RANK: u8;
    /// Java's conversion to `float`.
    fn single(self) -> f32;
    /// `(T) (self - other)` as Java computes it: wrapping for an integer type.
    fn difference(self, other: Self) -> Self;
    /// `Math.min(self, other)`: for a floating type NaN if either is, and `-0.0` below `0.0`.
    fn least(self, other: Self) -> Self;
    /// `Math.max(self, other)`.
    fn greatest(self, other: Self) -> Self;
}

macro_rules! integer_element {
    ($native:ty, $sum:ty, $rank:expr) => {
        impl Element for $native {
            const FLOATING: bool = false;
            fn double(self) -> f64 {
                self as f64
            }
            fn long(self) -> i64 {
                self as i64
            }
            fn abs_long(self) -> i64 {
                (self as $sum).wrapping_abs() as i64
            }
            fn order(&self, other: &Self) -> Ordering {
                self.cmp(other)
            }
            fn identity(self) -> u64 {
                self as u64
            }
            fn half_sum(self, other: Self) -> f64 {
                0.5 * ((self as $sum).wrapping_add(other as $sum) as f64)
            }
            fn nan() -> Self {
                unreachable!("an integer is never NaN")
            }
            const RANK: u8 = $rank;
            fn single(self) -> f32 {
                self as f32
            }
            fn difference(self, other: Self) -> Self {
                self.wrapping_sub(other)
            }
            fn least(self, other: Self) -> Self {
                self.min(other)
            }
            fn greatest(self, other: Self) -> Self {
                self.max(other)
            }
        }
    };
}

macro_rules! floating_element {
    ($native:ty, $rank:expr) => {
        impl Element for $native {
            const FLOATING: bool = true;
            fn double(self) -> f64 {
                f64::from(self)
            }
            fn long(self) -> i64 {
                self as i64
            }
            fn abs_long(self) -> i64 {
                unreachable!("the absolute sum of a floating type is a double")
            }
            fn order(&self, other: &Self) -> Ordering {
                // `total_cmp` puts a NaN with its sign bit set first; Java has it last.
                match (self.is_nan(), other.is_nan()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => Ordering::Greater,
                    (false, true) => Ordering::Less,
                    (false, false) => self.total_cmp(other),
                }
            }
            fn identity(self) -> u64 {
                if self.is_nan() {
                    u64::from(<$native>::NAN.to_bits())
                } else {
                    u64::from(self.to_bits())
                }
            }
            fn half_sum(self, other: Self) -> f64 {
                0.5 * f64::from(self + other)
            }
            fn nan() -> Self {
                <$native>::NAN
            }
            const RANK: u8 = $rank;
            fn single(self) -> f32 {
                self as f32
            }
            fn difference(self, other: Self) -> Self {
                self - other
            }
            fn least(self, other: Self) -> Self {
                if self.is_nan() || other.is_nan() {
                    <$native>::NAN
                } else if self.order(&other) == Ordering::Greater {
                    other
                } else {
                    self
                }
            }
            fn greatest(self, other: Self) -> Self {
                if self.is_nan() || other.is_nan() {
                    <$native>::NAN
                } else if self.order(&other) == Ordering::Less {
                    other
                } else {
                    self
                }
            }
        }
    };
}

integer_element!(i8, i32, 0);
integer_element!(i16, i32, 0);
integer_element!(u16, i32, 0);
integer_element!(i32, i32, 0);
integer_element!(i64, i64, 1);
floating_element!(f32, 2);
floating_element!(f64, 3);

/// The elements of one list entry, read in place from the child array.
struct Cells<'a, T: Element> {
    values: &'a [T],
    nulls: Option<&'a NullBuffer>,
    /// The entry's range in the child array.
    start: usize,
    end: usize,
}

impl<T: Element> Cells<'_, T> {
    fn len(&self) -> usize {
        self.end - self.start
    }

    /// The elements in order, `None` for a null one.
    fn iter(&self) -> impl Iterator<Item = Option<T>> + '_ {
        (self.start..self.end).map(|at| {
            self.nulls
                .is_none_or(|nulls| nulls.is_valid(at))
                .then(|| self.values[at])
        })
    }

    /// The elements that are not null.
    fn present(&self) -> impl Iterator<Item = T> + '_ {
        self.iter().flatten()
    }
}

/// The range of each entry of `list` in its child array, `None` for a null entry.
fn ranges(list: &ListArray) -> impl Iterator<Item = Option<(usize, usize)>> + '_ {
    let offsets = list.value_offsets();
    (0..list.len()).map(move |row| {
        list.is_valid(row)
            .then(|| (offsets[row] as usize, offsets[row + 1] as usize))
    })
}

fn each_cells<'a, T: Element>(
    list: &'a ListArray,
) -> impl Iterator<Item = Option<Cells<'a, T>>> + 'a {
    let child = list.values().as_primitive::<T::Arrow>();
    let values: &[T] = child.values();
    let nulls = child.nulls();
    ranges(list).map(move |range| {
        range.map(|(start, end)| Cells {
            values,
            nulls,
            start,
            end,
        })
    })
}

/// One value per entry: null for a null entry, for `None`, and for the value Java reserves
/// for null in the result's type.
fn reduce<T: Element, R: Number>(list: &ListArray, f: impl Fn(&Cells<T>) -> Option<R>) -> ArrayRef {
    let values: PrimitiveArray<R::Arrow> = each_cells::<T>(list)
        .map(|cells| cells.and_then(|cells| f(&cells)).filter(|v| *v != R::NULL))
        .collect();
    Arc::new(values)
}

/// A list column from the elements of all entries and each entry's length, `None` for a
/// null entry.
fn lists(
    elements: ArrayRef,
    lengths: impl Iterator<Item = Option<usize>>,
) -> Result<ArrayRef, TableFailure> {
    let mut valid = Vec::new();
    let lengths: Vec<usize> = lengths
        .map(|length| {
            valid.push(length.is_some());
            length.unwrap_or(0)
        })
        .collect();
    if i32::try_from(elements.len()).is_err() {
        return Err(failure("a column of arrays holds more than 2^31 elements"));
    }
    let field = Arc::new(Field::new("", elements.data_type().clone(), true));
    let nulls = valid.contains(&false).then(|| NullBuffer::from(valid));
    Ok(Arc::new(ListArray::new(
        field,
        OffsetBuffer::from_lengths(lengths),
        elements,
        nulls,
    )))
}

/// One array per entry, whose elements `f` appends; null for a null entry. An element
/// equal to the value Java reserves for null is null.
fn map_lists<T: Element, R: Number>(
    list: &ListArray,
    f: impl Fn(&Cells<T>, &mut Vec<R>),
) -> Result<ArrayRef, TableFailure> {
    let mut elements: Vec<R> = Vec::with_capacity(list.values().len());
    let lengths: Vec<Option<usize>> = each_cells::<T>(list)
        .map(|cells| {
            cells.map(|cells| {
                let before = elements.len();
                f(&cells, &mut elements);
                elements.len() - before
            })
        })
        .collect();
    let elements: PrimitiveArray<R::Arrow> = elements
        .into_iter()
        .map(|value| Some(value).filter(|value| *value != R::NULL))
        .collect();
    lists(Arc::new(elements), lengths.into_iter())
}

/// Behavior: `Numeric#sum(IntVector)` and its relatives for the integer types: the sum of
/// the elements that are not null, in a `long` that wraps; null when there is none.
fn integer_sum<T: Element>(cells: &Cells<T>, value: impl Fn(T) -> i64) -> Option<i64> {
    let mut sum = 0_i64;
    let mut any = false;
    for element in cells.present() {
        sum = sum.wrapping_add(value(element));
        any = true;
    }
    any.then_some(sum)
}

/// Behavior: `Numeric#sum(DoubleVector)` and `#sum(FloatVector)`, `#absSum` of the same:
/// a NaN element gives NaN, nulls are skipped, and the rest is added in order in a
/// `double`; null when there is no element that is not null.
fn floating_sum<T: Element>(cells: &Cells<T>, value: impl Fn(T) -> f64) -> Option<f64> {
    let mut sum = 0.0_f64;
    let mut any = false;
    for element in cells.iter() {
        match element {
            Some(element) if element.is_nan() => return Some(f64::NAN),
            Some(element) => {
                sum += value(element);
                any = true;
            }
            None => {}
        }
    }
    any.then_some(sum)
}

/// Behavior: `Numeric#product(IntVector)` and its relatives for the integer types: zero as
/// soon as an element is zero, otherwise the wrapping product of the elements that are not
/// null; null when there is none.
fn integer_product<T: Element>(cells: &Cells<T>) -> Option<i64> {
    let mut product = 1_i64;
    let mut any = false;
    for element in cells.present() {
        let element = element.long();
        if element == 0 {
            return Some(0);
        }
        product = product.wrapping_mul(element);
        any = true;
    }
    any.then_some(product)
}

/// Behavior: `Numeric#product(DoubleVector)` and `#product(FloatVector)`: a NaN element
/// gives NaN, and so does an infinity after a zero or a zero after an infinity, wherever
/// they stand; otherwise zero when an element is zero, and the product of the elements
/// that are not null. Null when there is none.
fn floating_product<T: Element>(cells: &Cells<T>) -> Option<f64> {
    let mut product = 1.0_f64;
    let (mut any, mut zero, mut infinite) = (false, false, false);
    for element in cells.present() {
        if element.is_nan() {
            return Some(f64::NAN);
        }
        if element.is_infinite() {
            if zero {
                return Some(f64::NAN);
            }
            infinite = true;
        } else if element.double() == 0.0 {
            if infinite {
                return Some(f64::NAN);
            }
            zero = true;
        }
        any = true;
        product *= element.double();
    }
    any.then_some(if zero { 0.0 } else { product })
}

/// Behavior: `Numeric#avg(IntVector)` and its relatives: the sum in a `double` over the
/// count of the elements that are not null; NaN when a `float` or `double` element is NaN;
/// null when there is no element that is not null.
fn avg<T: Element>(cells: &Cells<T>) -> Option<f64> {
    let mut sum = 0.0_f64;
    let mut count = 0.0_f64;
    for element in cells.present() {
        if element.is_nan() {
            return Some(f64::NAN);
        }
        sum += element.double();
        count += 1.0;
    }
    (count > 0.0).then(|| sum / count)
}

/// `java.lang.Math#ulp(double)` of a finite value.
fn ulp(value: f64) -> f64 {
    let magnitude = value.abs();
    if magnitude == f64::MAX {
        // The distance to the value below: there is none above.
        magnitude - f64::from_bits(magnitude.to_bits() - 1)
    } else {
        f64::from_bits(magnitude.to_bits() + 1) - magnitude
    }
}

/// Behavior: `Numeric#var(IntVector)` and its relatives: the sample variance of the
/// elements that are not null, from their sum and sum of squares in `double`. NaN when a
/// `float` or `double` element is NaN or infinite, when fewer than two elements are not
/// null, and when a sum overflows; zero when the difference of the sums is within one unit
/// in the last place of the sum of squares; null when every element is null.
fn var<T: Element>(cells: &Cells<T>) -> Option<f64> {
    let (mut sum, mut sum2, mut count) = (0.0_f64, 0.0_f64, 0_u64);
    for element in cells.present() {
        if element.is_nan() || element.is_infinite() {
            return Some(f64::NAN);
        }
        let value = element.double();
        sum += value;
        sum2 += value * value;
        count += 1;
    }
    if count == 0 {
        // Java's test is that every element is null, which an empty vector passes.
        return None;
    }
    if count == 1 || sum.is_infinite() || sum2.is_infinite() {
        return Some(f64::NAN);
    }
    let count = count as f64;
    let delta = sum2 - sum * (sum / count);
    let relative = delta / ulp(sum2);
    Some(if relative.abs() > 1.0 {
        delta / (count - 1.0)
    } else {
        0.0
    })
}

/// Behavior: `Numeric#max(IntVector)` and `#min(IntVector)` through `#indexOfMax` and
/// `#indexOfMin`, and the `float` and `double` overloads, which do the same after giving
/// NaN for a NaN element: the first of the greatest, or least, elements that are not null,
/// so the first of `-0.0` and `0.0`; null when there is none.
fn extreme<T: Element>(cells: &Cells<T>, max: bool) -> Option<T> {
    let mut best: Option<T> = None;
    for element in cells.present() {
        if element.is_nan() {
            return Some(T::nan());
        }
        let better = best.is_none_or(|best| if max { element > best } else { element < best });
        if better {
            best = Some(element);
        }
    }
    best
}

/// Behavior: `Numeric#median(IntVector)` and its relatives, with `ArraySort#sort`: null for
/// an empty vector; NaN when a `float` or `double` element is NaN; otherwise of the
/// elements that are not null, in order, the middle one, or half the sum of the two middle
/// ones, where the sum is computed in the element's type (`int` for the narrower ones) and
/// can wrap; null when every element is null.
fn median<T: Element>(cells: &Cells<T>) -> Option<f64> {
    let mut sorted: Vec<T> = cells.present().collect();
    if sorted.iter().any(|element| element.is_nan()) {
        return Some(f64::NAN);
    }
    if sorted.is_empty() {
        return None;
    }
    sorted.sort_unstable_by(T::order);
    let middle = sorted.len() / 2;
    Some(if sorted.len().is_multiple_of(2) {
        sorted[middle - 1].half_sum(sorted[middle])
    } else {
        sorted[middle].double()
    })
}

/// Behavior: `Basic#countDistinct(IntVector)` and its relatives: the number of different
/// elements that are not null, where two `float` or `double` values are one as
/// [`Element::identity`] says.
fn count_distinct<T: Element>(cells: &Cells<T>) -> Option<i64> {
    let mut seen: Vec<u64> = cells.present().map(Element::identity).collect();
    seen.sort_unstable();
    seen.dedup();
    Some(seen.len() as i64)
}

/// Behavior: `Sort#sort(IntVector)` and its relatives with `ArraySort#sort`: nulls first,
/// then the values in the order of `java.util.Arrays#sort`. `Sort#sortDescending` is that
/// order reversed, nulls last.
fn sort<T: Element>(cells: &Cells<T>, descending: bool, out: &mut Vec<T>) {
    let start = out.len();
    out.extend(std::iter::repeat_n(
        T::NULL,
        cells.len() - cells.present().count(),
    ));
    let values = out.len();
    out.extend(cells.present());
    out[values..].sort_unstable_by(T::order);
    if descending {
        out[start..].reverse();
    }
}

/// Behavior: `Basic#distinct(IntVector)` and its relatives: the elements that are not
/// null, each once, in order of first appearance, alike as [`Element::identity`] says.
fn distinct<T: Element>(cells: &Cells<T>, out: &mut Vec<T>) {
    let mut seen = HashSet::new();
    out.extend(
        cells
            .present()
            .filter(|element| seen.insert(element.identity())),
    );
}

/// Behavior: `Numeric#cumsum(IntVector)` and its relatives for the integer types: a
/// running `long` sum that wraps. A null element repeats the sum so far, which is null
/// until an element is not; a sum equal to the null `long` counts as none.
///
/// `Numeric#cumprod(IntVector)` and its relatives are the same with a product.
fn integer_cumulate<T: Element>(cells: &Cells<T>, out: &mut Vec<i64>, add: fn(i64, i64) -> i64) {
    let mut sum = i64::MIN;
    for element in cells.iter() {
        sum = match element {
            None => sum,
            Some(element) if sum == i64::MIN => element.long(),
            Some(element) => add(sum, element.long()),
        };
        out.push(sum);
    }
}

/// Behavior: `Numeric#cumsum(DoubleVector)` and `#cumsum(FloatVector)`: a running `double`
/// sum as for the integers, where from a NaN element on, or after a sum that is NaN, every
/// element is NaN.
///
/// `Numeric#cumprod(DoubleVector)` and `#cumprod(FloatVector)` are the same with a product.
fn floating_cumulate<T: Element>(cells: &Cells<T>, out: &mut Vec<f64>, add: fn(f64, f64) -> f64) {
    let null = <f64 as crate::eval::numeric::JavaPrimitive>::NULL;
    let mut elements = cells.iter();
    let Some(first) = elements.next() else {
        return;
    };
    let mut sum = first.map_or(null, T::double);
    out.push(sum);
    let mut poisoned = false;
    for element in elements {
        poisoned |= sum.is_nan() || element.is_some_and(|element| element.is_nan());
        sum = match element {
            _ if poisoned => f64::NAN,
            None => sum,
            Some(element) if sum == null => element.double(),
            Some(element) => add(sum, element.double()),
        };
        out.push(sum);
    }
}

/// Behavior: `Numeric#cummin(IntVector)`, `#cummax(IntVector)` and their relatives: the
/// least, or greatest, element so far by `Math.min` or `Math.max`, null until an element
/// is not null. A NaN stays.
fn running<T: Element>(cells: &Cells<T>, out: &mut Vec<T>, choose: fn(T, T) -> T) {
    let mut so_far: Option<T> = None;
    for element in cells.iter() {
        so_far = match (so_far, element) {
            (None, element) => element,
            (so_far, None) => so_far,
            (Some(so_far), Some(element)) => Some(choose(so_far, element)),
        };
        out.push(so_far.unwrap_or(T::NULL));
    }
}

/// Behavior: `Numeric#absAvg(IntVector)` and its relatives: the average of `Math.abs` of
/// the elements that are not null. For `float` and `double`, NaN at a NaN element and
/// positive infinity at an infinite one, whichever comes first. Null when no element is
/// not null. A `char` has no sign: `absAvg(CharVector)` is `avg`.
fn abs_avg<T: Element>(cells: &Cells<T>) -> Option<f64> {
    let mut sum = 0.0_f64;
    let mut count = 0.0_f64;
    for element in cells.present() {
        if element.is_nan() {
            return Some(f64::NAN);
        }
        if element.is_infinite() {
            return Some(f64::INFINITY);
        }
        sum += if T::FLOATING {
            element.double().abs()
        } else {
            element.abs_long() as f64
        };
        count += 1.0;
    }
    (count > 0.0).then(|| sum / count)
}

/// Behavior: `Numeric#ste(IntVector)` and its relatives: `std` over the square root of the
/// number of elements that are not null; null when `std` is.
fn ste<T: Element>(cells: &Cells<T>) -> Option<f64> {
    let std = var(cells)?.sqrt();
    Some(std / (cells.present().count() as f64).sqrt())
}

fn count_where<T: Element>(cells: &Cells<T>, counts: impl Fn(T) -> bool) -> Option<i64> {
    Some(cells.present().filter(|element| counts(*element)).count() as i64)
}

/// Behavior: `Numeric#indexOfMax(IntVector)`, `#indexOfMin(IntVector)` and their
/// relatives: the position of the first of the greatest, or least, elements that are not
/// null, an infinity among them. For `float` and `double`, null when an element is NaN.
/// Null when there is no element that is not null.
fn index_of_extreme<T: Element>(cells: &Cells<T>, max: bool) -> Option<i64> {
    let mut best: Option<(T, i64)> = None;
    for (index, element) in cells.iter().enumerate() {
        let Some(element) = element else {
            continue;
        };
        if element.is_nan() {
            return None;
        }
        let better = best.is_none_or(|(best, _)| if max { element > best } else { element < best });
        if better {
            best = Some((element, index as i64));
        }
    }
    best.map(|(_, index)| index)
}

/// `values[row]` as Java reads a `double` argument: a null is the value Java reserves.
fn double_at(values: &Float64Array, row: usize) -> f64 {
    if values.is_valid(row) {
        values.value(row)
    } else {
        <f64 as crate::eval::numeric::JavaPrimitive>::NULL
    }
}

/// Behavior: `Numeric#percentile(double, IntVector)` and its relatives: null for a null or
/// empty vector, then `IllegalArgumentException` for a percentile outside 0 to 1; NaN when
/// a `float` or `double` element is NaN; otherwise, of the `n` elements that are not null,
/// in order, the one at `Math.round(percentile * (n - 1))`; null when every element is
/// null. A NaN percentile is neither below 0 nor above 1 and rounds to position 0.
fn percentile<T: Element>(
    percentiles: &Float64Array,
    list: &ListArray,
) -> Result<ArrayRef, TableFailure> {
    let mut results: Vec<Option<T>> = Vec::with_capacity(list.len());
    for (row, cells) in each_cells::<T>(list).enumerate() {
        let Some(cells) = cells.filter(|cells| cells.len() > 0) else {
            results.push(None);
            continue;
        };
        let percentile = double_at(percentiles, row);
        if !(0.0..=1.0).contains(&percentile) && !percentile.is_nan() {
            return Err(failure(format!(
                "java.lang.IllegalArgumentException: Invalid percentile = {percentile}"
            )));
        }
        let mut sorted: Vec<T> = cells.present().collect();
        results.push(if sorted.iter().any(|element| element.is_nan()) {
            Some(T::nan())
        } else if sorted.is_empty() {
            None
        } else {
            sorted.sort_unstable_by(T::order);
            // `as` takes a NaN to 0, as Java's `Math.round` does.
            Some(sorted[(percentile * (sorted.len() - 1) as f64).round() as usize])
        });
    }
    let values: PrimitiveArray<T::Arrow> = results
        .into_iter()
        .map(|value| value.filter(|value| *value != T::NULL))
        .collect();
    Ok(Arc::new(values))
}

/// Behavior: `Numeric#diff(int, IntVector)` and its relatives: for each position, the
/// element `stride` positions on less the element, in the element's type, null when
/// either is null or the other position is outside the vector. A null stride is the
/// lowest `int`. `Numeric#diff(int, int...)` wraps its array in a vector, which requires
/// an array that is not null.
fn diff<T: Element>(
    strides: &Int32Array,
    list: &ListArray,
    array: bool,
) -> Result<ArrayRef, TableFailure> {
    let mut elements: Vec<Option<T>> = Vec::with_capacity(list.values().len());
    let mut lengths = Vec::with_capacity(list.len());
    for (row, cells) in each_cells::<T>(list).enumerate() {
        let Some(cells) = cells else {
            if array {
                return Err(failure(
                    "io.deephaven.base.verify.RequirementFailure: diff() of a null array",
                ));
            }
            lengths.push(None);
            continue;
        };
        let stride = if strides.is_valid(row) {
            strides.value(row)
        } else {
            i32::MIN
        };
        let values: Vec<Option<T>> = cells.iter().collect();
        for (index, value) in values.iter().enumerate() {
            // Java adds in `int`; a position below zero is outside the vector.
            let ahead = (index as i32).wrapping_add(stride);
            let ahead = usize::try_from(ahead)
                .ok()
                .and_then(|at| values.get(at).copied().flatten());
            elements.push(match (value, ahead) {
                (Some(value), Some(ahead)) => {
                    Some(ahead.difference(*value)).filter(|difference| *difference != T::NULL)
                }
                _ => None,
            });
        }
        lengths.push(Some(values.len()));
    }
    let elements: PrimitiveArray<T::Arrow> = elements.into_iter().collect();
    lists(Arc::new(elements), lengths.into_iter())
}

/// `a * b` as Java computes it for operands of the two types: in `int` or `long`, where
/// it wraps, or in `float` or `double`.
///
/// Behavior: binary numeric promotion, JLS 5.6.2
fn product<A: Element, B: Element>(a: A, b: B) -> f64 {
    match A::RANK.max(B::RANK) {
        0 => (a.long() as i32).wrapping_mul(b.long() as i32) as f64,
        1 => a.long().wrapping_mul(b.long()) as f64,
        2 => f64::from(a.single() * b.single()),
        _ => a.double() * b.double(),
    }
}

/// The pairs of elements of two lists of one length; `None` for a pair with a null.
fn pairs<'a, A: Element, B: Element>(
    values: &'a Cells<A>,
    weights: &'a Cells<B>,
) -> impl Iterator<Item = Option<(A, B)>> + 'a {
    values
        .iter()
        .zip(weights.iter())
        .map(|(a, b)| Some((a?, b?)))
}

/// Behavior: `Numeric#wsum(IntVector, IntVector)` and its relatives where neither element
/// is a `float` or `double`: the sum of the products in a `long` that wraps, over the
/// pairs where neither is null; null when there is none.
fn integer_wsum<A: Element, B: Element>(values: &Cells<A>, weights: &Cells<B>) -> Option<i64> {
    let mut sum = 0_i64;
    let mut any = false;
    for (value, weight) in pairs(values, weights).flatten() {
        sum = sum.wrapping_add(value.long().wrapping_mul(weight.long()));
        any = true;
    }
    any.then_some(sum)
}

/// Behavior: `Numeric#wsum(DoubleVector, IntVector)` and its relatives where an element
/// is a `float` or `double`: NaN at a NaN element of either, also one whose partner is
/// null; otherwise the sum of the products in `double` over the pairs where neither is
/// null; null when there is none.
fn floating_wsum<A: Element, B: Element>(values: &Cells<A>, weights: &Cells<B>) -> Option<f64> {
    let mut sum = 0.0_f64;
    let mut any = false;
    for (value, weight) in values.iter().zip(weights.iter()) {
        if value.is_some_and(|v| v.is_nan()) || weight.is_some_and(|w| w.is_nan()) {
            return Some(f64::NAN);
        }
        if let (Some(value), Some(weight)) = (value, weight) {
            sum += value.double() * weight.double();
            any = true;
        }
    }
    any.then_some(sum)
}

/// Behavior: `Numeric#wavg(IntVector, IntVector)` and its relatives: NaN at a NaN element
/// of either; otherwise, over the pairs where neither is null, the sum of the products,
/// each computed in the type Java promotes the two to, over the sum of the weights; null
/// when there is no such pair.
fn wavg<A: Element, B: Element>(values: &Cells<A>, weights: &Cells<B>) -> Option<f64> {
    let (mut sum, mut weight_sum) = (0.0_f64, 0.0_f64);
    let mut any = false;
    for (value, weight) in values.iter().zip(weights.iter()) {
        if value.is_some_and(|v| v.is_nan()) || weight.is_some_and(|w| w.is_nan()) {
            return Some(f64::NAN);
        }
        if let (Some(value), Some(weight)) = (value, weight) {
            sum += product(value, weight);
            weight_sum += weight.double();
            any = true;
        }
    }
    any.then(|| sum / weight_sum)
}

/// Behavior: `Numeric#cov(IntVector, IntVector)`, `#cor(IntVector, IntVector)` and their
/// relatives: NaN at a NaN or infinite element of either; otherwise, over the pairs where
/// neither is null, from the sums, the sums of squares and the sum of the products, each
/// product computed in the type Java promotes its operands to. Null when there is no such
/// pair.
fn cov_or_cor<A: Element, B: Element>(
    values: &Cells<A>,
    others: &Cells<B>,
    correlation: bool,
) -> Option<f64> {
    let (mut sum0, mut sum0_sq, mut sum1, mut sum1_sq, mut sum01) = (0.0_f64, 0.0, 0.0, 0.0, 0.0);
    let mut count = 0.0_f64;
    for (value, other) in values.iter().zip(others.iter()) {
        if value.is_some_and(|v| v.is_nan() || v.is_infinite())
            || other.is_some_and(|v| v.is_nan() || v.is_infinite())
        {
            return Some(f64::NAN);
        }
        if let (Some(value), Some(other)) = (value, other) {
            sum0 += value.double();
            sum0_sq += product(value, value);
            sum1 += other.double();
            sum1_sq += product(other, other);
            sum01 += product(value, other);
            count += 1.0;
        }
    }
    if count == 0.0 {
        return None;
    }
    let cov = sum01 / count - sum0 * sum1 / count / count;
    if !correlation {
        return Some(cov);
    }
    let var0 = sum0_sq / count - sum0 * sum0 / count / count;
    let var1 = sum1_sq / count - sum1 * sum1 / count / count;
    Some(cov / (var0 * var1).sqrt())
}

/// One value per row from the entries of two lists: null when either entry is null or
/// `f` gives none, and a failure when the two have different lengths.
fn reduce_two<A: Element, B: Element, R: Number>(
    first: &ListArray,
    second: &ListArray,
    different: impl Fn(usize, usize) -> String,
    f: impl Fn(&Cells<A>, &Cells<B>) -> Option<R>,
) -> Result<ArrayRef, TableFailure> {
    let values = each_cells::<A>(first)
        .zip(each_cells::<B>(second))
        .map(|entries| {
            let (Some(first), Some(second)) = entries else {
                return Ok(None);
            };
            if first.len() != second.len() {
                return Err(failure(different(first.len(), second.len())));
            }
            Ok(f(&first, &second).filter(|value| *value != R::NULL))
        })
        .collect::<Result<PrimitiveArray<R::Arrow>, TableFailure>>()?;
    Ok(Arc::new(values))
}

fn of_two<A: Element, B: Element>(
    function: VectorFn,
    first: &ListArray,
    second: &ListArray,
) -> Result<ArrayRef, TableFailure> {
    let sizes = |a: usize, b: usize| {
        format!("java.lang.IllegalArgumentException: Incompatible input sizes: {a}, {b}")
    };
    let lengths = |_: usize, _: usize| {
        "java.lang.IllegalArgumentException: Input arrays are different lengths!".to_string()
    };
    match function {
        VectorFn::WSum if A::FLOATING || B::FLOATING => {
            reduce_two::<A, B, f64>(first, second, sizes, floating_wsum)
        }
        VectorFn::WSum => reduce_two::<A, B, i64>(first, second, sizes, integer_wsum),
        VectorFn::WAvg => reduce_two::<A, B, f64>(first, second, sizes, wavg),
        VectorFn::Cov => {
            reduce_two::<A, B, f64>(first, second, lengths, |a, b| cov_or_cor(a, b, false))
        }
        VectorFn::Cor => {
            reduce_two::<A, B, f64>(first, second, lengths, |a, b| cov_or_cor(a, b, true))
        }
        other => unreachable!("{other:?} is not a function of two lists"),
    }
}

fn typed<T: Element>(function: VectorFn, list: &ListArray) -> Result<ArrayRef, TableFailure> {
    Ok(match function {
        VectorFn::Sum if T::FLOATING => reduce::<T, f64>(list, |c| floating_sum(c, T::double)),
        VectorFn::Sum => reduce::<T, i64>(list, |c| integer_sum(c, T::long)),
        // Behavior: `Numeric#absSum`: the sum of `Math.abs` of each element.
        VectorFn::AbsSum if T::FLOATING => {
            reduce::<T, f64>(list, |c| floating_sum(c, |e| e.abs().double()))
        }
        VectorFn::AbsSum => reduce::<T, i64>(list, |c| integer_sum(c, T::abs_long)),
        VectorFn::Product if T::FLOATING => reduce::<T, f64>(list, floating_product),
        VectorFn::Product => reduce::<T, i64>(list, integer_product),
        VectorFn::Avg => reduce::<T, f64>(list, avg),
        VectorFn::Var => reduce::<T, f64>(list, var),
        // Behavior: `Numeric#std`: the square root of `var`, null for null.
        VectorFn::Std => reduce::<T, f64>(list, |c| var(c).map(f64::sqrt)),
        VectorFn::Median => reduce::<T, f64>(list, median),
        VectorFn::Min => reduce::<T, T>(list, |c| extreme(c, false)),
        VectorFn::Max => reduce::<T, T>(list, |c| extreme(c, true)),
        VectorFn::CountDistinct => reduce::<T, i64>(list, count_distinct),
        VectorFn::Sort => map_lists::<T, T>(list, |c, out| sort(c, false, out))?,
        VectorFn::SortDescending => map_lists::<T, T>(list, |c, out| sort(c, true, out))?,
        VectorFn::Distinct => map_lists::<T, T>(list, distinct)?,
        VectorFn::CumSum if T::FLOATING => {
            map_lists::<T, f64>(list, |c, out| floating_cumulate(c, out, |a, b| a + b))?
        }
        VectorFn::CumSum => {
            map_lists::<T, i64>(list, |c, out| integer_cumulate(c, out, i64::wrapping_add))?
        }
        VectorFn::CumProd if T::FLOATING => {
            map_lists::<T, f64>(list, |c, out| floating_cumulate(c, out, |a, b| a * b))?
        }
        VectorFn::CumProd => {
            map_lists::<T, i64>(list, |c, out| integer_cumulate(c, out, i64::wrapping_mul))?
        }
        VectorFn::CumMin => map_lists::<T, T>(list, |c, out| running(c, out, T::least))?,
        VectorFn::CumMax => map_lists::<T, T>(list, |c, out| running(c, out, T::greatest))?,
        VectorFn::AbsAvg => reduce::<T, f64>(list, abs_avg),
        VectorFn::Ste => reduce::<T, f64>(list, ste),
        // Behavior: `Numeric#tstat`: `avg` over `ste`, null when either is.
        VectorFn::TStat => reduce::<T, f64>(list, |c| Some(avg(c)? / ste(c)?)),
        // Behavior: `Numeric#countPos`, `#countNeg` and `#countZero`: the elements that are
        // not null and above, below or equal to zero, which `-0.0` is and NaN is not.
        VectorFn::CountPos => reduce::<T, i64>(list, |c| count_where(c, |e| e > T::default())),
        VectorFn::CountNeg => reduce::<T, i64>(list, |c| count_where(c, |e| e < T::default())),
        VectorFn::CountZero => reduce::<T, i64>(list, |c| count_where(c, |e| e == T::default())),
        VectorFn::IndexOfMax => reduce::<T, i64>(list, |c| index_of_extreme(c, true)),
        VectorFn::IndexOfMin => reduce::<T, i64>(list, |c| index_of_extreme(c, false)),
        other => unreachable!("{other:?} does not depend on the element type"),
    })
}

/// Evaluates `$body` with `$t` naming the Rust type of the elements of a list.
macro_rules! in_element_type {
    ($list:expr, |$t:ident| $body:expr) => {
        match $list.values().data_type() {
            DataType::Int8 => {
                type $t = i8;
                $body
            }
            DataType::Int16 => {
                type $t = i16;
                $body
            }
            DataType::UInt16 => {
                type $t = u16;
                $body
            }
            DataType::Int32 => {
                type $t = i32;
                $body
            }
            DataType::Int64 => {
                type $t = i64;
                $body
            }
            DataType::Float32 => {
                type $t = f32;
                $body
            }
            DataType::Float64 => {
                type $t = f64;
                $body
            }
            other => unreachable!("the type checker passed a list of {other} as numbers"),
        }
    };
}

fn null_receiver(what: &str) -> TableFailure {
    failure(format!("java.lang.NullPointerException: {what} of null"))
}

/// The elements of the child of `list` at `positions`, as the elements of new entries of
/// the given lengths. A `None` position is a null element.
fn gather(
    list: &ListArray,
    positions: Vec<Option<u64>>,
    lengths: impl Iterator<Item = Option<usize>>,
) -> Result<ArrayRef, TableFailure> {
    let positions = UInt64Array::from(positions);
    let elements =
        arrow::compute::take(list.values().as_ref(), &positions, None).map_err(arrow_failure)?;
    lists(elements, lengths)
}

/// One element of each entry, `None` for null.
fn pick(
    list: &ListArray,
    position: impl Iterator<Item = Result<Option<u64>, TableFailure>>,
) -> Result<ArrayRef, TableFailure> {
    let positions = position.collect::<Result<UInt64Array, TableFailure>>()?;
    arrow::compute::take(list.values().as_ref(), &positions, None).map_err(arrow_failure)
}

fn long_at(array: &ArrayRef, row: usize) -> Option<i64> {
    let values: &Int64Array = array.as_primitive();
    values.is_valid(row).then(|| values.value(row))
}

/// The same entries with the child of `list` under the unnamed field of
/// `ColumnType::arrow_type`.
fn relabel(list: &ListArray) -> ArrayRef {
    let field = Arc::new(Field::new("", list.values().data_type().clone(), true));
    Arc::new(ListArray::new(
        field,
        list.offsets().clone(),
        list.values().clone(),
        list.nulls().cloned(),
    ))
}

/// Evaluates a function of a vector or array: `arguments[0]` is the list column and the
/// rest are the `long` (for [`VectorFn::ArrayGet`], `int`) index arguments. `percentile`
/// and `diff` have the list second, and `wsum`, `wavg`, `cov` and `cor` take two lists.
pub(super) fn evaluate(
    function: VectorFn,
    arguments: &[ArrayRef],
) -> Result<ArrayRef, TableFailure> {
    match function {
        VectorFn::Percentile => {
            let list = arguments[1].as_list::<i32>();
            return in_element_type!(list, |T| percentile::<T>(arguments[0].as_primitive(), list));
        }
        VectorFn::Diff | VectorFn::ArrayDiff => {
            let list = arguments[1].as_list::<i32>();
            let array = function == VectorFn::ArrayDiff;
            return in_element_type!(list, |T| diff::<T>(
                arguments[0].as_primitive(),
                list,
                array
            ));
        }
        VectorFn::WSum | VectorFn::WAvg | VectorFn::Cov | VectorFn::Cor => {
            let first = arguments[0].as_list::<i32>();
            let second = arguments[1].as_list::<i32>();
            return in_element_type!(first, |A| in_element_type!(second, |B| of_two::<A, B>(
                function, first, second
            )));
        }
        _ => {}
    }
    let list = arguments[0].as_list::<i32>();
    match function {
        // Behavior: `Basic#len(LongSizedDataStructure)` and `#len(int[])`: null for null.
        VectorFn::Len => Ok(Arc::new(
            ranges(list)
                .map(|range| range.map(|(start, end)| (end - start) as i64))
                .collect::<Int64Array>(),
        )),
        // Behavior: `Vector#size()`, called on the cell: a null cell throws.
        VectorFn::Size => ranges(list)
            .map(|range| match range {
                Some((start, end)) => Ok(Some((end - start) as i64)),
                None => Err(null_receiver("size()")),
            })
            .collect::<Result<Int64Array, TableFailure>>()
            .map(|sizes| Arc::new(sizes) as ArrayRef),
        // Behavior: `Vector#isEmpty()`.
        VectorFn::IsEmpty => ranges(list)
            .map(|range| match range {
                Some((start, end)) => Ok(Some(start == end)),
                None => Err(null_receiver("isEmpty()")),
            })
            .collect::<Result<BooleanArray, TableFailure>>()
            .map(|empty| Arc::new(empty) as ArrayRef),
        // Behavior: the `length` of a Java array.
        VectorFn::ArrayLength => ranges(list)
            .map(|range| match range {
                Some((start, end)) => Ok(Some((end - start) as i32)),
                None => Err(null_receiver("length")),
            })
            .collect::<Result<Int32Array, TableFailure>>()
            .map(|lengths| Arc::new(lengths) as ArrayRef),
        // Behavior: `Basic#count(IntVector)` and its relatives, and
        // `Basic#countObj(ObjectVector)`: the elements that are not null; null for null.
        VectorFn::Count => {
            let nulls = list.values().nulls();
            Ok(Arc::new(
                ranges(list)
                    .map(|range| {
                        range.map(|(start, end)| {
                            nulls.map_or(end - start, |nulls| {
                                (start..end).filter(|at| nulls.is_valid(*at)).count()
                            }) as i64
                        })
                    })
                    .collect::<Int64Array>(),
            ))
        }
        // Behavior: `Basic#first(IntVector)`, `#last(IntVector)`, `#firstObj` and `#lastObj`:
        // the element, null or not; null for a null or empty vector.
        VectorFn::First | VectorFn::Last => pick(
            list,
            ranges(list).map(|range| {
                Ok(range
                    .filter(|(start, end)| start < end)
                    .map(|(start, end)| {
                        if function == VectorFn::First {
                            start as u64
                        } else {
                            end as u64 - 1
                        }
                    }))
            }),
        ),
        // Behavior: `IntVectorColumnWrapper#get(long)`, `IntVectorDirect#get(long)` and
        // their relatives: null for an index outside the vector, and a null cell throws.
        // The index is a `long`, so a null `int` index is a negative number.
        VectorFn::Get => pick(
            list,
            ranges(list).enumerate().map(|(row, range)| {
                let (start, end) = range.ok_or_else(|| null_receiver("get()"))?;
                Ok(long_at(&arguments[1], row)
                    .filter(|index| *index >= 0 && (*index as u64) < (end - start) as u64)
                    .map(|index| start as u64 + index as u64))
            }),
        ),
        // Behavior: Java's array access, which `QueryLanguageParser#visit(ArrayAccessExpr)`
        // prints as it is: a null array and an index outside the array throw. A null `int`
        // index is `Integer.MIN_VALUE`, which is outside.
        VectorFn::ArrayGet => {
            let indexes: &Int32Array = arguments[1].as_primitive();
            pick(
                list,
                ranges(list).enumerate().map(|(row, range)| {
                    let (start, end) = range.ok_or_else(|| null_receiver("an index"))?;
                    let index = if indexes.is_valid(row) {
                        indexes.value(row)
                    } else {
                        i32::MIN
                    };
                    if index < 0 || index as usize >= end - start {
                        return Err(failure(format!(
                            "java.lang.ArrayIndexOutOfBoundsException: Index {index} out of \
                             bounds for length {}",
                            end - start
                        )));
                    }
                    Ok(Some((start + index as usize) as u64))
                }),
            )
        }
        VectorFn::SubVector => sub_vector(list, &arguments[1], &arguments[2]),
        // Behavior: `Basic#reverse(IntVector)` and its relatives: null for null.
        VectorFn::Reverse => {
            let positions = ranges(list)
                .flatten()
                .flat_map(|(start, end)| (start..end).rev().map(|at| Some(at as u64)))
                .collect();
            gather(
                list,
                positions,
                ranges(list).map(|range| range.map(|(start, end)| end - start)),
            )
        }
        // Behavior: `Basic#array(IntVector)` and its relatives: `toArray()`, null for null.
        VectorFn::Array => Ok(relabel(list)),
        // Behavior: `Basic#vec(int...)`, whose `IntVectorDirect` requires an array that is
        // not null.
        VectorFn::Vec => {
            if list.null_count() > 0 {
                return Err(failure(
                    "io.deephaven.base.verify.RequirementFailure: vec() of a null array",
                ));
            }
            Ok(relabel(list))
        }
        _ => in_element_type!(list, |T| typed::<T>(function, list)),
    }
}

/// Behavior: `IntVectorColumnWrapper#subVector(long, long)`, `IntVectorDirect#subVector`
/// with `IntVectorSlice`, and their relatives, for `from <= to`: a vector of `to - from`
/// elements, those at the positions `from..to` of the cell, null where a position is
/// outside it. A null cell throws.
fn sub_vector(list: &ListArray, from: &ArrayRef, to: &ArrayRef) -> Result<ArrayRef, TableFailure> {
    let mut positions = Vec::new();
    let mut lengths = Vec::with_capacity(list.len());
    for (row, range) in ranges(list).enumerate() {
        let (start, end) = range.ok_or_else(|| null_receiver("subVector()"))?;
        let (Some(from), Some(to)) = (long_at(from, row), long_at(to, row)) else {
            // Java reads a null `int` or `long` index as its reserved value, a number.
            unported!(
                "io.deephaven.vector.IntVector#subVector",
                "a null index, which Java takes for the lowest int or long"
            );
        };
        if from > to {
            // `IntVectorSlice` refuses a negative length; the column wrapper does not.
            unported!(
                "io.deephaven.engine.table.vectors.IntVectorColumnWrapper#subVector",
                "from {from} beyond to {to}"
            );
        }
        let length = usize::try_from(to - from)
            .ok()
            .filter(|length| i32::try_from(positions.len() + length).is_ok())
            .ok_or_else(|| failure("a subVector of more than 2^31 elements"))?;
        let size = (end - start) as i64;
        positions.extend((from..to).map(|position| {
            (0..size)
                .contains(&position)
                .then(|| start as u64 + position as u64)
        }));
        lengths.push(Some(length));
    }
    gather(list, positions, lengths.into_iter())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ulp_is_the_distance_to_the_next_double() {
        assert_eq!(ulp(1.0), f64::EPSILON);
        assert_eq!(ulp(-1.0), f64::EPSILON);
        assert_eq!(ulp(0.0), f64::from_bits(1));
        assert_eq!(ulp(f64::MAX), 2.0_f64.powi(971));
    }

    #[test]
    fn floats_order_as_arrays_sort_orders_them() {
        let mut values = [
            f64::NAN,
            1.0,
            0.0,
            -0.0,
            f64::NEG_INFINITY,
            -f64::NAN,
            f64::INFINITY,
        ];
        values.sort_unstable_by(Element::order);
        assert_eq!(values[0], f64::NEG_INFINITY);
        assert!(values[1] == 0.0 && values[1].is_sign_negative());
        assert!(values[2] == 0.0 && values[2].is_sign_positive());
        assert_eq!(values[3..5], [1.0, f64::INFINITY]);
        assert!(values[5].is_nan() && values[6].is_nan());
    }

    #[test]
    fn half_sums_wrap_in_the_type_java_adds_in() {
        assert_eq!(i32::MAX.half_sum(1), -1_073_741_824.0);
        assert_eq!(i8::MAX.half_sum(1), 64.0);
        assert_eq!(60_000_u16.half_sum(60_000), 60_000.0);
        assert_eq!(i64::MAX.half_sum(1), -4_611_686_018_427_387_904.0);
        assert_eq!(f32::MAX.half_sum(f32::MAX), f64::INFINITY);
    }
}
