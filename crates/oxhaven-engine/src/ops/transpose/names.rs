//! The column names of a transposed table: the text of a column-by value, and what Java
//! makes of it so that it is a column name.

use arrow::array::{Array, ArrayRef, AsArray};
use arrow::datatypes::{Int16Type, Int32Type, Int64Type, Int8Type, UInt16Type};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::unported;

use crate::ops::filter::java_double_to_string;

/// The words `javax.lang.model.SourceVersion#isKeyword` takes for keywords and literals.
const JAVA_KEYWORDS: &[&str] = &[
    "_",
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
    "false",
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
    "null",
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
    "true",
    "try",
    "void",
    "volatile",
    "while",
];

/// The variables the query language keeps for itself.
const QUERY_RESERVED: &[&str] = &["in", "not", "i", "ii", "k"];

/// Behavior: io.deephaven.api.util.NameValidator#isReserved
fn is_reserved(name: &str) -> bool {
    QUERY_RESERVED.contains(&name) || JAVA_KEYWORDS.contains(&name)
}

/// `names` as column names, in order. A reserved word gets the prefix `column_`; every
/// character that is not an ASCII letter, a digit, `_` or `$` is dropped; a name that then
/// starts with a digit gets the prefix too; and a name an earlier one already has gets the
/// first number from 2 on that makes it new. A name with nothing left, or left with a
/// reserved word, is refused with Java's text.
///
/// Behavior: io.deephaven.api.util.NameValidator#legalizeColumnNames(String[], boolean)
/// resolving conflicts, over #legalizeName
pub(super) fn legalize(names: &[String]) -> Result<Vec<String>, String> {
    let mut taken: Vec<String> = Vec::with_capacity(names.len());
    for original in names {
        let error = || format!("Can not legalize column name {original}");
        let mut name = original.clone();
        if is_reserved(&name) {
            name = format!("column_{name}");
        }
        name.retain(|c| c.is_ascii_alphanumeric() || c == '_' || c == '$');
        if name.is_empty() {
            return Err(error());
        }
        if name.starts_with(|c: char| c.is_ascii_digit()) {
            name = format!("column_{name}");
        }
        let mut unique = name.clone();
        let mut next = 2;
        while taken.contains(&unique) {
            unique = format!("{name}{next}");
            next += 1;
        }
        // What is left is ASCII and does not start with a digit, so it is a column name
        // unless dropping characters made it a reserved word.
        if is_reserved(&name) {
            return Err(error());
        }
        taken.push(unique);
    }
    Ok(taken)
}

/// The text of each value of a column-by column, as Java's `String.valueOf` of the boxed
/// value gives it; `null` for a null.
///
/// Not ported: the text of a `float` (`java.lang.Float#toString`) and of an `Instant`
/// (`java.time.Instant#toString`), and that of a vector or an array.
///
/// Behavior: the `String.valueOf(s.get(rowKey))` of
/// io.deephaven.engine.table.impl.util.KeyedTranspose#keyedTranspose
pub(super) fn value_texts(column_type: &ColumnType, values: &ArrayRef) -> Vec<String> {
    fn each<T>(values: &ArrayRef, text: impl Fn(usize) -> T) -> Vec<String>
    where
        T: Into<String>,
    {
        (0..values.len())
            .map(|row| {
                if values.is_null(row) {
                    "null".to_string()
                } else {
                    text(row).into()
                }
            })
            .collect()
    }
    match column_type {
        ColumnType::String => {
            let strings = values.as_string::<i32>();
            each(values, |row| strings.value(row))
        }
        ColumnType::Boolean => {
            let flags = values.as_boolean();
            each(values, |row| flags.value(row).to_string())
        }
        ColumnType::Byte => {
            let numbers = values.as_primitive::<Int8Type>();
            each(values, |row| numbers.value(row).to_string())
        }
        ColumnType::Short => {
            let numbers = values.as_primitive::<Int16Type>();
            each(values, |row| numbers.value(row).to_string())
        }
        ColumnType::Int => {
            let numbers = values.as_primitive::<Int32Type>();
            each(values, |row| numbers.value(row).to_string())
        }
        ColumnType::Long => {
            let numbers = values.as_primitive::<Int64Type>();
            each(values, |row| numbers.value(row).to_string())
        }
        ColumnType::Char => {
            let units = values.as_primitive::<UInt16Type>();
            // A lone surrogate has no text here; it is no character of a column name
            // either, so what takes its place is dropped like it.
            each(values, |row| String::from_utf16_lossy(&[units.value(row)]))
        }
        ColumnType::Double => {
            let numbers = values.as_primitive::<arrow::datatypes::Float64Type>();
            each(values, |row| java_double_to_string(numbers.value(row)))
        }
        other => unported!(
            "io.deephaven.engine.table.impl.util.KeyedTranspose#keyedTranspose",
            "a column-by column of type {other:?}, whose values Java names columns after \
             with String#valueOf"
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn legal(names: &[&str]) -> Result<Vec<String>, String> {
        legalize(
            &names
                .iter()
                .map(|name| name.to_string())
                .collect::<Vec<_>>(),
        )
    }

    /// Observed: the columns of `keyed_transpose` by a column with these values.
    #[test]
    fn names_are_made_legal_and_unique_as_java_makes_them() {
        assert_eq!(
            legal(&["1-2.3/4", "12", "a b", "ab", "null", "int", "a-b", "null"]).unwrap(),
            [
                "column_1234",
                "column_12",
                "ab",
                "ab2",
                "column_null",
                "column_int",
                "ab3",
                "column_null2"
            ]
        );
        assert_eq!(
            legal(&["0.0", "1.5", "2024-01-01T00:00:01Z", "true", "A"]).unwrap(),
            [
                "column_00",
                "column_15",
                "column_20240101T000001Z",
                "column_true",
                "A"
            ]
        );
    }

    #[test]
    fn a_name_with_nothing_left_or_left_reserved_is_refused() {
        assert_eq!(legal(&[""]).unwrap_err(), "Can not legalize column name ");
        assert_eq!(legal(&["-"]).unwrap_err(), "Can not legalize column name -");
        assert_eq!(
            legal(&["i-n-t"]).unwrap_err(),
            "Can not legalize column name i-n-t"
        );
    }
}
