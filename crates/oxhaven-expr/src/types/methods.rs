//! The instance methods of `java.lang.String` that formulas may call.
//!
//! Java lets a formula call any public method of any class. Oxhaven accepts the `String`
//! methods that Deephaven's documentation and tests use in formulas and whose result does
//! not depend on a locale table or a regular expression engine; every other method is
//! reported as unsupported, by name.

use oxhaven_contracts::schema::ColumnType;

#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum StringMethod {
    Length,
    IsEmpty,
    CharAt,
    Contains,
    StartsWith,
    EndsWith,
    IndexOf,
    Substring,
    ToUpperCase,
    ToLowerCase,
    Trim,
    Equals,
    CompareTo,
    Concat,
}

/// What a method parameter accepts.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Parameter {
    /// A Java `int`: also takes a `byte`, `short` or `char`, converted as `intCast` does.
    Int,
    /// A `String`, or for `contains` a `CharSequence`.
    String,
}

/// Whether `java.lang.String` (JDK 21) has a public method of this name: its own, static
/// or not, or inherited from `Object`.
pub(crate) fn is_string_method(name: &str) -> bool {
    const NAMES: [&str; 59] = [
        "charAt",
        "chars",
        "codePointAt",
        "codePointBefore",
        "codePointCount",
        "codePoints",
        "compareTo",
        "compareToIgnoreCase",
        "concat",
        "contains",
        "contentEquals",
        "describeConstable",
        "endsWith",
        "equals",
        "equalsIgnoreCase",
        "formatted",
        "getBytes",
        "getChars",
        "getClass",
        "hashCode",
        "indent",
        "indexOf",
        "intern",
        "isBlank",
        "isEmpty",
        "lastIndexOf",
        "length",
        "lines",
        "matches",
        "notify",
        "notifyAll",
        "offsetByCodePoints",
        "regionMatches",
        "repeat",
        "replace",
        "replaceAll",
        "replaceFirst",
        "resolveConstantDesc",
        "split",
        "splitWithDelimiters",
        "startsWith",
        "strip",
        "stripIndent",
        "stripLeading",
        "stripTrailing",
        "subSequence",
        "substring",
        "toCharArray",
        "toLowerCase",
        "toString",
        "toUpperCase",
        "transform",
        "translateEscapes",
        "trim",
        "wait",
        "valueOf",
        "copyValueOf",
        "format",
        "join",
    ];
    NAMES.contains(&name)
}

impl StringMethod {
    pub(crate) fn named(name: &str) -> Option<StringMethod> {
        Some(match name {
            "length" => StringMethod::Length,
            "isEmpty" => StringMethod::IsEmpty,
            "charAt" => StringMethod::CharAt,
            "contains" => StringMethod::Contains,
            "startsWith" => StringMethod::StartsWith,
            "endsWith" => StringMethod::EndsWith,
            "indexOf" => StringMethod::IndexOf,
            "substring" => StringMethod::Substring,
            "toUpperCase" => StringMethod::ToUpperCase,
            "toLowerCase" => StringMethod::ToLowerCase,
            "trim" => StringMethod::Trim,
            "equals" => StringMethod::Equals,
            "compareTo" => StringMethod::CompareTo,
            "concat" => StringMethod::Concat,
            _ => return None,
        })
    }

    /// The accepted parameter lists. Overloads of `java.lang.String` that are missing here
    /// (`indexOf(int)`, `startsWith(String, int)`, `toUpperCase(Locale)` and so on) are
    /// unsupported.
    pub(crate) fn signatures(self) -> &'static [&'static [Parameter]] {
        use Parameter::{Int, String};
        match self {
            StringMethod::Length
            | StringMethod::IsEmpty
            | StringMethod::ToUpperCase
            | StringMethod::ToLowerCase
            | StringMethod::Trim => &[&[]],
            StringMethod::CharAt => &[&[Int]],
            StringMethod::Substring => &[&[Int], &[Int, Int]],
            StringMethod::Contains
            | StringMethod::StartsWith
            | StringMethod::EndsWith
            | StringMethod::IndexOf
            | StringMethod::Equals
            | StringMethod::CompareTo
            | StringMethod::Concat => &[&[String]],
        }
    }

    /// Whether Java has another overload of the method, with as many parameters, that
    /// takes a value that is not a `String`: `equals(Object)` and `indexOf(int)`.
    pub(crate) fn has_other_overloads(self) -> bool {
        matches!(self, StringMethod::Equals | StringMethod::IndexOf)
    }

    /// The numbers of parameters of the overloads `java.lang.String` has beyond
    /// [`StringMethod::signatures`]: `indexOf(String, int)` and `indexOf(String, int, int)`,
    /// `startsWith(String, int)`, `toUpperCase(Locale)` and `toLowerCase(Locale)`.
    pub(crate) fn other_arities(self) -> &'static [usize] {
        match self {
            StringMethod::IndexOf => &[2, 3],
            StringMethod::StartsWith => &[2],
            StringMethod::ToUpperCase | StringMethod::ToLowerCase => &[1],
            _ => &[],
        }
    }

    /// The declared return type in `java.lang.String`.
    pub(crate) fn result_type(self) -> ColumnType {
        match self {
            StringMethod::Length | StringMethod::IndexOf | StringMethod::CompareTo => {
                ColumnType::Int
            }
            StringMethod::IsEmpty
            | StringMethod::Contains
            | StringMethod::StartsWith
            | StringMethod::EndsWith
            | StringMethod::Equals => ColumnType::Boolean,
            StringMethod::CharAt => ColumnType::Char,
            StringMethod::Substring
            | StringMethod::ToUpperCase
            | StringMethod::ToLowerCase
            | StringMethod::Trim
            | StringMethod::Concat => ColumnType::String,
        }
    }
}
