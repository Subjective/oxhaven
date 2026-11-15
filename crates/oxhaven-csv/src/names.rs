//! Column names: what Java makes of the headers of a file.

use crate::CsvError;

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

/// Behavior: `io.deephaven.api.util.NameValidator#isReserved`
fn is_reserved(name: &str) -> bool {
    QUERY_RESERVED.contains(&name) || JAVA_KEYWORDS.contains(&name)
}

/// The column names of a table read from a file with these headers, in order.
///
/// A hyphen or a space becomes an underscore; a reserved word gets the prefix `column_`;
/// every character that is not an ASCII letter, a digit, `_` or `$` is dropped; a name
/// that then starts with a digit gets the prefix too; and a name already taken by an
/// earlier column gets the first number from 2 on that makes it new. A header with
/// nothing left, or left with a reserved word, is an error.
///
/// Behavior: `io.deephaven.csv.ColumnNameLegalizer#apply`, which is
/// `io.deephaven.api.util.NameValidator#legalizeColumnNames` resolving conflicts
pub(crate) fn legalize(headers: &[String]) -> Result<Vec<String>, CsvError> {
    let mut taken: Vec<String> = Vec::with_capacity(headers.len());
    for header in headers {
        let error = || {
            CsvError::Invalid(format!(
                "io.deephaven.api.util.NameValidator$LegalizeNameException: Can not legalize \
                 column name {header}"
            ))
        };
        let mut name = header.replace(['-', ' '], "_");
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
        // what is left is ASCII, so it is a column name unless it is a reserved word
        if is_reserved(&name) {
            return Err(error());
        }
        taken.push(unique);
    }
    Ok(taken)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn legal(headers: &[&str]) -> Result<Vec<String>, CsvError> {
        legalize(&headers.iter().map(|h| h.to_string()).collect::<Vec<_>>())
    }

    /// The names the reference gives these headers (42.9).
    #[test]
    fn headers_become_the_names_java_gives_them() {
        let headers = [
            "First Name",
            "last-name",
            "2nd",
            "int",
            "i",
            "a.b",
            "A B",
            "A_B",
            "ünï",
            "$x",
            "_",
            "true",
            "var",
            "strictfp",
            "x y-z",
            "1",
            "_1",
            "\u{feff}Bom",
        ];
        let names = [
            "First_Name",
            "last_name",
            "column_2nd",
            "column_int",
            "column_i",
            "ab",
            "A_B",
            "A_B2",
            "n",
            "$x",
            "column__",
            "column_true",
            "var",
            "column_strictfp",
            "x_y_z",
            "column_1",
            "_1",
            "Bom",
        ];
        assert_eq!(legal(&headers).unwrap(), names);
    }

    #[test]
    fn a_taken_name_gets_the_next_free_number() {
        let names = legal(&["A", "A", "B", "B2", "B"]).unwrap();
        assert_eq!(names, ["A", "A2", "B", "B2", "B3"]);
        assert_eq!(legal(&["A2", "A", "A"]).unwrap(), ["A2", "A", "A3"]);
        assert_eq!(legal(&["A", "A", "A2"]).unwrap(), ["A", "A2", "A22"]);
        let names = legal(&["1", "1", "column_1"]).unwrap();
        assert_eq!(names, ["column_1", "column_12", "column_13"]);
    }

    #[test]
    fn a_header_that_leaves_no_name_is_an_error() {
        for headers in [&["A", ""][..], &["!!"], &["ü"], &["i!"]] {
            let error = legal(headers).unwrap_err().to_string();
            assert!(error.contains("Can not legalize column name"), "{error}");
        }
    }
}
