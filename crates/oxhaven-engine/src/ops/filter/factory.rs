//! From a filter string to a filter: which strings become match, range and pattern filters
//! and which become formulas.
//!
//! The choice changes results, because the three kinds compare differently; see `PARITY.md`.

use super::filters::{FilterSpec, MatchOptions, PatternSpec, RangeOp};
use super::literal::{convert_string_literal, java_trim};
use oxhaven_contracts::unported;
use regex::Regex;
use std::sync::LazyLock;

// Java's `\s`, `\S`, `\d` and `.` (without DOTALL) are narrower than the regex crate's.
const S: &str = r"[ \t\n\x0B\f\r]";
const NOT_S: &str = r"[^ \t\n\x0B\f\r]";
const DOT: &str = r"[^\n\r\x{85}\x{2028}\x{2029}]";
/// `\s*\Z`. Java's `\Z` also matches before a final line terminator, but the patterns are
/// used with `Matcher#matches`, which needs the whole input consumed, so only the end is left.
const END: &str = r"[ \t\n\x0B\f\r]*\z";
const ID: &str = r"[a-zA-Z_$][a-zA-Z0-9_$]*";
const STR: &str = r#"(?:"[^"]*")|(?:`[^`]*`)"#;

fn literal() -> String {
    let int = r"-?[0-9]+";
    let flt = r"[+-]?[0-9]*\.[0-9]+[f]?|[+-]?[0-9]+\.[0-9]*[f]?";
    let boolean = "[tT][rR][uU][eE]|[fF][aA][lL][sS][eE]";
    let character = format!("'{DOT}'");
    let datetime = "'[^']*'";
    format!("(?:{int})|(?:{flt})|(?:{boolean})|(?:{character})|(?:{STR})|(?:{datetime})")
}

fn compiled(pattern: String) -> Regex {
    match Regex::new(&pattern) {
        Ok(regex) => regex,
        Err(e) => panic!("a filter pattern does not compile: {e}"),
    }
}

/// `<column or literal> <op> <column or literal>`
static COMPARISON: LazyLock<Regex> = LazyLock::new(|| {
    let literal = literal();
    compiled(format!(
        r"\A{S}*(?:(?P<lid>{ID})|(?P<llit>{literal})){S}*(?P<op>(?:=|!|<|>)=?){S}*(?:(?P<rid>{ID})|(?P<rlit>{literal})){END}"
    ))
});

/// `<column> [icase] [not] in <value>, <value>, ...`
static IN: LazyLock<Regex> = LazyLock::new(|| {
    compiled(format!(
        r"(?s)\A{S}*(?P<col>{ID}){S}+(?P<icase>[iI][cC][aA][sS][eE]{S}+)?(?P<not>[nN][oO][tT]{S}+)?[iI][nN]{S}+(?P<values>.+?){END}"
    ))
});

/// `<column> [icase] [not] includes [any|all] <"string">, ...`
static INCLUDES: LazyLock<Regex> = LazyLock::new(|| {
    compiled(format!(
        r"\A{S}*(?P<col>{ID}){S}+(?P<icase>[iI][cC][aA][sS][eE]{S}+)?(?P<not>[nN][oO][tT]{S}+)?[iI][nN][cC][lL][uU][dD][eE][sS](?:{S}+(?P<mode>[aA][nN][yY]|[aA][lL][lL]){S}+)?{S}*(?P<values>(?:(?:{STR})(?:,{S}*)?)+){END}"
    ))
});

/// Anything else on one line.
static ANYTHING: LazyLock<Regex> =
    LazyLock::new(|| compiled(format!(r"\A{S}*(?P<body>{DOT}*{NOT_S}+){END}")));

/// Splits at commas that are outside double quotes and back ticks. Each piece is trimmed
/// and empty pieces are dropped.
///
/// Behavior: io.deephaven.util.text.SplitIgnoreQuotes#split
fn split_ignore_quotes(text: &str) -> Result<Vec<String>, String> {
    #[derive(PartialEq)]
    enum Mode {
        Normal,
        Quotes,
        BackTicks,
    }
    let mut mode = Mode::Normal;
    let mut values = Vec::new();
    let mut current = String::new();
    let mut finish = |current: &mut String| {
        let value = java_trim(current);
        if !value.is_empty() {
            values.push(value.to_string());
        }
        current.clear();
    };
    for c in text.chars() {
        match c {
            '"' => {
                mode = match mode {
                    Mode::Normal => Mode::Quotes,
                    Mode::Quotes => Mode::Normal,
                    Mode::BackTicks => Mode::BackTicks,
                };
                current.push(c);
            }
            '`' => {
                mode = match mode {
                    Mode::Normal => Mode::BackTicks,
                    Mode::BackTicks => Mode::Normal,
                    Mode::Quotes => Mode::Quotes,
                };
                current.push(c);
            }
            ',' if mode == Mode::Normal => finish(&mut current),
            _ => current.push(c),
        }
    }
    match mode {
        Mode::Quotes => Err(format!("Unmatched quote in expression {text}")),
        Mode::BackTicks => Err(format!("Unmatched back tick in expression {text}")),
        Mode::Normal => {
            finish(&mut current);
            Ok(values)
        }
    }
}

/// Behavior: the first factory registered in io.deephaven.engine.table.impl.select.WhereFilterFactory
/// and WhereFilterFactory#getWhereFilterOneSideColumn
fn comparison(text: &str) -> Option<Result<FilterSpec, String>> {
    let found = COMPARISON.captures(text)?;
    let group = |name: &str| found.name(name).map(|m| m.as_str());
    let (left, right) = (group("lid"), group("rid"));
    let formula = || FilterSpec::Condition(text.to_string());
    let (column, value, mirrored) = match (left, right) {
        (None, None) => return Some(Ok(formula())),
        (Some(column), Some(value)) => (column, value, false),
        (Some(column), None) => (column, group("rlit")?, false),
        (None, Some(column)) => (column, group("llit")?, true),
    };
    if matches!(column, "i" | "ii" | "k") {
        return Some(Ok(formula()));
    }
    let matching = |inverted| FilterSpec::Match {
        column: column.to_string(),
        values: vec![value.to_string()],
        options: MatchOptions {
            inverted,
            ignore_case: false,
            nan_match: false,
        },
        failover: Some(text.to_string()),
    };
    let range = |op: RangeOp| FilterSpec::Range {
        column: column.to_string(),
        op: if mirrored { op.mirror() } else { op },
        value: value.to_string(),
        failover: Some(text.to_string()),
    };
    Some(match group("op")? {
        "=" | "==" => Ok(matching(false)),
        "!=" => Ok(matching(true)),
        "<" => Ok(range(RangeOp::Less)),
        "<=" => Ok(range(RangeOp::LessOrEqual)),
        ">" => Ok(range(RangeOp::Greater)),
        ">=" => Ok(range(RangeOp::GreaterOrEqual)),
        op => Err(format!("Unexpected operator: {op}")),
    })
}

/// Behavior: the second factory registered in WhereFilterFactory
fn membership(text: &str) -> Option<Result<FilterSpec, String>> {
    let found = IN.captures(text)?;
    Some(
        split_ignore_quotes(&found["values"]).map(|values| FilterSpec::Match {
            column: found["col"].to_string(),
            values,
            // An explicit list of values matches NaN with NaN.
            options: MatchOptions {
                inverted: found.name("not").is_some(),
                ignore_case: found.name("icase").is_some(),
                nan_match: true,
            },
            failover: None,
        }),
    )
}

/// Behavior: the third factory registered in WhereFilterFactory,
/// WhereFilterFactory#stringContainsFilter, WhereFilterFactory#constructStringContainsRegex
fn includes(text: &str) -> Option<Result<FilterSpec, String>> {
    let found = INCLUDES.captures(text)?;
    let build = || -> Result<FilterSpec, String> {
        let values = split_ignore_quotes(&found["values"])?;
        let any = values.len() == 1
            || found
                .name("mode")
                .is_none_or(|mode| mode.as_str().eq_ignore_ascii_case("any"));
        let inverted = found.name("not").is_some();
        let needles = values
            .iter()
            .map(|value| convert_string_literal(value))
            .collect::<Result<Vec<String>, String>>()?;
        // Java searches for `a|b` when asked for any of the values, or for none of all of
        // them, and for `(?=.*a)(?=.*b).*` otherwise; an inverted filter then keeps the
        // values in which the search fails.
        let pattern = if inverted != any {
            PatternSpec::Any(needles)
        } else {
            PatternSpec::All(needles)
        };
        Ok(FilterSpec::Pattern {
            column: found["col"].to_string(),
            pattern,
            ignore_case: found.name("icase").is_some(),
            invert_pattern: inverted,
        })
    };
    Some(build())
}

/// The filter a filter string stands for.
///
/// The forms are tried in order: a comparison of a column with a column or a literal, `in`,
/// `includes`, and last a formula. A form whose text matches but which cannot be built is
/// skipped. A comparison keeps its text as a formula to fall back on when the column or
/// the value does not fit the table.
///
/// Behavior: io.deephaven.engine.table.impl.select.WhereFilterFactory#getExpression,
/// io.deephaven.api.expression.ExpressionParser#parse
pub(super) fn parse_filter(text: &str) -> Result<FilterSpec, String> {
    if text.starts_with("<ObfuscatedParserAnnotation>") {
        unported!(
            "io.deephaven.engine.table.impl.select.FormulaParserConfiguration#extractParserAndExpression"
        );
    }
    let mut first_problem = None;
    for form in [comparison, membership, includes] {
        match form(text) {
            Some(Ok(spec)) => return Ok(spec),
            Some(Err(problem)) => {
                first_problem.get_or_insert(problem);
            }
            None => {}
        }
    }
    if let Some(found) = ANYTHING.captures(text) {
        return Ok(FilterSpec::Condition(found["body"].to_string()));
    }
    Err(match first_problem {
        None => format!("Unable to parse expression: \"{text}\""),
        Some(problem) => format!("Failed to get expression for all matched patterns: {problem}"),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn matching(column: &str, values: &[&str], options: (bool, bool, bool)) -> FilterSpec {
        FilterSpec::Match {
            column: column.to_string(),
            values: values.iter().map(|v| v.to_string()).collect(),
            options: MatchOptions {
                inverted: options.0,
                ignore_case: options.1,
                nan_match: options.2,
            },
            failover: None,
        }
    }

    fn with_failover(spec: FilterSpec, text: &str) -> FilterSpec {
        match spec {
            FilterSpec::Match {
                column,
                values,
                options,
                ..
            } => FilterSpec::Match {
                column,
                values,
                options,
                failover: Some(text.to_string()),
            },
            other => other,
        }
    }

    fn range(column: &str, op: RangeOp, value: &str, text: &str) -> FilterSpec {
        FilterSpec::Range {
            column: column.to_string(),
            op,
            value: value.to_string(),
            failover: Some(text.to_string()),
        }
    }

    fn formula(text: &str) -> FilterSpec {
        FilterSpec::Condition(text.to_string())
    }

    #[test]
    fn equality_with_a_literal_is_a_match_filter_without_nan_matching() {
        for (text, inverted, value) in [
            ("A = 5", false, "5"),
            ("A == 5", false, "5"),
            ("A==-5", false, "-5"),
            ("  A != 5.5  ", true, "5.5"),
            ("A == `x y`", false, "`x y`"),
            ("A == \"x\"", false, "\"x\""),
            ("A != 'c'", true, "'c'"),
            (
                "A == '2024-01-01T00:00:00 UTC'",
                false,
                "'2024-01-01T00:00:00 UTC'",
            ),
            ("A == TRUE", false, "TRUE"),
            ("A == +.5f", false, "+.5f"),
            ("A == 5.", false, "5."),
            ("5 == A", false, "5"),
            ("A == B", false, "B"),
            ("A == null", false, "null"),
        ] {
            assert_eq!(
                parse_filter(text),
                Ok(with_failover(
                    matching("A", &[value], (inverted, false, false)),
                    text
                )),
                "{text}"
            );
        }
    }

    #[test]
    fn an_ordering_with_a_literal_is_a_range_filter_mirrored_when_the_column_is_on_the_right() {
        for (text, op, value) in [
            ("A < 5", RangeOp::Less, "5"),
            ("A <= 5", RangeOp::LessOrEqual, "5"),
            ("A > 5", RangeOp::Greater, "5"),
            ("A >= 5", RangeOp::GreaterOrEqual, "5"),
            ("5 < A", RangeOp::Greater, "5"),
            ("5 <= A", RangeOp::GreaterOrEqual, "5"),
            ("5 > A", RangeOp::Less, "5"),
            ("5 >= A", RangeOp::LessOrEqual, "5"),
            ("A > B", RangeOp::Greater, "B"),
            ("A>'x'", RangeOp::Greater, "'x'"),
        ] {
            assert_eq!(
                parse_filter(text),
                Ok(range("A", op, value, text)),
                "{text}"
            );
        }
    }

    #[test]
    fn other_comparisons_are_formulas() {
        for text in [
            "5 == 5",
            "i > 5",
            "ii == 5",
            "5 < k",
            "A ! 5",
            "A > 5 + 1",
            "A == 1e3",
            "A == 5L",
            "A == -B",
            "A.b == 5",
            "A === 5",
            "A > 5 && B < 3",
            "isNull(A)",
        ] {
            assert_eq!(parse_filter(text), Ok(formula(text)), "{text}");
        }
        // The formula loses the white space around it.
        assert_eq!(parse_filter("  A + 1 > 5 \n"), Ok(formula("A + 1 > 5")));
    }

    #[test]
    fn in_lists_are_match_filters_with_nan_matching() {
        assert_eq!(
            parse_filter("A in 1, 2, 3"),
            Ok(matching("A", &["1", "2", "3"], (false, false, true)))
        );
        assert_eq!(
            parse_filter("A not in 1"),
            Ok(matching("A", &["1"], (true, false, true)))
        );
        assert_eq!(
            parse_filter("A ICASE NOT IN `a`, \"b, c\" ,, d e "),
            Ok(matching(
                "A",
                &["`a`", "\"b, c\"", "d e"],
                (true, true, true)
            ))
        );
        assert_eq!(
            parse_filter("A in 1,\n2"),
            Ok(matching("A", &["1", "2"], (false, false, true)))
        );
        // Only white space after `in`: the list is empty.
        assert_eq!(
            parse_filter("A in   "),
            Ok(matching("A", &[], (false, false, true)))
        );
        // An unmatched quote is not a list; the text is then a formula.
        assert_eq!(parse_filter("A in \"x"), Ok(formula("A in \"x")));
        // `in` needs white space on both sides.
        assert_eq!(parse_filter("A in"), Ok(formula("A in")));
    }

    #[test]
    fn includes_is_a_pattern_filter() {
        let pattern = |pattern, ignore_case, invert_pattern| FilterSpec::Pattern {
            column: "S".to_string(),
            pattern,
            ignore_case,
            invert_pattern,
        };
        let strings = |values: &[&str]| values.iter().map(|v| v.to_string()).collect::<Vec<_>>();
        assert_eq!(
            parse_filter("S includes \"a\""),
            Ok(pattern(PatternSpec::Any(strings(&["a"])), false, false))
        );
        assert_eq!(
            parse_filter("S icase includes any \"a\", `b`"),
            Ok(pattern(PatternSpec::Any(strings(&["a", "b"])), true, false))
        );
        assert_eq!(
            parse_filter("S includes all \"a\",\"b\""),
            Ok(pattern(
                PatternSpec::All(strings(&["a", "b"])),
                false,
                false
            ))
        );
        // Java's inversion: `not includes any` keeps the values that lack at least one of
        // the strings, and `not includes all` those that have none of them.
        assert_eq!(
            parse_filter("S not includes any \"a\", \"b\""),
            Ok(pattern(PatternSpec::All(strings(&["a", "b"])), false, true))
        );
        assert_eq!(
            parse_filter("S not includes all \"a\", \"b\""),
            Ok(pattern(PatternSpec::Any(strings(&["a", "b"])), false, true))
        );
        // One value is always a search for that value.
        assert_eq!(
            parse_filter("S not includes all \"a\""),
            Ok(pattern(PatternSpec::All(strings(&["a"])), false, true))
        );
        // Without quotes it is a formula.
        assert_eq!(parse_filter("S includes a"), Ok(formula("S includes a")));
    }

    #[test]
    fn text_that_is_no_form_is_rejected() {
        assert_eq!(
            parse_filter(""),
            Err("Unable to parse expression: \"\"".to_string())
        );
        assert_eq!(
            parse_filter(" \t "),
            Err("Unable to parse expression: \" \t \"".to_string())
        );
        // A formula is one line.
        assert!(parse_filter("A > 5 &&\n B < 3").is_err());
        // The comparison form matched but has no such operator, and nothing else matched.
        assert_eq!(
            parse_filter("A !\n5"),
            Err(
                "Failed to get expression for all matched patterns: Unexpected operator: !"
                    .to_string()
            )
        );
    }
    fn unhex(text: &str) -> String {
        if text == "-" {
            return String::new();
        }
        let bytes = (0..text.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&text[at..at + 2], 16).unwrap())
            .collect();
        String::from_utf8(bytes).unwrap()
    }

    /// The result of `parse_filter` in the notation of `java_routing.txt`.
    fn described(result: Result<FilterSpec, String>) -> String {
        let regular = |inverted: bool| if inverted { "inverted" } else { "regular" };
        let case = |ignore: bool| if ignore { "icase" } else { "case" };
        match result {
            Ok(FilterSpec::Match {
                column,
                values,
                options,
                failover: Some(_),
            }) => format!(
                "M|{column}|{}|failover|{}",
                regular(options.inverted),
                values.join("\u{1f}")
            ),
            Ok(FilterSpec::Match {
                column,
                values,
                options,
                failover: None,
            }) => format!(
                "I|{column}|{}|{}|{}",
                regular(options.inverted),
                case(options.ignore_case),
                values.join("\u{1f}")
            ),
            Ok(FilterSpec::Range {
                column, op, value, ..
            }) => {
                let op = match op {
                    RangeOp::Less => "<",
                    RangeOp::LessOrEqual => "<=",
                    RangeOp::Greater => ">",
                    RangeOp::GreaterOrEqual => ">=",
                };
                format!("R|{column}|{op}|{value}")
            }
            Ok(FilterSpec::Pattern {
                column,
                pattern,
                ignore_case,
                invert_pattern,
            }) => {
                let (kind, needles) = match pattern {
                    PatternSpec::Any(needles) => ("any", needles),
                    PatternSpec::All(needles) => ("all", needles),
                    PatternSpec::Regex(regex) => ("regex", vec![regex]),
                };
                format!(
                    "P|{column}|{kind}|{}|{}|{}",
                    case(ignore_case),
                    regular(invert_pattern),
                    needles.join("\u{1f}")
                )
            }
            Ok(FilterSpec::Condition(text)) => format!("C|{text}"),
            Ok(other) => format!("unexpected {other:?}"),
            Err(problem) if problem.starts_with("Unable to parse") => "E|unable".to_string(),
            Err(_) => "E|failed".to_string(),
        }
    }

    /// `java_routing.txt` holds filter strings and what the patterns of `WhereFilterFactory`,
    /// run by a JDK 21 `java.util.regex`, make of them: which form matched and its parts.
    /// Both fields are UTF-8 in hexadecimal.
    #[test]
    fn strings_are_routed_as_java_regular_expressions_route_them() {
        let mut kinds = std::collections::BTreeMap::new();
        for line in include_str!("java_routing.txt").lines() {
            let (text, expected) = line.split_once(' ').unwrap();
            let (text, expected) = (unhex(text), unhex(expected));
            assert_eq!(described(parse_filter(&text)), expected, "{text:?}");
            *kinds.entry(expected[..1].to_string()).or_insert(0) += 1;
        }
        for kind in ["M", "I", "R", "P", "C", "E"] {
            assert!(kinds.get(kind).copied().unwrap_or(0) >= 30, "{kinds:?}");
        }
    }
}
