//! String pattern filters: substring search and regular expressions.
//!
//! Java builds every one of these as a `java.util.regex.Pattern`. The substring forms are
//! evaluated here directly, and a regular expression is translated to the `regex` crate's
//! syntax when it uses only constructs whose meaning is the same in both.

use oxhaven_contracts::unported;
use regex::Regex;

/// What a pattern filter looks for in a non-null string.
pub(super) enum Matcher {
    /// Any of the needles occurs. Java: `\Qa\E|\Qb\E` with `find`.
    Any {
        needles: Vec<String>,
        ignore_case: bool,
    },
    /// All of the needles occur after one position, with no line terminator between that
    /// position and the start of each. Java: `(?=.*\Qa\E)(?=.*\Qb\E).*` with `find`, where
    /// `.` does not match a line terminator.
    All {
        needles: Vec<String>,
        ignore_case: bool,
    },
    /// The whole string matches. Java: `Matcher#matches`.
    Full(Regex),
}

/// Behavior: `java.util.regex.Pattern#CASE_INSENSITIVE` without `UNICODE_CASE` folds only
/// US-ASCII letters.
fn folded(text: &str, ignore_case: bool) -> String {
    if ignore_case {
        text.to_ascii_lowercase()
    } else {
        text.to_string()
    }
}

/// The characters `.` does not match without `DOTALL`.
fn is_line_terminator(c: char) -> bool {
    matches!(c, '\n' | '\r' | '\u{85}' | '\u{2028}' | '\u{2029}')
}

impl Matcher {
    pub(super) fn any(needles: Vec<String>, ignore_case: bool) -> Self {
        let needles = needles.iter().map(|n| folded(n, ignore_case)).collect();
        Matcher::Any {
            needles,
            ignore_case,
        }
    }

    pub(super) fn all(needles: Vec<String>, ignore_case: bool) -> Self {
        let needles = needles.iter().map(|n| folded(n, ignore_case)).collect();
        Matcher::All {
            needles,
            ignore_case,
        }
    }

    pub(super) fn test(&self, value: &str) -> bool {
        match self {
            Matcher::Any {
                needles,
                ignore_case,
            } => {
                let value = folded(value, *ignore_case);
                needles.iter().any(|needle| value.contains(needle.as_str()))
            }
            Matcher::All {
                needles,
                ignore_case,
            } => {
                let value = folded(value, *ignore_case);
                // Each run between line terminators, with the index of the terminator that
                // ends it: a needle may start anywhere from the run's start to that index.
                let mut start = 0;
                let mut ends: Vec<(usize, usize)> = value
                    .char_indices()
                    .filter(|(_, c)| is_line_terminator(*c))
                    .map(|(at, c)| {
                        let run = (start, at);
                        start = at + c.len_utf8();
                        run
                    })
                    .collect();
                ends.push((start, value.len()));
                ends.into_iter().any(|(start, end)| {
                    needles.iter().all(|needle| {
                        value[start..]
                            .find(needle.as_str())
                            .is_some_and(|at| start + at <= end)
                    })
                })
            }
            Matcher::Full(regex) => regex.is_match(value),
        }
    }
}

fn not_ported(construct: &str) -> ! {
    unported!("java.util.regex.Pattern#compile", "{construct}")
}

fn hex(c: char) -> String {
    format!("\\x{{{:X}}}", u32::from(c))
}

/// One literal character outside a character class.
fn literal(out: &mut String, c: char, ignore_case: bool) {
    if ignore_case && c.is_ascii_alphabetic() {
        out.push('[');
        out.push(c.to_ascii_lowercase());
        out.push(c.to_ascii_uppercase());
        out.push(']');
    } else {
        out.push_str(&hex(c));
    }
}

/// The members of a predefined class, as the inside of a bracket expression. Java's
/// predefined classes are US-ASCII unless `UNICODE_CHARACTER_CLASS` is set.
fn predefined(c: char) -> Option<&'static str> {
    Some(match c.to_ascii_lowercase() {
        'd' => "0-9",
        'w' => "a-zA-Z_0-9",
        's' => " \\t\\n\\x0B\\f\\r",
        _ => return None,
    })
}

fn control(c: char) -> Option<char> {
    Some(match c {
        't' => '\t',
        'n' => '\n',
        'r' => '\r',
        'f' => '\u{0C}',
        _ => return None,
    })
}

/// A range inside a class, with the other case of its ASCII letters when case is ignored.
///
/// Behavior: java.util.regex.Pattern#CIRange
fn class_range(out: &mut String, low: char, high: char, ignore_case: bool) {
    out.push_str(&hex(low));
    if low != high {
        out.push('-');
        out.push_str(&hex(high));
    }
    if !ignore_case {
        return;
    }
    for (from, to, shift) in [('a', 'z', -32i32), ('A', 'Z', 32)] {
        let (first, last) = (low.max(from), high.min(to));
        if first <= last {
            let swap = |c: char| char::from_u32((u32::from(c) as i32 + shift) as u32).unwrap_or(c);
            out.push_str(&hex(swap(first)));
            out.push('-');
            out.push_str(&hex(swap(last)));
        }
    }
}

/// Translates the bracket expression that starts after `chars[at]` (the `[`) and returns the
/// index after its `]`.
fn class(chars: &[char], at: usize, ignore_case: bool, out: &mut String) -> Result<usize, String> {
    let unclosed = || "Unclosed character class".to_string();
    let mut i = at + 1;
    out.push('[');
    if chars.get(i) == Some(&'^') {
        out.push('^');
        i += 1;
    }
    if chars.get(i) == Some(&']') {
        not_ported("a character class that starts with ]");
    }
    loop {
        let c = *chars.get(i).ok_or_else(unclosed)?;
        i += 1;
        let low = match c {
            ']' => break,
            '[' => not_ported("a nested character class"),
            '&' if chars.get(i) == Some(&'&') => not_ported("a character class intersection"),
            '\\' => {
                let escaped = *chars.get(i).ok_or_else(unclosed)?;
                i += 1;
                if let Some(members) = predefined(escaped) {
                    if escaped.is_ascii_uppercase() {
                        out.push_str(&format!("[^{members}]"));
                    } else {
                        out.push_str(members);
                    }
                    if chars.get(i) == Some(&'-') && chars.get(i + 1) != Some(&']') {
                        not_ported("a character class range that starts with a predefined class");
                    }
                    continue;
                }
                match control(escaped) {
                    Some(c) => c,
                    None if escaped.is_alphanumeric() => {
                        not_ported(&format!("the escape \\{escaped} in a character class"))
                    }
                    None => escaped,
                }
            }
            c => c,
        };
        let range_end = match (chars.get(i), chars.get(i + 1)) {
            (Some('-'), Some(&end)) if end != ']' => Some(end),
            _ => None,
        };
        match range_end {
            None => class_range(out, low, low, ignore_case),
            Some('\\' | '[') => not_ported("a character class range that ends in an escape"),
            Some(high) => {
                i += 2;
                if high < low {
                    return Err(format!("Illegal character range {low}-{high}"));
                }
                class_range(out, low, high, ignore_case);
            }
        }
    }
    out.push(']');
    Ok(i)
}

/// Translates a `{n}`, `{n,}` or `{n,m}` that starts at `chars[at]` and returns the index
/// after it.
fn counted(chars: &[char], at: usize, out: &mut String) -> Result<usize, String> {
    let illegal = || "Illegal repetition".to_string();
    let close = chars[at..]
        .iter()
        .position(|&c| c == '}')
        .ok_or_else(illegal)?;
    let body: String = chars[at + 1..at + close].iter().collect();
    let number = |text: &str| -> Result<u32, String> {
        if text.is_empty() || !text.bytes().all(|b| b.is_ascii_digit()) {
            return Err(illegal());
        }
        text.parse().map_err(|_| illegal())
    };
    match body.split_once(',') {
        None => {
            number(&body)?;
        }
        Some((low, "")) => {
            number(low)?;
        }
        Some((low, high)) => {
            if number(low)? > number(high)? {
                return Err("Illegal repetition range".to_string());
            }
        }
    }
    out.push('{');
    out.push_str(&body);
    out.push('}');
    Ok(at + close + 1)
}

/// A regular expression in Java's syntax, compiled with `DOTALL` and optionally
/// `CASE_INSENSITIVE`, as a `regex` crate expression that matches the same whole strings.
///
/// The accepted subset is literals, `.`, bracket classes of literals and ranges, the
/// predefined classes `\d \w \s` and their negations, groups, alternation, the greedy and
/// lazy quantifiers, `\Q...\E`, a leading `^` and a trailing `$`. An invalid pattern is an
/// error; a valid construct outside the subset is `unported!`.
///
/// Behavior: java.util.regex.Pattern#compile(String, int), java.util.regex.Matcher#matches
pub(super) fn translate(pattern: &str, ignore_case: bool) -> Result<String, String> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    let mut depth = 0usize;
    // Whether the last thing translated is something a quantifier can apply to.
    let mut atom = false;
    while i < chars.len() {
        let c = chars[i];
        i += 1;
        match c {
            '\\' => {
                let escaped = *chars
                    .get(i)
                    .ok_or("Unexpected internal error: trailing backslash")?;
                i += 1;
                if let Some(members) = predefined(escaped) {
                    let negate = if escaped.is_ascii_uppercase() {
                        "^"
                    } else {
                        ""
                    };
                    out.push_str(&format!("[{negate}{members}]"));
                } else if let Some(c) = control(escaped) {
                    out.push_str(&hex(c));
                } else if escaped == 'Q' {
                    let rest = &chars[i..];
                    let end = rest
                        .windows(2)
                        .position(|w| w == ['\\', 'E'])
                        .unwrap_or(rest.len());
                    if end == 0 {
                        not_ported("an empty \\Q\\E quotation");
                    }
                    out.push_str("(?:");
                    rest[..end]
                        .iter()
                        .for_each(|&c| literal(&mut out, c, ignore_case));
                    out.push(')');
                    i = (i + end + 2).min(chars.len());
                } else if escaped.is_alphanumeric() {
                    not_ported(&format!("the escape \\{escaped}"));
                } else {
                    literal(&mut out, escaped, ignore_case);
                }
                atom = true;
            }
            '[' => {
                i = class(&chars, i - 1, ignore_case, &mut out)?;
                atom = true;
            }
            '(' => {
                if chars.get(i) == Some(&'?') {
                    if chars.get(i + 1) != Some(&':') {
                        not_ported("a group that starts with (? other than (?:");
                    }
                    i += 2;
                }
                out.push_str("(?:");
                depth += 1;
                atom = false;
            }
            ')' => {
                depth = depth.checked_sub(1).ok_or("Unmatched closing ')'")?;
                out.push(')');
                atom = true;
            }
            '|' => {
                out.push('|');
                atom = false;
            }
            '.' => {
                out.push_str("(?s:.)");
                atom = true;
            }
            '*' | '+' | '?' | '{' => {
                if !atom {
                    if c == '{' {
                        not_ported("{ where no repetition can apply");
                    }
                    return Err(format!("Dangling meta character '{c}'"));
                }
                if c == '{' {
                    i = counted(&chars, i - 1, &mut out)?;
                } else {
                    out.push(c);
                }
                match chars.get(i) {
                    Some('?') => {
                        out.push('?');
                        i += 1;
                    }
                    Some('+') => not_ported("a possessive quantifier"),
                    _ => {}
                }
                atom = false;
            }
            '^' if i == 1 && !matches!(chars.get(i), Some('*' | '+' | '?' | '{')) => {}
            '$' if i == chars.len() => {}
            '^' | '$' => not_ported(&format!("{c} inside the pattern")),
            c => {
                literal(&mut out, c, ignore_case);
                atom = true;
            }
        }
    }
    if depth != 0 {
        return Err("Unclosed group".to_string());
    }
    Ok(format!("\\A(?:{out})\\z"))
}

/// Behavior: java.util.regex.Pattern#compile(String, int) with `DOTALL`
pub(super) fn full_match(pattern: &str, ignore_case: bool) -> Result<Matcher, String> {
    let translated = translate(pattern, ignore_case)?;
    match Regex::new(&translated) {
        Ok(regex) => Ok(Matcher::Full(regex)),
        Err(e) => not_ported(&format!("the regex crate refuses {pattern:?}: {e}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn full(pattern: &str, ignore_case: bool, value: &str) -> bool {
        full_match(pattern, ignore_case).unwrap().test(value)
    }

    #[test]
    fn a_regex_must_match_the_whole_string() {
        assert!(full("a.c", false, "abc"));
        assert!(!full("a.c", false, "abcd"));
        assert!(!full("b", false, "abc"));
        assert!(full(".*b.*", false, "abc"));
        assert!(full("^abc$", false, "abc"));
        assert!(full("a|bc", false, "bc"));
        assert!(full("(?:ab)+c?", false, "abab"));
        assert!(full("a{2,3}", false, "aaa"));
        assert!(!full("a{2,3}", false, "aaaa"));
        assert!(full("a+?b", false, "aab"));
    }

    #[test]
    fn dot_matches_line_terminators() {
        assert!(full("a.b", false, "a\nb"));
    }

    #[test]
    fn predefined_classes_are_ascii() {
        assert!(full("\\d+", false, "0123"));
        // ARABIC-INDIC DIGIT ONE is \d to the regex crate but not to Java.
        assert!(!full("\\d", false, "\u{661}"));
        assert!(full("\\D", false, "\u{661}"));
        assert!(full("\\w+", false, "aZ_9"));
        assert!(!full("\\w", false, "\u{e9}"));
        assert!(full("\\s", false, "\u{0B}"));
        assert!(!full("\\s", false, "\u{a0}"));
        assert!(full("[\\d.]+", false, "1.5"));
        assert!(full("[a\\D]", false, "x"));
    }

    #[test]
    fn case_is_ignored_for_ascii_letters_only() {
        assert!(full("abc", true, "aBC"));
        assert!(!full("abc", false, "aBC"));
        assert!(full("[a-c]+", true, "AbC"));
        assert!(full("[^a-c]", true, "d"));
        assert!(!full("[^a-c]", true, "B"));
        // KELVIN SIGN folds to k in Unicode, not in US-ASCII.
        assert!(!full("k", true, "\u{212a}"));
        assert!(!full("\u{e9}", true, "\u{c9}"));
        assert!(full("\\Qa.b\\E", true, "A.B"));
        assert!(!full("\\Qa.b\\E", true, "AxB"));
    }

    #[test]
    fn metacharacters_are_literal_when_escaped_or_in_a_class() {
        assert!(full("a\\.b", false, "a.b"));
        assert!(!full("a\\.b", false, "axb"));
        assert!(full("[.*]+", false, ".*"));
        assert!(full("[a-]+", false, "a-"));
        assert!(full("a]b}", false, "a]b}"));
        assert!(full("\\t", false, "\t"));
    }

    #[test]
    fn invalid_patterns_are_errors() {
        for bad in [
            "*a", "a**", "(a", "a)", "[a", "a{2", "a{3,2}", "a\\", "[b-a]",
        ] {
            assert!(full_match(bad, false).is_err(), "{bad}");
        }
    }

    #[test]
    fn constructs_outside_the_subset_are_unported() {
        for pattern in [
            "(?=a)", "a\\b", "a*+", "\\1", "a^b", "a$b", "[a[b]]", "[a&&b]", "\\p{L}", "(?i)a",
        ] {
            let result = std::panic::catch_unwind(|| full_match(pattern, false).is_ok());
            assert!(result.is_err(), "{pattern} must be unported");
        }
    }

    #[test]
    fn any_and_all_search_for_substrings() {
        let any = Matcher::any(vec!["ab".into(), "X".into()], false);
        assert!(any.test("cabd") && any.test("X") && !any.test("x") && !any.test("a b"));
        let any = Matcher::any(vec!["aB".into()], true);
        assert!(any.test("xAbx") && !any.test("a"));
        assert!(Matcher::any(vec![String::new()], false).test(""));

        let all = Matcher::all(vec!["a".into(), "b".into()], false);
        assert!(all.test("xbxa") && !all.test("a") && !all.test("b"));
        // The lookaheads cannot see past a line terminator.
        assert!(!all.test("a\nb"));
        assert!(all.test("zz\nba"));
        let all = Matcher::all(vec!["a".into(), "\nb".into()], false);
        assert!(all.test("a\nb") && !all.test("\nba"));
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

    /// `java_patterns.txt` holds what a JDK 21 `java.util.regex` answers for the patterns the
    /// pattern filters build. Each line is a kind (`M`: a regular expression with `DOTALL`
    /// and `Matcher#matches`; `A` and `L`: the any and all searches of `includes` with
    /// `Matcher#find`), whether case is ignored, the pattern or the strings searched for,
    /// the value, and the answer. Text is UTF-8 in hexadecimal.
    #[test]
    fn patterns_answer_as_java_regular_expressions_answer() {
        let mut checked = 0;
        for line in include_str!("java_patterns.txt").lines() {
            let fields: Vec<&str> = line.split(' ').collect();
            let ignore_case = fields[1] == "1";
            let (pattern, value) = (unhex(fields[2]), unhex(fields[3]));
            let needles = || {
                pattern
                    .split('\u{1f}')
                    .map(String::from)
                    .collect::<Vec<_>>()
            };
            let matcher = match fields[0] {
                "M" => full_match(&pattern, ignore_case).unwrap(),
                "A" => Matcher::any(needles(), ignore_case),
                _ => Matcher::all(needles(), ignore_case),
            };
            assert_eq!(
                matcher.test(&value),
                fields[4] == "1",
                "{line}: {pattern:?} on {value:?}"
            );
            checked += 1;
        }
        assert_eq!(checked, 6512);
    }
}
