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

/// The character of an escape that names it by number, `escaped` being the letter after
/// the backslash and `chars[*i]` what follows it: `\xhh` (two hexadecimal digits),
/// `\x{h...h}` (a code point), `\uhhhh` (a UTF-16 unit; a high surrogate followed by
/// `\u` and a low one is the pair's code point) and `\0n`, `\0nn`, `\0mnn` (octal, `m`
/// at most 3). Moves `*i` past the escape. `None` when `escaped` is none of `x`, `u`, `0`.
/// A surrogate left alone names no character a string here can hold, and is not ported.
///
/// Behavior: the escapes of java.util.regex.Pattern, as its documentation lists them;
/// what JDK 21 answers and the messages of its errors are in `java_patterns.txt`
fn numeric_escape(chars: &[char], i: &mut usize, escaped: char) -> Result<Option<char>, String> {
    let hex_digits = |from: usize, count: usize| -> Option<u32> {
        let digits: String = chars.get(from..from + count)?.iter().collect();
        if digits.chars().all(|c| c.is_ascii_hexdigit()) {
            u32::from_str_radix(&digits, 16).ok()
        } else {
            None
        }
    };
    let code = match escaped {
        'x' if chars.get(*i) == Some(&'{') => {
            let illegal = || "Illegal hexadecimal escape sequence".to_string();
            let close = chars[*i..]
                .iter()
                .position(|&c| c == '}')
                .ok_or("Unclosed hexadecimal escape sequence")?;
            let digits: String = chars[*i + 1..*i + close].iter().collect();
            if digits.is_empty() || !digits.chars().all(|c| c.is_ascii_hexdigit()) {
                return Err(illegal());
            }
            let code = match digits.trim_start_matches('0') {
                "" => 0,
                significant if significant.len() <= 6 => {
                    u32::from_str_radix(significant, 16).map_err(|_| illegal())?
                }
                _ => u32::MAX,
            };
            if code > 0x10FFFF {
                return Err("Hexadecimal codepoint is too big".to_string());
            }
            *i += close + 1;
            code
        }
        'x' => {
            let code = hex_digits(*i, 2).ok_or("Illegal hexadecimal escape sequence")?;
            *i += 2;
            code
        }
        'u' => {
            let code = hex_digits(*i, 4).ok_or("Illegal Unicode escape sequence")?;
            *i += 4;
            let low = (chars.get(*i..*i + 2) == Some(&['\\', 'u'][..]))
                .then(|| hex_digits(*i + 2, 4))
                .flatten()
                .filter(|low| (0xDC00..=0xDFFF).contains(low));
            match low {
                Some(low) if (0xD800..=0xDBFF).contains(&code) => {
                    *i += 6;
                    0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00)
                }
                _ => code,
            }
        }
        '0' => {
            let octal = |at: usize| chars.get(at).and_then(|c| c.to_digit(8));
            let first = octal(*i).ok_or("Illegal octal escape sequence")?;
            let mut code = first;
            *i += 1;
            let longest = if first <= 3 { 2 } else { 1 };
            for _ in 0..longest {
                match octal(*i) {
                    Some(digit) => {
                        code = code * 8 + digit;
                        *i += 1;
                    }
                    None => break,
                }
            }
            code
        }
        _ => return Ok(None),
    };
    match char::from_u32(code) {
        Some(c) => Ok(Some(c)),
        None => not_ported(&format!("the surrogate \\u{code:04X} alone")),
    }
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
                if let Some(c) = numeric_escape(chars, &mut i, escaped)? {
                    c
                } else {
                    match control(escaped) {
                        Some(c) => c,
                        None if escaped.is_alphanumeric() => {
                            not_ported(&format!("the escape \\{escaped} in a character class"))
                        }
                        None => escaped,
                    }
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

/// The flags in force at a point of a pattern: `CASE_INSENSITIVE` and `DOTALL`, which the
/// filter sets and inline flags change until the end of the group they are in.
#[derive(Clone, Copy)]
struct Flags {
    ignore_case: bool,
    dotall: bool,
}

/// What follows `(` at `chars[at]`.
enum GroupStart {
    /// A group, capturing or not, with the flags inside it; and the index after its opening.
    Group(Flags, usize),
    /// `(?flags)`: the flags from here to the end of the enclosing group; and the index after
    /// the `)`.
    Flags(Flags, usize),
}

/// Reads what follows a `(` at `chars[at]`: a plain group, `(?:`, a named group
/// `(?<name>`, or inline flags `(?is-is)` and `(?is-is:`. Only `i` and `s` are read;
/// the other flags Java knows (`d`, `m`, `u`, `x`, `c`, `U`) and lookaround, atomic groups
/// and the like are not ported. A named group takes a name that starts with a US-ASCII
/// letter followed by letters and digits, once per pattern.
///
/// Behavior: the groups and flags of java.util.regex.Pattern, as its documentation lists
/// them; what JDK 21 answers and the messages of its errors are in `java_patterns.txt`
fn group_start(
    chars: &[char],
    at: usize,
    flags: Flags,
    names: &mut Vec<String>,
) -> Result<GroupStart, String> {
    if chars.get(at) != Some(&'?') {
        return Ok(GroupStart::Group(flags, at));
    }
    let mut i = at + 1;
    match chars.get(i) {
        Some(':') => return Ok(GroupStart::Group(flags, i + 1)),
        Some('<') if matches!(chars.get(i + 1), Some('=' | '!')) => not_ported("lookbehind"),
        Some('<') => {
            i += 1;
            let start = i;
            if !chars.get(i).is_some_and(char::is_ascii_alphabetic) {
                return Err("capturing group name does not start with a Latin letter".into());
            }
            while chars.get(i).is_some_and(char::is_ascii_alphanumeric) {
                i += 1;
            }
            if chars.get(i) != Some(&'>') {
                return Err("named capturing group is missing trailing '>'".into());
            }
            let name: String = chars[start..i].iter().collect();
            if names.contains(&name) {
                return Err(format!("Named capturing group <{name}> is already defined"));
            }
            names.push(name);
            return Ok(GroupStart::Group(flags, i + 1));
        }
        Some('=' | '!') => not_ported("lookahead"),
        Some('>') => not_ported("an atomic group"),
        _ => {}
    }
    let mut flags = flags;
    let mut on = true;
    loop {
        let c = *chars.get(i).ok_or("Unknown inline modifier")?;
        i += 1;
        match c {
            'i' => flags.ignore_case = on,
            's' => flags.dotall = on,
            '-' if on => on = false,
            ')' => return Ok(GroupStart::Flags(flags, i)),
            ':' => return Ok(GroupStart::Group(flags, i)),
            'd' | 'm' | 'u' | 'x' | 'c' | 'U' => not_ported(&format!("the inline flag {c}")),
            _ => return Err("Unknown inline modifier".into()),
        }
    }
}

/// A regular expression in Java's syntax, compiled with `DOTALL` and optionally
/// `CASE_INSENSITIVE`, as a `regex` crate expression that matches the same whole strings.
///
/// The accepted subset is literals, `.`, bracket classes of literals and ranges, the
/// predefined classes `\d \w \s` and their negations, groups (named ones too, which match
/// as any group does when only the whole string counts), the inline flags `i` and `s`,
/// alternation, the greedy and lazy quantifiers, `\Q...\E`, escapes by number (`\x`, `\u`,
/// `\0`), `^` (the start of the input, as without `MULTILINE`) and a trailing `$`. An
/// invalid pattern is an error; a valid construct outside the subset is `unported!`.
///
/// Behavior: java.util.regex.Pattern#compile(String, int), java.util.regex.Matcher#matches
pub(super) fn translate(pattern: &str, ignore_case: bool) -> Result<String, String> {
    let chars: Vec<char> = pattern.chars().collect();
    let mut out = String::new();
    let mut i = 0;
    let mut flags = Flags {
        ignore_case,
        dotall: true,
    };
    // The flags to restore at the end of each open group.
    let mut outer: Vec<Flags> = Vec::new();
    let mut names = Vec::new();
    // Whether the last thing translated is something a quantifier can apply to.
    let mut atom = false;
    let quantifier = |c: Option<&char>| matches!(c, Some('*' | '+' | '?' | '{'));
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
                } else if let Some(c) = numeric_escape(&chars, &mut i, escaped)? {
                    literal(&mut out, c, flags.ignore_case);
                } else if let Some(c) = control(escaped) {
                    out.push_str(&hex(c));
                } else if escaped == 'Q' {
                    // Java removes the quotation before it reads the pattern, quoting each
                    // character: a quantifier after it applies to the last one, and an
                    // empty one leaves nothing behind.
                    let rest = &chars[i..];
                    let end = rest
                        .windows(2)
                        .position(|w| w == ['\\', 'E'])
                        .unwrap_or(rest.len());
                    rest[..end]
                        .iter()
                        .for_each(|&c| literal(&mut out, c, flags.ignore_case));
                    i = (i + end + 2).min(chars.len());
                    if end == 0 {
                        continue;
                    }
                } else if escaped.is_alphanumeric() {
                    not_ported(&format!("the escape \\{escaped}"));
                } else {
                    literal(&mut out, escaped, flags.ignore_case);
                }
                atom = true;
            }
            '[' => {
                i = class(&chars, i - 1, flags.ignore_case, &mut out)?;
                atom = true;
            }
            '(' => match group_start(&chars, i, flags, &mut names)? {
                GroupStart::Group(inner, after) => {
                    outer.push(flags);
                    flags = inner;
                    i = after;
                    out.push_str("(?:");
                    atom = false;
                }
                GroupStart::Flags(changed, after) => {
                    flags = changed;
                    i = after;
                    atom = false;
                }
            },
            ')' => {
                flags = outer.pop().ok_or("Unmatched closing ')'")?;
                out.push(')');
                atom = true;
            }
            '|' => {
                out.push('|');
                atom = false;
            }
            '.' => {
                out.push_str(if flags.dotall {
                    "(?s:.)"
                } else {
                    "[^\\n\\r\\x{85}\\x{2028}\\x{2029}]"
                });
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
            // Without `MULTILINE`, `^` is the start of the input wherever it stands.
            '^' if !quantifier(chars.get(i)) => {
                out.push_str("\\A");
                atom = false;
            }
            '$' if i == chars.len() => {}
            '^' | '$' => not_ported(&format!("{c} inside the pattern")),
            c => {
                literal(&mut out, c, flags.ignore_case);
                atom = true;
            }
        }
    }
    if !outer.is_empty() {
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
            "(?=a)",
            "a\\b",
            "a*+",
            "\\1",
            "a$b",
            "[a[b]]",
            "[a&&b]",
            "\\p{L}",
            "(?<=a)b",
            "(?<!a)b",
            "(?!a)",
            "(?>a)",
            "(?x)a",
            "(?m)a",
            "(?d)a",
            "(?u)a",
            "(?U)a",
            "(?c)a",
            "(?-m)a",
            "(?<n>a)\\k<n>",
            "\\uD83D",
            "\\x{DC00}",
            "^*",
            "(?i)(a)\\1",
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
    /// the value, and the answer; or `E`, whether case is ignored, and a regular expression
    /// that Java refuses to compile. Text is UTF-8 in hexadecimal.
    #[test]
    fn patterns_answer_as_java_regular_expressions_answer() {
        let mut checked = 0;
        for line in include_str!("java_patterns.txt").lines() {
            let fields: Vec<&str> = line.split(' ').collect();
            let ignore_case = fields[1] == "1";
            if fields[0] == "E" {
                let pattern = unhex(fields[2]);
                assert!(
                    full_match(&pattern, ignore_case).is_err(),
                    "{line}: {pattern:?}"
                );
                checked += 1;
                continue;
            }
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
        assert_eq!(checked, 14152);
    }
}
