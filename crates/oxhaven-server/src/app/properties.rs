//! The text format of an `.app` file: Java's properties format, as far as an `.app` file
//! can use it.

use std::collections::BTreeMap;

/// Whether `c` separates a key from its value like a blank: space, tab or form feed.
fn is_blank(c: char) -> bool {
    matches!(c, ' ' | '\t' | '\u{c}')
}

/// The logical lines of a properties text: natural lines end with `\n`, `\r` or `\r\n`,
/// leading blanks are dropped, blank lines and lines that start with `#` or `!` are left
/// out, and a line that ends with an odd number of backslashes goes on with the next
/// natural line, without that backslash and without the next line's leading blanks.
fn logical_lines(text: &str) -> Vec<String> {
    let mut natural = text
        .split('\n')
        .flat_map(|line| line.strip_suffix('\r').unwrap_or(line).split('\r'));
    let mut lines = Vec::new();
    while let Some(line) = natural.next() {
        let line = line.trim_start_matches(is_blank);
        if line.is_empty() || line.starts_with(['#', '!']) {
            continue;
        }
        let mut logical = line.to_string();
        while logical.chars().rev().take_while(|c| *c == '\\').count() % 2 == 1 {
            logical.pop();
            match natural.next() {
                Some(next) => logical.push_str(next.trim_start_matches(is_blank)),
                None => break,
            }
        }
        lines.push(logical);
    }
    lines
}

/// Replaces the escapes of a key or a value: `\t`, `\n`, `\r`, `\f`, `\uXXXX`, and any
/// other escaped character by itself.
fn unescape(text: &str) -> Result<String, String> {
    let mut out = String::with_capacity(text.len());
    // the first half of a character outside the basic plane, given as two `\u` escapes
    let mut high: Option<u16> = None;
    let unpaired = || {
        "not ported: java.util.Properties#load of a \\uxxxx escape that is half of a \
         surrogate pair without the other half"
            .to_string()
    };
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        let mut unit = None;
        if c != '\\' {
            out.push(c);
        } else {
            match chars.next() {
                Some('u') => {
                    let digits: String = chars.by_ref().take(4).collect();
                    let valid = digits.len() == 4 && digits.chars().all(|d| d.is_ascii_hexdigit());
                    let value = u16::from_str_radix(&digits, 16).ok().filter(|_| valid);
                    unit = Some(value.ok_or_else(|| {
                        "java.lang.IllegalArgumentException: Malformed \\uxxxx encoding."
                            .to_string()
                    })?);
                }
                Some('t') => out.push('\t'),
                Some('n') => out.push('\n'),
                Some('r') => out.push('\r'),
                Some('f') => out.push('\u{c}'),
                Some(other) => out.push(other),
                None => {}
            }
        }
        match (high.take(), unit) {
            (None, None) => {}
            (None, Some(unit)) => match char::from_u32(u32::from(unit)) {
                Some(c) => out.push(c),
                None if (0xD800..0xDC00).contains(&unit) => high = Some(unit),
                None => return Err(unpaired()),
            },
            (Some(high), Some(low)) => {
                let pair = char::decode_utf16([high, low]).next();
                out.push(pair.and_then(Result::ok).ok_or_else(unpaired)?);
            }
            (Some(_), None) => return Err(unpaired()),
        }
    }
    if high.is_some() {
        return Err(unpaired());
    }
    Ok(out)
}

/// The keys and values of a properties text. A key ends at the first `=`, `:` or blank that
/// is not escaped; blanks around the separator are dropped, and the rest of the line is the
/// value, with its trailing blanks. A key given twice has its last value.
///
/// A `\u` escape that is half of a surrogate pair on its own is refused: Java keeps it in
/// its UTF-16 string, and a Rust string cannot hold it.
///
/// Behavior: `java.util.Properties#load(Reader)`
pub(super) fn parse(text: &str) -> Result<BTreeMap<String, String>, String> {
    let mut properties = BTreeMap::new();
    for line in logical_lines(text) {
        let mut key_end = line.len();
        let mut escaped = false;
        for (index, c) in line.char_indices() {
            if !escaped && (c == '=' || c == ':' || is_blank(c)) {
                key_end = index;
                break;
            }
            escaped = c == '\\' && !escaped;
        }
        let rest = line[key_end..].trim_start_matches(is_blank);
        let rest = match rest.strip_prefix(['=', ':']) {
            Some(rest) => rest.trim_start_matches(is_blank),
            None => rest,
        };
        properties.insert(unescape(&line[..key_end])?, unescape(rest)?);
    }
    Ok(properties)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pairs(text: &str) -> Vec<(String, String)> {
        parse(text).unwrap().into_iter().collect()
    }

    fn pair(key: &str, value: &str) -> (String, String) {
        (key.to_string(), value.to_string())
    }

    /// Each expectation in these tests is what `java.util.Properties#load` returned for the
    /// same text on the JDK the reference runs on (OpenJDK 21).
    #[test]
    fn keys_and_values_are_split_as_java_splits_them() {
        assert_eq!(
            pairs("# comment\n! another\n\ntype=script\n  scriptType   python\nid:a0\n"),
            [
                pair("id", "a0"),
                pair("scriptType", "python"),
                pair("type", "script")
            ]
        );
        // blanks around the separator go, those at the end of the value stay
        assert_eq!(
            pairs("name = Second \\\n   continued\r\nfile_7 :\t sub/s.py \r"),
            [
                pair("file_7", "sub/s.py "),
                pair("name", "Second continued")
            ]
        );
        // a continued line is not looked at for a comment, and an empty one ends it
        assert_eq!(
            pairs("a=1\\\n# not a comment\nb=2\\"),
            [pair("a", "1# not a comment"), pair("b", "2")]
        );
        assert_eq!(pairs("a=1\\\n\n b=2"), [pair("a", "1"), pair("b", "2")]);
        assert_eq!(
            pairs("a\\\n  b = c\n\\ lead=x\nk\\u0041=v\n = novalue\n:x\n"),
            [
                pair("", "x"),
                pair(" lead", "x"),
                pair("ab", "c"),
                pair("kA", "v")
            ]
        );
        assert_eq!(
            pairs("a=1\rb=2\r\n\rc=3\n\u{c} d \u{c}= \u{c}e\n"),
            [
                pair("a", "1"),
                pair("b", "2"),
                pair("c", "3"),
                pair("d", "e")
            ]
        );
        assert_eq!(pairs("t=a\\qb\\\\\\\n c"), [pair("t", "aqb\\c")]);
        // only the first separator counts, and a key may have no value
        assert_eq!(
            pairs("a=b=c\nd e f\nlonely\nx=1\nx=2"),
            [
                pair("a", "b=c"),
                pair("d", "e f"),
                pair("lonely", ""),
                pair("x", "2")
            ]
        );
    }

    #[test]
    fn escapes_are_replaced() {
        assert_eq!(
            pairs("id=e\\u0041\\tb\nname=N\\:a\\=b\\ c\\\\d\nk\\=1\\ 2=v\n"),
            [
                pair("id", "eA\tb"),
                pair("k=1 2", "v"),
                pair("name", "N:a=b c\\d")
            ]
        );
        // an even number of backslashes at the end of a line does not continue it
        assert_eq!(pairs("a=1\\\\\nb=2"), [pair("a", "1\\"), pair("b", "2")]);
        assert_eq!(pairs("snow=\\uD83D\\uDE00"), [pair("snow", "\u{1F600}")]);
        assert_eq!(
            parse("name=\\u00zz").unwrap_err(),
            "java.lang.IllegalArgumentException: Malformed \\uxxxx encoding."
        );
        assert!(parse("name=\\uD83Dx")
            .unwrap_err()
            .starts_with("not ported: java.util.Properties#load"));
    }
}
