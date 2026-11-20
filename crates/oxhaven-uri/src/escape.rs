//! Escaped octets: decoding them as `java.net.URI` does, and encoding as
//! `java.net.URLEncoder` does.

/// Decodes the escaped octets of a raw URI component. Each run of consecutive escapes is read
/// as UTF-8; a malformed sequence becomes one U+FFFD.
///
/// Behavior: java.net.URI#getPath (and the other decoded getters)
pub(crate) fn decode(raw: &str) -> String {
    decode_with(raw, false)
}

/// As [`decode`], but text between `[` and `]` (an IPv6 host) stays as it is.
///
/// Behavior: java.net.URI#getAuthority, java.net.URI#getSchemeSpecificPart
pub(crate) fn decode_outside_brackets(raw: &str) -> String {
    decode_with(raw, true)
}

fn decode_with(raw: &str, skip_brackets: bool) -> String {
    if !raw.contains('%') {
        return raw.to_owned();
    }
    let mut out = String::with_capacity(raw.len());
    let mut octets = Vec::new();
    let mut rest = raw;
    let mut in_brackets = false;
    while let Some(c) = rest.chars().next() {
        if skip_brackets {
            match c {
                '[' => in_brackets = true,
                ']' => in_brackets = false,
                _ => {}
            }
        }
        let b = rest.as_bytes();
        if c == '%'
            && !in_brackets
            && b.len() >= 3
            && b[1].is_ascii_hexdigit()
            && b[2].is_ascii_hexdigit()
        {
            let hex = |d: u8| (d as char).to_digit(16).unwrap_or(0) as u8;
            octets.push(hex(b[1]) << 4 | hex(b[2]));
            rest = &rest[3..];
            continue;
        }
        decode_utf8(&octets, &mut out);
        octets.clear();
        out.push(c);
        rest = &rest[c.len_utf8()..];
    }
    decode_utf8(&octets, &mut out);
    out
}

/// UTF-8 with replacement, as Java's decoder replaces: the longest prefix of a well-formed
/// sequence is one malformed unit. Unlike the Unicode recommendation (and Rust's
/// `from_utf8_lossy`), an encoded surrogate (`ED A0..BF ..`) counts as one unit.
fn decode_utf8(bytes: &[u8], out: &mut String) {
    let mut i = 0;
    while i < bytes.len() {
        let b0 = bytes[i];
        let (more, low, high) = match b0 {
            0x00..=0x7f => {
                out.push(char::from(b0));
                i += 1;
                continue;
            }
            0xc2..=0xdf => (1, 0x80, 0xbf),
            0xe0 => (2, 0xa0, 0xbf),
            0xe1..=0xef => (2, 0x80, 0xbf),
            0xf0 => (3, 0x90, 0xbf),
            0xf1..=0xf3 => (3, 0x80, 0xbf),
            0xf4 => (3, 0x80, 0x8f),
            _ => {
                out.push(char::REPLACEMENT_CHARACTER);
                i += 1;
                continue;
            }
        };
        let mut value = u32::from(b0) & (0x3f >> more);
        let mut j = i + 1;
        let mut complete = true;
        for k in 0..more {
            let (lo, hi) = if k == 0 { (low, high) } else { (0x80, 0xbf) };
            match bytes.get(j) {
                Some(&b) if (lo..=hi).contains(&b) => {
                    value = (value << 6) | u32::from(b & 0x3f);
                    j += 1;
                }
                _ => {
                    complete = false;
                    break;
                }
            }
        }
        let decoded = if complete {
            char::from_u32(value)
        } else {
            None
        };
        out.push(decoded.unwrap_or(char::REPLACEMENT_CHARACTER));
        i = j;
    }
}

/// Upper-cases the hexadecimal digits of every escaped octet, so that two raw components that
/// `java.net.URI#equals` calls equal become the same string.
pub(crate) fn normalize_escapes(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    let mut chars = raw.chars();
    while let Some(c) = chars.next() {
        out.push(c);
        if c == '%' {
            for d in chars.by_ref().take(2) {
                out.push(d.to_ascii_uppercase());
            }
        }
    }
    out
}

/// True when `java.net.URLEncoder` leaves every character of `s` as it is.
fn is_url_safe(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | '*' | '_')
}

/// Encodes `s` as `URLEncoder.encode(s, "UTF-8")` does: letters, digits and `.-*_` stay, a
/// space becomes `+`, and every other character becomes its UTF-8 octets as `%XX`.
///
/// Behavior: java.net.URLEncoder#encode(String, String)
pub(crate) fn url_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        if is_url_safe(c) {
            out.push(c);
        } else if c == ' ' {
            out.push('+');
        } else {
            let mut buf = [0; 4];
            for b in c.encode_utf8(&mut buf).bytes() {
                out.push_str(&format!("%{b:02X}"));
            }
        }
    }
    out
}

/// True when URL-encoding `part` leaves it unchanged: every character is an ASCII letter or
/// digit or one of `.-*_`. The empty string is safe.
///
/// Behavior: io.deephaven.uri.UriHelper#isUriSafe
pub fn is_uri_safe(part: &str) -> bool {
    part.chars().all(is_url_safe)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn surrogate_sequences_are_one_replacement() {
        assert_eq!(decode("%ED%A0%80%41"), "\u{fffd}A");
        assert_eq!(decode("%ED%9F%BF"), "\u{d7ff}");
        assert_eq!(decode("%E0%80%80"), "\u{fffd}\u{fffd}\u{fffd}");
    }

    #[test]
    fn url_encoding() {
        assert_eq!(url_encode("a b+c/é*"), "a+b%2Bc%2F%C3%A9*");
        assert!(is_uri_safe("a.b-c_d*E9"));
        assert!(is_uri_safe(""));
        assert!(!is_uri_safe("a~b"));
    }
}
