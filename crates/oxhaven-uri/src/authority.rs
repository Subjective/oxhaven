//! The authority of a URI, read as `java.net.URI` reads it.
//!
//! Java first reads an authority as server-based: optional user information, a host (a host
//! name, an IPv4 address, or an IPv6 address in brackets) and an optional port. When that
//! fails it falls back to a registry-based authority, which has no host or port at all, as
//! long as every character is legal there. Only an authority with a bracket that is not a
//! well-formed IPv6 host fails outright with the server-based reading's error.

use crate::syntax::{check_chars, fail, Class, Fail, MALFORMED_ESCAPE};

#[derive(Clone, Debug)]
pub(crate) struct Authority {
    pub(crate) range: (usize, usize),
    /// `None` for a registry-based authority.
    pub(crate) server: Option<Server>,
}

#[derive(Clone, Debug)]
pub(crate) struct Server {
    pub(crate) user_info: Option<(usize, usize)>,
    pub(crate) host: (usize, usize),
    pub(crate) port: Option<u32>,
}

/// Behavior: java.net.URI#URI(String), the authority component
pub(crate) fn parse(s: &str, start: usize, end: usize) -> Result<Authority, Fail> {
    // The registry-name characters are read first, up to the first that is not one; a
    // malformed escape on the way is an error at once.
    let registry = check_chars(
        s,
        start,
        end,
        Class::RegName,
        "Illegal character in authority",
    );
    if let Err(e) = registry {
        if e.reason == MALFORMED_ESCAPE {
            return Err(e);
        }
    }
    // An authority with a character that no server-based authority has either is no
    // authority at all.
    let server_chars = s[start..end]
        .chars()
        .all(|c| matches!(c, '[' | ']' | '%') || Class::RegName.allows(c));
    if !server_chars {
        registry?;
    }
    match server(s, start, end) {
        Ok(server) => Ok(Authority {
            range: (start, end),
            server: Some(server),
        }),
        // A bracket is legal in no registry name, so when the server-based reading fails, an
        // authority with one fails with that reading's error.
        Err(server_error) => {
            registry.map_err(|_| server_error)?;
            Ok(Authority {
                range: (start, end),
                server: None,
            })
        }
    }
}

fn server(s: &str, start: usize, end: usize) -> Result<Server, Fail> {
    let bytes = s.as_bytes();
    let mut p = start;
    let mut user_info = None;
    if let Some(at) = bytes[start..end].iter().position(|&b| b == b'@') {
        let at = start + at;
        check_chars(
            s,
            start,
            at,
            Class::UserInfo,
            "Illegal character in user info",
        )?;
        user_info = Some((start, at));
        p = at + 1;
    }

    let host_start = p;
    if bytes.get(p) == Some(&b'[') && p < end {
        let close = bytes[p + 1..end]
            .iter()
            .position(|&b| b == b']')
            .map(|i| p + 1 + i);
        let q = close.unwrap_or(end);
        if close.is_none() || q == p + 1 {
            return fail("Expected closing bracket for IPv6 address", q);
        }
        ipv6(s, p + 1, q)?;
        p = q + 1;
        if p < end && bytes[p] != b':' {
            return fail("Expected port number", p);
        }
    } else {
        p = match ipv4(s, p, end) {
            Some(q) if q == end || bytes[q] == b':' => q,
            _ => hostname(s, p, end)?,
        };
    }
    let host = (host_start, p);

    let mut port = None;
    if p < end {
        // bytes[p] is ':'
        let digits = p + 1;
        if let Some(bad) = bytes[digits..end].iter().position(|b| !b.is_ascii_digit()) {
            return fail("Illegal character in port number", digits + bad);
        }
        if digits < end {
            match s[digits..end].parse::<i32>() {
                Ok(n) => port = Some(n as u32),
                Err(_) => return fail("Malformed port number", digits),
            }
        }
    }
    Ok(Server {
        user_info,
        host,
        port,
    })
}

fn digits_value(s: &str, start: usize, end: usize) -> (usize, Option<u32>) {
    let len = s.as_bytes()[start..end]
        .iter()
        .take_while(|b| b.is_ascii_digit())
        .count();
    let value = s[start..start + len].parse::<u32>().ok();
    (start + len, value)
}

/// A dotted-quad IPv4 address at `start`, each part 0 to 255 (leading zeros allowed); returns
/// where it ends. A fourth part followed by another `.` is not an address.
fn ipv4(s: &str, start: usize, end: usize) -> Option<usize> {
    let bytes = s.as_bytes();
    let mut p = start;
    for part in 0..4 {
        if part > 0 {
            if bytes.get(p) != Some(&b'.') || p >= end {
                return None;
            }
            p += 1;
        }
        let (q, value) = digits_value(s, p, end);
        if q == p || value.is_none_or(|v| v > 255) {
            return None;
        }
        p = q;
    }
    if p < end && bytes[p] == b'.' {
        return None;
    }
    Some(p)
}

/// A host name at `start`: labels of letters, digits and inner hyphens separated by dots, with
/// an optional final dot; when there are two labels or more, the last starts with a letter.
/// Returns where it ends, which is `end` or a `:`.
fn hostname(s: &str, start: usize, end: usize) -> Result<usize, Fail> {
    let bytes = s.as_bytes();
    let mut p = start;
    let mut labels = 0;
    let mut last_label = start;
    while p < end && bytes[p].is_ascii_alphanumeric() {
        last_label = p;
        labels += 1;
        let mut q = p;
        while q < end && (bytes[q].is_ascii_alphanumeric() || bytes[q] == b'-') {
            q += 1;
        }
        while bytes[q - 1] == b'-' {
            q -= 1;
        }
        p = q;
        if p < end && bytes[p] == b'.' {
            p += 1;
        } else {
            break;
        }
    }
    if labels == 0 {
        if p < end && bytes[p] != b':' {
            return fail("Illegal character in hostname", p);
        }
        return fail("Expected hostname", start);
    }
    if p < end && bytes[p] != b':' {
        return fail("Illegal character in hostname", p);
    }
    if labels > 1 && !bytes[last_label].is_ascii_alphabetic() {
        return fail("Illegal character in hostname", last_label);
    }
    Ok(p)
}

/// The address between an IPv6 host's brackets, with an optional scope id after `%`.
///
/// Behavior: java.net.URI#URI(String), RFC 2373 addresses as RFC 2732 allows them
fn ipv6(s: &str, start: usize, end: usize) -> Result<(), Fail> {
    let bytes = s.as_bytes();
    let (address_end, scope) = match bytes[start..end].iter().position(|&b| b == b'%') {
        Some(i) => (start + i, Some(start + i + 1)),
        None => (end, None),
    };
    ipv6_address(s, start, address_end)?;
    if let Some(scope) = scope {
        if scope == end {
            return Err(Fail {
                reason: "scope id expected",
                at: None,
            });
        }
        if let Some(bad) = bytes[scope..end]
            .iter()
            .position(|&b| !(b.is_ascii_alphanumeric() || b == b'_' || b == b'.'))
        {
            return fail("Illegal character in scope id", scope + bad);
        }
    }
    Ok(())
}

fn ipv6_address(s: &str, start: usize, end: usize) -> Result<(), Fail> {
    let bytes = s.as_bytes();
    let at = |p: usize| if p < end { Some(bytes[p]) } else { None };
    let is_hex = |p: usize| at(p).is_some_and(|b| b.is_ascii_hexdigit());
    let double_colon = |p: usize| at(p) == Some(b':') && at(p + 1) == Some(b':');

    let mut bytes_seen = 0;
    let mut compressed = false;
    let mut p = start;
    if double_colon(p) {
        compressed = true;
        p += 2;
    } else {
        if !is_hex(p) {
            return fail("Malformed IPv6 address", start);
        }
        // The groups before any `::`; an IPv4 address cannot come first.
        let (q, n) = groups(s, p, end, false)?;
        bytes_seen += n;
        p = q;
        if double_colon(p) {
            compressed = true;
            p += 2;
        } else if p < end {
            return fail("Malformed IPv6 address", start);
        }
    }
    if compressed && p < end {
        let (q, n) = groups(s, p, end, true)?;
        bytes_seen += n;
        if q < end {
            return fail("Expected hex digits or IPv4 address", last_element(s, p, q));
        }
    }
    if bytes_seen > 16 {
        return fail("IPv6 address too long", start);
    }
    if compressed && bytes_seen == 16 {
        return fail("Malformed IPv6 address", start);
    }
    if !compressed && bytes_seen < 16 {
        return fail("IPv6 address too short", start);
    }
    Ok(())
}

/// Where the last element of a run of groups starts.
fn last_element(s: &str, start: usize, end: usize) -> usize {
    s.as_bytes()[start..end]
        .iter()
        .rposition(|&b| b == b':')
        .map_or(start, |i| start + i + 1)
}

/// Groups of up to four hexadecimal digits separated by single colons, the last possibly an
/// IPv4 address. Stops before a `::` the first time (`after_compression` false), or before any
/// character that cannot continue the run. Returns the end and the number of address bytes.
fn groups(
    s: &str,
    start: usize,
    end: usize,
    after_compression: bool,
) -> Result<(usize, usize), Fail> {
    let bytes = s.as_bytes();
    let at = |p: usize| if p < end { Some(bytes[p]) } else { None };
    let mut p = start;
    let mut count = 0;
    let mut first = true;
    loop {
        // An element: an IPv4 address (not as the very first element of the address) or a
        // group of hexadecimal digits.
        let element = p;
        let digits = bytes[p..end]
            .iter()
            .take_while(|b| b.is_ascii_digit())
            .count();
        let ipv4_allowed = !first || after_compression;
        if ipv4_allowed && digits > 0 && at(p + digits) == Some(b'.') {
            let q = ipv4_in_ipv6(s, p, end)?;
            if q != end {
                return fail("Expected hex digits or IPv4 address", element);
            }
            return Ok((q, count + 4));
        }
        let hex = bytes[p..end]
            .iter()
            .take_while(|b| b.is_ascii_hexdigit())
            .count();
        if hex == 0 {
            if p == end {
                return fail("Expected digits for an IPv6 address", p);
            }
            return fail("Expected hex digits or IPv4 address", p);
        }
        if hex > 4 {
            return fail("IPv6 hexadecimal digit sequence too long", p);
        }
        count += 2;
        p += hex;
        first = false;
        if at(p) != Some(b':') {
            return Ok((p, count));
        }
        if !after_compression && at(p + 1) == Some(b':') {
            return Ok((p, count));
        }
        p += 1;
    }
}

/// An IPv4 address that ends an IPv6 address; its errors are Java's.
fn ipv4_in_ipv6(s: &str, start: usize, end: usize) -> Result<usize, Fail> {
    let bytes = s.as_bytes();
    let mut p = start;
    for part in 0..4 {
        if part > 0 {
            if p >= end || bytes[p] != b'.' {
                return fail("Malformed IPv4 address", p);
            }
            p += 1;
        }
        let (q, value) = digits_value(s, p, end);
        if q == p || value.is_none_or(|v| v > 255) {
            return fail("Malformed IPv4 address", p);
        }
        p = q;
    }
    if p < end && bytes[p] == b'.' {
        return fail("Malformed IPv4 address", p);
    }
    Ok(p)
}
