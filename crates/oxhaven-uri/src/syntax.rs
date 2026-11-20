//! A URI as `java.net.URI` parses it.
//!
//! Deephaven's URI types are defined on top of `java.net.URI`: which inputs they accept, the
//! host and port they see, the decoded path they match and the text of most errors a user
//! gets all come from it. This module reproduces that behavior: RFC 2396 with the deviations
//! the class documents (RFC 2732 IPv6 hosts, an empty authority before a path, "other"
//! characters, a registry-based authority when the server-based reading fails). It was written
//! from the class's documentation and from what Java answered for the inputs of
//! `tasks/13.2a-uri/java-42.9.jsonl`; no JDK source was translated.
//!
//! Positions in errors count UTF-16 code units, as Java's do.

use std::fmt;
use std::hash::{Hash, Hasher};

use crate::authority::{self, Authority};
use crate::escape::{decode, decode_outside_brackets, normalize_escapes};

/// Why a string is not a URI: Java's `java.net.URISyntaxException`.
///
/// `Display` gives Java's `getMessage()`, which is also the message of the
/// `IllegalArgumentException` that `URI.create` throws.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UriSyntaxError {
    input: String,
    reason: &'static str,
    index: Option<usize>,
}

impl UriSyntaxError {
    /// The reason, for example `Illegal character in path`.
    pub fn reason(&self) -> &str {
        self.reason
    }

    /// Where the error is, in UTF-16 code units, when Java gives a position.
    pub fn index(&self) -> Option<usize> {
        self.index
    }

    /// The string that failed to parse.
    pub fn input(&self) -> &str {
        &self.input
    }
}

impl fmt::Display for UriSyntaxError {
    /// Behavior: java.net.URISyntaxException#getMessage
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.index {
            Some(index) => write!(f, "{} at index {}: {}", self.reason, index, self.input),
            None => write!(f, "{}: {}", self.reason, self.input),
        }
    }
}

impl std::error::Error for UriSyntaxError {}

pub(crate) const MALFORMED_ESCAPE: &str = "Malformed escape pair";

/// A failure at a byte offset of the input; turned into a [`UriSyntaxError`] at the top.
#[derive(Clone, Copy)]
pub(crate) struct Fail {
    pub(crate) reason: &'static str,
    /// Byte offset into the whole input, or `None` for Java's errors without a position.
    pub(crate) at: Option<usize>,
}

pub(crate) fn fail<T>(reason: &'static str, at: usize) -> Result<T, Fail> {
    Err(Fail {
        reason,
        at: Some(at),
    })
}

/// A parsed URI. Every value comes from [`Uri::parse`], so [`Uri::as_str`] (and `Display`) is
/// the input, as `java.net.URI#toString` is for a parsed URI.
#[derive(Clone, Debug)]
pub struct Uri {
    string: String,
    scheme: Option<(usize, usize)>,
    /// The raw scheme-specific part: between the scheme's colon and the fragment.
    ssp: (usize, usize),
    fragment: Option<(usize, usize)>,
    shape: Shape,
}

#[derive(Clone, Debug)]
enum Shape {
    Opaque,
    Hierarchical {
        authority: Option<Authority>,
        path: (usize, usize),
        query: Option<(usize, usize)>,
    },
}

/// Character classes of RFC 2396, plus Java's "other" characters.
#[derive(Clone, Copy)]
pub(crate) enum Class {
    /// `uric`: query, fragment and opaque part.
    Uric,
    /// Path characters: `pchar`, `/` and `;`.
    Path,
    /// User information.
    UserInfo,
    /// A registry-based authority (`reg_name`).
    RegName,
}

fn is_unreserved(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.' | '!' | '~' | '*' | '\'' | '(' | ')')
}

/// Java's `Character#isSpaceChar`: the Unicode categories Zs, Zl and Zp.
fn is_space_char(c: char) -> bool {
    matches!(
        c,
        ' ' | '\u{a0}' | '\u{1680}' | '\u{2000}'
            ..='\u{200a}' | '\u{2028}' | '\u{2029}' | '\u{202f}' | '\u{205f}' | '\u{3000}'
    )
}

/// An "other" character: not US-ASCII, not a control character (`Character#isISOControl`) and
/// not a space character.
fn is_other(c: char) -> bool {
    !c.is_ascii() && !matches!(c, '\u{80}'..='\u{9f}') && !is_space_char(c)
}

impl Class {
    pub(crate) fn allows(self, c: char) -> bool {
        if is_unreserved(c) || is_other(c) {
            return true;
        }
        match self {
            Class::Uric => matches!(
                c,
                ';' | '/' | '?' | ':' | '@' | '&' | '=' | '+' | '$' | ',' | '[' | ']'
            ),
            Class::Path => matches!(c, ':' | '@' | '&' | '=' | '+' | '$' | ',' | '/' | ';'),
            Class::UserInfo => matches!(c, ';' | ':' | '&' | '=' | '+' | '$' | ','),
            Class::RegName => matches!(c, '$' | ',' | ';' | ':' | '@' | '&' | '=' | '+'),
        }
    }
}

/// The first position in `s[start..end]` that is neither a character of `class` nor an escaped
/// octet, with Java's reason.
pub(crate) fn check_chars(
    s: &str,
    start: usize,
    end: usize,
    class: Class,
    illegal: &'static str,
) -> Result<(), Fail> {
    let bytes = s.as_bytes();
    let mut chars = s[start..end].char_indices();
    while let Some((offset, c)) = chars.next() {
        let at = start + offset;
        if c == '%' {
            let pair = bytes.get(at + 1..at + 3);
            if !pair.is_some_and(|p| p.iter().all(u8::is_ascii_hexdigit)) {
                return fail(MALFORMED_ESCAPE, at);
            }
            chars.next();
            chars.next();
        } else if !class.allows(c) {
            return fail(illegal, at);
        }
    }
    Ok(())
}

fn find_any(s: &str, from: usize, end: usize, set: &[u8]) -> Option<usize> {
    s.as_bytes()[from..end]
        .iter()
        .position(|b| set.contains(b))
        .map(|i| from + i)
}

impl Uri {
    /// Parses `input` as `java.net.URI.create` (and `new URI(String)`) do.
    ///
    /// Behavior: java.net.URI#URI(String)
    pub fn parse(input: &str) -> Result<Uri, UriSyntaxError> {
        parse(input).map_err(|f| UriSyntaxError {
            input: input.to_owned(),
            reason: f.reason,
            index: f.at.map(|at| input[..at].encode_utf16().count()),
        })
    }

    fn slice(&self, (start, end): (usize, usize)) -> &str {
        &self.string[start..end]
    }

    /// The URI as it was given.
    pub fn as_str(&self) -> &str {
        &self.string
    }

    /// The scheme, as written (not lowercased).
    pub fn scheme(&self) -> Option<&str> {
        self.scheme.map(|r| self.slice(r))
    }

    /// True for an absolute URI whose scheme-specific part does not begin with `/`.
    pub fn is_opaque(&self) -> bool {
        matches!(self.shape, Shape::Opaque)
    }

    /// The raw scheme-specific part: everything between the scheme and the fragment.
    pub fn raw_scheme_specific_part(&self) -> &str {
        self.slice(self.ssp)
    }

    /// The decoded scheme-specific part.
    pub fn scheme_specific_part(&self) -> String {
        decode_outside_brackets(self.raw_scheme_specific_part())
    }

    fn authority_parts(&self) -> Option<&Authority> {
        match &self.shape {
            Shape::Hierarchical { authority, .. } => authority.as_ref(),
            Shape::Opaque => None,
        }
    }

    /// The raw authority, if there is one.
    pub fn raw_authority(&self) -> Option<&str> {
        self.authority_parts().map(|a| self.slice(a.range))
    }

    /// The decoded authority.
    pub fn authority(&self) -> Option<String> {
        self.raw_authority().map(decode_outside_brackets)
    }

    /// The raw user information of a server-based authority.
    pub fn raw_user_info(&self) -> Option<&str> {
        let server = self.authority_parts()?.server.as_ref()?;
        server.user_info.map(|r| self.slice(r))
    }

    /// The decoded user information of a server-based authority.
    pub fn user_info(&self) -> Option<String> {
        self.raw_user_info().map(decode)
    }

    /// The host of a server-based authority, as written; an IPv6 address keeps its brackets.
    /// `None` when there is no authority or Java read it as registry-based.
    pub fn host(&self) -> Option<&str> {
        let server = self.authority_parts()?.server.as_ref()?;
        Some(self.slice(server.host))
    }

    /// The port of a server-based authority; `None` where Java's `getPort` is `-1`.
    pub fn port(&self) -> Option<u32> {
        self.authority_parts()?.server.as_ref()?.port
    }

    /// The raw path; `None` for an opaque URI.
    pub fn raw_path(&self) -> Option<&str> {
        match self.shape {
            Shape::Hierarchical { path, .. } => Some(self.slice(path)),
            Shape::Opaque => None,
        }
    }

    /// The decoded path; `None` for an opaque URI.
    pub fn path(&self) -> Option<String> {
        self.raw_path().map(decode)
    }

    /// The raw query.
    pub fn raw_query(&self) -> Option<&str> {
        match self.shape {
            Shape::Hierarchical { query, .. } => query.map(|r| self.slice(r)),
            Shape::Opaque => None,
        }
    }

    /// The decoded query.
    pub fn query(&self) -> Option<String> {
        self.raw_query().map(decode)
    }

    /// The raw fragment.
    pub fn raw_fragment(&self) -> Option<&str> {
        self.fragment.map(|r| self.slice(r))
    }

    /// The decoded fragment.
    pub fn fragment(&self) -> Option<String> {
        self.raw_fragment().map(decode)
    }

    /// The parts that equality compares, normalized as `java.net.URI#equals` compares them:
    /// scheme and host without regard to case, escaped octets without regard to the case of
    /// their hexadecimal digits, everything else exactly.
    fn key(&self) -> Key {
        let norm = |s: Option<&str>| s.map(normalize_escapes);
        let body = match &self.shape {
            Shape::Opaque => KeyBody::Opaque(normalize_escapes(self.raw_scheme_specific_part())),
            Shape::Hierarchical {
                authority, query, ..
            } => {
                let authority = authority.as_ref().map(|a| match &a.server {
                    Some(server) => KeyAuthority::Server {
                        user_info: norm(server.user_info.map(|r| self.slice(r))),
                        host: self.slice(server.host).to_ascii_lowercase(),
                        port: server.port,
                    },
                    None => KeyAuthority::Registry(normalize_escapes(self.slice(a.range))),
                });
                KeyBody::Hierarchical {
                    authority,
                    path: norm(self.raw_path()).unwrap_or_default(),
                    query: norm(query.map(|r| self.slice(r))),
                }
            }
        };
        Key {
            scheme: self.scheme().map(str::to_ascii_lowercase),
            fragment: norm(self.raw_fragment()),
            body,
        }
    }
}

#[derive(PartialEq, Eq, Hash)]
struct Key {
    scheme: Option<String>,
    fragment: Option<String>,
    body: KeyBody,
}

#[derive(PartialEq, Eq, Hash)]
enum KeyBody {
    Opaque(String),
    Hierarchical {
        authority: Option<KeyAuthority>,
        path: String,
        query: Option<String>,
    },
}

#[derive(PartialEq, Eq, Hash)]
enum KeyAuthority {
    Server {
        user_info: Option<String>,
        host: String,
        port: Option<u32>,
    },
    Registry(String),
}

impl PartialEq for Uri {
    /// Behavior: java.net.URI#equals
    fn eq(&self, other: &Uri) -> bool {
        self.key() == other.key()
    }
}

impl Eq for Uri {}

impl Hash for Uri {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key().hash(state);
    }
}

impl fmt::Display for Uri {
    /// Behavior: java.net.URI#toString
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.string)
    }
}

impl std::str::FromStr for Uri {
    type Err = UriSyntaxError;

    fn from_str(s: &str) -> Result<Uri, UriSyntaxError> {
        Uri::parse(s)
    }
}

/// Behavior: java.net.URI#URI(String)
fn parse(s: &str) -> Result<Uri, Fail> {
    let n = s.len();
    let bytes = s.as_bytes();
    let mut scheme = None;
    let mut p = 0;
    if let Some(colon) = find_any(s, 0, n, b":/?#").filter(|&i| bytes[i] == b':') {
        if colon == 0 {
            return fail("Expected scheme name", 0);
        }
        for (i, c) in s[..colon].char_indices() {
            let ok = if i == 0 {
                c.is_ascii_alphabetic()
            } else {
                c.is_ascii_alphanumeric() || matches!(c, '+' | '-' | '.')
            };
            if !ok {
                return fail("Illegal character in scheme name", i);
            }
        }
        scheme = Some((0, colon));
        p = colon + 1;
        if p == n || bytes[p] == b'#' {
            return fail("Expected scheme-specific part", p);
        }
    }
    let ssp_start = p;
    let ssp_end = find_any(s, p, n, b"#").unwrap_or(n);

    let shape = if scheme.is_some() && bytes[p] != b'/' {
        check_chars(
            s,
            p,
            ssp_end,
            Class::Uric,
            "Illegal character in opaque part",
        )?;
        Shape::Opaque
    } else {
        let mut authority = None;
        if s[p..].starts_with("//") {
            p += 2;
            let end = find_any(s, p, n, b"/?#").unwrap_or(n);
            if end == p {
                if p == n {
                    return fail("Expected authority", p);
                }
            } else {
                authority = Some(authority::parse(s, p, end)?);
            }
            p = end;
        }
        let path_end = find_any(s, p, n, b"?#").unwrap_or(n);
        check_chars(s, p, path_end, Class::Path, "Illegal character in path")?;
        let path = (p, path_end);
        let mut query = None;
        if bytes.get(path_end) == Some(&b'?') {
            let end = ssp_end;
            check_chars(
                s,
                path_end + 1,
                end,
                Class::Uric,
                "Illegal character in query",
            )?;
            query = Some((path_end + 1, end));
        }
        Shape::Hierarchical {
            authority,
            path,
            query,
        }
    };

    let mut fragment = None;
    if ssp_end < n {
        check_chars(
            s,
            ssp_end + 1,
            n,
            Class::Uric,
            "Illegal character in fragment",
        )?;
        fragment = Some((ssp_end + 1, n));
    }
    Ok(Uri {
        string: s.to_owned(),
        scheme,
        ssp: (ssp_start, ssp_end),
        fragment,
        shape,
    })
}
