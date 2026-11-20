//! `Uri::from_components`, the constructor the CSV resolver rebuilds a URI with. The expected
//! strings are what Deephaven 42.9's JVM answered for `new java.net.URI(scheme, userInfo,
//! host, port, path, query, fragment)` (`tasks/13.2b-uri-python/check.py`, "components"),
//! where an error is Java's `URISyntaxException` message.

use oxhaven_uri::Uri;

fn built(
    scheme: Option<&str>,
    user_info: Option<&str>,
    host: Option<&str>,
    port: Option<u32>,
    path: Option<&str>,
    query: Option<&str>,
    fragment: Option<&str>,
) -> String {
    match Uri::from_components(scheme, user_info, host, port, path, query, fragment) {
        Ok(uri) => uri.to_string(),
        Err(e) => format!("error: {e}"),
    }
}

#[test]
fn a_path_alone_has_no_authority() {
    let file = |path| built(Some("file"), None, None, None, Some(path), None, None);
    assert_eq!(file("/tmp/x.csv"), "file:/tmp/x.csv");
    // a decoded space is quoted again, and so is every `%`
    assert_eq!(file("/tmp/a b.csv"), "file:/tmp/a%20b.csv");
    assert_eq!(file("/tmp/a%41.csv"), "file:/tmp/a%2541.csv");
    assert_eq!(file("/tmp/100%.csv"), "file:/tmp/100%25.csv");
    assert_eq!(file("/a@b:c;d=e/f"), "file:/a@b:c;d=e/f");
    assert_eq!(file("/é/ü"), "file:/é/ü");
    assert_eq!(file("/a?b#c[d]"), "file:/a%3Fb%23c%5Bd%5D");
    assert_eq!(file("/\u{a0}x"), "file:/%C2%A0x");
}

#[test]
fn an_authority_query_and_fragment() {
    assert_eq!(
        built(
            Some("http"),
            Some("me you"),
            Some("host"),
            Some(8080),
            Some("/p"),
            Some("a=b c&[d]"),
            Some("f g"),
        ),
        "http://me%20you@host:8080/p?a=b%20c&[d]#f%20g"
    );
    assert_eq!(
        built(Some("http"), None, Some("::1"), None, Some(""), None, None),
        "http://[::1]"
    );
    assert_eq!(
        built(Some("http"), None, Some("[::1]"), Some(1), Some("/"), None, None),
        "http://[::1]:1/"
    );
}

#[test]
fn what_does_not_parse_is_an_error() {
    // an opaque URI has no path: nothing follows the scheme
    assert_eq!(
        built(Some("file"), None, None, None, None, None, None),
        "error: Expected scheme-specific part at index 5: file:"
    );
    assert_eq!(
        built(Some("file"), None, None, None, Some("x.csv"), None, None),
        "error: Relative path in absolute URI: file:x.csv"
    );
}
