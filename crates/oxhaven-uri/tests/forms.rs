//! One test per form and per kind of rejection, with values recorded from Deephaven 42.9 (the
//! same inputs are in `tasks/13.2a-uri/java-42.9.jsonl`, which `java_fixture.rs` checks in
//! full). These read as the specification of what 13.2b builds on.

use oxhaven_uri::{
    ApplicationUri, CustomUri, DeephavenTarget, FieldUri, QueryScopeUri, RemoteUri, StructuredUri,
    Uri, UriError,
};

fn message<T: std::fmt::Debug>(r: Result<T, UriError>) -> String {
    r.unwrap_err().to_string()
}

#[test]
fn local_forms() {
    let q = QueryScopeUri::parse("dh:///scope/my_table").unwrap();
    assert_eq!(q.variable_name(), "my_table");
    assert_eq!(q.to_string(), "dh:///scope/my_table");
    // Without an authority at all, as Java's path-only form.
    assert_eq!(
        QueryScopeUri::parse("dh:/scope/x").unwrap().to_string(),
        "dh:///scope/x"
    );

    let a = ApplicationUri::parse("dh:///app/my_app/field/my_table").unwrap();
    assert_eq!((a.application_id(), a.field_name()), ("my_app", "my_table"));
    assert_eq!(a.to_string(), "dh:///app/my_app/field/my_table");

    let f = FieldUri::parse("dh:///field/f").unwrap();
    assert_eq!(f.to_string(), "dh:///field/f");
}

#[test]
fn local_forms_need_the_dh_scheme_and_nothing_else() {
    assert_eq!(
        message(QueryScopeUri::parse("dh+plain:///scope/x")),
        "Invalid query scope URI 'dh+plain:///scope/x'"
    );
    assert_eq!(
        message(QueryScopeUri::parse("DH:///scope/x")),
        "Invalid query scope URI 'DH:///scope/x'"
    );
    assert_eq!(
        message(QueryScopeUri::parse("dh:///scope/x?q")),
        "Invalid query scope URI 'dh:///scope/x?q'"
    );
    assert_eq!(
        message(QueryScopeUri::parse("dh:///scope/")),
        "Invalid query scope URI 'dh:///scope/'"
    );
    // A line terminator is not matched by the pattern's `.`.
    assert_eq!(
        message(QueryScopeUri::parse("dh:///scope/a%0Ab")),
        "Invalid query scope URI 'dh:///scope/a%0Ab'"
    );
}

#[test]
fn names_must_be_url_safe_after_decoding() {
    assert_eq!(
        message(QueryScopeUri::parse("dh:///scope/a%20b")),
        "Invalid variable name 'a b'"
    );
    assert_eq!(
        message(QueryScopeUri::parse("dh:///scope/x/")),
        "Invalid variable name 'x/'"
    );
    assert_eq!(
        message(QueryScopeUri::parse("dh:///scope/a~b")),
        "Invalid variable name 'a~b'"
    );
    assert_eq!(
        QueryScopeUri::parse("dh:///scope/%41")
            .unwrap()
            .variable_name(),
        "A"
    );
    // The application id is the greedy group: the last `/field/` splits.
    assert_eq!(
        message(ApplicationUri::parse("dh:///app/a/field/b/field/c")),
        "Invalid application id 'a/field/b'"
    );
    assert_eq!(
        message(ApplicationUri::new("a", "b c")),
        "Invalid field name 'b c'"
    );
    assert_eq!(message(FieldUri::new("é")), "Invalid field name 'é'");
}

#[test]
fn empty_names_are_accepted_by_constructors() {
    assert_eq!(QueryScopeUri::new("").unwrap().to_string(), "dh:///scope/");
    assert_eq!(FieldUri::new("").unwrap().to_string(), "dh:///field/");
}

#[test]
fn targets() {
    let t = DeephavenTarget::parse("dh://host").unwrap();
    assert!(t.is_secure());
    assert_eq!(
        (t.host(), t.port(), t.port_or_default()),
        ("host", None, 443)
    );
    let t = DeephavenTarget::parse("dh+plain://host:42").unwrap();
    assert_eq!(
        (t.is_secure(), t.port(), t.port_or_default()),
        (false, Some(42), 42)
    );
    assert_eq!(
        DeephavenTarget::parse("dh+plain://host")
            .unwrap()
            .port_or_default(),
        10000
    );
    assert_eq!(
        DeephavenTarget::parse("dh://[::1]:5").unwrap().host(),
        "[::1]"
    );
    // An empty port is no port.
    assert_eq!(
        DeephavenTarget::parse("dh://host:").unwrap().to_string(),
        "dh://host"
    );
    // Java's int: any port up to 2^31 - 1.
    assert_eq!(
        DeephavenTarget::parse("dh://host:65536").unwrap().port(),
        Some(65536)
    );
}

#[test]
fn target_rejections() {
    for bad in [
        "dh://host/",
        "dh://host?bad=1",
        "dh://host#bad",
        "dh://user@host",
        "dh:///",
        "host",
        "dh://host:-1",
        "dh://host_name",
        "http://host",
    ] {
        assert_eq!(
            message(DeephavenTarget::parse(bad)),
            format!("Invalid target Deephaven URI '{bad}'")
        );
    }
    assert_eq!(
        message(DeephavenTarget::parse("dh://")),
        "Expected authority at index 5: dh://"
    );
    assert_eq!(
        message(DeephavenTarget::new(true, "host:80", None)),
        "Invalid host 'host:80'"
    );
    assert_eq!(
        message(DeephavenTarget::new(true, "host", Some(-1))),
        "Invalid host 'host'"
    );
    assert_eq!(
        message(DeephavenTarget::new(true, "[::1]", Some(-1))),
        "Illegal character in port number at index 11: dh://[::1]:-1"
    );
    assert_eq!(
        message(DeephavenTarget::new(true, "", None)),
        "Expected authority at index 5: dh://"
    );
}

#[test]
fn remote_forms() {
    for s in [
        "dh://host/scope/x",
        "dh+plain://host:10000/app/a/field/b",
        "dh://host/field/f",
        "dh://[::1]:8080/scope/x",
        "dh://gateway?uri=dh://host/scope/variable",
        "dh://gateway-1?uri=dh+plain://gateway-2?uri=dh://host/field/fieldName",
        "dh://gateway?uri=parquet%3A%2F%2F%2Fdata%2Ftest.parquet",
    ] {
        let r = RemoteUri::parse(s).unwrap();
        assert_eq!(r.to_string(), s);
        assert_eq!(RemoteUri::parse(&r.to_string()).unwrap(), r);
    }
    let r = RemoteUri::parse("dh://gw?uri=dh://h/field/f").unwrap();
    assert_eq!(r.target().host(), "gw");
    let StructuredUri::Remote(inner) = r.uri() else {
        panic!()
    };
    assert_eq!(inner.target().host(), "h");
    assert_eq!(
        inner.uri(),
        &StructuredUri::Field(FieldUri::new("f").unwrap())
    );
}

#[test]
fn proxied_custom_uris_are_url_encoded() {
    let gateway = DeephavenTarget::new(true, "host", None).unwrap();
    let custom =
        CustomUri::parse("some-protocol://user@some-host:15/some-path?someQuery=ok#myfragment")
            .unwrap();
    assert_eq!(
        StructuredUri::from(custom).target(gateway).to_string(),
        "dh://host?uri=some-protocol%3A%2F%2Fuser%40some-host%3A15%2Fsome-path%3FsomeQuery%3Dok%23myfragment"
    );
    // An unencoded custom inner URI is read, and written back encoded.
    let r = RemoteUri::parse("dh://gateway?uri=custom:///foo").unwrap();
    assert_eq!(r.to_string(), "dh://gateway?uri=custom%3A%2F%2F%2Ffoo");
}

#[test]
fn remote_rejections() {
    // An encoded dh URI that is not a remote form is no custom URI either.
    assert_eq!(
        message(RemoteUri::parse(
            "dh://host?uri=dh%3A%2F%2Fuser%40h%2Fscope%2Fx"
        )),
        "Invalid custom URI 'dh://user@h/scope/x'"
    );
    assert_eq!(
        message(RemoteUri::parse("dh://gw?uri=a%20b")),
        "Illegal character in path at index 1: a b"
    );
    assert_eq!(
        message(RemoteUri::parse("dh://gw?uri=")),
        "Invalid remote Deephaven URI 'dh://gw?uri='"
    );
    assert_eq!(
        message(RemoteUri::parse("dh://gw?uri=dh://host/scope/x#frag")),
        "Invalid remote Deephaven URI 'dh://gw?uri=dh://host/scope/x#frag'"
    );
    assert_eq!(
        message(RemoteUri::parse("dh://user@host/scope/x")),
        "Invalid remote Deephaven URI 'dh://user@host/scope/x'"
    );
    assert_eq!(
        message(RemoteUri::parse("dh://host/scope/a/b")),
        "Invalid variable name 'a/b'"
    );
}

#[test]
fn a_host_java_cannot_read_makes_a_local_uri() {
    // Java reads these authorities as registry-based, which have no host; the URI is then a
    // local path and the authority is dropped (tasks/13.2a-uri.md, "Java behaviour kept").
    for s in [
        "dh://host_name/scope/x",
        "dh://host:-1/scope/x",
        "dh://é.com/scope/x",
        "dh://h:80:80/scope/x",
    ] {
        assert_eq!(
            QueryScopeUri::parse(s).unwrap().to_string(),
            "dh:///scope/x"
        );
        assert_eq!(
            message(RemoteUri::parse(s)),
            format!("Invalid remote Deephaven URI '{s}'")
        );
    }
}

#[test]
fn syntax_errors_are_javas() {
    let cases = [
        ("dh:", "Expected scheme-specific part at index 3: dh:"),
        (":foo", "Expected scheme name at index 0: :foo"),
        (
            "1x://a",
            "Illegal character in scheme name at index 0: 1x://a",
        ),
        (
            "dh:///scope/a b",
            "Illegal character in path at index 13: dh:///scope/a b",
        ),
        (
            "dh:///scope/%zz",
            "Malformed escape pair at index 12: dh:///scope/%zz",
        ),
        (
            "x:a b",
            "Illegal character in opaque part at index 3: x:a b",
        ),
        (
            "x://h/p?q=a b",
            "Illegal character in query at index 11: x://h/p?q=a b",
        ),
        (
            "x://h/p#a#b",
            "Illegal character in fragment at index 9: x://h/p#a#b",
        ),
        (
            "dh://ho st/scope/x",
            "Illegal character in authority at index 7: dh://ho st/scope/x",
        ),
        (
            "x://h]/p",
            "Illegal character in hostname at index 5: x://h]/p",
        ),
        (
            "x://[::1]:1x/p",
            "Illegal character in port number at index 11: x://[::1]:1x/p",
        ),
        (
            "x://[::1]:99999999999",
            "Malformed port number at index 10: x://[::1]:99999999999",
        ),
        (
            "dh://[::1/scope/x",
            "Expected closing bracket for IPv6 address at index 9: dh://[::1/scope/x",
        ),
        (
            "x://[1:2:3]/p",
            "IPv6 address too short at index 5: x://[1:2:3]/p",
        ),
        ("x://[::%]/p", "scope id expected: x://[::%]/p"),
        // Positions count UTF-16 units.
        (
            "x://h/\u{1F600} b",
            "Illegal character in path at index 8: x://h/\u{1F600} b",
        ),
    ];
    for (input, expected) in cases {
        assert_eq!(
            Uri::parse(input).unwrap_err().to_string(),
            expected,
            "{input}"
        );
    }
}

#[test]
fn uri_equality_follows_java() {
    let eq = |a: &str, b: &str| Uri::parse(a).unwrap() == Uri::parse(b).unwrap();
    assert!(eq("X://H/a", "x://h/a"));
    assert!(eq("x://h/%2f", "x://h/%2F"));
    assert!(!eq("x://h/%41", "x://h/A"));
    assert!(!eq("x://h/A", "x://h/a"));
    assert!(!eq("x://h", "x://h/"));
    // Targets compare their host exactly.
    assert_ne!(
        RemoteUri::parse("dh://HOST/scope/x").unwrap(),
        RemoteUri::parse("dh://host/scope/x").unwrap()
    );
}
