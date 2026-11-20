//! Compares every answer Deephaven 42.9 gave in `tasks/13.2a-uri/java-42.9.jsonl` (written by
//! `tasks/13.2a-uri/probe.py`) with this crate's.

use oxhaven_uri::{
    is_local_path, is_remote_path, is_remote_query, is_remote_target, is_uri_safe, ApplicationUri,
    CustomUri, DeephavenTarget, FieldUri, QueryScopeUri, RemoteUri, StructuredUri, Uri,
};
use serde_json::{json, Value};
use std::collections::hash_map::DefaultHasher;
use std::fmt::Display;
use std::hash::{Hash, Hasher};

const FIXTURE: &str = include_str!("../../../tasks/13.2a-uri/java-42.9.jsonl");

fn opt(s: Option<impl Into<String>>) -> Value {
    s.map_or(Value::Null, |s| Value::String(s.into()))
}

fn target_desc(t: &DeephavenTarget) -> Value {
    json!({
        "secure": t.is_secure(),
        "host": t.host(),
        "port": t.port(),
        "string": t.to_string(),
    })
}

fn desc(u: &StructuredUri) -> Value {
    let mut v = match u {
        StructuredUri::QueryScope(q) => json!({"kind": "scope", "name": q.variable_name()}),
        StructuredUri::Application(a) => {
            json!({"kind": "app", "app": a.application_id(), "field": a.field_name()})
        }
        StructuredUri::Field(f) => json!({"kind": "field", "name": f.field_name()}),
        StructuredUri::Remote(r) => {
            json!({"kind": "remote", "target": target_desc(r.target()), "inner": desc(r.uri())})
        }
        StructuredUri::Custom(c) => json!({"kind": "custom", "uri": c.uri().to_string()}),
    };
    v["string"] = Value::String(u.to_string());
    v
}

fn components(u: &Uri) -> Value {
    json!({
        "string": u.as_str(),
        "scheme": opt(u.scheme()),
        "opaque": u.is_opaque(),
        "raw_ssp": u.raw_scheme_specific_part(),
        "ssp": u.scheme_specific_part(),
        "raw_authority": opt(u.raw_authority()),
        "authority": opt(u.authority()),
        "raw_user_info": opt(u.raw_user_info()),
        "user_info": opt(u.user_info()),
        "host": opt(u.host()),
        "port": u.port().map_or(-1, i64::from),
        "raw_path": opt(u.raw_path()),
        "path": opt(u.path()),
        "raw_query": opt(u.raw_query()),
        "query": opt(u.query()),
        "raw_fragment": opt(u.raw_fragment()),
        "fragment": opt(u.fragment()),
    })
}

fn err(e: impl Display) -> Value {
    json!({"err": {"class": "java.lang.IllegalArgumentException", "message": e.to_string()}})
}

fn result<T, E: Display>(r: Result<T, E>, ok: impl FnOnce(T) -> Value) -> Value {
    match r {
        Ok(v) => json!({ "ok": ok(v) }),
        Err(e) => err(e),
    }
}

struct Check {
    failures: Vec<String>,
    compared: usize,
}

impl Check {
    fn eq(&mut self, what: &str, input: &Value, ours: Value, java: &Value) {
        self.compared += 1;
        if &ours != java {
            self.failures.push(format!(
                "{what} of {input}:\n    ours: {ours}\n    java: {java}"
            ));
        }
    }
}

/// `x.toString()` parsed again gives an equal value; an error is recorded as Java's would be.
fn roundtrip<T: PartialEq + Display, E: Display>(
    value: &T,
    parse: impl Fn(&str) -> Result<T, E>,
) -> Value {
    result(parse(&value.to_string()), |again| json!(&again == value))
}

fn hash_of(u: &Uri) -> u64 {
    let mut h = DefaultHasher::new();
    u.hash(&mut h);
    h.finish()
}

fn check_parse(c: &mut Check, case: &Value) {
    let input_value = &case["input"];
    let input = input_value.as_str().unwrap();
    let uri = match Uri::parse(input) {
        Ok(uri) => uri,
        Err(e) => {
            c.eq("URI.create", input_value, err(e), &case["uri"]);
            return;
        }
    };
    let mut java_components = case["uri"]["ok"].clone();
    if java_components.is_null() {
        c.eq(
            "URI.create",
            input_value,
            json!({"ok": components(&uri)}),
            &case["uri"],
        );
        return;
    }
    java_components.as_object_mut().unwrap().remove("ascii");
    c.eq(
        "URI components",
        input_value,
        components(&uri),
        &java_components,
    );

    c.eq(
        "target well-formed",
        input_value,
        json!(DeephavenTarget::is_well_formed(&uri)),
        &case["target_well_formed"],
    );
    let target = DeephavenTarget::from_uri(&uri);
    c.eq(
        "DeephavenTarget.of",
        input_value,
        result(target.clone(), |t| target_desc(&t)),
        &case["target"],
    );
    if let Ok(t) = &target {
        c.eq(
            "target round trip",
            input_value,
            roundtrip(t, DeephavenTarget::parse),
            &case["target_roundtrip"],
        );
    }

    c.eq(
        "scope well-formed",
        input_value,
        json!(QueryScopeUri::is_well_formed(&uri)),
        &case["scope_well_formed"],
    );
    let scope = QueryScopeUri::from_uri(&uri);
    c.eq(
        "QueryScopeUri.of",
        input_value,
        result(scope.clone(), |u| desc(&u.into())),
        &case["scope"],
    );
    if let Ok(u) = &scope {
        c.eq(
            "scope round trip",
            input_value,
            roundtrip(u, QueryScopeUri::parse),
            &case["scope_roundtrip"],
        );
    }

    c.eq(
        "app well-formed",
        input_value,
        json!(ApplicationUri::is_well_formed(&uri)),
        &case["app_well_formed"],
    );
    let app = ApplicationUri::from_uri(&uri);
    c.eq(
        "ApplicationUri.of",
        input_value,
        result(app.clone(), |u| desc(&u.into())),
        &case["app"],
    );
    if let Ok(u) = &app {
        c.eq(
            "app round trip",
            input_value,
            roundtrip(u, ApplicationUri::parse),
            &case["app_roundtrip"],
        );
    }

    c.eq(
        "field well-formed",
        input_value,
        json!(FieldUri::is_well_formed(&uri)),
        &case["field_well_formed"],
    );
    let field = FieldUri::from_uri(&uri);
    c.eq(
        "FieldUri.of",
        input_value,
        result(field.clone(), |u| desc(&u.into())),
        &case["field"],
    );
    if let Ok(u) = &field {
        c.eq(
            "field round trip",
            input_value,
            roundtrip(u, FieldUri::parse),
            &case["field_roundtrip"],
        );
    }

    c.eq(
        "remote well-formed",
        input_value,
        json!(RemoteUri::is_well_formed(&uri)),
        &case["remote_well_formed"],
    );
    let remote = RemoteUri::from_uri(&uri);
    c.eq(
        "RemoteUri.of",
        input_value,
        result(remote.clone(), |u| desc(&u.into())),
        &case["remote"],
    );
    if let Ok(u) = &remote {
        c.eq(
            "remote round trip",
            input_value,
            roundtrip(u, RemoteUri::parse),
            &case["remote_roundtrip"],
        );
    }

    let custom = CustomUri::new(uri.clone());
    c.eq(
        "CustomUri.of",
        input_value,
        result(custom, |u| desc(&u.into())),
        &case["custom"],
    );

    c.eq(
        "isLocalPath",
        input_value,
        json!(is_local_path(&uri)),
        &case["is_local_path"],
    );
    c.eq(
        "isRemotePath",
        input_value,
        json!(is_remote_path(&uri)),
        &case["is_remote_path"],
    );
    c.eq(
        "isRemoteTarget",
        input_value,
        json!(is_remote_target(&uri)),
        &case["is_remote_target"],
    );
    c.eq(
        "isRemoteQuery",
        input_value,
        json!(is_remote_query(&uri)),
        &case["is_remote_query"],
    );
}

fn check_construct(c: &mut Check, case: &Value) {
    let name = case["name"].as_str().unwrap();
    let ours = match case["kind"].as_str().unwrap() {
        "scope" => result(QueryScopeUri::new(name), |u| desc(&u.into())),
        "field" => result(FieldUri::new(name), |u| desc(&u.into())),
        "app_id" => result(ApplicationUri::new(name, "f"), |u| desc(&u.into())),
        "app_field" => result(ApplicationUri::new("app", name), |u| desc(&u.into())),
        "uri_safe" => json!({"ok": is_uri_safe(name)}),
        other => panic!("unknown kind {other}"),
    };
    c.eq(
        case["kind"].as_str().unwrap(),
        &case["name"],
        ours,
        &case["result"],
    );
}

fn check_target(c: &mut Check, case: &Value) {
    let host = case["host"].as_str().unwrap();
    let port = case["port"].as_i64().map(|p| i32::try_from(p).unwrap());
    let secure = case["secure"].as_bool().unwrap();
    let ours = result(DeephavenTarget::new(secure, host, port), |t| {
        target_desc(&t)
    });
    let input = json!([host, port, secure]);
    c.eq("DeephavenTarget.builder", &input, ours, &case["result"]);
}

fn check_custom_remote(c: &mut Check, case: &Value) {
    let input = case["input"].as_str().unwrap();
    let gateway = DeephavenTarget::new(true, "gateway", None).unwrap();
    let ours = result(CustomUri::parse(input), |custom| {
        let remote = StructuredUri::from(custom).target(gateway);
        json!({"string": remote.to_string(), "roundtrip": roundtrip(&remote, RemoteUri::parse)})
    });
    c.eq("CustomUri.target", &case["input"], ours, &case["result"]);
}

fn check_equal(c: &mut Check, case: &Value) {
    let (a, b) = (case["a"].as_str().unwrap(), case["b"].as_str().unwrap());
    let (ua, ub) = (Uri::parse(a).unwrap(), Uri::parse(b).unwrap());
    let input = json!([a, b]);
    let equal = ua == ub;
    c.eq(
        "URI.equals",
        &input,
        json!(equal),
        &case["uri"]["ok"]["equal"],
    );
    if equal {
        // Equal values must hash alike; Java's hashCode agrees for these pairs too.
        c.eq(
            "URI.hashCode",
            &input,
            json!(hash_of(&ua) == hash_of(&ub)),
            &case["uri"]["ok"]["same_hash"],
        );
    }
    let remote = match (RemoteUri::parse(a), RemoteUri::parse(b)) {
        (Ok(ra), Ok(rb)) => json!({"ok": ra == rb}),
        (Err(e), _) | (_, Err(e)) => err(e),
    };
    c.eq("RemoteUri.equals", &input, remote, &case["remote"]);
}

#[test]
fn every_answer_matches_deephaven_42_9() {
    let mut check = Check {
        failures: Vec::new(),
        compared: 0,
    };
    let mut cases = 0;
    for line in FIXTURE.lines() {
        let case: Value = serde_json::from_str(line).unwrap();
        cases += 1;
        match case["section"].as_str().unwrap() {
            "parse" => check_parse(&mut check, &case),
            "construct" => check_construct(&mut check, &case),
            "target" => check_target(&mut check, &case),
            "custom_remote" => check_custom_remote(&mut check, &case),
            "equal" => check_equal(&mut check, &case),
            other => panic!("unknown section {other}"),
        }
    }
    assert!(cases > 2000, "the fixture has {cases} cases");
    let shown: Vec<_> = check.failures.iter().take(40).cloned().collect();
    assert!(
        check.failures.is_empty(),
        "{} of {} answers differ from Java's:\n{}",
        check.failures.len(),
        check.compared,
        shown.join("\n")
    );
}
