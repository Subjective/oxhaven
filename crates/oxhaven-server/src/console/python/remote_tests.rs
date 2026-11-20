//! `deephaven.uri` and `deephaven.barrage` against a second server of this crate, over a
//! socket.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use oxhaven_contracts::graph::UpdateGraph;
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_engine::table::in_memory_table;
use oxhaven_testkit::{definition, diff_tables, ints, strings, ScriptedTable, TableChange};
use oxhaven_uri::Uri;

use super::tests::Python;
use crate::session::ExportValue;
use crate::test_support::TestServer;

const PATIENCE: Duration = Duration::from_secs(20);

async fn eventually(what: &str, mut done: impl FnMut() -> bool) {
    let start = Instant::now();
    while !done() {
        assert!(start.elapsed() < PATIENCE, "never: {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// The remote server's tables: `ticking` (a scope variable that the test changes), `fixed`
/// (a scope variable and the field `fixed` of the application `my_app`).
struct Remote {
    server: TestServer,
    ticking: Arc<ScriptedTable>,
    fixed: Arc<dyn Table>,
}

async fn remote() -> Remote {
    let server = TestServer::start().await;
    let graph: Arc<dyn UpdateGraph> = server.state.graph.clone();
    let shape = definition(&[("K", ColumnType::Int), ("S", ColumnType::String)]);
    let ticking = ScriptedTable::new(
        graph,
        shape,
        RowSet::from_range(0, 1),
        vec![ints([1, 2]), strings(["a", "b"])],
    );
    let schema = Schema::new(vec![Field::new("A", DataType::Int32, true)]);
    let column = Int32Array::from(vec![Some(7), None]);
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(column)]).unwrap();
    let fixed: Arc<dyn Table> =
        in_memory_table(definition(&[("A", ColumnType::Int)]), &[batch]).unwrap();
    let scope = &server.state.scope;
    scope
        .put("ticking", ExportValue::Table(ticking.table()))
        .unwrap();
    scope.put("fixed", ExportValue::Table(fixed.clone())).unwrap();
    let app = server.state.applications.create("my_app", "My app").unwrap();
    app.set_field("fixed", ExportValue::Table(fixed.clone()))
        .unwrap();
    Remote {
        server,
        ticking,
        fixed,
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_tables_are_resolved_subscribed_and_snapshotted() {
    let remote = remote().await;
    let python = Python::start().await;
    let port = remote.server.address.port();
    python.run(&format!(
        r#"
from deephaven.uri import resolve
from deephaven.barrage import barrage_session, BarrageSession
from deephaven.table import Table

base = "dh+plain://127.0.0.1:{port}"
resolved = resolve(base + "/scope/ticking")
assert type(resolved) is Table and resolved.is_refreshing
field = resolve(base + "/app/my_app/field/fixed")
assert field.size == 2

session = barrage_session("127.0.0.1", {port})
assert isinstance(session, BarrageSession)
subscribed = session.subscribe(b"s/ticking")
assert subscribed.is_refreshing and subscribed.size == 2
snapped = session.snapshot(b"s/ticking")
assert not snapped.is_refreshing and snapped.size == 2
# a string ticket is its UTF-8 bytes
assert session.snapshot("s/ticking").size == 2

with barrage_session("127.0.0.1", {port}, auth_type="Anonymous", extra_headers={{"x-test": "1"}}) as other:
    from_field = other.snapshot(b"a/my_app/f/fixed")
assert from_field.size == 2

# a local value is what the variable holds; a missing one is None
local_value = [1, 2]
assert resolve("dh:///scope/local_value") is local_value
assert resolve("dh:///scope/missing") is None
assert resolve("dh:///app/missing/field/f") is None
# owner question 7.23: a host Java cannot read makes the URI local
assert resolve("dh://host_name/scope/local_value") is local_value
"#
    ));
    let ticking = remote.ticking.table();
    for name in ["resolved", "subscribed", "snapped"] {
        let copy = python.table(name);
        assert_eq!(diff_tables(ticking.as_ref(), copy.as_ref()), None, "{name}");
    }
    for name in ["field", "from_field"] {
        let copy = python.table(name);
        assert_eq!(diff_tables(remote.fixed.as_ref(), copy.as_ref()), None, "{name}");
    }

    // both subscriptions tick; the snapshot does not
    remote
        .ticking
        .push(TableChange::new().add([2], vec![ints([3]), strings(["c"])]));
    remote.server.state.graph.run_cycle();
    let resolved = python.table("resolved");
    let subscribed = python.table("subscribed");
    eventually("the subscriptions have the new row", || {
        [&resolved, &subscribed]
            .iter()
            .all(|copy| diff_tables(ticking.as_ref(), copy.as_ref()).is_none())
    })
    .await;
    assert_eq!(python.table("snapped").row_set().size(), 2);
    python.run("assert resolved.size == 3 and subscribed.size == 3 and snapped.size == 2");

    // closing the session fails its tables, not those of the resolver's own session
    python.run("session.close()\nsession.close()");
    eventually("the closed session's table fails", || subscribed.is_failed()).await;
    assert!(!resolved.is_failed());
    python.run("assert subscribed.is_failed and not resolved.is_failed");
}

/// The text after `DHError: <message> : ` of the error `code` raises.
fn raised(python: &Python, code: &str, message: &str) -> String {
    let code = format!(
        "from deephaven.uri import resolve\nfrom deephaven.barrage import barrage_session\n{code}"
    );
    let error = python.console.evaluate(&code).error.expect("the code fails");
    let last = error.trim_end().lines().last().unwrap().to_string();
    let prefix = format!("deephaven.dherror.DHError: {message} : ");
    last.strip_prefix(&prefix)
        .unwrap_or_else(|| panic!("not a DHError of {message}: {last}"))
        .to_string()
}

#[tokio::test(flavor = "multi_thread")]
async fn failures_are_dherrors_with_the_references_messages() {
    let remote = remote().await;
    let python = Python::start().await;
    let port = remote.server.address.port();
    let resolving = |uri: &str| raised(&python, &format!("resolve({uri:?})"), RESOLVE);
    const RESOLVE: &str = "failed to resolve the URI.";
    const SESSION: &str = "failed to get a barrage session to the target remote Deephaven server.";

    assert_eq!(
        resolving("dh+plain:///scope/x"),
        "java.lang.UnsupportedOperationException: Unable to find resolver for uri \
         'dh+plain:///scope/x'"
    );
    assert_eq!(
        resolving("dh:///scope/a b"),
        "java.lang.IllegalArgumentException: Illegal character in path at index 13: \
         dh:///scope/a b"
    );
    assert_eq!(
        raised(&python, "resolve(5)", RESOLVE),
        "no matching Java method overloads found"
    );
    assert_eq!(
        resolving("dh:///scope/a%2Fb"),
        "java.lang.IllegalArgumentException: Invalid variable name 'a/b'"
    );
    assert_eq!(
        resolving(&format!(
            "dh+plain://127.0.0.1:{port}?uri=dh%2Bplain%3A%2F%2Fo%2Fscope%2Fx"
        )),
        "java.lang.UnsupportedOperationException: Proxying not supported yet, see \
         https://github.com/deephaven/deephaven-core/issues/1483"
    );
    const TLS: &str = "TLS connections to remote servers are not ported yet: \
                       io.deephaven.client.impl.ChannelHelper#channelBuilder \
                       (useTransportSecurity)";
    assert_eq!(resolving(&format!("dh://127.0.0.1:{port}/scope/fixed")), TLS);
    let missing = resolving(&format!("dh+plain://127.0.0.1:{port}/scope/missing"));
    assert!(missing.starts_with("NotFound: "), "{missing}");

    let session = |arguments: &str| {
        raised(&python, &format!("barrage_session({arguments})"), SESSION)
    };
    assert_eq!(session(&format!("'127.0.0.1', {port}, use_tls=True")), TLS);
    assert_eq!(
        session(&format!("'127.0.0.1', {port}, tls_root_certs=b'pem'")),
        "tls_root_certs is provided but use_tls is False"
    );
    assert_eq!(
        session("'my_host'"),
        "java.lang.IllegalArgumentException: Invalid target Deephaven URI \
         'dh+plain://my_host:10000'"
    );
    let unparsable = Uri::parse("dh+plain://a b:10000").unwrap_err();
    assert_eq!(
        session("'a b'"),
        format!("java.net.URISyntaxException: {unparsable}")
    );

    let subscribing = raised(
        &python,
        &format!("barrage_session('127.0.0.1', {port}).subscribe(b's/missing')"),
        "failed to subscribe to the remote table with the provided ticket.",
    );
    assert!(subscribing.starts_with("NotFound: "), "{subscribing}");
    let closed = raised(
        &python,
        &format!("s = barrage_session('127.0.0.1', {port})\ns.close()\ns.snapshot(b's/fixed')"),
        "failed to take a snapshot of the remote table with the provided ticket.",
    );
    assert_eq!(closed, "the Barrage session is closed");
}
