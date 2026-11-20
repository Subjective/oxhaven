//! Choosing a resolver, the errors of Java's resolvers, and resolving against a second
//! server over a socket.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use oxhaven_contracts::graph::{UpdateGraph, UpdateSource};
use oxhaven_contracts::rowset::RowSet;
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use oxhaven_engine::table::in_memory_table;
use oxhaven_testkit::{definition, diff_tables, ints, strings, ScriptedTable, TableChange};

use super::*;
use crate::test_support::TestServer;

/// The resolver chosen for `s`, or the text of the error.
fn chosen(s: &str) -> Result<Resolver, String> {
    let uri = parse(s).map_err(|e| e.to_string())?;
    resolver_for(&uri).map_err(|e| e.to_string())
}

fn none_for(s: &str) -> Result<Resolver, String> {
    Err(format!(
        "java.lang.UnsupportedOperationException: Unable to find resolver for uri '{s}'"
    ))
}

#[test]
fn a_resolver_is_chosen_by_scheme_then_by_shape() {
    use Resolver::*;
    let cases = [
        ("dh:///scope/t", Ok(QueryScope)),
        ("dh:/scope/t", Ok(QueryScope)),
        ("dh:///app/my_app/field/f", Ok(Application)),
        ("dh://host/scope/t", Ok(BarrageTable)),
        ("dh://host:443/app/a/field/f", Ok(BarrageTable)),
        ("dh+plain://host:10000/scope/t", Ok(BarrageTable)),
        ("dh+plain://host/field/f", Ok(BarrageTable)),
        // proxied and custom inner URIs are taken, to be refused with a better error
        (
            "dh+plain://gateway?uri=dh%2Bplain%3A%2F%2Fother%2Fscope%2Fx",
            Ok(BarrageTable),
        ),
        ("dh+plain://gateway?uri=https%3A%2F%2Fexample.com%2Fx", Ok(BarrageTable)),
        // owner question 7.23: no host Java can read, so local
        ("dh://host_name/scope/t", Ok(QueryScope)),
        ("dh://host:99999999999/app/a/field/f", Ok(Application)),
        ("csv:///tmp/x.csv", Ok(Csv)),
        ("csv:x.csv", Ok(Csv)),
        ("csv+https://example.com/x.csv?q=1", Ok(Csv)),
        ("http+csv://example.com/x.csv", Ok(Csv)),
        ("file+csv:/x.csv", Ok(Csv)),
        ("parquet:///tmp/x.parquet", Ok(Parquet)),
        ("parquet:/tmp/x.parquet", Ok(Parquet)),
    ];
    for (s, expected) in cases {
        assert_eq!(chosen(s), expected, "{s}");
    }
    for s in [
        // no local resolver for a field without an application
        "dh:///field/f",
        // `dh+plain` is the Barrage resolver's only, and needs a host
        "dh+plain:///scope/x",
        // schemes are matched exactly
        "DH:///scope/x",
        "Csv:///x.csv",
        "parquet://host/x.parquet",
        "parquet:x.parquet",
        "parquet:///x.parquet?q",
        "dh://host/other/x",
        "dh://host",
        "https://example.com/x",
        // a relative URI has no scheme
        "scope/x",
        "",
    ] {
        assert_eq!(chosen(s), none_for(s), "{s}");
    }
}

#[test]
fn a_string_that_is_no_uri_is_an_illegal_argument() {
    for (s, message) in [
        ("dh:///scope/a b", "Illegal character in path at index 13: dh:///scope/a b"),
        ("dh:", "Expected scheme-specific part at index 3: dh:"),
        ("dh://", "Expected authority at index 5: dh://"),
    ] {
        assert_eq!(
            chosen(s),
            Err(format!("java.lang.IllegalArgumentException: {message}")),
            "{s}"
        );
    }
}

/// The shape is checked when choosing, the names when resolving.
#[tokio::test(flavor = "multi_thread")]
async fn names_are_checked_by_the_resolver_chosen() {
    let local = TestServer::start().await;
    let resolvers = Arc::new(UriResolvers::new(&local.state));
    for (s, message) in [
        ("dh:///scope/a/b", "Invalid variable name 'a/b'"),
        ("dh:///scope/a%20b", "Invalid variable name 'a b'"),
        ("dh:///app/a/field/b%20c", "Invalid field name 'b c'"),
        ("dh+plain://h/scope/a/b", "Invalid variable name 'a/b'"),
    ] {
        assert_eq!(
            resolve(&resolvers, s).await.unwrap_err(),
            format!("java.lang.IllegalArgumentException: {message}"),
            "{s}"
        );
    }
    assert!(resolvers.sessions().is_empty());
}

#[test]
fn an_authority_without_a_host_is_found() {
    let authority = |s: &str| unread_authority(&Uri::parse(s).unwrap()).map(str::to_string);
    assert_eq!(authority("dh://host_name/scope/t").as_deref(), Some("host_name"));
    assert_eq!(authority("dh://h:99999999999/scope/t").as_deref(), Some("h:99999999999"));
    assert_eq!(authority("dh://host/scope/t"), None);
    assert_eq!(authority("dh:///scope/t"), None);
    assert_eq!(authority("dh:/scope/t"), None);
    // Java's answer, kept: the URI is resolved here
    assert!(check_unread_host(&Uri::parse("dh://host_name/scope/t").unwrap()).is_ok());
}

#[test]
fn remote_tickets_and_the_forms_java_does_not_resolve() {
    let ticket = |s: &str| {
        remote_ticket(&RemoteUri::parse(s).unwrap())
            .map(|t| String::from_utf8(t).unwrap())
            .map_err(|e| e.to_string())
    };
    assert_eq!(ticket("dh+plain://h/scope/t").unwrap(), "s/t");
    assert_eq!(ticket("dh+plain://h/app/my.app/field/f").unwrap(), "a/my.app/f/f");
    // the target's host is the application id
    assert_eq!(ticket("dh://server.example/field/f").unwrap(), "a/server.example/f/f");
    assert_eq!(
        ticket("dh+plain://g?uri=dh%2Bplain%3A%2F%2Fother%2Fscope%2Fx").unwrap_err(),
        "java.lang.UnsupportedOperationException: Proxying not supported yet, see \
         https://github.com/deephaven/deephaven-core/issues/1483"
    );
    assert_eq!(
        ticket("dh+plain://g?uri=https%3A%2F%2Fexample.com%2Fx").unwrap_err(),
        "java.lang.UnsupportedOperationException: Remote custom URIs not supported yet, see \
         https://github.com/deephaven/deephaven-core/issues/1483"
    );
}

#[test]
fn a_csv_uri_is_rebuilt_as_a_file_or_web_url() {
    let rebuilt = |s: &str| csv_string(&Uri::parse(s).unwrap()).map_err(|e| e.to_string());
    assert_eq!(rebuilt("csv:///tmp/x.csv").unwrap(), "file:/tmp/x.csv");
    assert_eq!(rebuilt("csv:/tmp/x.csv").unwrap(), "file:/tmp/x.csv");
    assert_eq!(rebuilt("csv+file:///tmp/x.csv").unwrap(), "file:/tmp/x.csv");
    // decoded, then quoted again: Java reads the file `/tmp/a%20b.csv`
    assert_eq!(rebuilt("csv:///tmp/a%20b.csv").unwrap(), "file:/tmp/a%20b.csv");
    assert_eq!(rebuilt("csv:///tmp/%C3%A9.csv").unwrap(), "file:/tmp/é.csv");
    assert_eq!(
        rebuilt("csv+https://user@example.com:8443/a%20b.csv?q=1#f").unwrap(),
        "https://user@example.com:8443/a%20b.csv?q=1#f"
    );
    assert_eq!(rebuilt("csv://host/x.csv").unwrap(), "file://host/x.csv");
    // an authority without a host is dropped (owner question 7.23)
    assert_eq!(rebuilt("csv://host_name/x.csv").unwrap(), "file:/x.csv");
    // an opaque URI has no path
    assert_eq!(
        rebuilt("csv:x.csv").unwrap_err(),
        "java.lang.IllegalArgumentException: java.net.URISyntaxException: Expected \
         scheme-specific part at index 5: file:"
    );
}

#[test]
fn a_target_without_a_port_has_its_schemes_default() {
    let config = |s: &str| client_config(&DeephavenTarget::parse(s).unwrap());
    let plain = config("dh+plain://h").unwrap();
    assert_eq!((plain.host.as_str(), plain.port, plain.use_tls), ("h", 10000, false));
    let secure = config("dh://h").unwrap();
    assert_eq!((secure.port, secure.use_tls), (443, true));
    assert_eq!(config("dh+plain://[::1]:7").unwrap().host, "[::1]");
    assert_eq!(config("dh+plain://h").unwrap().authorization, "Anonymous");
    let refused = config("dh+plain://h:65536").unwrap_err().to_string();
    assert_eq!(
        refused,
        "invalid client configuration: port 65536 of dh+plain://h:65536 is out of range"
    );
}

const PATIENCE: Duration = Duration::from_secs(20);

/// Runs `done` until it is true, failing the test with `what` after a while.
async fn eventually(what: &str, mut done: impl FnMut() -> bool) {
    let start = Instant::now();
    while !done() {
        assert!(start.elapsed() < PATIENCE, "never: {what}");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

/// A port nothing listens on.
async fn closed_port() -> u16 {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    listener.local_addr().unwrap().port()
}

/// Resolves on a blocking thread, as a script's thread waits.
async fn resolve(resolvers: &Arc<UriResolvers>, s: &str) -> Result<Option<ExportValue>, String> {
    let resolvers = resolvers.clone();
    let s = s.to_string();
    tokio::task::spawn_blocking(move || resolvers.resolve(&s).map_err(|e| e.to_string()))
        .await
        .unwrap()
}

/// A static table of one `int` column `A`.
fn fixed(values: &[Option<i32>]) -> Arc<dyn Table> {
    let schema = Schema::new(vec![Field::new("A", DataType::Int32, true)]);
    let column = Int32Array::from(values.to_vec());
    let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(column)]).unwrap();
    in_memory_table(definition(&[("A", ColumnType::Int)]), &[batch]).unwrap()
}

fn table_of(value: Option<ExportValue>) -> Arc<dyn Table> {
    value.expect("a value").as_table().expect("a table").clone()
}

#[tokio::test(flavor = "multi_thread")]
async fn local_variables_and_fields_resolve_and_missing_ones_are_none() {
    let local = TestServer::start().await;
    let resolvers = Arc::new(UriResolvers::new(&local.state));
    let x = fixed(&[Some(1)]);
    local
        .state
        .scope
        .put("x", ExportValue::Table(x.clone()))
        .unwrap();
    let app = local.state.applications.create("my_app", "My app").unwrap();
    app.set_field("f", ExportValue::Table(x.clone())).unwrap();

    for s in ["dh:///scope/x", "dh:/scope/x", "dh://host_name/scope/x"] {
        let found = table_of(resolve(&resolvers, s).await.unwrap());
        assert!(Arc::ptr_eq(&found, &x), "{s}");
    }
    let field = table_of(resolve(&resolvers, "dh:///app/my_app/field/f").await.unwrap());
    assert!(Arc::ptr_eq(&field, &x));
    for s in [
        "dh:///scope/missing",
        "dh:///app/my_app/field/missing",
        "dh:///app/other/field/f",
    ] {
        assert!(resolve(&resolvers, s).await.unwrap().is_none(), "{s}");
    }
    assert_eq!(
        resolve(&resolvers, "dh:///field/f").await.unwrap_err(),
        "java.lang.UnsupportedOperationException: Unable to find resolver for uri \
         'dh:///field/f'"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_tables_are_subscribed_through_one_session_per_target() {
    let remote = TestServer::start().await;
    let local = TestServer::start().await;
    let resolvers = Arc::new(UriResolvers::new(&local.state));
    let port = remote.address.port();

    let shape = definition(&[("K", ColumnType::Int), ("S", ColumnType::String)]);
    let graph: Arc<dyn UpdateGraph> = remote.state.graph.clone();
    let scripted = ScriptedTable::new(
        graph,
        shape.clone(),
        RowSet::from_range(0, 1),
        vec![ints([1, 2]), strings(["a", "b"])],
    );
    let ticking = scripted.table();
    remote
        .state
        .scope
        .put("ticking", ExportValue::Table(ticking.clone()))
        .unwrap();
    let fixed = fixed(&[Some(7), None]);
    let app = remote.state.applications.create("my_app", "My app").unwrap();
    app.set_field("fixed", ExportValue::Table(fixed.clone())).unwrap();
    // `dh+plain://<host>/field/<name>` names a field of the application whose id is the host
    let by_host = remote.state.applications.create("127.0.0.1", "By host").unwrap();
    by_host
        .set_field("fixed", ExportValue::Table(fixed.clone()))
        .unwrap();

    let base = format!("dh+plain://127.0.0.1:{port}");
    let copy = table_of(resolve(&resolvers, &format!("{base}/scope/ticking")).await.unwrap());
    assert!(copy.is_refreshing());
    assert_eq!(diff_tables(ticking.as_ref(), copy.as_ref()), None);
    for path in ["app/my_app/field/fixed", "field/fixed"] {
        let field = table_of(resolve(&resolvers, &format!("{base}/{path}")).await.unwrap());
        assert_eq!(diff_tables(fixed.as_ref(), field.as_ref()), None, "{path}");
    }
    assert_eq!(resolvers.sessions().len(), 1);

    // the copy ticks: the remote change wakes this server's graph, which has no timer
    scripted.push(TableChange::new().add([2], vec![ints([3]), strings(["c"])]));
    remote.state.graph.run_cycle();
    eventually("the copy has the new row", || {
        copy.row_set().size() == 3 && diff_tables(ticking.as_ref(), copy.as_ref()).is_none()
    })
    .await;
    assert!(!copy.is_failed());

    // the remote server's own error, through the client
    let missing = resolve(&resolvers, &format!("{base}/scope/missing")).await;
    assert!(missing.unwrap_err().starts_with("NotFound: "));
    // another spelling of the same server is another target, as in Java
    resolve(&resolvers, &format!("dh+plain://localhost:{port}/scope/ticking"))
        .await
        .ok();
    assert!(resolvers.sessions().len() >= 1);
}

#[tokio::test(flavor = "multi_thread")]
async fn remote_failures_come_before_or_without_a_session() {
    let local = TestServer::start().await;
    let resolvers = Arc::new(UriResolvers::new(&local.state));
    let port = closed_port().await;
    // refused before any connection: a connection to this port would fail otherwise
    for (inner, message) in [
        (
            "dh%2Bplain%3A%2F%2Fother%2Fscope%2Fx",
            "Proxying not supported yet",
        ),
        ("https%3A%2F%2Fexample.com%2Fx", "Remote custom URIs not supported yet"),
    ] {
        let s = format!("dh+plain://127.0.0.1:{port}?uri={inner}");
        let refused = resolve(&resolvers, &s).await.unwrap_err();
        assert_eq!(
            refused,
            format!(
                "java.lang.UnsupportedOperationException: {message}, see \
                 https://github.com/deephaven/deephaven-core/issues/1483"
            )
        );
    }
    // TLS is refused by name
    let tls = resolve(&resolvers, &format!("dh://127.0.0.1:{port}/scope/t")).await;
    assert_eq!(
        tls.unwrap_err(),
        "TLS connections to remote servers are not ported yet: \
         io.deephaven.client.impl.ChannelHelper#channelBuilder (useTransportSecurity)"
    );
    // a failed connection is not kept
    let refused = resolve(&resolvers, &format!("dh+plain://127.0.0.1:{port}/scope/t")).await;
    assert!(refused.unwrap_err().starts_with("cannot connect: "));
    assert!(resolvers.sessions().is_empty());
}

/// A source of the local graph that resolves a remote URI inside the cycle.
struct ResolvesInCycle {
    resolvers: Arc<UriResolvers>,
    uri: String,
    answer: Mutex<Option<String>>,
}

impl UpdateSource for ResolvesInCycle {
    fn refresh(&self) {
        let mut answer = self.answer.lock().unwrap();
        if answer.is_none() {
            let resolved = self.resolvers.resolve(&self.uri);
            *answer = Some(resolved.err().map(|e| e.to_string()).unwrap_or_default());
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_subscription_is_refused_inside_a_cycle() {
    let remote = TestServer::start().await;
    let fixed = fixed(&[Some(1)]);
    remote
        .state
        .scope
        .put("t", ExportValue::Table(fixed))
        .unwrap();
    let local = TestServer::start().await;
    let resolvers = Arc::new(UriResolvers::new(&local.state));
    let source = Arc::new(ResolvesInCycle {
        resolvers,
        uri: format!("dh+plain://127.0.0.1:{}/scope/t", remote.address.port()),
        answer: Mutex::new(None),
    });
    let as_source: Arc<dyn UpdateSource> = source.clone();
    local.state.graph.add_source(&as_source);
    let graph = local.state.graph.clone();
    tokio::task::spawn_blocking(move || graph.run_cycle())
        .await
        .unwrap();
    assert_eq!(
        source.answer.lock().unwrap().as_deref(),
        Some(
            "a remote table cannot be subscribed to inside an update cycle: the subscription \
             waits for a cycle"
        )
    );
}

/// The wait for a subscription gives the lock up: a thread that holds the shared lock, as
/// a console command does, subscribes while the graph wants to run a cycle.
#[tokio::test(flavor = "multi_thread")]
async fn a_subscription_under_the_shared_lock_lets_cycles_run() {
    let remote = TestServer::start().await;
    let fixed = fixed(&[Some(1)]);
    remote
        .state
        .scope
        .put("t", ExportValue::Table(fixed.clone()))
        .unwrap();
    let local = TestServer::start().await;
    let resolvers = Arc::new(UriResolvers::new(&local.state));
    let s = format!("dh+plain://127.0.0.1:{}/scope/t", remote.address.port());
    let graph = local.state.graph.clone();
    let (sender, receiver) = std::sync::mpsc::channel();
    // a thread of its own, which a lost lock leaves stuck without stopping the test
    std::thread::spawn(move || {
        let stepped = graph.stepped();
        stepped.acquire_shared();
        // a cycle that waits for the lock, so no new shared hold is given out
        let cycle = std::thread::spawn({
            let graph = graph.clone();
            move || graph.run_cycle()
        });
        std::thread::sleep(Duration::from_millis(50));
        let copy = resolvers.resolve(&s);
        stepped.release_shared();
        cycle.join().unwrap();
        let _ = sender.send(copy);
    });
    let copy = receiver
        .recv_timeout(PATIENCE)
        .expect("the subscription kept the cycle out, and the cycle the subscription");
    let copy = table_of(copy.unwrap());
    assert_eq!(diff_tables(fixed.as_ref(), copy.as_ref()), None);
}

#[cfg(feature = "python")]
#[tokio::test(flavor = "multi_thread")]
async fn csv_and_parquet_uris_read_files() {
    let local = TestServer::start().await;
    let resolvers = Arc::new(UriResolvers::new(&local.state));
    let directory = std::env::temp_dir().join(format!("oxhaven-uri-{}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let csv = directory.join("t.csv");
    std::fs::write(&csv, "A,B\n1,x\n2,y\n").unwrap();
    let path = csv.to_str().unwrap();
    for s in [format!("csv://{path}"), format!("file+csv://{path}")] {
        let table = table_of(resolve(&resolvers, &s).await.unwrap());
        assert_eq!(table.row_set().size(), 2, "{s}");
    }
    // a web URL is not read yet
    let web = resolve(&resolvers, "csv+https://example.com/x.csv").await;
    assert_eq!(
        web.unwrap_err(),
        "not ported yet: io.deephaven.csv.CsvTools#readCsv(URL, CsvSpecs): reading the URL \
         https://example.com/x.csv"
    );
    let missing = resolve(&resolvers, &format!("parquet://{path}.parquet")).await;
    assert!(missing.unwrap_err().contains("t.csv.parquet"));
    std::fs::remove_dir_all(&directory).unwrap();
}
