//! Tests of the Python session. They run only in a build with the `python` feature:
//! `cargo test -p oxhaven-server --features python -- console::python`.
//!
//! The expectations are what the Java reference answered to the same code
//! (`oracle/console/probe.py`), except where a comment says how Java differs. A process has
//! one interpreter, so the tests take turns.

use std::sync::Arc;

use arrow::array::{Array, Float64Array, Int32Array, StringArray};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_proto::grpc::table_reference::Ref;
use oxhaven_proto::grpc::table_service_client::TableServiceClient;
use oxhaven_proto::grpc::{FetchTableRequest, TableReference, Ticket};
use oxhaven_proto::script::console_service_client::ConsoleServiceClient;
use oxhaven_proto::script::{
    ExecuteCommandRequest, GetConsoleTypesRequest, LogSubscriptionRequest, StartConsoleRequest,
};
use oxhaven_testkit::TableSnapshot;
use tokio::sync::{Mutex, MutexGuard};
use tokio_stream::StreamExt;

use super::super::{Changes, Console, Variable};
use crate::session::export_id_to_ticket;
use crate::test_support::{with_auth, TestServer};

static ONE_INTERPRETER: Mutex<()> = Mutex::const_new(());

/// A server with a Python console whose session has no variables.
pub(super) struct Python {
    pub(super) server: TestServer,
    pub(super) console: Arc<Console>,
    _turn: MutexGuard<'static, ()>,
}

impl Python {
    pub(super) async fn start() -> Self {
        let turn = ONE_INTERPRETER.lock().await;
        let server = TestServer::start_with(|state| super::start(state).unwrap()).await;
        let console = server.state.console.clone().unwrap();
        Self {
            server,
            console,
            _turn: turn,
        }
    }

    /// Runs code that must not fail.
    pub(super) fn run(&self, code: &str) -> Changes {
        let changes = self.console.evaluate(code);
        assert_eq!(changes.error, None, "{code}");
        changes
    }

    /// The table of a scope variable, as a scope ticket resolves it.
    pub(super) fn table(&self, name: &str) -> Arc<dyn Table> {
        let value = self.server.state.scope.get(name);
        value
            .unwrap_or_else(|| panic!("no scope variable {name}"))
            .as_table()
            .unwrap()
            .clone()
    }

    /// What the code printed since `since`: `(level, message)` of each record.
    fn printed(&self, since: i64) -> Vec<(String, String)> {
        let mut updates = self.server.state.logs.subscribe(LogSubscriptionRequest {
            last_seen_log_timestamp: since,
            levels: Vec::new(),
        });
        let mut records = Vec::new();
        while let Ok(Ok(record)) = updates.try_recv() {
            records.push((record.log_level, record.message));
        }
        records
    }
}

fn names(variables: &[Variable]) -> Vec<&str> {
    variables
        .iter()
        .map(|variable| {
            assert_eq!(variable.type_name, "Table");
            variable.name.as_str()
        })
        .collect()
}

/// `created / updated / removed`, by name.
fn summary(changes: &Changes) -> String {
    format!(
        "{} / {} / {}",
        names(&changes.created).join(","),
        names(&changes.updated).join(","),
        names(&changes.removed).join(",")
    )
}

fn ints(table: &dyn Table, column: usize) -> Vec<Option<i32>> {
    let snapshot = TableSnapshot::current(table).unwrap();
    let values = snapshot.columns[column]
        .as_any()
        .downcast_ref::<Int32Array>()
        .expect("an int column");
    values.iter().collect()
}

#[tokio::test]
async fn a_table_assigned_at_top_level_is_a_scope_variable() {
    let python = Python::start().await;
    let imported = python.run("from deephaven import empty_table");
    assert_eq!(summary(&imported), " /  / ");

    let created = python.run("t = empty_table(5).update([\"X = i\"])");
    assert_eq!(summary(&created), "t /  / ");
    let t = python.table("t");
    assert_eq!(t.definition().columns().len(), 1);
    assert_eq!(t.definition().columns()[0].name, "X");
    assert_eq!(t.definition().columns()[0].column_type, ColumnType::Int);
    assert_eq!(
        ints(t.as_ref(), 0),
        [Some(0), Some(1), Some(2), Some(3), Some(4)]
    );
    assert!(!t.is_refreshing());
}

#[tokio::test]
async fn the_console_runs_python_for_a_client() {
    let python = Python::start().await;
    let bearer = python.server.login().await;
    let mut console = ConsoleServiceClient::new(python.server.channel.clone());
    let types = console
        .get_console_types(GetConsoleTypesRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(types.console_types, ["python"]);
    console
        .start_console(with_auth(
            StartConsoleRequest {
                result_id: Some(export_id_to_ticket(1)),
                session_type: "python".to_string(),
            },
            &bearer,
        ))
        .await
        .unwrap();
    let mut logs = console
        .subscribe_to_logs(with_auth(LogSubscriptionRequest::default(), &bearer))
        .await
        .unwrap()
        .into_inner();

    let mut execute = async |code: &str| {
        let request = ExecuteCommandRequest {
            console_id: Some(export_id_to_ticket(1)),
            code: code.to_string(),
            ..Default::default()
        };
        let answer = console.execute_command(with_auth(request, &bearer)).await;
        answer.unwrap().into_inner()
    };
    execute("from deephaven import empty_table").await;
    let answer = execute("t = empty_table(5).update([\"X = i\"])\nprint(t.size)").await;
    assert_eq!(answer.error_message, "");
    let changes = answer.changes.unwrap();
    assert!(changes.updated.is_empty() && changes.removed.is_empty());
    let [field] = changes.created.as_slice() else {
        panic!("one created field: {changes:?}");
    };
    let typed = field.typed_ticket.as_ref().unwrap();
    assert_eq!(typed.ticket.as_ref().unwrap().ticket, b"s/t");
    assert_eq!(typed.r#type, "Table");
    assert_eq!(field.field_name, "t");
    assert_eq!(field.field_description, "query scope variable");
    assert_eq!(field.application_id, "scope");

    let printed = logs.next().await.unwrap().unwrap();
    assert_eq!(
        (printed.log_level.as_str(), printed.message.as_str()),
        ("STDOUT", "5\n")
    );

    let fetched = TableServiceClient::new(python.server.channel.clone())
        .fetch_table(with_auth(
            FetchTableRequest {
                source_id: Some(TableReference {
                    r#ref: Some(Ref::Ticket(Ticket {
                        ticket: b"s/t".to_vec(),
                    })),
                }),
                result_id: Some(export_id_to_ticket(2)),
            },
            &bearer,
        ))
        .await
        .unwrap()
        .into_inner();
    assert!(fetched.success && fetched.is_static);
    assert_eq!(fetched.size, 5);

    let failed = execute("1 / 0").await;
    assert_eq!(
        failed.error_message,
        "Error in Python interpreter:\nTraceback (most recent call last):\n  File \
         \"<string>\", line 1, in <module>\nZeroDivisionError: division by zero\n"
    );
    assert_eq!(failed.changes, Some(Default::default()));
}

#[tokio::test]
async fn only_variables_that_hold_tables_are_reported() {
    let python = Python::start().await;
    python.run("from deephaven import empty_table");
    assert_eq!(summary(&python.run("t = empty_table(5)")), "t /  / ");
    assert_eq!(summary(&python.run("n = 41 + 1\ns = 'text'")), " /  / ");
    // another table under the same name; the same table again is no change
    assert_eq!(summary(&python.run("t = empty_table(3)")), " / t / ");
    assert_eq!(python.table("t").row_set().size(), 3);
    assert_eq!(summary(&python.run("t = t")), " /  / ");
    // a second name for a table, a new table, and a name that goes away
    python.run("v = empty_table(1)");
    let mixed = python.run("a = t\nb = t.view(['X = i'])\ndel v");
    assert_eq!(summary(&mixed), "a,b /  / v");
    assert!(Arc::ptr_eq(&python.table("a"), &python.table("t")));
    assert!(python.server.state.scope.get("v").is_none());
    // a table name that now holds something else
    assert_eq!(summary(&python.run("a = None")), " /  / a");
    assert_eq!(summary(&python.run("b = 7")), " /  / b");
    assert_eq!(summary(&python.run("b = empty_table(2)")), "b /  / ");
    assert_eq!(summary(&python.run("")), " /  / ");
}

#[tokio::test]
async fn what_python_prints_is_logged_line_by_line() {
    let python = Python::start().await;
    python.run("n = 42");
    let since = python.printed(0).len();
    python.run("print(\"hello from the console\")\nprint(n)");
    // as a module body, not as the interactive interpreter: a bare expression prints nothing
    python.run("n");
    python.run("import sys\nsys.stderr.write(\"to stderr\\n\")");
    python.run("print('two\\nlines, the last one unfinished', end='')");
    let printed = python.printed(0).split_off(since);
    let expected = [
        ("STDOUT", "hello from the console\n"),
        ("STDOUT", "42\n"),
        ("STDERR", "to stderr\n"),
        ("STDOUT", "two\n"),
        ("STDOUT", "lines, the last one unfinished"),
    ];
    let printed: Vec<(&str, &str)> = printed
        .iter()
        .map(|(level, message)| (level.as_str(), message.as_str()))
        .collect();
    assert_eq!(printed, expected);
}

#[tokio::test]
async fn an_error_is_the_text_the_interpreter_prints_and_keeps_earlier_assignments() {
    let python = Python::start().await;
    let error = |code: &str| {
        let changes = python.console.evaluate(code);
        (changes.error.clone().expect("an error"), summary(&changes))
    };
    assert_eq!(
        error("t = = 1").0,
        "Error in Python interpreter:\n  File \"<string>\", line 1\n    t = = 1\n        \
         ^\nSyntaxError: invalid syntax\n"
    );
    assert_eq!(
        error("u = no_such_name").0,
        "Error in Python interpreter:\nTraceback (most recent call last):\n  File \
         \"<string>\", line 1, in <module>\nNameError: name 'no_such_name' is not defined\n"
    );
    let (text, changed) = error(
        "from deephaven import empty_table\nv = empty_table(1)\nraise ValueError(\"after v\")",
    );
    assert_eq!(
        text,
        "Error in Python interpreter:\nTraceback (most recent call last):\n  File \
         \"<string>\", line 3, in <module>\nValueError: after v\n"
    );
    assert_eq!(changed, "v /  / ");
    assert_eq!(python.table("v").row_set().size(), 1);

    // An engine error is a DHError that names the operation, as in the reference package;
    // the text underneath is this engine's, where Java's is a Java stack trace.
    let (text, changed) = error("w = empty_table(1).update([\"X = nosuchcolumn\"])");
    assert!(
        text.contains("deephaven.dherror.DHError: table update operation failed. : "),
        "{text}"
    );
    assert!(text.contains("nosuchcolumn"), "{text}");
    assert!(
        text.contains("File \"<oxhaven>/deephaven/table.py\""),
        "{text}"
    );
    assert_eq!(changed, " /  / ");

    // the session goes on after an error, a SystemExit included
    assert!(error("raise SystemExit(3)").0.ends_with("SystemExit: 3\n"));
    assert_eq!(summary(&python.run("x = empty_table(2)")), "x /  / ");
}

#[tokio::test]
async fn new_table_and_table_operations_go_through_the_table_service_handlers() {
    let python = Python::start().await;
    python.run(
        "from deephaven import new_table, time_table\n\
         from deephaven.column import int_col, string_col, double_col\n\
         s = new_table([int_col('Id', [1, 2, None]), string_col('Sym', ['A', None, 'C']), \
         double_col('Price', [1.5, 2, None])])\n\
         f = s.where('Id > 1').update_view('Twice = Id * 2').tail(1)\n\
         ticking = time_table('PT1S').update(['N = ii']).tail(3)",
    );
    let s = python.table("s");
    let snapshot = TableSnapshot::current(s.as_ref()).unwrap();
    assert_eq!(ints(s.as_ref(), 0), [Some(1), Some(2), None]);
    let symbols = snapshot.columns[1].as_any().downcast_ref::<StringArray>();
    let symbols: Vec<_> = symbols.unwrap().iter().collect();
    assert_eq!(symbols, [Some("A"), None, Some("C")]);
    let prices = snapshot.columns[2].as_any().downcast_ref::<Float64Array>();
    let prices: Vec<_> = prices.unwrap().iter().collect();
    assert_eq!(prices, [Some(1.5), Some(2.0), None]);

    let f = python.table("f");
    let names: Vec<_> = f
        .definition()
        .columns()
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(names, ["Id", "Sym", "Price", "Twice"]);
    assert_eq!(ints(f.as_ref(), 0), [Some(2)]);
    assert_eq!(ints(f.as_ref(), 3), [Some(4)]);

    let ticking = python.table("ticking");
    assert!(ticking.is_refreshing());
    let columns = ticking.definition().columns();
    assert_eq!(columns[0].column_type, ColumnType::Instant);
    assert_eq!(columns[1].column_type, ColumnType::Long);

    // what the handler refuses for a client, it refuses for Python, with the text
    let refused = python
        .console
        .evaluate("bad = s.where('NoSuch > 1')")
        .error
        .unwrap();
    assert!(
        refused.contains("DHError: table where operation failed."),
        "{refused}"
    );
    let wrong_type = python
        .console
        .evaluate("bad = new_table([int_col('Id', ['x'])])")
        .error
        .unwrap();
    assert!(
        wrong_type.contains("DHError: failed to create a new time table."),
        "{wrong_type}"
    );
    let unknown = python
        .console
        .evaluate("import _oxhaven\n_oxhaven.table_op('{\"no_such_operation\": {}}', [])")
        .error
        .unwrap();
    assert!(
        unknown.contains("ValueError: not a table operation"),
        "{unknown}"
    );
    assert!(python.server.state.scope.get("bad").is_none());
}

#[tokio::test]
async fn a_bound_table_is_a_python_variable() {
    let python = Python::start().await;
    python.run("from deephaven import empty_table\nt = empty_table(4)");
    python
        .console
        .bind_table("bound", python.table("t"))
        .unwrap();
    assert!(Arc::ptr_eq(&python.table("bound"), &python.table("t")));
    let since = python.printed(0).len();
    let used = python.run("c = bound.head(1)\nprint(bound.size, type(bound).__name__)");
    assert_eq!(summary(&used), "c /  / ");
    assert_eq!(
        python.printed(0).split_off(since),
        [("STDOUT".to_string(), "4 Table\n".to_string())]
    );
}
