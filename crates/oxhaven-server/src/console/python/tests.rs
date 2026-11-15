//! Tests of the Python session. They run only in a build with the `python` feature:
//! `cargo test -p oxhaven-server --features python -- console::python`.
//!
//! The expectations are what the Java reference answered to the same code
//! (`oracle/console/probe.py`), except where a comment says how Java differs. A process has
//! one interpreter, so the tests take turns.

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Array, Float64Array, Int32Array, StringArray};
use oxhaven_contracts::schema::ColumnType;
use oxhaven_contracts::table::Table;
use oxhaven_proto::grpc::application_service_client::ApplicationServiceClient;
use oxhaven_proto::grpc::session_service_client::SessionServiceClient;
use oxhaven_proto::grpc::table_reference::Ref;
use oxhaven_proto::grpc::table_service_client::TableServiceClient;
use oxhaven_proto::grpc::{
    FetchTableRequest, FieldInfo, ListFieldsRequest, PublishRequest, SelectOrUpdateRequest,
    TableReference, Ticket,
};
use oxhaven_proto::script::console_service_client::ConsoleServiceClient;
use oxhaven_proto::script::{
    ExecuteCommandRequest, GetConsoleTypesRequest, LogSubscriptionRequest, StartConsoleRequest,
};
use oxhaven_testkit::TableSnapshot;
use tokio::sync::{Mutex, MutexGuard};
use tokio_stream::StreamExt;

use super::super::{Changes, Console, Stop, Variable};
use crate::session::{export_id_to_ticket, ExportValue};
use crate::test_support::{with_auth, TestServer};

pub(super) static ONE_INTERPRETER: Mutex<()> = Mutex::const_new(());

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

/// Observed on Java 42.9 with the same code: the two lines written to the descriptor are
/// in the server's own output and not in the log stream.
#[tokio::test]
async fn what_is_written_to_the_descriptors_is_not_in_the_log() {
    let python = Python::start().await;
    python.run(
        "import os, sys\n\
         print('to sys.stdout')\n\
         os.write(2, b'(a test wrote this line to descriptor 2)\\n')\n\
         os.system('echo \"(a test wrote this line from a subprocess)\" 1>&2')\n\
         sys.stderr.write('to sys.stderr\\n')",
    );
    assert_eq!(
        python.printed(0),
        [
            ("STDOUT".to_string(), "to sys.stdout\n".to_string()),
            ("STDERR".to_string(), "to sys.stderr\n".to_string()),
        ]
    );
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
        wrong_type.contains("DHError: failed to create an InputColumn (Id)."),
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

/// `name: type = first value, ...` of each column of the scope variable `name`.
fn described(python: &Python, name: &str) -> Vec<String> {
    let table = python.table(name);
    let snapshot = TableSnapshot::current(table.as_ref()).unwrap();
    let columns = table.definition().columns().iter();
    columns
        .zip(&snapshot.columns)
        .map(|(column, values)| {
            let cells: Vec<String> = (0..values.len())
                .map(|row| arrow::util::display::array_value_to_string(values, row).unwrap())
                .collect();
            format!(
                "{}: {:?} = {}",
                column.name,
                column.column_type,
                cells.join(", ")
            )
        })
        .collect()
}

#[tokio::test]
async fn the_scope_tickets_and_the_python_globals_are_one_set_of_variables() {
    let python = Python::start().await;
    python.run("from deephaven import empty_table\nt = empty_table(4)\nn = 41 + 1\nnone = None");
    let scope = &python.server.state.scope;

    // what a client publishes to a scope ticket is a global, a table as the package's Table
    scope
        .put("published", ExportValue::Table(python.table("t")))
        .unwrap();
    let since = python.printed(0).len();
    let used = python.run(
        "print(type(published).__module__, type(published).__name__, published.size)\n\
         head = published.head(1)",
    );
    assert_eq!(summary(&used), "head /  / ");
    assert_eq!(
        python.printed(0).split_off(since),
        [(
            "STDOUT".to_string(),
            "deephaven.table Table 4\n".to_string()
        )]
    );

    // any variable resolves: what is no table as an object, which can be published again
    let number = scope.get("n").expect("a variable that holds a number");
    assert!(number.as_table().is_none());
    scope.put("m", number).unwrap();
    python.run("assert m == 42 and m is n, m");
    // None is no value, as a variable that does not exist is none
    assert!(scope.get("none").is_none());
    assert!(scope.get("nothing").is_none());
    let names = scope.names();
    for name in ["head", "m", "n", "none", "published", "t"] {
        assert!(names.contains(&name.to_string()), "{name} in {names:?}");
    }

    // Java wraps any object of the server for Python; here only tables have a wrapper
    let refused = scope.put("unit", ExportValue::unit()).unwrap_err();
    assert!(refused.contains("not ported"), "{refused}");
    python.run("assert 'unit' not in globals()");
}

#[tokio::test]
async fn a_variable_set_by_a_thread_is_listed_without_a_command() {
    let python = Python::start().await;
    let bearer = python.server.login().await;
    let mut fields = ApplicationServiceClient::new(python.server.channel.clone())
        .list_fields(with_auth(ListFieldsRequest {}, &bearer))
        .await
        .unwrap()
        .into_inner();
    let mut next = async || {
        let message = tokio::time::timeout(std::time::Duration::from_secs(5), fields.next()).await;
        let message = message
            .expect("a message within five seconds")
            .unwrap()
            .unwrap();
        let name = |field: &FieldInfo| field.field_name.clone();
        (
            message.created.iter().map(name).collect::<Vec<_>>(),
            message.removed.iter().map(name).collect::<Vec<_>>(),
        )
    };
    assert_eq!(next().await, (vec![], vec![]));

    let started = python.run(
        "from deephaven import empty_table\n\
         import threading\n\
         def _later():\n    global threaded\n    threaded = empty_table(2)\n\
         threading.Timer(0.3, _later).start()",
    );
    assert_eq!(summary(&started), " /  / ");
    assert_eq!(next().await, (vec!["threaded".to_string()], vec![]));
    assert_eq!(python.table("threaded").row_set().size(), 2);

    // the next command does not report it again
    assert_eq!(summary(&python.run("u = threaded.head(1)")), "u /  / ");
    assert_eq!(next().await, (vec!["u".to_string()], vec![]));
    python.run(
        "def _drop():\n    global threaded\n    del threaded\n\
         threading.Timer(0.1, _drop).start()",
    );
    assert_eq!(next().await, (vec![], vec!["threaded".to_string()]));
}

#[tokio::test]
async fn a_formula_names_python_variables_with_the_types_java_gives_them() {
    let python = Python::start().await;
    python.run(
        "from deephaven import empty_table\n\
         xi = 41\nxsh = 300\nxint = 100000\nxl = 2**40\nxf = 1.5\nxs = 'abc'\nxb = True\n\
         r = empty_table(2).update(['A = xi', 'B = xsh', 'C = xint', 'D = xl + i', \
         'E = xf * 2', 'F = xs + `d`', 'G = xb', 'H = xi + i'])",
    );
    assert_eq!(
        described(&python, "r"),
        [
            "A: Byte = 41, 41",
            "B: Short = 300, 300",
            "C: Int = 100000, 100000",
            "D: Long = 1099511627776, 1099511627777",
            "E: Double = 3.0, 3.0",
            "F: String = abcd, abcd",
            "G: Boolean = true, true",
            "H: Int = 41, 42",
        ]
    );

    // a column comes before a variable, and the variable has the value of the moment the
    // table is made
    python.run(
        "c = empty_table(1).update(['xi = 5', 'Y = xi'])\n\
         cv = 5\ncaptured = empty_table(1).update_view('Y = cv + 1')\ncv = 1000",
    );
    assert_eq!(described(&python, "c"), ["xi: Int = 5", "Y: Int = 5"]);
    assert_eq!(described(&python, "captured"), ["Y: Int = 6"]);

    // filters
    python.run(
        "b = empty_table(50).update('X = i')\n\
         w = b.where('X > xi && X < 44')\nm = b.where('X = xi')\nflag = b.where('xb').head(1)",
    );
    assert_eq!(described(&python, "w"), ["X: Int = 42, 43"]);
    assert_eq!(described(&python, "m"), ["X: Int = 41"]);
    assert_eq!(described(&python, "flag"), ["X: Int = 0"]);

    // refused as Java refuses it, and refused because Java's result is not ported
    for (code, expected) in [
        (
            "bad = b.update('Y = -xi')",
            "the negation of the query scope variable xi",
        ),
        (
            "bad = b.update('Y = nosuch + 1')",
            "cannot find column nosuch",
        ),
        (
            "lst = [1, 2]\nbad = b.update('Y = lst')",
            "which holds a Python list",
        ),
        (
            "bad = b.update('Y = threading')",
            "cannot find column threading",
        ),
        (
            "none = None\nbad = b.update('Y = none')",
            "which holds None",
        ),
        ("bad = b.where('X >= xf')", "Failed to convert value <xf>"),
    ] {
        let error = python.console.evaluate(code).error.unwrap_or_default();
        assert!(error.contains(expected), "{code}: {error}");
    }
    assert!(python.server.state.scope.get("bad").is_none());
}

#[tokio::test]
async fn a_formula_called_in_a_function_names_its_locals_then_the_globals() {
    let python = Python::start().await;
    python.run(
        "from deephaven import empty_table\n\
         xi = 41\n\
         def local():\n    xi = 7\n    zz = 2\n    return empty_table(1).update(['A = xi', 'B = zz + xi'])\n\
         f = local()\n\
         def reads_global():\n    return empty_table(1).update('A = xi + 1')\n\
         g = reads_global()\n\
         class K:\n    q = 3\n    t = empty_table(1).update('A = q')\n\
         k = K.t\n\
         lam = (lambda zz: empty_table(1).update('A = zz'))('text')\n\
         def filtered():\n    low = 2\n    return empty_table(4).update('X = i').where('X >= low')\n\
         w = filtered()\n\
         top = empty_table(1).update('A = xi')",
    );
    assert_eq!(described(&python, "f"), ["A: Byte = 7", "B: Int = 9"]);
    assert_eq!(described(&python, "g"), ["A: Int = 42"]);
    assert_eq!(described(&python, "k"), ["A: Byte = 3"]);
    assert_eq!(described(&python, "lam"), ["A: String = text"]);
    assert_eq!(described(&python, "w"), ["X: Int = 2, 3"]);
    assert_eq!(described(&python, "top"), ["A: Byte = 41"]);

    // the locals of the caller only: not those of a function further out, used or not
    for code in [
        "def helper(t, f):\n    return t.update(f)\n\
         def outer():\n    zz = 5\n    return helper(empty_table(1), 'A = zz')\n\
         bad = outer()",
        "def enclosing():\n    zz = 7\n    def inner():\n        return empty_table(1).update('A = zz')\n    return inner()\n\
         bad = enclosing()",
    ] {
        let error = python.console.evaluate(code).error.unwrap_or_default();
        assert!(error.contains("cannot find column zz"), "{code}: {error}");
    }
}

#[tokio::test]
async fn an_int_beyond_64_bits_fails_every_formula_while_it_is_a_variable() {
    let python = Python::start().await;
    let bearer = python.server.login().await;
    python
        .run("from deephaven import empty_table\nb = empty_table(3).update('X = i')\nbig = 2**63");
    let mut tables = TableServiceClient::new(python.server.channel.clone());
    let mut update = async |result: i32| {
        let request = SelectOrUpdateRequest {
            result_id: Some(export_id_to_ticket(result)),
            source_id: Some(TableReference {
                r#ref: Some(Ref::Ticket(Ticket {
                    ticket: b"s/b".to_vec(),
                })),
            }),
            column_specs: vec!["Z = X + 1".to_string()],
        };
        tables.update(with_auth(request, &bearer)).await
    };

    // Java: OverflowError, for the formulas of Python and of every client
    let failed = python.console.evaluate("bad = b.update('Y = X + 1')");
    let error = failed.error.unwrap_or_default();
    assert!(error.contains("int too big to convert"), "{error}");
    let refused = update(1).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::InvalidArgument);
    // a column that is only copied involves no formula
    python.run("copied = b.update('Y = X')\nviewed = b.view('X')");
    // in a function the variable is one of its globals
    let in_function = python
        .console
        .evaluate("def f():\n    return b.update('Y = X + 1')\nbad = f()");
    let error = in_function.error.unwrap_or_default();
    assert!(error.contains("int too big to convert"), "{error}");

    python.run("big = 2**63 - 1\nok = b.update(['Y = X + 1', 'Z = big'])");
    assert_eq!(
        described(&python, "ok")[1..],
        [
            "Y: Int = 1, 2, 3",
            "Z: Long = 9223372036854775807, 9223372036854775807, 9223372036854775807"
        ]
    );
    let updated = update(2).await.unwrap().into_inner();
    assert!(updated.success);
}

#[tokio::test]
async fn a_client_formula_names_the_variables_of_the_session() {
    let python = Python::start().await;
    let bearer = python.server.login().await;
    python.run("from deephaven import empty_table\nb = empty_table(3).update('X = i')\nxi = 41\nxs = 'abc'");
    let mut tables = TableServiceClient::new(python.server.channel.clone());
    let request = SelectOrUpdateRequest {
        result_id: Some(export_id_to_ticket(1)),
        source_id: Some(TableReference {
            r#ref: Some(Ref::Ticket(Ticket {
                ticket: b"s/b".to_vec(),
            })),
        }),
        column_specs: vec!["Z = xi + X".to_string(), "S = xs".to_string()],
    };
    let updated = tables.update(with_auth(request, &bearer)).await;
    assert!(updated.unwrap().into_inner().success);
    // the result, made a variable so that it can be read here
    python
        .server
        .state
        .tickets
        .resolve(None, b"s/b", "test")
        .unwrap();
    let mut sessions = SessionServiceClient::new(python.server.channel.clone());
    sessions
        .publish_from_ticket(with_auth(
            PublishRequest {
                source_id: Some(export_id_to_ticket(1)),
                result_id: Some(Ticket {
                    ticket: b"s/from_client".to_vec(),
                }),
            },
            &bearer,
        ))
        .await
        .unwrap();
    assert_eq!(
        described(&python, "from_client"),
        [
            "X: Int = 0, 1, 2",
            "Z: Int = 41, 42, 43",
            "S: String = abc, abc, abc"
        ]
    );
    python.run("assert from_client.size == 3 and from_client.column_names == ['X', 'Z', 'S']");
}

impl Python {
    /// Runs `code` and asks for it to stop `after` it was started. Returns what the
    /// command answered and how long it ran.
    async fn run_and_stop(&self, code: &str, after: Duration) -> (Changes, Duration) {
        let stop = Stop::new();
        let started = Instant::now();
        let running = tokio::task::spawn_blocking({
            let (console, code, stop) = (self.console.clone(), code.to_string(), stop.clone());
            move || console.evaluate_stoppable(&code, &stop)
        });
        tokio::time::sleep(after).await;
        stop.request();
        let changes = tokio::time::timeout(Duration::from_secs(20), running)
            .await
            .unwrap_or_else(|_| panic!("the command was not stopped within 20 seconds: {code}"))
            .unwrap();
        (changes, started.elapsed())
    }
}

/// The gate of task 6.15. On Java 42.9 the command runs on after its call is cancelled,
/// for ever if it is this one, and no table ticks again.
#[tokio::test]
async fn a_command_that_never_ends_is_stopped_when_its_client_cancels_the_call() {
    let python = Python::start().await;
    let bearer = python.server.login().await;
    let mut console = ConsoleServiceClient::new(python.server.channel.clone());
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
    let command = |code: &str| {
        let request = ExecuteCommandRequest {
            console_id: Some(export_id_to_ticket(1)),
            code: code.to_string(),
            ..Default::default()
        };
        let mut console = console.clone();
        let request = with_auth(request, &bearer);
        async move { console.execute_command(request).await }
    };
    let answer = command("from deephaven import time_table\nticking = time_table('PT0.01S')");
    assert_eq!(answer.await.unwrap().into_inner().error_message, "");
    let graph = &python.server.state.graph;
    let ticking = python.table("ticking");

    // the client gives up on a command that would run for ever
    let endless = command("started = 1\nwhile True:\n    pass\nnever = 1");
    let cycles = graph.completed_cycles();
    assert!(tokio::time::timeout(Duration::from_millis(500), endless)
        .await
        .is_err());
    // while it ran it held the graph's lock: the cycle asked for now could not run
    graph.request_cycle();

    // the next command of the same client runs, and sees what the stopped one had done
    let next = tokio::time::timeout(
        Duration::from_secs(20),
        command("after = started + 1\nprint(after)"),
    );
    let next = next.await.expect("the session runs commands again");
    assert_eq!(next.unwrap().into_inner().error_message, "");
    assert!(python.server.state.scope.get("after").is_some());
    assert!(python.server.state.scope.get("never").is_none());

    // and tables tick again
    let rows = ticking.row_set().size();
    for _ in 0..500 {
        if graph.completed_cycles() > cycles && ticking.row_set().size() > rows {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        graph.request_cycle();
    }
    assert!(graph.completed_cycles() > cycles, "no cycle ran");
    assert!(ticking.row_set().size() > rows, "the table did not tick");
}

#[tokio::test]
async fn a_stopped_command_ends_with_keyboard_interrupt_where_it_was() {
    let python = Python::start().await;
    let stopped = async |code: &'static str| {
        let (changes, ran) = python.run_and_stop(code, Duration::from_millis(300)).await;
        assert!(ran < Duration::from_secs(10), "{code} ran for {ran:?}");
        changes
            .error
            .unwrap_or_else(|| panic!("{code} was not interrupted"))
    };

    // Python code, between two instructions
    let looping = stopped("n = 0\nwhile True:\n    n += 1").await;
    assert!(
        looping.starts_with("Error in Python interpreter:\nTraceback (most recent call last):\n")
            && looping.ends_with("KeyboardInterrupt\n"),
        "{looping}"
    );
    assert!(python.server.state.scope.get("n").is_some());

    // calls that block in the interpreter, which a signal wakes
    let sleeping = stopped("import time\ntime.sleep(600)\nslept = 1").await;
    assert!(
        sleeping.contains("line 2, in <module>") && sleeping.ends_with("KeyboardInterrupt\n"),
        "{sleeping}"
    );
    let waiting = stopped("import threading\nthreading.Event().wait()").await;
    assert!(waiting.ends_with("KeyboardInterrupt\n"), "{waiting}");
    let child = stopped("import subprocess\nsubprocess.run(['sleep', '600'])").await;
    assert!(child.ends_with("KeyboardInterrupt\n"), "{child}");
    assert!(python.server.state.scope.get("slept").is_none());

    // No interrupt is left over for the commands that follow, and the standard streams
    // and the table operations work as before.
    let since = python.printed(0).len();
    for round in 0..40 {
        let changes = python.run(&format!(
            "from deephaven import empty_table\n\
             later = empty_table({round} + 1).update(['X = i'])\n\
             print(later.size)"
        ));
        assert_eq!(
            summary(&changes),
            if round == 0 {
                "later /  / "
            } else {
                " / later / "
            }
        );
    }
    assert_eq!(python.printed(0).len(), since + 40);
    assert_eq!(python.table("later").row_set().size(), 40);
}

#[tokio::test]
async fn a_command_stopped_before_it_starts_does_not_run() {
    let python = Python::start().await;
    let stop = Stop::new();
    stop.request();
    let changes = python
        .console
        .evaluate_stoppable("ran = 1\nprint('ran')", &stop);
    assert_eq!(
        changes.error.as_deref(),
        Some("Error in Python interpreter:\nKeyboardInterrupt\n")
    );
    assert!(python.server.state.scope.get("ran").is_none());
    assert_eq!(python.printed(0), []);
    // a request after the command has ended interrupts nothing
    let stop = Stop::new();
    assert_eq!(
        python.console.evaluate_stoppable("ran = 2", &stop).error,
        None
    );
    stop.request();
    python.run("ran = 3");
}

#[tokio::test]
async fn code_that_catches_the_interrupt_goes_on_until_it_is_stopped_again() {
    let python = Python::start().await;
    let code = "import time\n\
                try:\n    time.sleep(600)\nexcept KeyboardInterrupt:\n    caught = 1\n\
                time.sleep(600)\n\
                finished = 1";
    let stop = Stop::new();
    let running = tokio::task::spawn_blocking({
        let (console, stop) = (python.console.clone(), stop.clone());
        move || console.evaluate_stoppable(code, &stop)
    });
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop.request();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!running.is_finished());
    stop.request();
    let changes = tokio::time::timeout(Duration::from_secs(20), running);
    let error = changes.await.expect("stopped").unwrap().error.unwrap();
    assert!(error.contains("line 6, in <module>"), "{error}");
    assert!(python.server.state.scope.get("caught").is_some());
    assert!(python.server.state.scope.get("finished").is_none());
}

/// The interpreter's lock is released while the engine works, and the operation the
/// command called is finished, not abandoned: the interrupt is raised when it returns.
#[tokio::test]
async fn other_threads_of_a_script_are_not_interrupted() {
    let python = Python::start().await;
    python.run(
        "import threading, time\n\
         ticks = []\n\
         def count():\n    \
             for n in range(40):\n        ticks.append(n)\n        time.sleep(0.02)\n\
         counter = threading.Thread(target=count)\n\
         counter.start()",
    );
    let (changes, _) = python
        .run_and_stop("while True:\n    pass", Duration::from_millis(200))
        .await;
    assert!(changes.error.unwrap().ends_with("KeyboardInterrupt\n"));
    python.run(
        "counter.join()\nfrom deephaven import empty_table\ncounted = empty_table(len(ticks))",
    );
    assert_eq!(python.table("counted").row_set().size(), 40);
}
