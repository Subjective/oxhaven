//! Tests of script applications run by the Python session, and of `deephaven.appmode`.
//!
//! The application directory of the first test was given to Java 42.9
//! (`-Ddeephaven.application.dir`), and what its scripts noted and what `ListFields`
//! listed there is what is expected here, except where a comment says how Java differs.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::Duration;

use oxhaven_proto::grpc::application_service_client::ApplicationServiceClient;
use oxhaven_proto::grpc::{FieldInfo, FieldsChangeUpdate, ListFieldsRequest};
use tokio_stream::StreamExt;
use tonic::Streaming;

use super::tests::Python;
use crate::app::{demo, script};
use crate::test_support::with_auth;

/// A directory of its own under the system's temporary directory, removed when dropped.
struct Sandbox {
    base: PathBuf,
}

impl Sandbox {
    fn new() -> Self {
        let base = std::env::temp_dir().join(format!("oxhaven-pyapps-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(base.join("app.d")).unwrap();
        Self { base }
    }

    fn dir(&self) -> PathBuf {
        self.base.join("app.d")
    }

    /// Writes a file, by its path relative to the application directory.
    fn write(&self, path: &str, text: &str) {
        let path = self.dir().join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, text).unwrap();
    }
}

impl Drop for Sandbox {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.base);
    }
}

impl Python {
    /// Starts the applications of `dir` as the server does before it accepts clients.
    fn load(&self, dir: &Path) -> Result<Vec<String>, String> {
        let applications = &self.server.state.applications;
        let started = script::load(dir, applications, Some(&self.console))?;
        Ok(started.iter().map(|app| app.id().to_string()).collect())
    }

    async fn list_fields(&self, bearer: &str) -> Streaming<FieldsChangeUpdate> {
        ApplicationServiceClient::new(self.server.channel.clone())
            .list_fields(with_auth(ListFieldsRequest {}, bearer))
            .await
            .unwrap()
            .into_inner()
    }
}

async fn next(updates: &mut Streaming<FieldsChangeUpdate>) -> FieldsChangeUpdate {
    tokio::time::timeout(Duration::from_secs(5), updates.next())
        .await
        .expect("a message within five seconds")
        .expect("an open stream")
        .unwrap()
}

/// `application id|application name|field|type|ticket|description` of each field.
fn described(fields: &[FieldInfo]) -> Vec<String> {
    fields
        .iter()
        .map(|field| {
            let typed = field.typed_ticket.as_ref().unwrap();
            format!(
                "{}|{}|{}|{}|{}|{}",
                field.application_id,
                field.application_name,
                field.field_name,
                typed.r#type,
                String::from_utf8_lossy(&typed.ticket.as_ref().unwrap().ticket),
                field.field_description
            )
        })
        .collect()
}

const S_PY: &str = r#"import os, sys
from deephaven import empty_table
from deephaven.appmode import get_app_state
order = ['a0/s']
info = {}
_app0 = get_app_state()
info['repr0'] = repr(_app0)
info['name'] = __name__
_app0['zeta'] = empty_table(1)
_app0['alpha'] = empty_table(2)
scope_table = empty_table(3)
"#;

const A_PY: &str = r#"from deephaven.appmode import get_app_state
order.append('a1/a')
app = get_app_state()
info['same_state_object'] = app is _app0
info['repr1'] = repr(app)
app['t1'] = empty_table(4)
app['t2'] = empty_table(5)
"#;

const B_PY: &str = r#"order.append('a1/b')
info['get_type'] = type(app['t1']).__name__
app['t1'] = empty_table(6)
info['t1_size'] = app['t1'].size
try:
    del app['t2']
    info['del'] = 'ok'
except Exception as e:
    info['del'] = repr(e)
info['fields_after_del'] = sorted(app.fields.keys())
try:
    del app['missing']
    info['del_missing'] = 'ok'
except Exception as e:
    info['del_missing'] = repr(e)
try:
    app['missing']
except Exception as e:
    info['get_missing'] = repr(e)
app['num'] = 5
info['get_num'] = repr(app['num'])
_lst = [1, 2]
app['lst'] = _lst
info['get_lst_same'] = app['lst'] is _lst
try:
    app['none'] = None
    info['set_none'] = 'ok'
except Exception as e:
    info['set_none'] = f'{type(e).__name__}: {e}'.split('\n')[0]
try:
    info['get_none'] = repr(app['none'])
except Exception as e:
    info['get_none'] = repr(e)
try:
    app['bad\x01name'] = empty_table(1)
    info['set_bad'] = 'ok'
except Exception as e:
    info['set_bad'] = f'{type(e).__name__}: {e}'.split('\n')[0]
app[7] = empty_table(1)
from deephaven import new_table
from deephaven.column import int_col
app['pt'] = new_table([int_col('K', [1, 2])]).partition_by('K')
info['get_pt'] = type(app['pt']).__name__
info['str'] = str(app)
info['fields'] = {k: type(v).__name__ for k, v in app.fields.items()}
"#;

const DOTDOT_PY: &str = r#"order.append('a1/dotdot')
import threading
def _other():
    try:
        get_app_state()
        info['thread_state'] = 'ok'
    except Exception as e:
        info['thread_state'] = f'{type(e).__name__}: {e}'
_t = threading.Thread(target=_other); _t.start(); _t.join()
"#;

const C_PY: &str = r#"order.append('a1/c')
from deephaven import input_table, dtypes
_it = input_table({'A': dtypes.int32})
_it.add(new_table([int_col('A', [1])]))
info['input_add'] = _it.size
from deephaven import time_table
app['tick'] = time_table('PT1S')
"#;

/// What the scripts noted on Java 42.9, with the two entries Java has otherwise:
/// `get_pt` is `PartitionedTableImpl` there, the jpy object of the Java class, which the
/// reference package does not wrap; and `fields` is in the order of Java's hash table.
const NOTED: &str = r#"assert order == ['a0/s', 'a1/a', 'a1/b', 'a1/abs', 'a1/dotdot', 'a1/c'], order
_java = {
    'repr0': 'id: a0, name: Second continued',
    'name': '__main__',
    'same_state_object': False,
    'repr1': 'id: a1, name: First',
    'get_type': 'Table',
    't1_size': 6,
    'del': "KeyError('t2')",
    'fields_after_del': ['t1'],
    'del_missing': "KeyError('missing')",
    'get_missing': "KeyError('missing')",
    'get_num': '5',
    'get_lst_same': True,
    'set_none': 'RuntimeError: java.lang.NullPointerException: value',
    'get_none': "KeyError('none')",
    'set_bad': 'RuntimeError: java.lang.IllegalArgumentException: name() is invalid, must be printable ascii',
    'get_pt': 'PartitionedTable',
    'str': 'id: a1, name: First',
    'fields': {'7': 'Table', 'lst': 'list', 'num': 'int', 'pt': 'PartitionedTable', 't1': 'Table'},
    'thread_state': 'DHError: failed to get the application state. : java.lang.IllegalStateException: Should not be getting application state outside runUnderContext',
    'input_add': 1,
}
for _key in sorted(set(info) | set(_java)):
    assert info.get(_key) == _java.get(_key), (_key, info.get(_key), _java.get(_key))
assert list(info['fields']) == sorted(info['fields']), info['fields']
"#;

#[tokio::test]
async fn the_applications_of_a_directory_start_as_on_java() {
    let sandbox = Sandbox::new();
    let outside = sandbox.base.join("outside");
    sandbox.write(
        "10_first.app",
        &format!(
            "# comment\n! another comment\ntype=script\nscriptType=Python\nid=a1\n\
             name=First\nfile_1=b.py\nfile_0=sub/a.py\nfile_10=c.py\nfile_2={}/abs.py\n\
             file_3=../outside/dotdot.py\n",
            outside.display()
        ),
    );
    sandbox.write(
        "05_second.app",
        "type : script\n  scriptType   python\nid:a0\nname = Second \\\n   continued\n\
         enabled=TRUE\nfile_7 = sub/../s.py\nunknown_key=ignored\n",
    );
    sandbox.write(
        "20_disabled.app",
        "type=script\nscriptType=python\nid=dis\nname=Disabled\nenabled=false\nfile_0=never.py\n",
    );
    sandbox.write("30_qst.app", "type=qst\nid=q\n");
    sandbox.write("s.py", S_PY);
    sandbox.write("sub/a.py", A_PY);
    sandbox.write("b.py", B_PY);
    sandbox.write("c.py", C_PY);
    sandbox.write("../outside/abs.py", "order.append('a1/abs')\n");
    sandbox.write("../outside/dotdot.py", DOTDOT_PY);

    let python = Python::start().await;
    assert_eq!(python.load(&sandbox.dir()).unwrap(), ["a0", "a1"]);
    python.run(NOTED);

    // A client that connects now is told the fields in the order they came to be, the
    // tables the scripts left in the scope among them, after the script that left them.
    let bearer = python.server.login().await;
    let mut updates = python.list_fields(&bearer).await;
    let initial = next(&mut updates).await;
    assert_eq!(
        described(&initial.created),
        [
            "a0|Second continued|zeta|Table|a/a0/f/zeta|",
            "a0|Second continued|alpha|Table|a/a0/f/alpha|",
            "scope||scope_table|Table|s/scope_table|query scope variable",
            "a1|First|t1|Table|a/a1/f/t1|",
            "a1|First|num||a/a1/f/num|",
            "a1|First|lst||a/a1/f/lst|",
            "a1|First|7|Table|a/a1/f/7|",
            "a1|First|pt|PartitionedTable|a/a1/f/pt|",
            "a1|First|tick|Table|a/a1/f/tick|",
            "scope||_it|Table|s/_it|query scope variable",
        ]
    );
    assert!(initial.updated.is_empty() && initial.removed.is_empty());

    // Nothing is an application's script any more.
    python.run(
        "try:\n    get_app_state()\n    raise AssertionError('no error')\n\
         except Exception as e:\n    assert type(e).__name__ == 'DHError', e\n    \
         assert str(e) == info['thread_state'][9:], e",
    );

    // The states the scripts kept still set and remove fields, and clients are told.
    python.run("app['late'] = empty_table(1); _app0['zeta'] = empty_table(9)");
    let update = next(&mut updates).await;
    assert_eq!(described(&update.created), ["a1|First|late|Table|a/a1/f/late|"]);
    assert_eq!(
        described(&update.updated),
        ["a0|Second continued|zeta|Table|a/a0/f/zeta|"]
    );
    assert!(update.removed.is_empty());

    python.run(
        "try:\n    del app['late']\n    raise AssertionError('no error')\n\
         except KeyError as e:\n    assert repr(e) == \"KeyError('late')\", e",
    );
    let update = next(&mut updates).await;
    assert_eq!(described(&update.removed), ["a1|First|late|Table|a/a1/f/late|"]);
    assert!(update.created.is_empty() && update.updated.is_empty());

    // a field that came and went within one message is not mentioned; one that was
    // removed and set again is updated; and a description is the Java method's argument
    python.run(
        "app['late2'] = empty_table(1); app.j_app_state.removeField('late2')\n\
         app.j_app_state.removeField('tick'); app['tick'] = empty_table(1)\n\
         _app0.j_app_state.setField('described', empty_table(1), 'A description')",
    );
    let update = next(&mut updates).await;
    assert_eq!(
        described(&update.created),
        ["a0|Second continued|described|Table|a/a0/f/described|A description"]
    );
    assert_eq!(described(&update.updated), ["a1|First|tick|Table|a/a1/f/tick|"]);
    assert!(update.removed.is_empty());
}

/// Observed on Java 42.9: a script that sets a, b, c and a again is listed as b, c, a.
#[tokio::test]
async fn a_field_a_script_replaces_is_listed_behind_the_others() {
    let sandbox = Sandbox::new();
    sandbox.write(
        "1.app",
        "type=script\nscriptType=python\nid=r\nname=R\nfile_0=r.py\n",
    );
    sandbox.write(
        "r.py",
        "from deephaven import empty_table\nfrom deephaven.appmode import get_app_state\n\
         app = get_app_state()\napp['a'] = empty_table(1)\napp['b'] = empty_table(1)\n\
         app['c'] = empty_table(1)\napp['a'] = empty_table(2)\n",
    );
    let python = Python::start().await;
    assert_eq!(python.load(&sandbox.dir()).unwrap(), ["r"]);
    let bearer = python.server.login().await;
    let initial = next(&mut python.list_fields(&bearer).await).await;
    assert_eq!(
        described(&initial.created),
        ["r|R|b|Table|a/r/f/b|", "r|R|c|Table|a/r/f/c|", "r|R|a|Table|a/r/f/a|"]
    );
}

/// Observed on Java 42.9 for each of the three: the server does not start.
#[tokio::test]
async fn a_script_that_fails_ends_the_start() {
    let cases = [
        ("raise ValueError('boom')\n", "ValueError: boom"),
        ("def (:\n", "SyntaxError: invalid syntax"),
        ("import sys\nsys.exit(3)\n", "SystemExit: 3"),
    ];
    for (code, expected) in cases {
        let sandbox = Sandbox::new();
        sandbox.write(
            "1.app",
            "type=script\nscriptType=python\nid=f1\nname=Fails\nfile_0=ok.py\n\
             file_1=boom.py\nfile_2=after.py\n",
        );
        sandbox.write(
            "2.app",
            "type=script\nscriptType=python\nid=f2\nname=After\nfile_0=after.py\n",
        );
        sandbox.write(
            "ok.py",
            "from deephaven import empty_table\nfrom deephaven.appmode import get_app_state\n\
             get_app_state()['before'] = empty_table(1)\n",
        );
        sandbox.write("boom.py", code);
        sandbox.write("after.py", "ran_after = True\n");

        let python = Python::start().await;
        let error = python.load(&sandbox.dir()).unwrap_err();
        assert!(
            error.contains("1.app: Error in Python interpreter:") && error.contains(expected),
            "{error}"
        );
        python.run("assert 'ran_after' not in globals()");
        assert!(python.server.state.applications.get("f2").is_none());
    }
}

/// `oracle/ide/app.d` is the application the Java reference loads for the IDE acceptance
/// test (`oracle/ide/bin/start-java-ide.sh <port> app`). This server loads the same files
/// and offers what its built-in demo application offers.
#[tokio::test]
async fn the_application_of_the_ide_test_starts_from_the_files_java_loads() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../oracle/ide/app.d");
    let python = Python::start().await;
    // the script finds its module through this variable, as on Java
    python.run(&format!(
        "import os; os.environ['OXHAVEN_IDE_APP_DIR'] = {:?}",
        dir.to_str().unwrap()
    ));
    assert_eq!(python.load(&dir).unwrap(), [demo::APPLICATION_ID]);

    let bearer = python.server.login().await;
    let initial = next(&mut python.list_fields(&bearer).await).await;
    // nothing is left in the scope
    assert_eq!(
        described(&initial.created),
        [
            "ide_app|IDE acceptance tables|ide_static|Table|a/ide_app/f/ide_static|",
            "ide_app|IDE acceptance tables|ide_ticking|Table|a/ide_app/f/ide_ticking|",
        ]
    );
    let application = python.server.state.applications.get(demo::APPLICATION_ID);
    let application = application.unwrap();
    assert_eq!(application.name(), demo::APPLICATION_NAME);
    let table = |name: &str| application.field(name).unwrap().as_table().unwrap().clone();
    let built_in = demo::static_table().unwrap();
    let from_script = table(demo::STATIC_FIELD);
    assert_eq!(from_script.definition(), built_in.definition());
    assert_eq!(from_script.row_set().size(), 5);
    let ticking = table(demo::TICKING_FIELD);
    assert!(ticking.is_refreshing());
    let columns = ticking.definition().columns().iter();
    let names: Vec<&str> = columns.map(|column| column.name.as_str()).collect();
    assert_eq!(names, ["Timestamp", "N", "Sq"]);
}
