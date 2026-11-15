//! Tests of application files and of the start of script applications, with a script
//! session that needs no interpreter. The expectations are what Java 42.9 did with the
//! same files (`docs/CONSOLE.md`, "Applications defined by scripts"); the same directory
//! run by Python is in `console::python::appmode_tests`.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use oxhaven_engine::table::empty_table;

use super::script::{self, find, load, parse, ApplicationConfig, ScriptApplication};
use super::Applications;
use crate::app::FormulaValue;
use crate::console::{Changes, Console, ScriptSession, Stop};
use crate::session::ExportValue;

/// A directory of its own under the system's temporary directory, removed when dropped.
struct Sandbox {
    base: PathBuf,
}

impl Sandbox {
    fn new() -> Self {
        let base = std::env::temp_dir().join(format!("oxhaven-apps-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(base.join("app.d")).unwrap();
        Self { base }
    }

    /// The application directory.
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

/// A script session of the language `language` whose scripts are lines: `field <name>`
/// sets a field of the application the script starts, `fail <text>` ends the script with
/// an error, and anything else does nothing. It keeps what it ran.
struct LineSession {
    language: &'static str,
    /// `<application id>|<code>` of each script, in the order they ran.
    ran: Mutex<Vec<String>>,
}

impl ScriptSession for LineSession {
    fn script_type(&self) -> &str {
        self.language
    }

    fn evaluate(&self, code: &str, _stop: &Arc<Stop>) -> Changes {
        let application = script::current();
        let id = application.as_ref().map_or("none", |app| app.id());
        self.ran.lock().unwrap().push(format!("{id}|{code}"));
        let mut changes = Changes::default();
        for line in code.lines() {
            match line.split_once(' ') {
                Some(("field", name)) => application
                    .as_ref()
                    .expect("a script of an application")
                    .set_field(name, ExportValue::Table(empty_table(1)))
                    .unwrap(),
                Some(("fail", text)) => {
                    changes.error = Some(text.to_string());
                    break;
                }
                _ => {}
            }
        }
        changes
    }

    fn observe(&self) -> Changes {
        Changes::default()
    }

    fn put(&self, _name: &str, _value: ExportValue) -> Result<Changes, String> {
        Err("no variables".to_string())
    }

    fn get(&self, _name: &str) -> Option<ExportValue> {
        None
    }

    fn names(&self) -> Vec<String> {
        Vec::new()
    }

    fn formula_values(&self, names: &[&str]) -> Result<Vec<Option<FormulaValue>>, String> {
        Ok(vec![None; names.len()])
    }
}

/// The applications of a server, a console of `language`, and what the console ran.
fn console(language: &'static str) -> (Arc<Applications>, Arc<Console>, Arc<LineSession>) {
    let applications = Applications::new();
    let session = Arc::new(LineSession {
        language,
        ran: Mutex::default(),
    });
    let console = Console::new(session.clone(), applications.clone());
    (applications, console, session)
}

const SCRIPT: &str = "type=script\nscriptType=python\n";

fn script_of(text: &str) -> ScriptApplication {
    match parse(text).unwrap() {
        ApplicationConfig::Script(script) => script,
        other => panic!("not a script application: {other:?}"),
    }
}

fn files(script: &ScriptApplication) -> Vec<&str> {
    let files = script.files.iter();
    files.map(|file| file.to_str().unwrap()).collect()
}

#[test]
fn an_application_file_is_read_as_java_reads_it() {
    let script = script_of(
        "# a comment\ntype : script\n  scriptType   Python\nid:a0\nname = Second \\\n   \
         continued\nenabled=TRUE\nfile_7 = sub/../s.py\nunknown_key=ignored\n",
    );
    assert_eq!(
        (script.id.as_str(), script.name.as_str()),
        ("a0", "Second continued")
    );
    assert_eq!(script.script_type, "python");
    assert!(script.enabled);
    assert_eq!(files(&script), ["sub/../s.py"]);
    assert_eq!(
        ApplicationConfig::Script(script).to_string(),
        "ScriptApplication{id=a0, name=Second continued, files=[sub/../s.py], \
         isEnabled=true, scriptType=python}"
    );

    // the scripts are in the order of their numbers, not of their keys
    let script = script_of(&format!(
        "{SCRIPT}id=a1\nname=First\nfile_1=b.py\nfile_0=sub/a.py\nfile_10=c.py\n\
         file_2=/abs/outside.py\nfile_3=../outside/dotdot.py\n"
    ));
    assert_eq!(
        files(&script),
        [
            "sub/a.py",
            "b.py",
            "/abs/outside.py",
            "../outside/dotdot.py",
            "c.py"
        ]
    );

    // keys that only look like scripts are not, and only `true` enables
    let script = script_of(&format!(
        "{SCRIPT}id=x\nname=X\nfile_=a.py\nfile_x=b.py\nFILE_0=c.py\nfile_-1=d.py\n\
         file_5=ok.py\nenabled=yes\n"
    ));
    assert_eq!(files(&script), ["ok.py"]);
    assert!(!script.enabled);
    assert!(!script_of(&format!("{SCRIPT}id=x\nname=X\nfile_0=a\nenabled=false")).enabled);

    // an id and a name may be empty
    let script = script_of(&format!("{SCRIPT}id=\nname=\nfile_0=ok.py\n"));
    assert_eq!((script.id.as_str(), script.name.as_str()), ("", ""));

    // a qst application is read, whatever else its file says, and is never enabled
    let qst = parse("type=qst\nenabled=true\n").unwrap();
    assert_eq!(qst, ApplicationConfig::Qst);
    assert!(!qst.is_enabled());
    assert_eq!(qst.to_string(), "QSTApplication{}");
}

#[test]
fn an_application_file_that_java_refuses_is_refused_with_its_text() {
    let refused = |text: &str| parse(text).unwrap_err();
    assert_eq!(
        refused("scriptType=python\nid=x\nname=X\nfile_0=ok.py\n"),
        "java.lang.IllegalArgumentException: Application property 'type' not specified"
    );
    assert_eq!(
        refused("type=nonsense\n"),
        "java.lang.IllegalArgumentException: Unexpected type nonsense"
    );
    assert_eq!(
        refused(&format!("{SCRIPT}name=X\nfile_0=ok.py\n")),
        "java.lang.NullPointerException: id"
    );
    assert_eq!(
        refused(&format!("{SCRIPT}id=x\nfile_0=ok.py\n")),
        "java.lang.NullPointerException: name"
    );
    assert_eq!(
        refused("type=script\nid=x\nname=X\nfile_0=ok.py\n"),
        "java.lang.NullPointerException: scriptType"
    );
    assert_eq!(
        refused(&format!("{SCRIPT}id=x\nname=X\n")),
        "java.lang.IllegalArgumentException: Must specify at least one file to execute"
    );
    assert_eq!(
        refused(&format!("{SCRIPT}id=x\nname=X\nfile_99999999999=ok.py\n")),
        "java.lang.NumberFormatException: For input string: \"99999999999\""
    );
    assert_eq!(
        refused(&format!("{SCRIPT}id=x\nname=\\u00zz\nfile_0=ok.py\n")),
        "java.lang.IllegalArgumentException: Malformed \\uxxxx encoding."
    );
}

#[test]
fn what_is_not_ported_is_refused_by_the_name_of_the_java_method() {
    let refused = |text: &str| parse(text).unwrap_err();
    // Java fails these too when the class does not exist, enabled or not
    assert!(refused("type=static\nclass=com.example.App\nenabled=false\n")
        .starts_with("not ported: io.deephaven.appmode.StaticClassApplication#parse"));
    assert!(refused("type=dynamic\nclass=com.example.App\n")
        .starts_with("not ported: io.deephaven.appmode.DynamicApplication#parse"));
    // Java runs these in the order of a hash table
    let same_number = refused(&format!(
        "{SCRIPT}id=x\nname=X\nfile_1=one.py\nfile_01=zero_one.py\n"
    ));
    assert!(
        same_number.starts_with("not ported: io.deephaven.appmode.ApplicationUtil#findFilesFrom"),
        "{same_number}"
    );
}

#[test]
fn application_files_are_found_in_the_order_of_their_names() {
    let sandbox = Sandbox::new();
    let app = |id: &str| format!("{SCRIPT}id={id}\nname=N\nfile_0=s\n");
    sandbox.write("10_first.app", &app("a1"));
    sandbox.write("05_second.app", &app("a0"));
    sandbox.write("Z.app", &app("upper"));
    sandbox.write("a.app", &app("lower"));
    // none of these is an application file
    sandbox.write("40_upper.APP", "type=nonsense");
    sandbox.write("notes.txt", "type=nonsense");
    sandbox.write("50_dir.app/inside.txt", "type=nonsense");
    sandbox.write("sub/nested.app", "type=nonsense");
    sandbox.write("../linked.txt", "type=nonsense");
    std::os::unix::fs::symlink("../linked.txt", sandbox.dir().join("60_link.app")).unwrap();

    let found = find(&sandbox.dir()).unwrap();
    let ids: Vec<String> = found
        .iter()
        .map(|(path, config)| {
            let ApplicationConfig::Script(script) = config else {
                panic!("not a script application")
            };
            format!(
                "{}={}",
                path.file_name().unwrap().to_str().unwrap(),
                script.id
            )
        })
        .collect();
    assert_eq!(
        ids,
        ["05_second.app=a0", "10_first.app=a1", "Z.app=upper", "a.app=lower"]
    );

    // a directory that does not exist has no applications, and that is not an error
    assert_eq!(find(&sandbox.dir().join("missing")).unwrap(), []);
    // a directory that is a file is one
    assert!(find(&sandbox.dir().join("notes.txt")).is_err());

    // one file that cannot be read fails them all, and the error names it
    fs::write(sandbox.dir().join("70_latin1.app"), b"type=script\nname=caf\xe9\n").unwrap();
    let error = find(&sandbox.dir()).unwrap_err();
    assert!(
        error.contains("70_latin1.app") && error.contains("MalformedInputException"),
        "{error}"
    );
    fs::remove_file(sandbox.dir().join("70_latin1.app")).unwrap();
    sandbox.write("80_bad.app", "type=nonsense\n");
    let error = find(&sandbox.dir()).unwrap_err();
    assert!(
        error.ends_with("80_bad.app: java.lang.IllegalArgumentException: Unexpected type nonsense"),
        "{error}"
    );
}

/// The ids of the applications [`load`] started.
fn started(
    dir: &Path,
    applications: &Arc<Applications>,
    console: Option<&Arc<Console>>,
) -> Result<Vec<String>, String> {
    let applications = load(dir, applications, console)?;
    Ok(applications.iter().map(|app| app.id().to_string()).collect())
}

#[tokio::test]
async fn applications_start_in_order_and_their_scripts_run_in_order() {
    let sandbox = Sandbox::new();
    let outside = sandbox.base.join("outside");
    sandbox.write(
        "10_first.app",
        &format!(
            "{SCRIPT}id=a1\nname=First\nfile_1=b.py\nfile_0=sub/a.py\nfile_10=c.py\n\
             file_2={}/abs.py\nfile_3=../outside/dotdot.py\n",
            outside.display()
        ),
    );
    sandbox.write(
        "05_second.app",
        "type=script\nscriptType=Python\nid=a0\nname=Second\nfile_7=sub/../s.py\n",
    );
    sandbox.write("20_disabled.app", &format!("{SCRIPT}id=dis\nname=D\nenabled=false\nfile_0=never.py\n"));
    sandbox.write("30_qst.app", "type=qst\n");
    // the text of a script is its file without the blanks around it
    sandbox.write("s.py", "\n\n   field zeta\nfield alpha\n\n");
    sandbox.write("sub/a.py", "field t1");
    sandbox.write("b.py", "field t2\n");
    sandbox.write("c.py", "c");
    sandbox.write("../outside/abs.py", "abs");
    sandbox.write("../outside/dotdot.py", "dotdot");

    let (applications, console, session) = console("Python");
    let ids = started(&sandbox.dir(), &applications, Some(&console)).unwrap();
    assert_eq!(ids, ["a0", "a1"]);
    assert_eq!(
        *session.ran.lock().unwrap(),
        [
            "a0|field zeta\nfield alpha",
            "a1|field t1",
            "a1|field t2",
            "a1|abs",
            "a1|dotdot",
            "a1|c"
        ]
    );
    let fields = |id: &str| applications.get(id).unwrap().field_names();
    assert_eq!(fields("a0"), ["alpha", "zeta"]);
    assert_eq!(fields("a1"), ["t1", "t2"]);
    assert_eq!(applications.get("a1").unwrap().name(), "First");
    assert!(applications.get("dis").is_none());
    // no script runs any more
    assert!(script::current().is_none());
}

#[tokio::test]
async fn a_script_that_fails_ends_the_start() {
    let sandbox = Sandbox::new();
    sandbox.write("1.app", &format!("{SCRIPT}id=f1\nname=Fails\nfile_0=ok.py\nfile_1=boom.py\nfile_2=after.py\n"));
    sandbox.write("2.app", &format!("{SCRIPT}id=f2\nname=After\nfile_0=after.py\n"));
    sandbox.write("ok.py", "field before");
    sandbox.write("boom.py", "fail ValueError: boom");
    sandbox.write("after.py", "field after");

    let (applications, console, session) = console("Python");
    let error = started(&sandbox.dir(), &applications, Some(&console)).unwrap_err();
    assert!(error.ends_with("1.app: ValueError: boom"), "{error}");
    // neither the scripts after it nor the applications after it ran
    assert_eq!(
        *session.ran.lock().unwrap(),
        ["f1|field before", "f1|fail ValueError: boom"]
    );
    assert!(applications.get("f2").is_none());
    assert!(script::current().is_none());
}

#[tokio::test]
async fn an_application_that_cannot_start_ends_the_start_with_javas_text() {
    let failure = |app: &str, language: Option<&'static str>| {
        let sandbox = Sandbox::new();
        sandbox.write("1.app", app);
        sandbox.write("ok.py", "field before");
        sandbox.write("dir.py/inside", "");
        let (applications, console, _session) = console(language.unwrap_or("Python"));
        let console = language.map(|_| &console);
        let error = started(&sandbox.dir(), &applications, console).unwrap_err();
        let (_, text) = error.split_once("1.app: ").expect("the file is named");
        text.to_string()
    };
    let app = |script_type: &str, file: &str| {
        format!("type=script\nscriptType={script_type}\nid=x\nname=X\nfile_0={file}\n")
    };
    assert_eq!(
        failure(&app("groovy", "ok.groovy"), Some("Python")),
        "java.lang.IllegalArgumentException: Cannot instantiate Groovy application on a \
         Python app session"
    );
    assert_eq!(
        failure(&app("ruby", "ok.rb"), Some("Python")),
        "java.lang.UnsupportedOperationException: Currently do not support script sessions \
         of type 'ruby'"
    );
    // a console of another language, and a server without a console
    assert_eq!(
        failure(&app("python", "ok.py"), Some("Words")),
        "java.lang.IllegalArgumentException: Cannot instantiate Python application on a \
         Words app session"
    );
    assert_eq!(
        failure(&app("python", "ok.py"), None),
        "java.lang.IllegalArgumentException: Cannot instantiate Python application on a \
         NoLanguage app session"
    );
    assert_eq!(
        failure(&app("python", "nope.py"), Some("Python")),
        "java.lang.IllegalArgumentException: File nope.py doesn't exist."
    );
    let unreadable = failure(&app("python", "dir.py"), Some("Python"));
    assert!(
        unreadable.starts_with(
            "io.deephaven.UncheckedDeephavenException: could not read script file "
        ),
        "{unreadable}"
    );
    assert_eq!(
        failure(
            "type=script\nscriptType=python\nid=scope\nname=X\nfile_0=ok.py\n",
            Some("Python")
        ),
        "java.lang.IllegalArgumentException: Applications cannot re-use the id 'scope'; it \
         is reserved for QueryScope use only"
    );
}

#[tokio::test]
async fn two_applications_with_one_id_end_the_start() {
    let sandbox = Sandbox::new();
    sandbox.write("1.app", &format!("{SCRIPT}id=same\nname=One\nfile_0=ok.py\n"));
    sandbox.write("2.app", &format!("{SCRIPT}id=same\nname=Two\nfile_0=ok.py\n"));
    sandbox.write("ok.py", "field before");
    let (applications, console, _session) = console("Python");
    let error = started(&sandbox.dir(), &applications, Some(&console)).unwrap_err();
    assert!(
        error.ends_with(
            "2.app: java.lang.IllegalArgumentException: Duplicate application found for \
             app_id same"
        ),
        "{error}"
    );
}

#[tokio::test]
async fn a_directory_without_applications_starts_nothing() {
    let sandbox = Sandbox::new();
    let (applications, console, session) = console("Python");
    for dir in [sandbox.dir(), sandbox.dir().join("missing")] {
        assert_eq!(started(&dir, &applications, Some(&console)).unwrap(), [""; 0]);
    }
    // and needs no console
    assert_eq!(started(&sandbox.dir(), &applications, None).unwrap(), [""; 0]);
    assert!(session.ran.lock().unwrap().is_empty());
}
