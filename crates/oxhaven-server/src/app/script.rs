//! Applications defined by scripts: a directory of `.app` files, each naming scripts that
//! the script session runs before the server accepts clients (`oxhaven-server --app-dir`,
//! Java's property `deephaven.application.dir`).
//!
//! What Java does was read in the classes cited below and observed on Java 42.9 with
//! application directories made for the purpose; `docs/CONSOLE.md`, "Applications defined
//! by scripts", lists the observations and what differs here.
//!
//! # The files
//!
//! An application file is a regular file, not a link, whose name ends in `.app`. The files
//! of the directory are read in the order of their names, byte by byte. A directory that
//! does not exist holds no applications, and the server starts. Each file is UTF-8 text in
//! Java's properties format ([`super::properties`]) with these keys; others are ignored:
//!
//! | Key | Meaning |
//! | --- | --- |
//! | `type` | `script`. `qst` is read and never enabled, as in Java. `static` and `dynamic` name Java classes and are refused. |
//! | `scriptType` | the language, compared in lower case; it has to be the console's |
//! | `id` | what tickets name the application by: `a/<id>/f/<field>` |
//! | `name` | what a user interface shows |
//! | `enabled` | `true` in any case enables the application, anything else disables it; enabled when absent |
//! | `file_<number>` | a script; the scripts run in the order of their numbers |
//!
//! # Paths
//!
//! The value of a `file_<number>` key is used as Java uses it: an absolute path is taken as
//! it is, and a relative one is appended to the application directory. Nothing is
//! normalized and nothing is confined: `../other/script.py` and `/etc/script.py` are read
//! if the server's user may read them (observed on Java: both ran). The application
//! directory is the operator's, like the command line; whoever can write an `.app` file
//! there can run any code in the server anyway.
//!
//! # What a script sees
//!
//! A script runs as a command of the console does: in the session's global variables, under
//! the update graph's lock, with the same output streams. Its variables stay, the next
//! script and every console see them, and a table left in one is listed as a query scope
//! variable. Only while it runs, and only on the thread it runs on, [`current`] is its
//! application, which `deephaven.appmode.get_app_state()` returns. An object the script
//! keeps of it sets and removes fields later as well.
//!
//! # Failures
//!
//! A file that cannot be read as an application, a script that cannot be read, and a
//! script that ends with an error each fail [`load`], and the server does not start: so
//! does Java's. The applications before it have run by then.

use std::cell::RefCell;
use std::fmt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::fields::{Application, Applications};
use super::properties;
use crate::console::Console;

/// An application of type `script`, as its file describes it.
///
/// Behavior: `io.deephaven.appmode.ScriptApplication`
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct ScriptApplication {
    pub id: String,
    pub name: String,
    /// In lower case.
    pub script_type: String,
    /// In the order they run in, as the file spells them.
    pub files: Vec<PathBuf>,
    pub enabled: bool,
}

/// What an `.app` file describes.
///
/// Behavior: `io.deephaven.appmode.ApplicationConfig`
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ApplicationConfig {
    Script(ScriptApplication),
    /// Java reads the type `qst` and nothing of the file, and such an application is never
    /// enabled.
    ///
    /// Behavior: `io.deephaven.appmode.QSTApplication#isEnabled`
    Qst,
}

impl ApplicationConfig {
    /// Behavior: `ApplicationConfig#isEnabled`
    pub fn is_enabled(&self) -> bool {
        match self {
            ApplicationConfig::Script(script) => script.enabled,
            ApplicationConfig::Qst => false,
        }
    }
}

/// As Java's log names an application.
impl fmt::Display for ApplicationConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ApplicationConfig::Script(script) => {
                let files: Vec<_> = script.files.iter().map(|file| file.display()).collect();
                write!(f, "ScriptApplication{{id={}, name={}, files=[", script.id, script.name)?;
                for (index, file) in files.iter().enumerate() {
                    let separator = if index == 0 { "" } else { ", " };
                    write!(f, "{separator}{file}")?;
                }
                write!(
                    f,
                    "], isEnabled={}, scriptType={}}}",
                    script.enabled, script.script_type
                )
            }
            ApplicationConfig::Qst => f.write_str("QSTApplication{}"),
        }
    }
}

/// The scripts of an application in the order of the numbers of their keys. A key is
/// `file_` and one or more of the digits 0 to 9; any other key is not a script.
///
/// Two keys with the same number (`file_1` and `file_01`) are refused: Java runs them in
/// the order its hash table of the keys happens to have (observed: `file_001`, `file_1`,
/// `file_01`), which nothing here reproduces.
///
/// Behavior: `io.deephaven.appmode.ApplicationUtil#findFilesFrom`
fn files_of<'a>(
    properties: impl Iterator<Item = (&'a String, &'a String)>,
) -> Result<Vec<PathBuf>, String> {
    let mut files: Vec<(i32, &str, &str)> = Vec::new();
    for (key, value) in properties {
        let Some(digits) = key.strip_prefix("file_") else {
            continue;
        };
        if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
            continue;
        }
        let order: i32 = digits.parse().map_err(|_| {
            format!("java.lang.NumberFormatException: For input string: \"{digits}\"")
        })?;
        if let Some((_, other, _)) = files.iter().find(|(existing, _, _)| *existing == order) {
            return Err(format!(
                "not ported: io.deephaven.appmode.ApplicationUtil#findFilesFrom for two \
                 scripts with the same number ({other} and {key}); Java runs them in the \
                 order of a hash table"
            ));
        }
        files.push((order, key, value));
    }
    files.sort_by_key(|(order, _, _)| *order);
    Ok(files
        .into_iter()
        .map(|(_, _, value)| PathBuf::from(value))
        .collect())
}

/// Reads the text of an `.app` file. The messages are Java's.
///
/// Behavior: `io.deephaven.appmode.ApplicationConfigImpl#parse`, `ScriptApplication#parse`,
/// `ScriptApplication#checkNotEmpty`, `ApplicationUtil#isEnabled`
pub fn parse(text: &str) -> Result<ApplicationConfig, String> {
    let properties = properties::parse(text)?;
    let property = |key: &str| properties.get(key).map(String::as_str);
    match property("type") {
        None => {
            return Err(
                "java.lang.IllegalArgumentException: Application property 'type' not specified"
                    .to_string(),
            )
        }
        Some("script") => {}
        Some("qst") => return Ok(ApplicationConfig::Qst),
        // both name a Java class that makes the application
        Some("static") => {
            return Err(
                "not ported: io.deephaven.appmode.StaticClassApplication#parse (an \
                 application of type static is a Java class)"
                    .to_string(),
            )
        }
        Some("dynamic") => {
            return Err(
                "not ported: io.deephaven.appmode.DynamicApplication#parse (an application \
                 of type dynamic is a Java class)"
                    .to_string(),
            )
        }
        Some(other) => {
            return Err(format!(
                "java.lang.IllegalArgumentException: Unexpected type {other}"
            ))
        }
    }
    // Java reads the keys in this order, and the first that is missing is the error
    let enabled = property("enabled").is_none_or(|value| value.eq_ignore_ascii_case("true"));
    let missing = |key: &str| format!("java.lang.NullPointerException: {key}");
    let id = property("id").ok_or_else(|| missing("id"))?;
    let name = property("name").ok_or_else(|| missing("name"))?;
    let script_type = property("scriptType").ok_or_else(|| missing("scriptType"))?;
    let files = files_of(properties.iter())?;
    if files.is_empty() {
        return Err(
            "java.lang.IllegalArgumentException: Must specify at least one file to execute"
                .to_string(),
        );
    }
    Ok(ApplicationConfig::Script(ScriptApplication {
        id: id.to_string(),
        name: name.to_string(),
        script_type: script_type.to_lowercase(),
        files,
        enabled,
    }))
}

/// The applications of the directory `dir`, in the order of their file names, each with
/// the path of its file. None if there is no such directory.
///
/// Java also leaves out a file its user may not read; here such a file is an error.
///
/// Behavior: `io.deephaven.appmode.ApplicationConfigImpl#find`, `#isAppFile`, and
/// `ApplicationInjector#run`, which takes a missing directory for an empty one
pub fn find(dir: &Path) -> Result<Vec<(PathBuf, ApplicationConfig)>, String> {
    let failed = |path: &Path, error: &dyn fmt::Display| {
        format!("cannot read {}: {error}", path.display())
    };
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(failed(dir, &error)),
    };
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| failed(dir, &error))?;
        let name = entry.file_name();
        // the type of the entry itself: a link to a file is not a file
        let is_file = entry
            .file_type()
            .map_err(|error| failed(&entry.path(), &error))?
            .is_file();
        if is_file && name.as_encoded_bytes().ends_with(b".app") {
            paths.push(entry.path());
        }
    }
    paths.sort();
    let mut configs = Vec::with_capacity(paths.len());
    for path in paths {
        let bytes = std::fs::read(&path).map_err(|error| failed(&path, &error))?;
        let text = String::from_utf8(bytes).map_err(|error| {
            let at = error.utf8_error().valid_up_to();
            failed(
                &path,
                &format!("java.nio.charset.MalformedInputException: not UTF-8 at byte {at}"),
            )
        })?;
        let config = parse(&text).map_err(|error| format!("{}: {error}", path.display()))?;
        configs.push((path, config));
    }
    Ok(configs)
}

thread_local! {
    /// The application whose script runs on this thread.
    ///
    /// Behavior: `io.deephaven.appmode.ApplicationContext#states`
    static CURRENT: RefCell<Option<Application>> = const { RefCell::new(None) };
}

/// Runs `run` on this thread as a script of `application`: [`current`] is the application
/// until `run` returns, and nothing after it.
///
/// Behavior: `ApplicationContext#runUnderContext`
pub fn under_context<R>(application: &Application, run: impl FnOnce() -> R) -> R {
    struct Clear;
    impl Drop for Clear {
        fn drop(&mut self) {
            CURRENT.set(None);
        }
    }
    CURRENT.set(Some(application.clone()));
    let _clear = Clear;
    run()
}

/// The application whose script runs on this thread; `None` on any other thread and when
/// the scripts have run.
///
/// Behavior: `ApplicationContext#get`
pub fn current() -> Option<Application> {
    CURRENT.with_borrow(Clone::clone)
}

/// The text of a script: the bytes of the file as UTF-8, a byte sequence that is not
/// UTF-8 as U+FFFD, without the spaces and control characters at its start and its end.
///
/// Behavior: `io.deephaven.base.FileUtils#readTextFile(File)`,
/// `ApplicationFactory#absolutePath`, `AbstractScriptSession#evaluateScript(Path)`
fn read_script(dir: &Path, file: &Path) -> Result<String, String> {
    // an absolute `file` replaces `dir`, and `..` is left to the file system
    let path = dir.join(file);
    if !path.exists() {
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        return Err(format!(
            "java.lang.IllegalArgumentException: File {name} doesn't exist."
        ));
    }
    let bytes = std::fs::read(&path).map_err(|error| {
        format!(
            "io.deephaven.UncheckedDeephavenException: could not read script file {}: {error}",
            path.display()
        )
    })?;
    let text = String::from_utf8_lossy(&bytes);
    Ok(text.trim_matches(|c| c <= ' ').to_string())
}

/// Starts one application: its scripts run in order in `console`, each as a script of the
/// application. The first that fails ends the start.
///
/// Behavior: `io.deephaven.server.appmode.ApplicationFactory#visit(ScriptApplication)`,
/// `#evaluateScripts`
fn start(
    dir: &Path,
    script: &ScriptApplication,
    applications: &Arc<Applications>,
    console: Option<&Arc<Console>>,
) -> Result<Application, String> {
    // the session of a Java server without a console is of the type NoLanguage
    let session_type = console.map_or("NoLanguage", |console| console.script_type());
    let (language, console) = match script.script_type.as_str() {
        "groovy" => ("Groovy", None),
        "python" => (
            "Python",
            console.filter(|_| session_type.eq_ignore_ascii_case("python")),
        ),
        other => {
            return Err(format!(
                "java.lang.UnsupportedOperationException: Currently do not support script \
                 sessions of type '{other}'"
            ))
        }
    };
    let Some(console) = console else {
        return Err(format!(
            "java.lang.IllegalArgumentException: Cannot instantiate {language} application \
             on a {session_type} app session"
        ));
    };
    let application = applications
        .create(script.id.clone(), script.name.clone())
        .map_err(|error| format!("java.lang.IllegalArgumentException: {error}"))?;
    for file in &script.files {
        let code = read_script(dir, file)?;
        if let Some(error) = console.evaluate_script(&code, &application).error {
            return Err(error);
        }
    }
    Ok(application)
}

/// Starts the enabled applications of the directory `dir`, in the order of their file
/// names, with the scripts run by `console`. Returns them, or the first failure; the
/// server must not start after a failure.
///
/// Must be called before the server accepts clients, on a thread that may block.
///
/// Behavior: `io.deephaven.server.appmode.ApplicationInjector#run`, `#loadApplication`
pub fn load(
    dir: &Path,
    applications: &Arc<Applications>,
    console: Option<&Arc<Console>>,
) -> Result<Vec<Application>, String> {
    tracing::info!("Finding custom application(s) in '{}'...", dir.display());
    let configs = find(dir).inspect_err(|error| {
        tracing::error!("Failed to read custom application config(s): {error}");
    })?;
    if configs.is_empty() {
        tracing::warn!("No custom application(s) found...");
    }
    let mut started = Vec::new();
    for (path, config) in &configs {
        let ApplicationConfig::Script(script) = config else {
            tracing::info!("Skipping disabled custom application: {config}");
            continue;
        };
        if !script.enabled {
            tracing::info!("Skipping disabled custom application: {config}");
            continue;
        }
        tracing::info!("Starting application '{config}'");
        let application = start(dir, script, applications, console)
            .map_err(|error| format!("application {}: {error}", path.display()))?;
        tracing::info!("\tfound {} exports", application.field_names().len());
        started.push(application);
    }
    Ok(started)
}
