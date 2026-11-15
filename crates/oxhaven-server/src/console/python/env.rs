//! Where the embedded interpreter finds its packages.
//!
//! The server is linked against the libpython of one Python minor version. The standard
//! library and the installed packages come from an environment of that version: the one a
//! [`PythonEnv`] names, which is a virtual environment or a Python installation, or else the
//! installation the linked Python finds by itself. An environment of another version is
//! refused before the interpreter starts, because its packages would be compiled for
//! another interpreter, or silently not be found at all.
//!
//! No Java counterpart: the Java server is started by a Python process and lives in that
//! process's environment, or starts the `python` its configuration names through jpy.
//! `docs/CONSOLE.md`, "The Python environment", has the rules a user sees.

#[cfg(test)]
mod tests;

use std::ffi::{CStr, CString};
use std::fmt;
use std::mem::MaybeUninit;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;

use pyo3::ffi;
use pyo3::prelude::*;

/// The environment variable that an activated virtual environment sets to its directory.
pub const VIRTUAL_ENV: &str = "VIRTUAL_ENV";

/// The name of the file that makes a directory a virtual environment.
const VENV_CONFIG: &str = "pyvenv.cfg";

/// What the user chose for the interpreter: the options `--python-home` and `--python-path`
/// of the server.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PythonEnv {
    /// A virtual environment, or the prefix of a Python installation. `None` leaves the
    /// choice to the linked Python.
    pub home: Option<PathBuf>,
    /// Directories searched for modules before the environment's own, as `PYTHONPATH` is.
    pub path: Vec<PathBuf>,
}

impl PythonEnv {
    /// The environment the process was started in: the activated virtual environment, if
    /// there is one.
    pub fn from_environment() -> Self {
        Self {
            home: virtual_env(),
            path: Vec::new(),
        }
    }
}

/// The directory `VIRTUAL_ENV` names, if it names one.
pub fn virtual_env() -> Option<PathBuf> {
    std::env::var_os(VIRTUAL_ENV)
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
}

/// A Python minor version, `3.13`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct MinorVersion {
    pub major: u32,
    pub minor: u32,
}

impl MinorVersion {
    /// The first two numbers of a version text: of `3.13.2`, of `3.13.2.final.0`, or of
    /// `3.13.2 (main, ...) [GCC ...]`.
    fn parse(text: &str) -> Option<Self> {
        let mut numbers = text.trim().split('.');
        let major = numbers.next()?.parse().ok()?;
        let minor = numbers.next()?;
        let digits = minor
            .find(|c: char| !c.is_ascii_digit())
            .unwrap_or(minor.len());
        let minor = minor[..digits].parse().ok()?;
        Some(Self { major, minor })
    }
}

impl fmt::Display for MinorVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.major, self.minor)
    }
}

/// The interpreter of this process, once it runs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Interpreter {
    /// `platform.python_version()`, which clients read as `python.version`.
    pub version: String,
    /// `sys.prefix`: the environment whose packages are importable.
    pub prefix: String,
}

/// The version of the libpython this server is linked against.
pub fn linked_version() -> MinorVersion {
    // SAFETY: `Py_GetVersion` may be called before the interpreter is initialized, and
    // returns a pointer to a NUL-terminated string with static lifetime.
    let text = unsafe { CStr::from_ptr(ffi::Py_GetVersion()) }.to_string_lossy();
    MinorVersion::parse(&text).expect("libpython reports its version as major.minor")
}

/// How the interpreter is told where its environment is.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Layout {
    /// Nothing is said: the linked Python looks for its installation.
    Linked,
    /// The interpreter is started as the `python` of a virtual environment would be, so
    /// that it reads `pyvenv.cfg` and takes the standard library from the base
    /// installation and the packages from the environment.
    Venv { home: PathBuf, executable: PathBuf },
    /// The prefix of an installation, as `PYTHONHOME` names one.
    Installation { home: PathBuf },
}

/// The version `pyvenv.cfg` says its environment was made for. `venv` writes
/// `version = 3.13.2`, virtualenv `version_info = 3.13.2.final.0`, uv
/// `version_info = 3.13.2`.
fn declared_version(config: &str) -> Option<MinorVersion> {
    let value_of = |wanted: &str| {
        config.lines().find_map(|line| {
            let (key, value) = line.split_once('=')?;
            (key.trim() == wanted).then(|| MinorVersion::parse(value))?
        })
    };
    value_of("version").or_else(|| value_of("version_info"))
}

/// The Python versions that have a library directory under `home`, sorted.
fn library_versions(home: &Path) -> Vec<MinorVersion> {
    let Ok(entries) = std::fs::read_dir(home.join("lib")) else {
        return Vec::new();
    };
    let mut versions: Vec<MinorVersion> = entries
        .filter_map(|entry| {
            let name = entry.ok()?.file_name();
            MinorVersion::parse(name.to_str()?.strip_prefix("python")?)
        })
        .collect();
    versions.sort();
    versions
}

fn wrong_version(home: &Path, found: MinorVersion, linked: MinorVersion) -> String {
    format!(
        "the Python environment {} is for Python {found}, and this server is linked against \
         Python {linked}: name an environment made with Python {linked}, or build the server \
         against Python {found} (PYO3_PYTHON, docs/CONSOLE.md)",
        home.display()
    )
}

/// Decides how to start the interpreter in `env`, or says why it cannot be started there.
fn layout(env: &PythonEnv, linked: MinorVersion) -> Result<Layout, String> {
    for directory in &env.path {
        if !directory.is_dir() {
            return Err(format!(
                "the Python path entry {} is not a directory",
                directory.display()
            ));
        }
    }
    let Some(home) = &env.home else {
        return Ok(Layout::Linked);
    };
    let home = home.canonicalize().map_err(|error| {
        format!(
            "the Python environment {} cannot be used: {error}",
            home.display()
        )
    })?;
    let library = home.join("lib").join(format!("python{linked}"));
    let others = || {
        let versions = library_versions(&home);
        versions.into_iter().find(|version| *version != linked)
    };
    if let Ok(config) = std::fs::read_to_string(home.join(VENV_CONFIG)) {
        match declared_version(&config) {
            Some(declared) if declared != linked => {
                return Err(wrong_version(&home, declared, linked))
            }
            // The directory layout is that of Unix; on Windows the file's word is taken.
            _ if cfg!(unix) && !library.join("site-packages").is_dir() => {
                return Err(match others() {
                    Some(found) => wrong_version(&home, found, linked),
                    None => format!(
                        "the Python environment {} has no {}",
                        home.display(),
                        library.join("site-packages").display()
                    ),
                })
            }
            _ => {}
        }
        let executable = if cfg!(windows) {
            home.join("Scripts").join("python.exe")
        } else {
            home.join("bin").join("python")
        };
        return Ok(Layout::Venv { home, executable });
    }
    if cfg!(unix) && !library.join("os.py").is_file() {
        return Err(match others() {
            Some(found) => wrong_version(&home, found, linked),
            None => format!(
                "{} is neither a virtual environment (it has no {VENV_CONFIG}) nor a Python \
                 {linked} installation (it has no {})",
                home.display(),
                library.join("os.py").display()
            ),
        });
    }
    Ok(Layout::Installation { home })
}

/// A path as the C string that `PyConfig_SetBytesString` decodes.
fn c_path(path: &Path) -> Result<CString, String> {
    CString::new(path.as_os_str().as_encoded_bytes())
        .map_err(|_| format!("the path {} contains a NUL", path.display()))
}

/// Initializes the interpreter with `layout` and releases the interpreter lock, so that
/// any thread can take it with `Python::attach`.
///
/// The interpreter must not be initialized yet.
fn start_interpreter(layout: &Layout) -> Result<(), String> {
    let (executable, home) = match layout {
        Layout::Linked => (None, None),
        Layout::Venv { executable, .. } => (Some(c_path(executable)?), None),
        Layout::Installation { home } => (None, Some(c_path(home)?)),
    };
    // SAFETY: `PyConfig_InitPythonConfig` initializes every field of the structure it is
    // given, so it may be read afterwards. The fields that are set are set through
    // `PyConfig_SetBytesString`, which copies the NUL-terminated strings that `executable`
    // and `home` own until the end of this function. The structure is cleared once,
    // after its last use. `Py_InitializeFromConfig` is called once per process: the caller
    // holds the lock of `STARTED` and has seen that no interpreter is initialized. It
    // returns with the interpreter lock held by this thread, which is what
    // `PyEval_SaveThread` requires. A status's message is a static string or null.
    unsafe {
        let mut config = MaybeUninit::<ffi::PyConfig>::uninit();
        ffi::PyConfig_InitPythonConfig(config.as_mut_ptr());
        let config = config.assume_init_mut();
        // as PyO3's own initialization: ctrl-c stays with the server, not with Python
        config.install_signal_handlers = 0;
        let mut status = ffi::PyStatus_Ok();
        if let Some(executable) = &executable {
            let field = std::ptr::addr_of_mut!(config.executable);
            status = ffi::PyConfig_SetBytesString(config, field, executable.as_ptr());
        }
        if let Some(home) = &home {
            let field = std::ptr::addr_of_mut!(config.home);
            status = ffi::PyConfig_SetBytesString(config, field, home.as_ptr());
        }
        if ffi::PyStatus_Exception(status) == 0 {
            status = ffi::Py_InitializeFromConfig(config);
        }
        ffi::PyConfig_Clear(config);
        if ffi::PyStatus_Exception(status) != 0 {
            let message = if status.err_msg.is_null() {
                "no reason given".into()
            } else {
                CStr::from_ptr(status.err_msg).to_string_lossy()
            };
            return Err(format!("the Python interpreter did not start: {message}"));
        }
        ffi::PyEval_SaveThread();
    }
    Ok(())
}

/// Puts the directories of `--python-path` first on `sys.path` and reads what the
/// interpreter says about itself.
fn describe(py: Python<'_>, env: &PythonEnv) -> PyResult<Interpreter> {
    let sys = py.import("sys")?;
    let path = sys.getattr("path")?;
    for directory in env.path.iter().rev() {
        // as text: an entry of `sys.path` is a `str`, not a `pathlib.Path`
        path.call_method1("insert", (0, directory.as_os_str()))?;
    }
    Ok(Interpreter {
        version: py
            .import("platform")?
            .call_method0("python_version")?
            .extract()?,
        prefix: sys.getattr("prefix")?.extract()?,
    })
}

fn start(env: &PythonEnv) -> Result<Interpreter, String> {
    let layout = layout(env, linked_version())?;
    // SAFETY: `Py_IsInitialized` may be called at any time.
    if unsafe { ffi::Py_IsInitialized() } == 0 {
        start_interpreter(&layout)?;
    } else if layout != Layout::Linked {
        return Err(
            "the Python interpreter of this process was started before its environment \
             was chosen"
                .to_string(),
        );
    }
    let interpreter = Python::attach(|py| describe(py, env))
        .map_err(|error| format!("the Python interpreter did not start: {error}"))?;
    if let Layout::Venv { home, .. } = &layout {
        // An interpreter that did not recognize the environment runs, without its packages.
        if Path::new(&interpreter.prefix).canonicalize().ok().as_ref() != Some(home) {
            return Err(format!(
                "the Python interpreter did not adopt the virtual environment {}: its \
                 prefix is {}",
                home.display(),
                interpreter.prefix
            ));
        }
    }
    Ok(interpreter)
}

/// The environment the interpreter of this process was first asked to start in, and what
/// came of it. A process has one interpreter, and it is initialized once.
static STARTED: OnceLock<(PythonEnv, Result<Interpreter, String>)> = OnceLock::new();

/// Starts the interpreter of this process in `env`, or does nothing if it already runs
/// there. Fails when the environment is not one of the linked Python version, when the
/// interpreter does not start in it, and when it already runs in another environment.
pub fn initialize(env: &PythonEnv) -> Result<Interpreter, String> {
    let (first, outcome) = STARTED.get_or_init(|| (env.clone(), start(env)));
    if first != env {
        return Err(format!(
            "the Python interpreter of this process already runs in another environment \
             ({first:?}), and cannot be moved to {env:?}"
        ));
    }
    outcome.clone()
}

/// The interpreter of this process, started in the environment the process was started in
/// (`VIRTUAL_ENV`) unless [`initialize`] chose another before.
pub fn interpreter() -> Result<Interpreter, String> {
    match STARTED.get() {
        Some((_, outcome)) => outcome.clone(),
        None => initialize(&PythonEnv::from_environment()),
    }
}
