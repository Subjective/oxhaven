//! Tests of the interpreter's environment.
//!
//! A process initializes its interpreter once, so a test that needs an interpreter in a
//! particular environment runs this test binary again as a child process, with only the
//! test [`child_reports_its_environment`], which prints what the interpreter found.

use std::path::{Path, PathBuf};
use std::process::Command;

use super::*;

const LINKED: MinorVersion = MinorVersion {
    major: 3,
    minor: 13,
};
const OTHER: MinorVersion = MinorVersion {
    major: 3,
    minor: 12,
};

/// A directory that is removed when the test ends.
struct Scratch(PathBuf);

impl Scratch {
    fn new(name: &str) -> Self {
        let directory =
            std::env::temp_dir().join(format!("oxhaven-env-{}-{name}", std::process::id()));
        let _ = std::fs::remove_dir_all(&directory);
        std::fs::create_dir_all(&directory).unwrap();
        Self(directory.canonicalize().unwrap())
    }

    /// Writes `text` to the file `relative`, with its directories.
    fn write(&self, relative: &str, text: &str) -> &Self {
        let file = self.0.join(relative);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, text).unwrap();
        self
    }

    fn env(&self) -> PythonEnv {
        PythonEnv {
            home: Some(self.0.clone()),
            path: Vec::new(),
        }
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn a_version_is_the_first_two_numbers() {
    let version = |text| MinorVersion::parse(text).map(|version| version.to_string());
    assert_eq!(version("3.13.2").as_deref(), Some("3.13"));
    assert_eq!(version("3.9").as_deref(), Some("3.9"));
    assert_eq!(version(" 3.14.0rc1 ").as_deref(), Some("3.14"));
    assert_eq!(version("3.13.2.final.0").as_deref(), Some("3.13"));
    assert_eq!(
        version("3.13.18 (main, Oct 26 2026) [GCC 13]").as_deref(),
        Some("3.13")
    );
    assert_eq!(version("3"), None);
    assert_eq!(version("three.one"), None);
    assert_eq!(version(""), None);
}

#[test]
fn the_version_of_an_environment_is_read_as_each_tool_writes_it() {
    let venv = "home = /usr/bin\ninclude-system-site-packages = false\nversion = 3.12.3\n";
    assert_eq!(declared_version(venv), Some(OTHER));
    let virtualenv = "home = /usr/bin\nimplementation = CPython\n\
                      version_info = 3.13.1.final.0\nvirtualenv = 20.29\nversion = 3.13.1\n";
    assert_eq!(declared_version(virtualenv), Some(LINKED));
    let uv = "home = /usr/bin\nimplementation = CPython\nuv = 0.9.1\nversion_info = 3.12.9\n";
    assert_eq!(declared_version(uv), Some(OTHER));
    assert_eq!(declared_version("home = /usr/bin\n"), None);
    // a key that only ends in the word is not the key
    assert_eq!(declared_version("uv_version = 3.12.9\n"), None);
}

#[test]
fn no_choice_leaves_the_environment_to_the_linked_python() {
    assert_eq!(layout(&PythonEnv::default(), LINKED), Ok(Layout::Linked));
}

#[cfg(unix)]
#[test]
fn a_virtual_environment_of_the_linked_version_is_started_as_its_python() {
    let scratch = Scratch::new("venv-ok");
    scratch
        .write("pyvenv.cfg", "home = /usr/bin\nversion = 3.13.4\n")
        .write("lib/python3.13/site-packages/marker.py", "");
    assert_eq!(
        layout(&scratch.env(), LINKED),
        Ok(Layout::Venv {
            home: scratch.0.clone(),
            executable: scratch.0.join("bin").join("python"),
        })
    );
}

#[cfg(unix)]
#[test]
fn a_virtual_environment_of_another_version_is_refused_with_both_versions() {
    let scratch = Scratch::new("venv-other");
    scratch
        .write("pyvenv.cfg", "home = /usr/bin\nversion = 3.12.3\n")
        .write("lib/python3.12/site-packages/marker.py", "");
    let refused = layout(&scratch.env(), LINKED).unwrap_err();
    assert!(
        refused.starts_with(&format!(
            "the Python environment {} is for Python 3.12, and this server is linked \
             against Python 3.13:",
            scratch.0.display()
        )),
        "{refused}"
    );

    // without a version in the file, the library directory says which Python it is for
    scratch.write("pyvenv.cfg", "home = /usr/bin\n");
    let by_directory = layout(&scratch.env(), LINKED).unwrap_err();
    assert_eq!(by_directory, refused);

    // and the same environment is accepted by a server linked against its version
    assert!(matches!(
        layout(&scratch.env(), OTHER),
        Ok(Layout::Venv { .. })
    ));
}

#[cfg(unix)]
#[test]
fn a_virtual_environment_without_packages_directory_is_refused() {
    let scratch = Scratch::new("venv-empty");
    scratch.write("pyvenv.cfg", "home = /usr/bin\n");
    let refused = layout(&scratch.env(), LINKED).unwrap_err();
    assert_eq!(
        refused,
        format!(
            "the Python environment {0} has no {0}/lib/python3.13/site-packages",
            scratch.0.display()
        )
    );
}

#[cfg(unix)]
#[test]
fn an_installation_is_recognized_by_its_standard_library() {
    let scratch = Scratch::new("installation");
    scratch.write("lib/python3.13/os.py", "");
    assert_eq!(
        layout(&scratch.env(), LINKED),
        Ok(Layout::Installation {
            home: scratch.0.clone()
        })
    );
    let refused = layout(&scratch.env(), OTHER).unwrap_err();
    assert!(
        refused.contains("is for Python 3.13, and this server is linked against Python 3.12"),
        "{refused}"
    );
}

#[test]
fn a_directory_that_is_no_environment_is_refused() {
    let scratch = Scratch::new("nothing");
    let refused = layout(&scratch.env(), LINKED).unwrap_err();
    if cfg!(unix) {
        assert!(
            refused.contains("is neither a virtual environment")
                && refused.contains("nor a Python 3.13 installation"),
            "{refused}"
        );
    }

    let missing = PythonEnv {
        home: Some(scratch.0.join("absent")),
        path: Vec::new(),
    };
    let refused = layout(&missing, LINKED).unwrap_err();
    assert!(refused.contains("cannot be used"), "{refused}");

    let no_such_path = PythonEnv {
        home: None,
        path: vec![scratch.0.join("absent")],
    };
    let refused = layout(&no_such_path, LINKED).unwrap_err();
    assert!(refused.ends_with("absent is not a directory"), "{refused}");
}

#[test]
fn the_interpreter_is_the_linked_version_and_stays_in_its_environment() {
    let interpreter = interpreter().unwrap();
    assert_eq!(
        MinorVersion::parse(&interpreter.version),
        Some(linked_version())
    );
    assert_eq!(interpreter, super::interpreter().unwrap());

    let elsewhere = PythonEnv {
        home: Some(PathBuf::from("/oxhaven/no/such/environment")),
        path: Vec::new(),
    };
    if STARTED.get().map(|(first, _)| first) != Some(&elsewhere) {
        let refused = initialize(&elsewhere).unwrap_err();
        assert!(
            refused.contains("already runs in another environment"),
            "{refused}"
        );
    }
}

/// Behavior: `PythonDeephavenSession#setPythonVersion`, `ConfigServiceGrpcImpl`
#[tokio::test]
async fn a_server_with_a_python_console_reports_the_python_version() {
    use crate::test_support::{with_auth, TestServer};
    use oxhaven_proto::grpc::config_service_client::ConfigServiceClient;
    use oxhaven_proto::grpc::config_value::Kind;
    use oxhaven_proto::grpc::ConfigurationConstantsRequest;

    let _turn = super::super::tests::ONE_INTERPRETER.lock().await;
    let server = TestServer::start_with(|state| super::super::start(state).unwrap()).await;
    let bearer = server.login().await;
    let response = ConfigServiceClient::new(server.channel.clone())
        .get_configuration_constants(with_auth(ConfigurationConstantsRequest {}, &bearer))
        .await
        .unwrap()
        .into_inner();
    let reported = response.config_values.get("python.version");
    let expected: String = Python::attach(|py| {
        py.import("platform")?
            .call_method0("python_version")?
            .extract()
    })
    .unwrap();
    assert_eq!(
        reported.and_then(|value| value.kind.clone()),
        Some(Kind::StringValue(expected))
    );
    assert_eq!(response.config_values.len(), 10);
}

// --- an interpreter per environment, in child processes -------------------------------

const CHILD: &str = "OXHAVEN_ENV_TEST_CHILD";
const CHILD_HOME: &str = "OXHAVEN_ENV_TEST_HOME";
const CHILD_PATH: &str = "OXHAVEN_ENV_TEST_PATH";
const REPORT: &str = "oxhaven-env-report ";

/// Not a test of its own: in a child process started by [`child`], starts the interpreter
/// in the environment the parent named and prints what it found. Otherwise does nothing.
#[test]
fn child_reports_its_environment() {
    let Some(mode) = std::env::var_os(CHILD) else {
        return;
    };
    let env = if mode == "options" {
        PythonEnv {
            home: std::env::var_os(CHILD_HOME).map(PathBuf::from),
            path: std::env::var_os(CHILD_PATH)
                .map(|paths| std::env::split_paths(&paths).collect())
                .unwrap_or_default(),
        }
    } else {
        PythonEnv::from_environment()
    };
    let report = |key: &str, value: &str| println!("{REPORT}{key}={value}");
    match initialize(&env) {
        Err(error) => report("error", &error),
        Ok(interpreter) => {
            report("prefix", &interpreter.prefix);
            report("version", &interpreter.version);
            let (marker, base_packages): (String, bool) = Python::attach(|py| {
                let code = c"
import sys
try:
    import oxhaven_env_marker
    marker = oxhaven_env_marker.WHERE
except ImportError:
    marker = 'absent'
base = sys.base_prefix.rstrip('/') + '/'
base_packages = any(
    entry.startswith(base) and entry.rstrip('/').endswith('-packages') for entry in sys.path
)
";
                let scope = pyo3::types::PyDict::new(py);
                py.run(code, Some(&scope), None)?;
                PyResult::Ok((
                    scope.get_item("marker")?.unwrap().extract()?,
                    scope.get_item("base_packages")?.unwrap().extract()?,
                ))
            })
            .unwrap();
            report("marker", &marker);
            report("base_packages", if base_packages { "yes" } else { "no" });
        }
    }
}

/// Runs [`child_reports_its_environment`] in a new process with `variables` and returns
/// its report as `key=value` lines.
fn child(variables: &[(&str, &Path)], mode: &str) -> Vec<String> {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "console::python::env::tests::child_reports_its_environment",
            "--exact",
            "--nocapture",
        ])
        .env(CHILD, mode)
        .env_remove(VIRTUAL_ENV)
        .env_remove(CHILD_HOME)
        .env_remove(CHILD_PATH);
    for (name, value) in variables {
        command.env(name, value);
    }
    let output = command.output().unwrap();
    let text = String::from_utf8_lossy(&output.stdout);
    assert!(
        output.status.success(),
        "{text}\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
    text.lines()
        .filter_map(|line| {
            line.split_once(REPORT)
                .map(|(_, report)| report.to_string())
        })
        .collect()
}

/// The `python` of the installation this test process's interpreter takes its standard
/// library from.
#[cfg(unix)]
fn base_python() -> PathBuf {
    interpreter().unwrap();
    let base: String =
        Python::attach(|py| py.import("sys")?.getattr("base_prefix")?.extract()).unwrap();
    PathBuf::from(base)
        .join("bin")
        .join(format!("python{}", linked_version()))
}

/// A virtual environment made by `python -m venv`, whose packages are the one module
/// `oxhaven_env_marker`.
#[cfg(unix)]
fn venv(scratch: &Scratch) -> PathBuf {
    let directory = scratch.0.join("venv");
    let made = Command::new(base_python())
        .args(["-m", "venv", "--without-pip"])
        .arg(&directory)
        .env_remove(VIRTUAL_ENV)
        .output()
        .unwrap();
    assert!(made.status.success(), "{made:?}");
    scratch.write(
        &format!(
            "venv/lib/python{}/site-packages/oxhaven_env_marker.py",
            linked_version()
        ),
        "WHERE = 'venv'\n",
    );
    directory
}

#[cfg(unix)]
#[test]
fn an_activated_virtual_environment_supplies_the_packages_and_hides_the_base_ones() {
    let scratch = Scratch::new("child-venv");
    let directory = venv(&scratch);
    let report = child(&[(VIRTUAL_ENV, &directory)], "environment");
    assert_eq!(
        report,
        vec![
            format!("prefix={}", directory.display()),
            format!("version={}", interpreter().unwrap().version),
            "marker=venv".to_string(),
            "base_packages=no".to_string(),
        ]
    );
}

#[cfg(unix)]
#[test]
fn the_home_option_names_a_virtual_environment_or_an_installation() {
    let scratch = Scratch::new("child-home");
    let directory = venv(&scratch);
    let report = child(&[(CHILD_HOME, &directory)], "options");
    assert_eq!(report[0], format!("prefix={}", directory.display()));
    assert_eq!(report[2..], ["marker=venv", "base_packages=no"]);

    let installation = base_python();
    let installation = installation.parent().unwrap().parent().unwrap();
    let installation = &installation.canonicalize().unwrap();
    let report = child(&[(CHILD_HOME, installation)], "options");
    assert_eq!(report[0], format!("prefix={}", installation.display()));
    assert_eq!(report[2], "marker=absent");
}

#[cfg(unix)]
#[test]
fn path_directories_are_searched_before_the_environment() {
    let scratch = Scratch::new("child-path");
    let directory = venv(&scratch);
    scratch.write("first/oxhaven_env_marker.py", "WHERE = 'first'\n");
    scratch.write("second/oxhaven_env_marker.py", "WHERE = 'second'\n");
    let path = std::env::join_paths([scratch.0.join("first"), scratch.0.join("second")]).unwrap();
    let report = child(
        &[(CHILD_HOME, &directory), (CHILD_PATH, Path::new(&path))],
        "options",
    );
    assert_eq!(report[0], format!("prefix={}", directory.display()));
    assert_eq!(report[2], "marker=first");

    // without a home, the path is added to what the linked Python finds
    let report = child(&[(CHILD_PATH, &scratch.0.join("second"))], "options");
    assert_eq!(report[2], "marker=second");
}

#[cfg(unix)]
#[test]
fn an_environment_of_another_version_stops_the_interpreter_from_starting() {
    let scratch = Scratch::new("child-other");
    let linked = linked_version();
    let other = MinorVersion {
        major: linked.major,
        minor: linked.minor - 1,
    };
    scratch
        .write(
            "pyvenv.cfg",
            &format!("home = /usr/bin\nversion = {other}.1\n"),
        )
        .write(&format!("lib/python{other}/site-packages/m.py"), "");
    let report = child(&[(VIRTUAL_ENV, &scratch.0)], "environment");
    assert_eq!(report.len(), 1, "{report:?}");
    assert!(
        report[0].starts_with(&format!(
            "error=the Python environment {} is for Python {other}, and this server is \
             linked against Python {linked}:",
            scratch.0.display()
        )),
        "{report:?}"
    );
}
