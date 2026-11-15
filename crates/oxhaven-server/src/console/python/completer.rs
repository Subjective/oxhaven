//! Completion of Python by the library `jedi`, running in the session's interpreter and
//! looking at the session's variables.
//!
//! The Python side is the package `deephaven_internal.auto_completer` of this server
//! (`python/deephaven_internal`), whose object `jedi_settings` keeps the documents and
//! asks `jedi`. As in Java, the server makes it a variable of the session when a client
//! opens a completion stream, and finds it there for every message: a script may switch
//! it off (`jedi_settings.mode = 'OFF'`), and one that deletes the variable breaks
//! completion until the next stream is opened.
//!
//! `jedi` and `docstring_parser` come from the Python environment of the server
//! (`docs/CONSOLE.md`, "The Python environment"); neither is part of this server. When
//! the package cannot be imported, the session has no completer from then on and says so
//! in the log once.
//!
//! Nothing here takes the update graph's lock or the session's snapshot lock, so none of
//! it waits for a running command; `console/completer.rs` says how the interpreter is
//! shared with one.
//!
//! # Text
//!
//! A client counts columns in UTF-16 code units, and Java hands `jedi` a document in
//! which every code unit is one character: jpy makes a Python `str` of a Java `String`
//! unit by unit, so a character outside the basic plane is two surrogate characters
//! there. Observed on Java 42.9: a completion after such a character is answered at the
//! client's column. The documents here are kept in that form for that reason.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict, PyModule, PyString};

use super::super::completer::{Completer, Completion, Signature};

/// What Java evaluates in the session when a client opens a completion stream.
///
/// Behavior: `ConsoleServiceGrpcImpl#autoCompleteStream`
const SETUP: &str = "from deephaven_internal.auto_completer import jedi_settings ; \
                     jedi_settings.set_scope(globals())";

/// The variable of the session that holds the completer.
const VARIABLE: &str = "jedi_settings";

/// Taken, before the interpreter lock, for as long as `jedi` computes one answer.
static ONE_ANSWER: Mutex<()> = Mutex::new(());

/// A Python `str` whose characters are the code units of `text`.
fn from_units<'py>(py: Python<'py>, units: &[u16]) -> PyResult<Bound<'py, PyAny>> {
    // UTF-8 extended to surrogates, which Python decodes one by one with `surrogatepass`
    let mut bytes = Vec::with_capacity(units.len());
    for &unit in units {
        match char::from_u32(u32::from(unit)) {
            Some(character) => {
                bytes.extend_from_slice(character.encode_utf8(&mut [0; 4]).as_bytes());
            }
            None => bytes.extend_from_slice(&[
                0xE0 | (unit >> 12) as u8,
                0x80 | ((unit >> 6) & 0x3F) as u8,
                0x80 | (unit & 0x3F) as u8,
            ]),
        }
    }
    PyBytes::new(py, &bytes).call_method1("decode", ("utf-8", "surrogatepass"))
}

/// The UTF-16 code units of a Python `str`, a surrogate character as itself.
fn to_units(text: &Bound<'_, PyAny>) -> PyResult<Vec<u16>> {
    let text = text.cast::<PyString>().map_err(|_| {
        let type_name = text.get_type().name();
        let type_name = type_name.map_or_else(|_| "object".to_string(), |name| name.to_string());
        PyTypeError::new_err(format!("expected a str, got {type_name}"))
    })?;
    let bytes: Vec<u8> = text
        .call_method1("encode", ("utf-16-le", "surrogatepass"))?
        .extract()?;
    Ok(bytes
        .chunks_exact(2)
        .map(|pair| u16::from_le_bytes([pair[0], pair[1]]))
        .collect())
}

/// A Python `str` as text for a client: surrogate characters that pair are the character
/// they encode, as when Java reads the `str` into a `String`.
fn text_of(text: &Bound<'_, PyAny>) -> PyResult<String> {
    Ok(String::from_utf16_lossy(&to_units(text)?))
}

/// The completer of a Python session: the object the session's variable `jedi_settings`
/// holds at the time of each call.
pub(super) struct PythonCompleter {
    globals: Py<PyDict>,
}

impl PythonCompleter {
    fn with<T>(
        &self,
        ask: impl FnOnce(Python<'_>, &Bound<'_, PyAny>) -> PyResult<T>,
    ) -> Result<T, String> {
        Python::attach(|py| {
            let settings = self.globals.bind(py).get_item(VARIABLE)?.ok_or_else(|| {
                pyo3::exceptions::PyNameError::new_err(format!(
                    "Missing variable {VARIABLE}: the session no longer holds the completer"
                ))
            })?;
            ask(py, &settings)
        })
        .map_err(|error: PyErr| error.to_string())
    }

    /// Asks `jedi` through `method`, which takes a position.
    fn at<T>(
        &self,
        method: &str,
        position: (&str, i32, i32, i32),
        read: impl FnOnce(&Bound<'_, PyAny>) -> PyResult<T>,
    ) -> Result<T, String> {
        let _one = ONE_ANSWER.lock().unwrap_or_else(PoisonError::into_inner);
        self.with(|_, settings| read(&settings.call_method1(method, position)?))
    }
}

impl Completer for PythonCompleter {
    fn open_document(&self, text: &str, uri: &str, version: i32) -> Result<(), String> {
        let units: Vec<u16> = text.encode_utf16().collect();
        self.with(|py, settings| {
            let text = from_units(py, &units)?;
            settings.call_method1("open_doc", (text, uri, version))?;
            Ok(())
        })
    }

    fn document(&self, uri: &str) -> Result<Vec<u16>, String> {
        self.with(|_, settings| to_units(&settings.call_method1("get_doc", (uri,))?))
    }

    fn update_document(&self, text: &[u16], uri: &str, version: i32) -> Result<(), String> {
        self.with(|py, settings| {
            settings.call_method1("update_doc", (from_units(py, text)?, uri, version))?;
            Ok(())
        })
    }

    fn close_document(&self, uri: &str) -> Result<(), String> {
        self.with(|_, settings| settings.call_method1("close_doc", (uri,)).map(drop))
    }

    fn is_enabled(&self) -> Result<bool, String> {
        self.with(|_, settings| settings.call_method0("is_enabled")?.is_truthy())
    }

    /// Behavior: `PythonAutoCompleteObserver#getCompletionItems`, which reads each
    /// result as the list `[name, start, detail, docstring, kind]`
    fn completions(
        &self,
        uri: &str,
        version: i32,
        line: i32,
        column: i32,
    ) -> Result<Vec<Completion>, String> {
        self.at("do_completion", (uri, version, line, column), |results| {
            let results: Vec<Vec<Bound<'_, PyAny>>> = results.extract()?;
            let completion = |fields: Vec<Bound<'_, PyAny>>| match &fields[..] {
                [name, start, detail, docstring, kind, ..] => Ok(Completion {
                    name: text_of(name)?,
                    start: start.extract()?,
                    detail: text_of(detail)?,
                    docstring: text_of(docstring)?,
                    kind: kind.extract()?,
                }),
                _ => Err(PyTypeError::new_err(
                    "Expected list-of-lists from jedi_settings.do_completion",
                )),
            };
            results.into_iter().map(completion).collect()
        })
    }

    /// Behavior: `PythonAutoCompleteObserver#getSignatureHelp`, which reads each result
    /// as the list `[label, documentation, [[label, documentation], ...], active]`
    fn signatures(
        &self,
        uri: &str,
        version: i32,
        line: i32,
        column: i32,
    ) -> Result<Vec<Signature>, String> {
        self.at(
            "do_signature_help",
            (uri, version, line, column),
            |results| {
                let results: Vec<Vec<Bound<'_, PyAny>>> = results.extract()?;
                let malformed = || {
                    PyTypeError::new_err(
                        "Expected list-of-lists from jedi_settings.do_signature_help",
                    )
                };
                let signature = |fields: Vec<Bound<'_, PyAny>>| match &fields[..] {
                    [label, documentation, parameters, active, ..] => {
                        let parameters: Vec<Vec<Bound<'_, PyAny>>> = parameters.extract()?;
                        let parameter = |fields: Vec<Bound<'_, PyAny>>| match &fields[..] {
                            [label, documentation, ..] => {
                                Ok((text_of(label)?, text_of(documentation)?))
                            }
                            _ => Err(malformed()),
                        };
                        Ok(Signature {
                            label: text_of(label)?,
                            documentation: text_of(documentation)?,
                            parameters: parameters
                                .into_iter()
                                .map(parameter)
                                .collect::<PyResult<_>>()?,
                            active_parameter: active.extract()?,
                        })
                    }
                    _ => Err(malformed()),
                };
                results.into_iter().map(signature).collect()
            },
        )
    }

    fn hover(&self, uri: &str, version: i32, line: i32, column: i32) -> Result<String, String> {
        self.at("do_hover", (uri, version, line, column), text_of)
    }
}

/// Makes the completer a variable of the session whose globals are `globals`, as Java
/// does for each stream a client opens, and returns it; `None` if the package that holds
/// it cannot be imported. That is logged the first time and sets `warned`, and a session
/// that was warned has no completer from then on and logs nothing more, as in Java
/// (`ConsoleServiceGrpcImpl#ALREADY_WARNED_ABOUT_NO_AUTOCOMPLETE`).
///
/// The setup runs on the calling thread under the interpreter lock alone; Java evaluates
/// it as a command of the session, which waits for a command that is running.
///
/// Behavior: `ConsoleServiceGrpcImpl#autoCompleteStream`
pub(super) fn ready(
    helpers: &Py<PyModule>,
    globals: &Py<PyDict>,
    warned: &AtomicBool,
) -> Result<Option<Arc<dyn Completer>>, String> {
    if warned.load(Ordering::SeqCst) {
        return Ok(None);
    }
    tracing::info!("Evaluating command: {SETUP}");
    let found = Python::attach(|py| -> PyResult<_> {
        let globals = globals.bind(py);
        // an error of the script is not an error of this: Java looks for the variable
        // whatever the script did
        let error: Option<String> = helpers
            .bind(py)
            .call_method1("run", (SETUP, globals))?
            .extract()?;
        let Some(settings) = globals.get_item(VARIABLE)? else {
            return Ok(Err(error));
        };
        Ok(Ok(settings.call_method0("can_jedi")?.is_truthy()?))
    });
    let can_jedi = match found {
        Ok(Ok(can_jedi)) => can_jedi,
        Ok(Err(import_error)) => {
            if !warned.swap(true, Ordering::SeqCst) {
                tracing::error!("Autocomplete package not found; disabling autocomplete.");
                tracing::error!(
                    "Completion in the console needs the packages jedi and docstring_parser \
                     in the server's Python environment (docs/CONSOLE.md)."
                );
                if let Some(import_error) = import_error {
                    tracing::error!("{}", import_error.trim_end());
                }
            }
            false
        }
        // Java lets the exception of `can_jedi` out of the call. That happens when the
        // import failed and the session has a variable of that name of its own, which was
        // not observed.
        Err(error) => {
            return Err(format!(
                "not ported: io.deephaven.server.console.ConsoleServiceGrpcImpl#autoCompleteStream \
                 when the session's variable {VARIABLE} is not the completer ({error})"
            ));
        }
    };
    if can_jedi {
        tracing::info!("Using jedi for python autocomplete");
        let globals = Python::attach(|py| globals.clone_ref(py));
        Ok(Some(Arc::new(PythonCompleter { globals })))
    } else {
        tracing::info!(
            "No jedi dependency available in python environment; disabling autocomplete."
        );
        Ok(None)
    }
}
