//! Lists the Python modules under `python/deephaven` and `python/deephaven_internal`, and
//! the module `python/jpy.py`, so that the server binary can hold their source
//! (`src/console/python/mod.rs`, `MODULES`). A module added to a package is picked up by
//! its file being there; nothing else has to change.
//!
//! No Java counterpart: the Java server imports the two packages from the Python
//! environment.

use std::env;
use std::fmt::Write as _;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

/// Every `.py` file under `directory`, as paths relative to `root`, sorted.
fn python_files(root: &Path, directory: &Path, found: &mut Vec<PathBuf>) -> io::Result<()> {
    for entry in fs::read_dir(directory)? {
        let path = entry?.path();
        if path.is_dir() {
            python_files(root, &path, found)?;
        } else if path.extension().is_some_and(|extension| extension == "py") {
            found.push(
                path.strip_prefix(root)
                    .expect("under the root")
                    .to_path_buf(),
            );
        }
    }
    found.sort();
    Ok(())
}

fn main() -> io::Result<()> {
    let root = PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("set by cargo")).join("python");
    println!("cargo:rerun-if-changed={}", root.display());
    let mut files = Vec::new();
    for package in ["deephaven", "deephaven_internal"] {
        python_files(&root, &root.join(package), &mut files)?;
    }
    // the one module that is no part of a package (`docs/CONSOLE.md`, "The jpy module")
    files.push(PathBuf::from("jpy.py"));

    // `(module name, is a package, source)`, a package before its modules
    let mut list = String::from("&[\n");
    for file in &files {
        let mut parts: Vec<String> = file
            .with_extension("")
            .components()
            .map(|part| part.as_os_str().to_string_lossy().into_owned())
            .collect();
        let is_package = parts.last().is_some_and(|last| last == "__init__");
        if is_package {
            parts.pop();
        }
        writeln!(
            list,
            "    ({:?}, {is_package}, include_str!({:?})),",
            parts.join("."),
            root.join(file).display().to_string()
        )
        .expect("writing to a string");
    }
    list.push_str("]\n");
    let out = PathBuf::from(env::var("OUT_DIR").expect("set by cargo"));
    fs::write(out.join("python_modules.rs"), list)
}
