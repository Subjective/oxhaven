//! JS plugins: the files a plugin package adds to the web IDE, served under
//! `/js-plugins/<name>/` and listed in `/js-plugins/manifest.json`.
//!
//! Java copies the directory of each plugin into a zip file when the plugin is registered
//! and serves that copy, so a file changed later is served as it was. Here the copy is kept
//! in memory; `docs/JS_PLUGINS.md` has what was observed of Java 42.9 and what differs.
//!
//! One rule is this server's own: a file of a plugin's directory is copied only when its real
//! location, with links followed, is inside that directory. Java copies whatever a link to a
//! file leads to.
//!
//! Behavior: `io.deephaven.server.jetty.JsPlugins#register`,
//! `io.deephaven.server.jetty.JsPluginsZipFilesystem#add`, `io.deephaven.plugin.js.JsPlugin`,
//! `io.deephaven.server.plugin.js.JsPluginManifestEntry`

use std::collections::hash_map::DefaultHasher;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::hash::{Hash, Hasher};
use std::path::Path;
use std::sync::{OnceLock, RwLock};

use bytes::Bytes;

/// A JS plugin as a package describes it.
#[derive(Debug, Clone)]
pub struct JsPlugin {
    /// The name in the manifest, and the place of the files under `/js-plugins/`.
    pub name: String,
    pub version: String,
    /// The main file, relative to `path` or absolute.
    pub main: String,
    /// The directory of the files.
    pub path: String,
    /// The loader configuration, as JSON text.
    pub loader: Option<String>,
}

/// What is at a place under `/js-plugins/`.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Node {
    Directory,
    File { content: Bytes, etag: String },
}

#[derive(Default)]
struct State {
    names: Vec<String>,
    /// The manifest's entries, each the JSON of one plugin.
    entries: Vec<String>,
    /// Every directory and file by its segments below `/js-plugins/`. The root is not in it.
    tree: BTreeMap<Vec<String>, Node>,
}

/// The JS plugins of a server.
pub struct JsPlugins {
    state: RwLock<State>,
}

impl Default for JsPlugins {
    fn default() -> Self {
        Self::new()
    }
}

/// The JS plugins of this process. There is one set per process because the plugins are
/// registered by the one Python interpreter a process has.
pub fn registry() -> &'static JsPlugins {
    static REGISTRY: OnceLock<JsPlugins> = OnceLock::new();
    REGISTRY.get_or_init(JsPlugins::new)
}

const MANIFEST: &str = "manifest.json";

impl JsPlugins {
    /// No plugins: the manifest lists none.
    pub fn new() -> Self {
        let mut state = State::default();
        state.write_manifest();
        Self {
            state: RwLock::new(state),
        }
    }

    /// How many plugins are registered.
    pub fn len(&self) -> usize {
        self.read().names.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, State> {
        self.state.read().unwrap_or_else(|e| e.into_inner())
    }

    /// What is at `segments` below `/js-plugins/`. A segment with `?` or a character outside
    /// ASCII is never found: Java registers such a name, copies such a file, and answers
    /// `404` for it.
    pub(crate) fn node(&self, segments: &[String]) -> Option<Node> {
        if segments.is_empty() {
            return Some(Node::Directory);
        }
        if segments
            .iter()
            .any(|segment| segment.contains('?') || !segment.is_ascii())
        {
            return None;
        }
        self.read().tree.get(segments).cloned()
    }

    /// Registers a plugin: checks it, copies the files of its directory and lists it in the
    /// manifest. An `Err` is the text of Java's exception, class name first, and nothing of
    /// the plugin is then served.
    ///
    /// Java leaves the files it copied before a failure in place and serves them; which
    /// those are depends on the order the directory is read in.
    ///
    /// Behavior: `io.deephaven.plugin.js.JsPlugin#checkMain`, `#checkPath`,
    /// `io.deephaven.server.jetty.JsPluginsZipFilesystem#add`, `#checkExisting`,
    /// `io.deephaven.server.jetty.CopyHelper#copyRecursive`
    pub fn register(&self, plugin: &JsPlugin) -> Result<(), String> {
        let path = java_path(&plugin.path);
        // what the reference's Python adapter does before it builds the Java plugin:
        // `path.relativize(path.resolve(main))`
        let main = relativize(&path, &resolve(&path, &java_path(&plugin.main)))
            .map_err(|text| format!("java.lang.IllegalArgumentException: {text}"))?;
        let main_path = resolve(&path, &main);
        if !Path::new(or_here(&main_path)).is_file() {
            return Err(format!(
                "java.lang.IllegalArgumentException: main ('{main_path}') must exist and be a \
                 regular file"
            ));
        }
        if !Path::new(or_here(&path)).is_dir() {
            return Err(format!(
                "java.lang.IllegalArgumentException: path ('{path}') must exist and be a \
                 directory"
            ));
        }

        let mut state = self.state.write().unwrap_or_else(|e| e.into_inner());
        if state.names.contains(&plugin.name) {
            return Err(format!(
                "java.lang.IllegalArgumentException: js plugin with name '{}' already exists. See \
                 https://github.com/deephaven/deephaven-core/issues/3048",
                plugin.name
            ));
        }
        // The place of the files: the name as a path of the zip file, where a backslash
        // separates as a slash does.
        let mut destination: Vec<String> = Vec::new();
        for segment in plugin.name.split(['/', '\\']) {
            match segment {
                "" | "." => {}
                ".." => {
                    destination.pop();
                }
                _ => destination.push(segment.to_string()),
            }
        }
        if plugin.name.is_empty() {
            // the root has no parent to create
            return Err("java.lang.NullPointerException: Cannot invoke \
                 \"java.nio.file.Path.getFileSystem()\" because \"path\" is null"
                .to_string());
        }
        let exists = |at: &[String]| -> String {
            format!(
                "java.io.UncheckedIOException: java.nio.file.FileAlreadyExistsException: /{}",
                at.join("/")
            )
        };
        if destination.is_empty() || state.tree.contains_key(&destination) {
            return Err(exists(std::slice::from_ref(&plugin.name)));
        }
        for depth in 1..destination.len() {
            if let Some(Node::File { .. }) = state.tree.get(&destination[..depth]) {
                return Err(exists(&destination[..depth]));
            }
        }

        let mut copied = BTreeMap::new();
        copy(Path::new(or_here(&path)), &destination, &mut copied)?;

        for depth in 1..destination.len() {
            state
                .tree
                .entry(destination[..depth].to_vec())
                .or_insert(Node::Directory);
        }
        state.tree.extend(copied);
        state.names.push(plugin.name.clone());
        let mut entry = format!(
            "{{\"name\":{},\"version\":{},\"main\":{}",
            json_string(&plugin.name),
            json_string(&plugin.version),
            json_string(&main)
        );
        if let Some(loader) = &plugin.loader {
            let _ = write!(entry, ",\"loader\":{loader}");
        }
        entry.push('}');
        state.entries.push(entry);
        state.write_manifest();
        Ok(())
    }
}

impl State {
    /// Behavior: `io.deephaven.server.jetty.JsPluginsZipFilesystem#writeManifest`
    fn write_manifest(&mut self) {
        let text = format!("{{\"plugins\":[{}]}}", self.entries.join(","));
        let mut hasher = DefaultHasher::new();
        text.hash(&mut hasher);
        let etag = format!("W/\"{:x}-{:x}\"", hasher.finish(), text.len());
        self.tree.insert(
            vec![MANIFEST.to_string()],
            Node::File {
                content: Bytes::from(text),
                etag,
            },
        );
    }
}

/// The empty path is the working directory to Java.
fn or_here(path: &str) -> &str {
    if path.is_empty() {
        "."
    } else {
        path
    }
}

/// Copies the directory `source` to `destination` in `copied`.
///
/// What a link leads to decides: a directory becomes an empty directory, as in Java, also
/// when `source` itself is a link; a file is copied when its real location is inside
/// `source`, and left out with a warning otherwise, where Java copies it; a link that leads
/// nowhere fails the registration, as in Java.
///
/// Behavior: `io.deephaven.server.jetty.CopyHelper#copyRecursive`, which walks without
/// following links and copies each entry with `java.nio.file.Files#copy`, which follows them
fn copy(
    source: &Path,
    destination: &[String],
    copied: &mut BTreeMap<Vec<String>, Node>,
) -> Result<(), String> {
    let io = |kind: &str, path: &Path| {
        format!(
            "java.io.UncheckedIOException: java.nio.file.{kind}: {}",
            path.display()
        )
    };
    let failed = |error: std::io::Error, path: &Path| {
        let kind = match error.kind() {
            std::io::ErrorKind::NotFound => "NoSuchFileException",
            std::io::ErrorKind::PermissionDenied => "AccessDeniedException",
            _ => "FileSystemException",
        };
        io(kind, path)
    };
    copied.insert(destination.to_vec(), Node::Directory);
    let is_link = source
        .symlink_metadata()
        .map_err(|e| failed(e, source))?
        .file_type()
        .is_symlink();
    if is_link {
        return Ok(());
    }
    let root = source.canonicalize().map_err(|e| failed(e, source))?;
    let mut pending = vec![(source.to_path_buf(), destination.to_vec())];
    while let Some((directory, at)) = pending.pop() {
        let entries = std::fs::read_dir(&directory).map_err(|e| failed(e, &directory))?;
        for entry in entries {
            let entry = entry.map_err(|e| failed(e, &directory))?;
            let path = entry.path();
            let Some(name) = entry.file_name().to_str().map(str::to_string) else {
                tracing::warn!(
                    "JS plugin file {} is left out: its name is not UTF-8",
                    path.display()
                );
                continue;
            };
            let mut place = at.clone();
            if let Some((before, _)) = name.split_once('\\') {
                // the zip file takes the backslash for a separator and has no such directory
                place.push(before.to_string());
                return Err(format!(
                    "java.io.UncheckedIOException: java.nio.file.NoSuchFileException: /{}",
                    place.join("/")
                ));
            }
            place.push(name);
            let own = entry.file_type().map_err(|e| failed(e, &path))?;
            if own.is_dir() {
                copied.insert(place.clone(), Node::Directory);
                pending.push((path, place));
                continue;
            }
            // a link: what it leads to
            let metadata = std::fs::metadata(&path).map_err(|e| failed(e, &path))?;
            if metadata.is_dir() {
                copied.insert(place, Node::Directory);
                continue;
            }
            if !metadata.is_file() {
                tracing::warn!(
                    "JS plugin file {} is left out: it is no regular file",
                    path.display()
                );
                continue;
            }
            if own.is_symlink() {
                let real = path.canonicalize().map_err(|e| failed(e, &path))?;
                if !real.starts_with(&root) {
                    tracing::warn!(
                        "JS plugin file {} is left out: it is a link that leads out of {}",
                        path.display(),
                        source.display()
                    );
                    continue;
                }
            }
            let content = std::fs::read(&path).map_err(|e| failed(e, &path))?;
            let etag = super::etag(&metadata);
            copied.insert(
                place,
                Node::File {
                    content: Bytes::from(content),
                    etag,
                },
            );
        }
    }
    Ok(())
}

/// A path as `java.nio.file.Path#of` keeps it: without repeated or trailing slashes.
fn java_path(text: &str) -> String {
    let absolute = text.starts_with('/');
    let joined = text
        .split('/')
        .filter(|segment| !segment.is_empty())
        .collect::<Vec<_>>()
        .join("/");
    if absolute {
        format!("/{joined}")
    } else {
        joined
    }
}

/// Behavior: `java.nio.file.Path#resolve`
fn resolve(base: &str, other: &str) -> String {
    if other.starts_with('/') || base.is_empty() {
        other.to_string()
    } else if other.is_empty() {
        base.to_string()
    } else if base == "/" {
        format!("/{other}")
    } else {
        format!("{base}/{other}")
    }
}

/// The names of a path with `.` dropped and `..` resolved where a name precedes it.
///
/// Behavior: `java.nio.file.Path#normalize`
fn normalized(path: &str) -> Vec<&str> {
    let absolute = path.starts_with('/');
    let mut names: Vec<&str> = Vec::new();
    for name in path.split('/') {
        match name {
            "" | "." => {}
            ".." => match names.last() {
                Some(&last) if last != ".." => {
                    names.pop();
                }
                // above the root there is nothing
                None if absolute => {}
                _ => names.push(".."),
            },
            _ => names.push(name),
        }
    }
    names
}

/// The path from `base` to `other`, both as text.
///
/// Behavior: `java.nio.file.Path#relativize` of the JDK's Unix paths, which normalizes both
fn relativize(base: &str, other: &str) -> Result<String, String> {
    if base.starts_with('/') != other.starts_with('/') {
        return Err("'other' is different type of Path".to_string());
    }
    let (from, to) = (normalized(base), normalized(other));
    let common = from.iter().zip(&to).take_while(|(a, b)| a == b).count();
    let mut names = vec![".."; from.len() - common];
    names.extend(&to[common..]);
    Ok(names.join("/"))
}

/// A string as the manifest has it.
///
/// Behavior: the Jackson `ObjectMapper` of `JsPluginsZipFilesystem#writeManifest`, which
/// escapes the quote, the backslash and the control characters and nothing else
pub fn json_string(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\t' => out.push_str("\\t"),
            '\n' => out.push_str("\\n"),
            '\u{c}' => out.push_str("\\f"),
            '\r' => out.push_str("\\r"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04X}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn paths_resolve_and_relativize_as_java_paths() {
        // base, main, path.relativize(path.resolve(main)): recorded from Java 21
        for (base, main, expected) in [
            ("/a/b", "index.js", "index.js"),
            ("/a/b", "dist/../index.js", "index.js"),
            ("/a/b", "../c/x.js", "../c/x.js"),
            ("/a/b", "/a/b/x.js", "x.js"),
            ("/a/b", "/x.js", "../../x.js"),
            ("/a/b/", "./x.js", "x.js"),
            ("/a//b", ".//x.js", "x.js"),
            ("/a/../b", "x.js", "x.js"),
            ("rel", "../x.js", "../x.js"),
            ("../rel", "d/../x.js", "x.js"),
            ("rel/..", "x.js", "x.js"),
            (".", "x.js", "x.js"),
            ("", "x.js", "x.js"),
            ("/a/b", "", ""),
            ("/a/b", ".", ""),
            ("/a/b", "..", ".."),
            ("/a/b", "x.js/", "x.js"),
            ("/", "x.js", "x.js"),
            ("/..", "x.js", "x.js"),
            ("a/../../b", "c/../x.js", "x.js"),
            ("/a/b", "sub/./x.js", "sub/x.js"),
            ("../..", "./x.js", "x.js"),
        ] {
            let base = java_path(base);
            let resolved = resolve(&base, &java_path(main));
            assert_eq!(
                relativize(&base, &resolved).as_deref(),
                Ok(expected),
                "{base} {main}"
            );
        }
        assert_eq!(resolve("/a/b", "dist/../index.js"), "/a/b/dist/../index.js");
        assert_eq!(java_path("/a//b/"), "/a/b");
        assert_eq!(
            relativize("rel", "/x.js"),
            Err("'other' is different type of Path".to_string())
        );
    }

    #[test]
    fn strings_are_escaped_as_jackson_escapes_them() {
        assert_eq!(
            json_string("\u{0}\u{1f}\u{7f}\t\r\u{8}\u{c}\n"),
            "\"\\u0000\\u001F\u{7f}\\t\\r\\b\\f\\n\""
        );
        assert_eq!(json_string("é\"<\\/\u{2028}"), "\"é\\\"<\\\\/\u{2028}\"");
    }
}
