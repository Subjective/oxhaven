# JS plugins

A Python plugin package may register a JS plugin: a directory of files that the web client
loads to draw the objects of the package's type. The server lists the plugins in
`/js-plugins/manifest.json` and serves their files under `/js-plugins/<name>/`.

Everything below that says "Java" was observed on Java 42.9 (JDK 21, jpy 2.2.0) with
`deephaven-plugin-plotly-express` 0.22.0 and a probe package that registers plugins of every
shape tried. The Java classes are `io.deephaven.server.plugin.python.CallbackAdapter`
(`registerJsPlugin`), `io.deephaven.plugin.js.JsPlugin`,
`io.deephaven.server.plugin.js.JsPluginsZipFilesystem` (`add`, `CopyHelper#copyRecursive`),
`io.deephaven.server.plugin.js.JsPluginManifestEntry` and, for the serving,
`io.deephaven.server.jetty.JsPlugins` with `NoCacheFilter`.

## Shape

| Part | Where |
| --- | --- |
| The plugins of the process: the copies, the manifest | `crates/oxhaven-server/src/web/plugins.rs` |
| The routes under `/js-plugins` | `crates/oxhaven-server/src/web/mod.rs` (`plugin_file`) |
| What Python calls to register | `_oxhaven.register_js_plugin` (`console/python/plugin.rs`) |
| The checks jpy makes of the arguments, the loader as JSON | `deephaven_internal/plugin/js/__init__.py` |
| The class a package subclasses | `deephaven/plugin/js.py` |

A plugin is copied when it is registered and the copy is served, as Java copies it into a zip
file: a file that changes later, or is added or removed, is not seen. The copy here is held
in memory. The plugins belong to the process, as the object types do.

## Registration

In the order Java checks, with Java's texts:

1. `path` is no `pathlib.Path`: `Exception: Expecting pathlib.Path, is type(js_plugin.path())=..., js_plugin=...`.
   `main`, `name` or `version` that is no string or `None` fails with the text jpy gives for
   the Java method it could not call.
2. `main` is no regular file under `path`: `main ('<path>/<main>') must exist and be a regular file`.
3. A plugin of the name exists: `js plugin with name '<name>' already exists. See https://github.com/deephaven/deephaven-core/issues/3048`.
4. The files are copied.

A registration that fails stops the server from starting, as in Java, unless the package
catches the error.

The name is a path of the zip file to Java, and is one here: `a/b` is served under
`/js-plugins/a/b/`, `x/../up` under `/js-plugins/up/`, a leading or trailing slash and a
doubled one are dropped, a backslash separates as a slash does, and a plugin may be named
below another plugin. A name whose place is taken by a file or another plugin's directory is
refused with `java.io.UncheckedIOException: java.nio.file.FileAlreadyExistsException: /<place>`.
An empty name is refused with the text of Java's `NullPointerException`.

The manifest is `{"plugins":[{"name":..,"version":..,"main":..}]}`, compact, in the order of
registration. `loader` follows `main` when the plugin has one; it is whatever JSON the
plugin's `loader` holds. A `loader` value that JSON cannot hold is refused with
`NotImplementedError` naming `JsPluginManifestEntry#loader`.

## Serving

| Request | Answer |
| --- | --- |
| `/js-plugins/manifest.json` | the manifest |
| A file of a plugin | the file, the content type by its extension, none for an extension Jetty does not know (`.map`, `.ts`, `.md`, no extension) |
| `/js-plugins` | 404 |
| `/js-plugins/` or a directory with a slash and no `index.html` | 403 |
| A directory without a slash | 302 to the same with the slash |
| A file with a slash after it, what does not exist | 404 |
| Another method than `GET` | as for the other files of the web client |

Every answer has the headers of Java's `NoCacheFilter` (`Cache-Control: no-cache,
must-revalidate, pre-check=0, post-check=0`, `Expires` of 1970, `Pragma: no-cache`) and an
`ETag`.

## What a request can reach

A request reaches the copies and nothing else: there is no file system behind the route.

- `..` in a path is resolved against the URL before anything is looked up, as Jetty does, so
  `/js-plugins/a/../../ide/` is `/ide/`. It cannot name anything outside the server's routes.
- An encoded dot segment, slash, backslash or percent sign (`%2e%2e`, `%2f`, `%5c`, `%25`) and
  a control character are answered 400, as by Jetty.
- `;parameters` of a segment are dropped, as by Jetty; a segment that is only a parameter is
  answered 400.
- A file or plugin whose name has `?`, `%` or a character outside ASCII is registered and
  listed in the manifest and cannot be fetched, on either server.

## Links

Java walks the directory without following links and then copies each entry with
`Files.copy`, which follows them. Observed:

| In the plugin's directory | Java | Here |
| --- | --- | --- |
| A link to a file inside the directory | the file's content is served | the same |
| A link to a file outside the directory (anywhere the server may read; another plugin; through a chain of links) | the file's content is served | **not copied**: 404, a warning in the log that names the link |
| `main` is such a link | listed and served | listed, and 404 |
| A link to a directory | an empty directory | the same |
| The plugin's directory is itself a link | listed; nothing is served | the same |
| A link to nothing | registration fails with `NoSuchFileException`; the files copied before it stay served, which files depends on the order the directory is read | registration fails with the same text; nothing of the plugin is served |
| A hard link | served | the same |
| A file name with a backslash | registration fails with `NoSuchFileException` | the same |

The second row is the one difference made on purpose, by the rule of this task: nothing is
served whose real location is outside the plugin's directory. It follows the ruling of
`PARITY.md` A8 for notebook storage and is not yet a ruling for this route; see "For the
owner".

## Differences from Java

| What | Java | Here |
| --- | --- | --- |
| A link to a file outside the plugin's directory | served | not served (above) |
| A registration that fails while copying | what was copied stays served | nothing of the plugin is served |
| The copy | a zip file in a temporary directory | memory |
| A `loader` with a character outside the BMP | jpy passes it as two surrogates, each written as UTF-8 | UTF-8 |
| A URL with `#` in its path | 400 | the path up to the `#` |
| A special file (a pipe, a socket) in the directory | not probed | left out with a warning |
| `Range`, `Accept-Encoding: gzip`, the value of the `ETag`, `OPTIONS` | 206, gzip, Jetty's weak tag, 200 with the cache headers | as for the other files this server serves (`web/mod.rs`) |

## For the owner

Not decided here:

- **Java lets a link escape.** A JS plugin with a link to any file the server's user can read
  has that file served to whoever can reach the port; the files of a plugin need no login.
  The link has to be in an installed package, so it is the package's author who chooses.
  This implementation serves nothing outside the directory. If the ruling is to do as Java
  does, the check is one condition in `plugins.rs` (`copy`).
- A plugin whose `main` is a link out of its directory is listed in the manifest and its
  main file is 404, so the web client fails to load it. Refusing the registration would be
  the other choice; Java has no such refusal to name.
- A failed copy leaves part of a plugin served in Java. Here the registration is whole or
  nothing.
- Names and files with `?`, `%` or a character outside ASCII are accepted and can never be
  fetched, in Java and here.
