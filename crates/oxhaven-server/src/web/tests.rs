use super::*;
use http_body_util::BodyExt;
use std::fs;

/// A web root in the temporary directory, removed when dropped.
struct Fixture {
    dir: PathBuf,
    root: WebRoot,
    plugins: plugins::JsPlugins,
}

impl Fixture {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!("oxhaven-web-{}", uuid::Uuid::new_v4()));
        for sub in [
            "root/ide/assets",
            "root/iframe/table",
            "root/jsapi",
            "outside",
        ] {
            fs::create_dir_all(dir.join(sub)).unwrap();
        }
        for (path, content) in [
            ("root/ide/index.html", "<html>ide</html>"),
            ("root/ide/manifest.json", "{}"),
            ("root/ide/assets/app-1a2b.js", "app()"),
            ("root/ide/assets/app-1a2b.js.map", "{}"),
            ("root/ide/assets/font.woff2", "font"),
            ("root/iframe/table/index.html", "<html>table</html>"),
            ("root/jsapi/dh-core.js", "export {}"),
            ("outside/secret.txt", "secret"),
        ] {
            fs::write(dir.join(path), content).unwrap();
        }
        let root = WebRoot::open(&dir.join("root")).unwrap();
        Self {
            dir,
            root,
            plugins: plugins::JsPlugins::new(),
        }
    }

    async fn get(&self, uri: &str) -> Response<Body> {
        self.get_with(uri, &[]).await
    }

    async fn get_with(&self, uri: &str, headers: &[(&str, &str)]) -> Response<Body> {
        let mut request = Request::new(());
        *request.uri_mut() = uri.parse().unwrap();
        for (name, value) in headers {
            request.headers_mut().insert(
                http::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                value.parse().unwrap(),
            );
        }
        get(Some(&self.root), &self.plugins, &request).await
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

async fn text(response: Response<Body>) -> String {
    let bytes = response.into_body().collect().await.unwrap().to_bytes();
    String::from_utf8(bytes.to_vec()).unwrap()
}

fn header<'a>(response: &'a Response<Body>, name: &str) -> Option<&'a str> {
    response.headers().get(name).map(|v| v.to_str().unwrap())
}

#[tokio::test]
async fn the_root_redirects_to_the_ide_and_keeps_the_query() {
    let f = Fixture::new();
    let response = f.get("/").await;
    assert_eq!(response.status(), StatusCode::FOUND);
    assert_eq!(header(&response, "location"), Some("/ide/"));
    let response = f.get("/?psk=abc&x=1").await;
    assert_eq!(header(&response, "location"), Some("/ide/?psk=abc&x=1"));
    // also on a server that has no files
    let mut request = Request::new(());
    *request.uri_mut() = "/".parse().unwrap();
    assert_eq!(
        get(None, &plugins::JsPlugins::new(), &request)
            .await
            .status(),
        StatusCode::FOUND
    );
}

#[tokio::test]
async fn a_directory_redirects_to_its_slash_and_serves_its_index() {
    let f = Fixture::new();
    let response = f.get("/ide?x=1").await;
    assert_eq!(response.status(), StatusCode::FOUND);
    assert_eq!(header(&response, "location"), Some("/ide/?x=1"));
    assert_eq!(header(&response, "pragma"), Some("no-cache"));

    let response = f.get("/ide/").await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header(&response, "content-type"), Some("text/html"));
    assert_eq!(text(response).await, "<html>ide</html>");

    // a directory without an index is not listed
    let response = f.get("/ide/assets/").await;
    assert_eq!(response.status(), StatusCode::FORBIDDEN);
    // a file is not a directory
    assert_eq!(
        f.get("/ide/index.html/").await.status(),
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn files_have_the_content_type_of_their_extension() {
    let f = Fixture::new();
    for (path, expected) in [
        ("/ide/index.html", Some("text/html")),
        ("/ide/manifest.json", Some("application/json")),
        ("/ide/assets/app-1a2b.js", Some("text/javascript")),
        ("/ide/assets/font.woff2", Some("font/woff2")),
        ("/ide/assets/app-1a2b.js.map", None),
    ] {
        let response = f.get(path).await;
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        assert_eq!(header(&response, "content-type"), expected, "{path}");
    }
    assert_eq!(text(f.get("/jsapi/dh-core.js").await).await, "export {}");
}

#[tokio::test]
async fn assets_are_cached_for_a_year_and_everything_else_is_revalidated() {
    let f = Fixture::new();
    let asset = f.get("/ide/assets/app-1a2b.js").await;
    assert_eq!(
        header(&asset, "cache-control"),
        Some("max-age=31536000, public, immutable")
    );
    assert_eq!(header(&asset, "pragma"), None);
    let expires = httpdate::parse_http_date(header(&asset, "expires").unwrap()).unwrap();
    let ahead = expires.duration_since(SystemTime::now()).unwrap();
    assert!(
        ahead > Duration::from_secs(YEAR_IN_SECONDS - 60),
        "{ahead:?}"
    );

    for path in ["/ide/index.html", "/iframe/table/", "/jsapi/dh-core.js"] {
        let response = f.get(path).await;
        assert_eq!(
            header(&response, "cache-control"),
            Some("no-cache, must-revalidate, pre-check=0, post-check=0"),
            "{path}"
        );
        assert_eq!(header(&response, "pragma"), Some("no-cache"));
        assert_eq!(
            header(&response, "expires"),
            Some("Thu, 01 Jan 1970 00:00:00 GMT")
        );
    }
}

#[test]
fn the_caching_rules_follow_the_application_paths() {
    assert_eq!(caching("/ide/assets/x.js"), Caching::Forever);
    assert_eq!(caching("/iframe/widget/assets/x.js"), Caching::Forever);
    assert_eq!(caching("/ide/x.js"), Caching::Revalidate);
    assert_eq!(caching("/ide"), Caching::Revalidate);
    assert_eq!(caching("/jsapi/dh-core.js"), Caching::Revalidate);
    assert_eq!(caching("/js-plugins/manifest.json"), Caching::Revalidate);
    assert_eq!(caching("/iframe/"), Caching::Unspecified);
    assert_eq!(caching("/idea/assets/x.js"), Caching::Unspecified);
    assert_eq!(caching("/other"), Caching::Unspecified);
}

#[tokio::test]
async fn a_matching_if_none_match_is_answered_not_modified() {
    let f = Fixture::new();
    let first = f.get("/jsapi/dh-core.js").await;
    let etag = header(&first, "etag").unwrap().to_string();
    assert!(etag.starts_with("W/\""), "{etag}");

    let again = f
        .get_with("/jsapi/dh-core.js", &[("if-none-match", &etag)])
        .await;
    assert_eq!(again.status(), StatusCode::NOT_MODIFIED);
    assert_eq!(header(&again, "etag"), Some(etag.as_str()));
    assert_eq!(header(&again, "pragma"), Some("no-cache"));
    assert_eq!(text(again).await, "");

    let other = f
        .get_with("/jsapi/dh-core.js", &[("if-none-match", "W/\"other\"")])
        .await;
    assert_eq!(other.status(), StatusCode::OK);
    let listed = f
        .get_with(
            "/jsapi/dh-core.js",
            &[("if-none-match", &format!("\"a\", {etag}"))],
        )
        .await;
    assert_eq!(listed.status(), StatusCode::NOT_MODIFIED);

    // a changed file has another tag
    fs::write(f.dir.join("root/jsapi/dh-core.js"), "export { changed }").unwrap();
    let changed = f
        .get_with("/jsapi/dh-core.js", &[("if-none-match", &etag)])
        .await;
    assert_eq!(changed.status(), StatusCode::OK);

    // a date is no reason not to send the file
    let dated = f
        .get_with(
            "/jsapi/dh-core.js",
            &[("if-modified-since", "Fri, 01 Jan 2100 00:00:00 GMT")],
        )
        .await;
    assert_eq!(dated.status(), StatusCode::OK);
    assert_eq!(header(&dated, "last-modified"), None);
}

#[tokio::test]
async fn nothing_outside_the_web_root_is_served() {
    let f = Fixture::new();
    assert_eq!(f.get("/ide/nope.js").await.status(), StatusCode::NOT_FOUND);
    for path in [
        "/../outside/secret.txt",
        "/ide/../../outside/secret.txt",
        "/ide/%2e%2e/%2e%2e/outside/secret.txt",
        "/ide/..%2f..%2foutside/secret.txt",
        "/ide//index.html",
        "/ide/%zz",
    ] {
        assert_eq!(
            f.get(path).await.status(),
            StatusCode::BAD_REQUEST,
            "{path}"
        );
    }
    // dot segments inside the root are resolved
    assert_eq!(
        f.get("/ide/../jsapi/./dh-core.js").await.status(),
        StatusCode::OK
    );
    assert_eq!(f.get("/ide/index%2ehtml").await.status(), StatusCode::OK);

    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(
            f.dir.join("outside/secret.txt"),
            f.dir.join("root/ide/link.txt"),
        )
        .unwrap();
        assert_eq!(f.get("/ide/link.txt").await.status(), StatusCode::NOT_FOUND);
    }
}

#[tokio::test]
async fn the_plugin_manifest_is_empty_with_or_without_a_web_root() {
    let mut request = Request::new(());
    *request.uri_mut() = "/js-plugins/manifest.json".parse().unwrap();
    let plugins = plugins::JsPlugins::new();
    let response = get(None, &plugins, &request).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header(&response, "content-type"), Some("application/json"));
    assert_eq!(header(&response, "pragma"), Some("no-cache"));
    assert_eq!(text(response).await, "{\"plugins\":[]}");

    *request.uri_mut() = "/ide/".parse().unwrap();
    assert_eq!(
        get(None, &plugins, &request).await.status(),
        StatusCode::NOT_FOUND
    );
}

#[test]
fn a_web_root_must_be_a_directory() {
    let f = Fixture::new();
    assert!(WebRoot::open(&f.dir.join("missing")).is_err());
    assert!(WebRoot::open(&f.dir.join("root/ide/index.html")).is_err());
}

// JS plugins. What is expected is what Java 42.9 answered to the same plugins and requests
// (`docs/JS_PLUGINS.md`), except where a comment says `DIFFERENCE`.

impl Fixture {
    /// Writes `files` below `plugins/<dir>` and returns that directory.
    fn plugin_dir(&self, dir: &str, files: &[(&str, &str)]) -> String {
        let base = self.dir.join("plugins").join(dir);
        fs::create_dir_all(&base).unwrap();
        for (path, content) in files {
            let path = base.join(path);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, content).unwrap();
        }
        base.to_str().unwrap().to_string()
    }

    fn register(&self, name: &str, main: &str, path: &str) -> Result<(), String> {
        self.plugins.register(&plugins::JsPlugin {
            name: name.to_string(),
            version: "1".to_string(),
            main: main.to_string(),
            path: path.to_string(),
            loader: None,
        })
    }

    async fn status(&self, uri: &str) -> StatusCode {
        self.get(uri).await.status()
    }

    async fn manifest(&self) -> String {
        text(self.get("/js-plugins/manifest.json").await).await
    }
}

#[tokio::test]
async fn a_js_plugin_is_listed_and_its_files_are_served() {
    let f = Fixture::new();
    let path = f.plugin_dir(
        "express",
        &[
            ("package.json", "{\"name\": \"x\"}"),
            ("dist/bundle/index.js", "export default 1;"),
            ("dist/bundle/style.css", "a{}"),
            ("dist/index.js.map", "{}"),
            ("dist/index.d.ts", "export {};"),
            ("sub/index.html", "<html>sub</html>"),
            (".hidden", "hidden"),
            ("a b.txt", "space"),
        ],
    );
    f.plugins
        .register(&plugins::JsPlugin {
            name: "@deephaven/js-plugin-x".to_string(),
            version: "0.22.0".to_string(),
            main: "dist/bundle/index.js".to_string(),
            path: path.clone(),
            loader: Some("{\"b\":1,\"a\":[1.0E7,null]}".to_string()),
        })
        .unwrap();
    f.register("second", "dist/../package.json", &path).unwrap();
    assert_eq!(
        f.manifest().await,
        "{\"plugins\":[{\"name\":\"@deephaven/js-plugin-x\",\"version\":\"0.22.0\",\
         \"main\":\"dist/bundle/index.js\",\"loader\":{\"b\":1,\"a\":[1.0E7,null]}},\
         {\"name\":\"second\",\"version\":\"1\",\"main\":\"package.json\"}]}"
    );

    let base = "/js-plugins/@deephaven/js-plugin-x";
    for (file, content_type, content) in [
        (
            "dist/bundle/index.js",
            Some("text/javascript"),
            "export default 1;",
        ),
        ("dist/bundle/style.css", Some("text/css"), "a{}"),
        ("dist/index.js.map", None, "{}"),
        ("dist/index.d.ts", None, "export {};"),
        (
            "package.json",
            Some("application/json"),
            "{\"name\": \"x\"}",
        ),
        (".hidden", None, "hidden"),
        ("a%20b.txt", Some("text/plain"), "space"),
        ("sub/", Some("text/html"), "<html>sub</html>"),
        (
            "dist/bundle/index.js?v=1",
            Some("text/javascript"),
            "export default 1;",
        ),
        (
            "dist/bundle/index.js;v=1",
            Some("text/javascript"),
            "export default 1;",
        ),
        (
            "dist/../package.json",
            Some("application/json"),
            "{\"name\": \"x\"}",
        ),
    ] {
        let response = f.get(&format!("{base}/{file}")).await;
        assert_eq!(response.status(), StatusCode::OK, "{file}");
        assert_eq!(header(&response, "content-type"), content_type, "{file}");
        assert_eq!(
            header(&response, "cache-control"),
            Some("no-cache, must-revalidate, pre-check=0, post-check=0"),
            "{file}"
        );
        assert_eq!(header(&response, "pragma"), Some("no-cache"));
        assert!(header(&response, "etag").unwrap().starts_with("W/\""));
        assert_eq!(text(response).await, content, "{file}");
    }
    // the encoded `@` names the same plugin
    assert_eq!(
        f.status("/js-plugins/%40deephaven/js-plugin-x/package.json")
            .await,
        StatusCode::OK
    );

    // a directory redirects to its slash, and has no listing
    for directory in ["/js-plugins/@deephaven", base, "/js-plugins/second/dist"] {
        let response = f.get(directory).await;
        assert_eq!(response.status(), StatusCode::FOUND, "{directory}");
        assert_eq!(
            header(&response, "location"),
            Some(format!("{directory}/").as_str())
        );
        assert_eq!(header(&response, "pragma"), Some("no-cache"));
        assert_eq!(
            f.status(&format!("{directory}/")).await,
            StatusCode::FORBIDDEN
        );
    }
    // the prefix itself is no directory to redirect to
    assert_eq!(f.status("/js-plugins").await, StatusCode::NOT_FOUND);
    assert_eq!(f.status("/js-plugins/").await, StatusCode::FORBIDDEN);
    for missing in [
        "/js-plugins/second/nothing.js",
        "/js-plugins/second/package.json/",
        "/js-plugins/second/PACKAGE.JSON",
        "/js-plugins/SECOND/package.json",
        "/js-plugins/third/package.json",
        "/js-plugins/manifest.json/",
        "/js-plugins/package.json",
    ] {
        assert_eq!(f.status(missing).await, StatusCode::NOT_FOUND, "{missing}");
    }

    let etag = header(&f.get(&format!("{base}/package.json")).await, "etag")
        .unwrap()
        .to_string();
    let again = f
        .get_with(&format!("{base}/package.json"), &[("if-none-match", &etag)])
        .await;
    assert_eq!(again.status(), StatusCode::NOT_MODIFIED);
    let manifest = f.get("/js-plugins/manifest.json").await;
    let etag = header(&manifest, "etag").unwrap().to_string();
    f.register("third", "package.json", &path).unwrap();
    let changed = f
        .get_with("/js-plugins/manifest.json", &[("if-none-match", &etag)])
        .await;
    assert_eq!(changed.status(), StatusCode::OK);
    // `*` is not taken for a match
    let star = f
        .get_with(&format!("{base}/package.json"), &[("if-none-match", "*")])
        .await;
    assert_eq!(star.status(), StatusCode::OK);
}

#[tokio::test]
async fn a_js_plugin_is_served_as_it_was_when_it_was_registered() {
    let f = Fixture::new();
    let path = f.plugin_dir("p", &[("index.js", "before")]);
    f.register("p", "index.js", &path).unwrap();
    fs::write(Path::new(&path).join("index.js"), "after").unwrap();
    fs::write(Path::new(&path).join("later.js"), "later").unwrap();
    assert_eq!(text(f.get("/js-plugins/p/index.js").await).await, "before");
    assert_eq!(
        f.status("/js-plugins/p/later.js").await,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn a_request_does_not_leave_the_js_plugins() {
    let f = Fixture::new();
    let path = f.plugin_dir("p", &[("index.js", "p"), ("dist/x.js", "x")]);
    fs::write(f.dir.join("plugins/secret.txt"), "secret").unwrap();
    f.register("p", "index.js", &path).unwrap();
    // `..` is resolved in the URL, which then names another resource or none
    assert_eq!(
        text(f.get("/js-plugins/p/dist/../index.js").await).await,
        "p"
    );
    assert_eq!(
        text(f.get("/js-plugins/p/../../ide/index.html").await).await,
        "<html>ide</html>"
    );
    for (uri, status) in [
        ("/js-plugins/p/../secret.txt", StatusCode::NOT_FOUND),
        (
            "/js-plugins/p/../../outside/secret.txt",
            StatusCode::NOT_FOUND,
        ),
        (
            "/js-plugins/p/../../../../etc/passwd",
            StatusCode::BAD_REQUEST,
        ),
        ("/js-plugins/../../etc/passwd", StatusCode::BAD_REQUEST),
        ("/js-plugins/p/%2e%2e/secret.txt", StatusCode::BAD_REQUEST),
        ("/js-plugins/p/..%2fsecret.txt", StatusCode::BAD_REQUEST),
        ("/js-plugins/p/..%5csecret.txt", StatusCode::BAD_REQUEST),
        ("/js-plugins/p/..;/secret.txt", StatusCode::BAD_REQUEST),
        ("/js-plugins/p/;x/index.js", StatusCode::BAD_REQUEST),
        ("/js-plugins/p/.;x/index.js", StatusCode::BAD_REQUEST),
        ("/js-plugins/p//index.js", StatusCode::BAD_REQUEST),
        ("/js-plugins/p/dist%2fx.js", StatusCode::BAD_REQUEST),
        ("/js-plugins/p/index.js%00", StatusCode::BAD_REQUEST),
        ("/js-plugins/p/index.js%0A", StatusCode::BAD_REQUEST),
        ("/js-plugins/p/index%2541.js", StatusCode::BAD_REQUEST),
        ("/js-plugins/p/index.js;/", StatusCode::NOT_FOUND),
        ("/js-plugins/p/%3B/index.js", StatusCode::NOT_FOUND),
        ("/js-plugins/p;x=1/index.js", StatusCode::OK),
        ("/js-plugins;x/p/index.js", StatusCode::OK),
        ("/js-plugins/p/index%2Ejs", StatusCode::OK),
    ] {
        let response = f.get(uri).await;
        assert_eq!(response.status(), status, "{uri}");
        if status == StatusCode::OK {
            assert_eq!(header(&response, "pragma"), Some("no-cache"), "{uri}");
        }
    }
}

#[cfg(unix)]
#[tokio::test]
async fn a_link_of_a_js_plugin_is_followed_only_inside_its_directory() {
    use std::os::unix::fs::symlink;
    let f = Fixture::new();
    let path = f.plugin_dir(
        "links",
        &[("index.js", "main"), ("real/inside.js", "inside")],
    );
    let other = f.plugin_dir("other", &[("index.js", "other")]);
    let dir = Path::new(&path);
    let secret = f.dir.join("outside/secret.txt");
    symlink(&secret, dir.join("out-file.txt")).unwrap();
    symlink("../../outside/secret.txt", dir.join("rel-out.txt")).unwrap();
    symlink("rel-out.txt", dir.join("chain-out.txt")).unwrap();
    symlink(
        Path::new(&other).join("index.js"),
        dir.join("other-plugin.js"),
    )
    .unwrap();
    symlink(f.dir.join("outside"), dir.join("out-dir")).unwrap();
    symlink("index.js", dir.join("in-file.js")).unwrap();
    symlink(dir.join("real/inside.js"), dir.join("in-abs.js")).unwrap();
    symlink("in-file.js", dir.join("chain.js")).unwrap();
    symlink("../index.js", dir.join("real/up-link.js")).unwrap();
    symlink("real", dir.join("in-dir")).unwrap();
    symlink(".", dir.join("self-dir")).unwrap();
    fs::hard_link(&secret, dir.join("hard.txt")).unwrap();
    f.register("links", "index.js", &path).unwrap();
    f.register("other", "index.js", &other).unwrap();

    for (file, content) in [
        ("index.js", "main"),
        ("real/inside.js", "inside"),
        ("in-file.js", "main"),
        ("in-abs.js", "inside"),
        ("chain.js", "main"),
        ("real/up-link.js", "main"),
        // a hard link is a file of the directory
        ("hard.txt", "secret"),
    ] {
        let response = f.get(&format!("/js-plugins/links/{file}")).await;
        assert_eq!(response.status(), StatusCode::OK, "{file}");
        assert_eq!(text(response).await, content, "{file}");
    }
    // DIFFERENCE: Java serves what each of these links leads to
    for file in [
        "out-file.txt",
        "rel-out.txt",
        "chain-out.txt",
        "other-plugin.js",
    ] {
        assert_eq!(
            f.status(&format!("/js-plugins/links/{file}")).await,
            StatusCode::NOT_FOUND,
            "{file}"
        );
    }
    // a link to a directory is an empty directory, wherever it leads, as in Java
    for (directory, inner) in [
        ("out-dir", "secret.txt"),
        ("in-dir", "inside.js"),
        ("self-dir", "index.js"),
    ] {
        let uri = format!("/js-plugins/links/{directory}");
        assert_eq!(f.status(&uri).await, StatusCode::FOUND, "{directory}");
        assert_eq!(f.status(&format!("{uri}/")).await, StatusCode::FORBIDDEN);
        assert_eq!(
            f.status(&format!("{uri}/{inner}")).await,
            StatusCode::NOT_FOUND
        );
    }

    // a plugin whose directory is a link is listed and has no files, as in Java
    symlink(&other, f.dir.join("plugins/linked-root")).unwrap();
    let linked = f.dir.join("plugins/linked-root");
    f.register("linked-root", "index.js", linked.to_str().unwrap())
        .unwrap();
    assert!(f.manifest().await.contains("{\"name\":\"linked-root\","));
    assert_eq!(f.status("/js-plugins/linked-root").await, StatusCode::FOUND);
    assert_eq!(
        f.status("/js-plugins/linked-root/index.js").await,
        StatusCode::NOT_FOUND
    );

    // DIFFERENCE: a main file that is a link out of the directory is listed and not served,
    // where Java serves it
    let mainlink = f.plugin_dir("mainlink", &[]);
    symlink(&secret, Path::new(&mainlink).join("index.js")).unwrap();
    f.register("mainlink", "index.js", &mainlink).unwrap();
    assert!(f.manifest().await.contains("{\"name\":\"mainlink\","));
    assert_eq!(
        f.status("/js-plugins/mainlink/index.js").await,
        StatusCode::NOT_FOUND
    );

    // a link that leads nowhere fails the registration
    let dangling = f.plugin_dir("dangling", &[("index.js", "x")]);
    symlink("nothing", Path::new(&dangling).join("gone")).unwrap();
    assert_eq!(
        f.register("dangling", "index.js", &dangling),
        Err(format!(
            "java.io.UncheckedIOException: java.nio.file.NoSuchFileException: {dangling}/gone"
        ))
    );
    assert!(!f.manifest().await.contains("dangling"));
    // DIFFERENCE: Java serves the files it copied before it met the link
    assert_eq!(
        f.status("/js-plugins/dangling").await,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn a_js_plugin_that_cannot_be_registered_is_refused_as_java_refuses_it() {
    let f = Fixture::new();
    let p = f.plugin_dir("p", &[("index.js", "p"), ("dist/x.js", "x")]);
    let q = f.plugin_dir("q", &[("index.js", "q")]);
    let outside = f.plugin_dir("outside", &[("inner.js", "inner")]);
    let illegal = "java.lang.IllegalArgumentException";
    let exists = "java.io.UncheckedIOException: java.nio.file.FileAlreadyExistsException";
    f.register("p", "index.js", &p).unwrap();

    let again = format!(
        "{illegal}: js plugin with name 'p' already exists. See \
         https://github.com/deephaven/deephaven-core/issues/3048"
    );
    assert_eq!(f.register("p", "index.js", &q), Err(again));
    // a name is a path: it may not be a place that is taken
    for name in ["..", ".", "a/..", "manifest.json", "p/dist"] {
        assert_eq!(
            f.register(name, "index.js", &q),
            Err(format!("{exists}: /{name}")),
            "{name}"
        );
    }
    assert_eq!(
        f.register("p/index.js/sub", "index.js", &q),
        Err(format!("{exists}: /p/index.js"))
    );
    assert_eq!(
        f.register("", "index.js", &q),
        Err("java.lang.NullPointerException: Cannot invoke \
             \"java.nio.file.Path.getFileSystem()\" because \"path\" is null"
            .to_string())
    );
    for (main, path, text) in [
        (
            "nope.js",
            p.clone(),
            format!("main ('{p}/nope.js') must exist and be a regular file"),
        ),
        (
            "dist",
            p.clone(),
            format!("main ('{p}/dist') must exist and be a regular file"),
        ),
        (
            "",
            p.clone(),
            format!("main ('{p}') must exist and be a regular file"),
        ),
        (
            "index.js",
            format!("{p}/nothing"),
            format!("main ('{p}/nothing/index.js') must exist and be a regular file"),
        ),
        (
            "index.js",
            format!("{p}/index.js"),
            format!("main ('{p}/index.js/index.js') must exist and be a regular file"),
        ),
        (
            &format!("{outside}/inner.js"),
            format!("{p}/nothing"),
            format!("main ('{p}/nothing/../../outside/inner.js') must exist and be a regular file"),
        ),
        (
            &format!("{outside}/inner.js"),
            "relative".to_string(),
            "'other' is different type of Path".to_string(),
        ),
    ] {
        assert_eq!(
            f.register("refused", main, &path),
            Err(format!("{illegal}: {text}")),
            "{main} {path}"
        );
    }
    // a file name with a backslash has no place in Java's copy
    let backslash = f.plugin_dir("backslash", &[("index.js", "x"), ("b\\s.js", "x")]);
    assert_eq!(
        f.register("backslash", "index.js", &backslash),
        Err(
            "java.io.UncheckedIOException: java.nio.file.NoSuchFileException: /backslash/b"
                .to_string()
        )
    );
    // nothing of a refused plugin is listed or served
    assert_eq!(
        f.manifest().await,
        "{\"plugins\":[{\"name\":\"p\",\"version\":\"1\",\"main\":\"index.js\"}]}"
    );
    assert_eq!(f.status("/js-plugins/refused").await, StatusCode::NOT_FOUND);
    assert_eq!(
        f.status("/js-plugins/backslash/index.js").await,
        StatusCode::NOT_FOUND
    );
    assert_eq!(text(f.get("/js-plugins/p/index.js").await).await, "p");

    // names Java takes: the place is the name as a path
    for (name, place) in [
        ("p/newsub", "p/newsub"),
        ("p/a/b", "p/a/b"),
        ("x/../up", "up"),
        ("./dotted", "dotted"),
        ("d//s", "d/s"),
        ("/abs", "abs"),
        ("trail/", "trail"),
        ("b\\s", "b/s"),
        ("a b", "a%20b"),
        ("P", "P"),
        ("se;mi", "se%3Bmi"),
    ] {
        f.register(name, "index.js", &q).unwrap();
        let response = f.get(&format!("/js-plugins/{place}/index.js")).await;
        assert_eq!(response.status(), StatusCode::OK, "{name}");
        assert_eq!(text(response).await, "q", "{name}");
    }
    assert!(f.manifest().await.contains("{\"name\":\"b\\\\s\","));
    // the directories above a plugin exist, and `x` of `x/../up` does not
    assert_eq!(f.status("/js-plugins/p/a").await, StatusCode::FOUND);
    assert_eq!(f.status("/js-plugins/x").await, StatusCode::NOT_FOUND);
    // a main file outside the directory is listed as the path to it
    f.register("main-outside", "../outside/inner.js", &p)
        .unwrap();
    f.register("main-absolute", &format!("{outside}/inner.js"), &p)
        .unwrap();
    let manifest = f.manifest().await;
    for name in ["main-outside", "main-absolute"] {
        assert!(
            manifest.contains(&format!(
                "{{\"name\":\"{name}\",\"version\":\"1\",\"main\":\"../outside/inner.js\"}}"
            )),
            "{manifest}"
        );
    }
    assert_eq!(
        f.status("/js-plugins/main-outside/../outside/inner.js")
            .await,
        StatusCode::NOT_FOUND
    );
}

#[tokio::test]
async fn names_java_cannot_serve_are_not_served() {
    let f = Fixture::new();
    let p = f.plugin_dir(
        "p",
        &[
            ("index.js", "p"),
            ("café.js", "x"),
            ("q?x.js", "x"),
            ("h#x.js", "x"),
        ],
    );
    f.register("p", "index.js", &p).unwrap();
    for name in ["café", "q?x"] {
        f.register(name, "index.js", &p).unwrap();
    }
    assert!(f.manifest().await.contains("{\"name\":\"café\","));
    for (uri, status) in [
        ("/js-plugins/p/caf%C3%A9.js", StatusCode::NOT_FOUND),
        ("/js-plugins/p/q%3Fx.js", StatusCode::NOT_FOUND),
        ("/js-plugins/p/h%23x.js", StatusCode::OK),
        ("/js-plugins/caf%C3%A9/index.js", StatusCode::NOT_FOUND),
        ("/js-plugins/q%3Fx/index.js", StatusCode::NOT_FOUND),
    ] {
        assert_eq!(f.status(uri).await, status, "{uri}");
    }
}
