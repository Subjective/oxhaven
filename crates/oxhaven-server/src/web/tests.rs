use super::*;
use http_body_util::BodyExt;
use std::fs;

/// A web root in the temporary directory, removed when dropped.
struct Fixture {
    dir: PathBuf,
    root: WebRoot,
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
        Self { dir, root }
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
        get(Some(&self.root), &request).await
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
    assert_eq!(get(None, &request).await.status(), StatusCode::FOUND);
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
    let response = get(None, &request).await;
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(header(&response, "content-type"), Some("application/json"));
    assert_eq!(header(&response, "pragma"), Some("no-cache"));
    assert_eq!(text(response).await, "{\"plugins\":[]}");

    *request.uri_mut() = "/ide/".parse().unwrap();
    assert_eq!(get(None, &request).await.status(), StatusCode::NOT_FOUND);
}

#[test]
fn a_web_root_must_be_a_directory() {
    let f = Fixture::new();
    assert!(WebRoot::open(&f.dir.join("missing")).is_err());
    assert!(WebRoot::open(&f.dir.join("root/ide/index.html")).is_err());
}
