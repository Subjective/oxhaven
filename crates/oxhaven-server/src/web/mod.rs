//! The static files of the web IDE.
//!
//! Java serves the web IDE, the embeddable widgets and the JS API from a jar on its class
//! path, at `/ide/`, `/iframe/` and `/jsapi/`. Here they come from a directory, the web
//! root, that holds what that jar has under `META-INF/resources`;
//! `scripts/extract-web-root.sh` makes one from an installed `deephaven-server` wheel. The
//! files are another project's build output and are not part of this repository. A server
//! without a web root answers `404` for them and serves gRPC as usual.
//!
//! What is the same as Java, as seen from its responses: the redirects, which paths are
//! refused, the content types, the cache headers and that a matching `If-None-Match` is
//! answered `304`. What differs: the `ETag` values (opaque to a client; Java's are Jetty's
//! hash of the resource), no `Content-Encoding: gzip`, and no byte ranges.
//!
//! Behavior: `io.deephaven.server.jetty.JettyBackedGrpcServer`

#[cfg(test)]
mod tests;

use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use bytes::Bytes;
use http::header::{
    HeaderValue, CACHE_CONTROL, CONTENT_TYPE, ETAG, EXPIRES, IF_NONE_MATCH, LOCATION, PRAGMA,
};
use http::{Request, Response, StatusCode};
use http_body_util::Full;

use crate::transport::{empty_response, Body};

/// A directory with the web IDE's files, laid out as the URLs are.
#[derive(Debug)]
pub struct WebRoot {
    dir: PathBuf,
}

impl WebRoot {
    /// `dir` must be an existing directory.
    pub fn open(dir: &Path) -> Result<Self, String> {
        let canonical = dir
            .canonicalize()
            .map_err(|e| format!("web root {}: {e}", dir.display()))?;
        if !canonical.is_dir() {
            return Err(format!("web root {} is not a directory", dir.display()));
        }
        Ok(Self { dir: canonical })
    }
}

/// How long a browser may keep a response.
#[derive(Clone, Copy, PartialEq, Debug)]
enum Caching {
    /// A file under an application's `assets/`, whose name carries a hash of its content.
    Forever,
    /// Everything else of an application: checked with the server on each use.
    Revalidate,
    /// A path no caching rule covers.
    Unspecified,
}

const YEAR_IN_SECONDS: u64 = 365 * 24 * 60 * 60;

/// Whether `path` is under the servlet path pattern `prefix*`, where `prefix` ends with a
/// slash: the prefix itself with or without its last slash, or anything below it.
fn under(path: &str, prefix: &str) -> bool {
    path.starts_with(prefix) || path == &prefix[..prefix.len() - 1]
}

/// Behavior: `JettyBackedGrpcServer` (the `NoCacheFilter` and `CacheFilter` mappings)
fn caching(path: &str) -> Caching {
    const APPS: [&str; 4] = [
        "/ide/",
        "/iframe/table/",
        "/iframe/chart/",
        "/iframe/widget/",
    ];
    for app in APPS {
        if under(path, &format!("{app}assets/")) {
            return Caching::Forever;
        }
        if under(path, app) {
            return Caching::Revalidate;
        }
    }
    if under(path, "/jsapi/") || under(path, "/js-plugins/") {
        return Caching::Revalidate;
    }
    Caching::Unspecified
}

/// Behavior: `io.deephaven.server.jetty.NoCacheFilter#doFilter`, `CacheFilter#doFilter`
fn add_cache_headers(response: &mut Response<Body>, caching: Caching, now: SystemTime) {
    let headers = response.headers_mut();
    match caching {
        Caching::Forever => {
            let expires = httpdate::fmt_http_date(now + Duration::from_secs(YEAR_IN_SECONDS));
            if let Ok(value) = HeaderValue::from_str(&expires) {
                headers.insert(EXPIRES, value);
            }
            headers.insert(
                CACHE_CONTROL,
                HeaderValue::from_static("max-age=31536000, public, immutable"),
            );
        }
        Caching::Revalidate => {
            headers.insert(
                EXPIRES,
                HeaderValue::from_static("Thu, 01 Jan 1970 00:00:00 GMT"),
            );
            headers.insert(PRAGMA, HeaderValue::from_static("no-cache"));
            headers.insert(
                CACHE_CONTROL,
                HeaderValue::from_static("no-cache, must-revalidate, pre-check=0, post-check=0"),
            );
        }
        Caching::Unspecified => {}
    }
}

/// The content type of a file by its extension, for the kinds of file the web IDE has. A
/// file of another kind is sent without a content type, as Java sends a `.map` file.
fn content_type(path: &Path) -> Option<&'static str> {
    let extension = path.extension()?.to_str()?.to_ascii_lowercase();
    Some(match extension.as_str() {
        "html" | "htm" => "text/html",
        "js" | "mjs" => "text/javascript",
        "css" => "text/css",
        "json" => "application/json",
        "wasm" => "application/wasm",
        "png" => "image/png",
        "gif" => "image/gif",
        "jpg" | "jpeg" => "image/jpeg",
        "svg" => "image/svg+xml",
        "ico" => "image/vnd.microsoft.icon",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "txt" => "text/plain",
        _ => return None,
    })
}

/// What a URL path names.
#[derive(Debug, PartialEq)]
enum Target {
    /// The path below the web root, with `.` and `..` resolved.
    Path(Vec<String>),
    /// A path Java refuses with `400`: an empty segment, a `..` above the root, an encoded
    /// `.` or `..` segment, or a percent escape that is none.
    Bad,
}

fn percent_decode(segment: &str) -> Option<String> {
    let bytes = segment.as_bytes();
    let mut decoded = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok()?;
            decoded.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            decoded.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(decoded).ok()
}

/// Resolves a URL path. The result never leaves the root and none of its segments holds a
/// path separator.
fn resolve(path: &str) -> Target {
    let Some(path) = path.strip_prefix('/') else {
        return Target::Bad;
    };
    let mut resolved = Vec::new();
    let mut segments = path.split('/').peekable();
    while let Some(segment) = segments.next() {
        match segment {
            // a trailing slash names a directory; an empty segment elsewhere is refused
            "" if segments.peek().is_none() => {}
            "" => return Target::Bad,
            "." => {}
            ".." => {
                if resolved.pop().is_none() {
                    return Target::Bad;
                }
            }
            _ => match percent_decode(segment) {
                Some(decoded)
                    if decoded != "."
                        && decoded != ".."
                        && !decoded.contains(['/', '\\', '\0']) =>
                {
                    resolved.push(decoded)
                }
                _ => return Target::Bad,
            },
        }
    }
    Target::Path(resolved)
}

/// An error page.
pub(crate) fn error(status: StatusCode) -> Response<Body> {
    let reason = status.canonical_reason().unwrap_or("Error");
    let page = format!(
        "<html><head><title>{code} {reason}</title></head><body><h2>HTTP ERROR {code} \
         {reason}</h2></body></html>\n",
        code = status.as_u16()
    );
    let mut response = Response::new(Body::new(Full::new(Bytes::from(page))));
    *response.status_mut() = status;
    let headers = response.headers_mut();
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_static("must-revalidate,no-cache,no-store"),
    );
    headers.insert(
        CONTENT_TYPE,
        HeaderValue::from_static("text/html;charset=iso-8859-1"),
    );
    response
}

fn redirect(location: &str) -> Response<Body> {
    let mut response = empty_response(StatusCode::FOUND);
    match HeaderValue::from_str(location) {
        Ok(value) => {
            response.headers_mut().insert(LOCATION, value);
            response
        }
        Err(_) => error(StatusCode::BAD_REQUEST),
    }
}

fn with_query(path: &str, query: Option<&str>) -> String {
    match query {
        Some(query) => format!("{path}?{query}"),
        None => path.to_string(),
    }
}

/// An `ETag` that changes when the file does. It is weak, as Java's are.
fn etag(metadata: &std::fs::Metadata) -> String {
    let modified = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |since| since.as_nanos());
    format!("W/\"{modified:x}-{:x}\"", metadata.len())
}

/// Whether an `If-None-Match` header names this `ETag`. The comparison is the weak one:
/// the `W/` prefix does not count.
fn matches_etag(if_none_match: &str, etag: &str) -> bool {
    let opaque = |tag: &str| tag.trim().trim_start_matches("W/").to_string();
    if_none_match
        .split(',')
        .any(|candidate| candidate.trim() == "*" || opaque(candidate) == opaque(etag))
}

/// The list of JS plugins, which Java builds from the plugins installed in the server.
/// This server has none.
///
/// Behavior: `io.deephaven.server.jetty.JsPlugins`
const JS_PLUGINS_MANIFEST_PATH: &str = "/js-plugins/manifest.json";
const JS_PLUGINS_MANIFEST: &str = "{\"plugins\":[]}";

/// Answers a `GET` that is not a gRPC call.
///
/// - `/` redirects to `/ide/`, keeping the query string, whether or not there is a web
///   root.
/// - A directory without its trailing slash redirects to the same path with it. With it,
///   its `index.html` is served, or `403` if it has none.
/// - A file is served with its content type, cache headers and an `ETag`; a request whose
///   `If-None-Match` names that `ETag` gets `304`. `If-Modified-Since` is ignored.
/// - Anything else is `404`.
///
/// Behavior: `io.deephaven.server.jetty.HomeFilter#doFilter`, `DropIfModifiedSinceHeader`,
/// `JettyBackedGrpcServer` (`dirAllowed` is false)
pub(crate) async fn get<B>(root: Option<&WebRoot>, request: &Request<B>) -> Response<Body> {
    let path = request.uri().path();
    let query = request.uri().query();
    if path == "/" {
        return redirect(&with_query("/ide/", query));
    }
    let caching = caching(path);
    let now = SystemTime::now();
    let mut response = if path == JS_PLUGINS_MANIFEST_PATH {
        let mut response = Response::new(Body::new(Full::new(Bytes::from_static(
            JS_PLUGINS_MANIFEST.as_bytes(),
        ))));
        response
            .headers_mut()
            .insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        response
    } else {
        let if_none_match = request
            .headers()
            .get(IF_NONE_MATCH)
            .and_then(|value| value.to_str().ok());
        match resolve(path) {
            Target::Bad => return error(StatusCode::BAD_REQUEST),
            Target::Path(segments) => match root {
                None => error(StatusCode::NOT_FOUND),
                Some(root) => file(root, path, query, &segments, if_none_match).await,
            },
        }
    };
    // 304 is among the redirections
    if response.status().is_success() || response.status().is_redirection() {
        add_cache_headers(&mut response, caching, now);
    }
    response
}

async fn file(
    root: &WebRoot,
    url_path: &str,
    query: Option<&str>,
    segments: &[String],
    if_none_match: Option<&str>,
) -> Response<Body> {
    let mut path = root.dir.clone();
    path.extend(segments);
    let Ok(mut metadata) = tokio::fs::metadata(&path).await else {
        return error(StatusCode::NOT_FOUND);
    };
    if metadata.is_dir() {
        if !url_path.ends_with('/') {
            return redirect(&with_query(&format!("{url_path}/"), query));
        }
        path.push("index.html");
        metadata = match tokio::fs::metadata(&path).await {
            Ok(metadata) if metadata.is_file() => metadata,
            _ => return error(StatusCode::FORBIDDEN),
        };
    } else if url_path.ends_with('/') {
        // a file is not a directory
        return error(StatusCode::NOT_FOUND);
    }
    // a link in the web root must not lead out of it
    match tokio::fs::canonicalize(&path).await {
        Ok(canonical) if canonical.starts_with(&root.dir) => {}
        _ => return error(StatusCode::NOT_FOUND),
    }
    let etag = etag(&metadata);
    let not_modified = if_none_match.is_some_and(|header| matches_etag(header, &etag));
    let mut response = if not_modified {
        empty_response(StatusCode::NOT_MODIFIED)
    } else {
        match tokio::fs::read(&path).await {
            Ok(content) => Response::new(Body::new(Full::new(Bytes::from(content)))),
            Err(_) => return error(StatusCode::NOT_FOUND),
        }
    };
    let headers = response.headers_mut();
    if let Ok(value) = HeaderValue::from_str(&etag) {
        headers.insert(ETAG, value);
    }
    if !not_modified {
        if let Some(content_type) = content_type(&path) {
            headers.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
        }
    }
    response
}
