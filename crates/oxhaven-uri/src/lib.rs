//! Deephaven's URIs: parsing and formatting, as Deephaven 42.9's `io.deephaven.uri` package
//! does them.
//!
//! - [`Uri`] is a URI as `java.net.URI` reads it; every Deephaven type is defined on top of it.
//! - [`DeephavenTarget`] names a server: `dh://host[:port]` (TLS) or `dh+plain://host[:port]`.
//! - [`QueryScopeUri`] (`dh:///scope/<name>`), [`ApplicationUri`]
//!   (`dh:///app/<id>/field/<name>`) and [`FieldUri`] (`dh:///field/<name>`) name local
//!   resources; [`RemoteUri`] puts one of them, a proxied URI or a [`CustomUri`] behind a target.
//!
//! The crate does no resolving and no networking. It has no dependencies, so the server and a
//! client can both use it. `tasks/13.2a-uri.md` says what resolvers built on it must do.
//!
//! Errors carry Java's messages: a user of `deephaven.uri.resolve` sees the same text.

mod authority;
mod deephaven;
mod escape;
mod syntax;

pub use deephaven::{
    is_deephaven_scheme, is_local_path, is_remote_path, is_remote_query, is_remote_target,
    ApplicationUri, CustomUri, DeephavenTarget, FieldUri, QueryScopeUri, RemoteUri, StructuredUri,
    UriError, DEFAULT_PLAINTEXT_PORT, DEFAULT_TLS_PORT, LOCAL_SCHEME, PLAINTEXT_SCHEME,
    SECURE_SCHEME,
};
pub use escape::is_uri_safe;
pub use syntax::{Uri, UriSyntaxError};
