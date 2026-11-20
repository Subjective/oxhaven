//! Deephaven's structured URIs: targets, local and remote resources, and custom URIs.

use std::fmt;

use crate::escape::{is_uri_safe, url_encode};
use crate::syntax::{Uri, UriSyntaxError};

/// The scheme of a remote URI over a secure (TLS) connection, and of every local URI.
pub const SECURE_SCHEME: &str = "dh";
/// The scheme of a remote URI over a plaintext connection.
pub const PLAINTEXT_SCHEME: &str = "dh+plain";
/// The scheme of a local (non-remote) Deephaven URI.
pub const LOCAL_SCHEME: &str = SECURE_SCHEME;
/// The port a client uses for a secure target without one.
///
/// Behavior: io.deephaven.client.impl.ChannelHelper#DEFAULT_TLS_PORT
pub const DEFAULT_TLS_PORT: u32 = 443;
/// The port a client uses for a plaintext target without one.
///
/// Behavior: io.deephaven.client.impl.ChannelHelper#DEFAULT_PLAINTEXT_PORT
pub const DEFAULT_PLAINTEXT_PORT: u32 = 10000;

/// Why a string or a value is not a valid Deephaven URI. Java throws
/// `IllegalArgumentException` for both kinds; `Display` is its message.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UriError {
    /// The string is not a URI at all.
    Syntax(UriSyntaxError),
    /// A URI, or a name, host or port, that the Deephaven type does not accept.
    Invalid(String),
}

impl fmt::Display for UriError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            UriError::Syntax(e) => e.fmt(f),
            UriError::Invalid(message) => f.write_str(message),
        }
    }
}

impl std::error::Error for UriError {}

impl From<UriSyntaxError> for UriError {
    fn from(e: UriSyntaxError) -> UriError {
        UriError::Syntax(e)
    }
}

fn invalid<T>(message: String) -> Result<T, UriError> {
    Err(UriError::Invalid(message))
}

/// True for `dh` and `dh+plain`.
///
/// Behavior: io.deephaven.uri.DeephavenUri#isValidScheme
pub fn is_deephaven_scheme(scheme: Option<&str>) -> bool {
    matches!(scheme, Some(SECURE_SCHEME | PLAINTEXT_SCHEME))
}

fn is_local_scheme(scheme: Option<&str>) -> bool {
    scheme == Some(LOCAL_SCHEME)
}

fn no_query_user_info_or_fragment(uri: &Uri) -> bool {
    !uri.is_opaque() && uri.raw_user_info().is_none() && uri.raw_fragment().is_none()
}

/// A URI with only a scheme and a path that starts with `/`: no host (a registry-based
/// authority has none, so it counts as local), query, user information or fragment.
///
/// Behavior: io.deephaven.uri.UriHelper#isLocalPath
pub fn is_local_path(uri: &Uri) -> bool {
    uri.host().is_none()
        && no_query_user_info_or_fragment(uri)
        && uri.path().is_some_and(|p| p.starts_with('/'))
        && uri.raw_query().is_none()
}

/// A URI with a scheme, a host and a path that starts with `/`, and nothing else.
///
/// Behavior: io.deephaven.uri.UriHelper#isRemotePath
pub fn is_remote_path(uri: &Uri) -> bool {
    uri.host().is_some()
        && no_query_user_info_or_fragment(uri)
        && uri.path().is_some_and(|p| p.starts_with('/'))
        && uri.raw_query().is_none()
}

/// A URI with a scheme and a host (and port), and an empty path.
///
/// Behavior: io.deephaven.uri.UriHelper#isRemoteTarget
pub fn is_remote_target(uri: &Uri) -> bool {
    uri.host().is_some()
        && no_query_user_info_or_fragment(uri)
        && uri.raw_path() == Some("")
        && uri.raw_query().is_none()
}

/// A URI with a scheme, a host, a query and an empty path.
///
/// Behavior: io.deephaven.uri.UriHelper#isRemoteQuery
pub fn is_remote_query(uri: &Uri) -> bool {
    uri.host().is_some()
        && no_query_user_info_or_fragment(uri)
        && uri.raw_path() == Some("")
        && uri.raw_query().is_some()
}

/// Java's `.` (no line terminators) over a whole capture group.
fn regex_dot_plus(s: &str) -> bool {
    !s.is_empty()
        && !s
            .chars()
            .any(|c| matches!(c, '\n' | '\r' | '\u{85}' | '\u{2028}' | '\u{2029}'))
}

/// `^/scope/(.+)$` on a decoded path.
fn match_scope(path: &str) -> Option<&str> {
    path.strip_prefix("/scope/").filter(|n| regex_dot_plus(n))
}

/// `^/field/(.+)$` on a decoded path.
fn match_field(path: &str) -> Option<&str> {
    path.strip_prefix("/field/").filter(|n| regex_dot_plus(n))
}

/// `^/app/(.+)/field/(.+)$` on a decoded path; the first group is greedy, so the split is at
/// the last `/field/` that leaves both groups non-empty.
fn match_app(path: &str) -> Option<(&str, &str)> {
    const SEP: &str = "/field/";
    let rest = path.strip_prefix("/app/")?;
    if !regex_dot_plus(rest) {
        return None;
    }
    rest.rmatch_indices(SEP)
        .map(|(i, _)| (&rest[..i], &rest[i + SEP.len()..]))
        .find(|(app, field)| !app.is_empty() && !field.is_empty())
}

/// `^uri=(.+)$` on a decoded query.
fn match_proxy(query: &str) -> Option<&str> {
    query.strip_prefix("uri=").filter(|u| regex_dot_plus(u))
}

/// The information needed to connect to a remote Deephaven server: whether the connection is
/// secure, a host, and an optional port.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct DeephavenTarget {
    secure: bool,
    host: String,
    port: Option<u32>,
}

impl DeephavenTarget {
    /// A target, validated as Java's builder validates it: the target's URI string must parse
    /// and give back exactly `host`. `port` is Java's `int`; a negative one is rejected.
    ///
    /// Behavior: io.deephaven.uri.DeephavenTarget#checkHostPort
    pub fn new(secure: bool, host: &str, port: Option<i32>) -> Result<DeephavenTarget, UriError> {
        let scheme = if secure {
            SECURE_SCHEME
        } else {
            PLAINTEXT_SCHEME
        };
        let text = match port {
            Some(port) => format!("{scheme}://{host}:{port}"),
            None => format!("{scheme}://{host}"),
        };
        let uri = Uri::parse(&text)?;
        if uri.host() != Some(host) {
            return invalid(format!("Invalid host '{host}'"));
        }
        Ok(DeephavenTarget {
            secure,
            host: host.to_owned(),
            port: uri.port(),
        })
    }

    /// True for `dh` and `dh+plain`.
    ///
    /// Behavior: io.deephaven.uri.DeephavenTarget#isValidScheme
    pub fn is_valid_scheme(scheme: Option<&str>) -> bool {
        is_deephaven_scheme(scheme)
    }

    /// True for `dh://host[:port]` and `dh+plain://host[:port]`, with nothing else.
    ///
    /// Behavior: io.deephaven.uri.DeephavenTarget#isWellFormed
    pub fn is_well_formed(uri: &Uri) -> bool {
        Self::is_valid_scheme(uri.scheme()) && is_remote_target(uri)
    }

    /// Reads a target URI: `dh://host`, `dh://host:port`, `dh+plain://host` or
    /// `dh+plain://host:port`.
    ///
    /// Behavior: io.deephaven.uri.DeephavenTarget#of
    pub fn from_uri(uri: &Uri) -> Result<DeephavenTarget, UriError> {
        if !Self::is_well_formed(uri) {
            return invalid(format!("Invalid target Deephaven URI '{uri}'"));
        }
        Self::from_parts(uri)
    }

    /// Parses `s` as a URI, then as a target.
    pub fn parse(s: &str) -> Result<DeephavenTarget, UriError> {
        Self::from_uri(&Uri::parse(s)?)
    }

    /// The target of a URI already known to have a Deephaven scheme and a host.
    ///
    /// Behavior: io.deephaven.uri.DeephavenTarget#from
    fn from_parts(uri: &Uri) -> Result<DeephavenTarget, UriError> {
        let secure = match uri.scheme() {
            Some(SECURE_SCHEME) => true,
            Some(PLAINTEXT_SCHEME) => false,
            other => {
                return invalid(format!(
                    "Invalid Deephaven target scheme '{}'",
                    other.unwrap_or("null")
                ))
            }
        };
        let host = uri
            .host()
            .ok_or_else(|| UriError::Invalid(format!("Invalid target Deephaven URI '{uri}'")))?;
        // A port parsed from a URI fits Java's int.
        let port = uri.port().map(|p| p as i32);
        DeephavenTarget::new(secure, host, port)
    }

    /// True for `dh`, typically TLS.
    pub fn is_secure(&self) -> bool {
        self.secure
    }

    /// The host name or address; an IPv6 address keeps its brackets.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The port, when the target names one.
    pub fn port(&self) -> Option<u32> {
        self.port
    }

    /// The port a client connects to: the target's, or the default for its scheme.
    ///
    /// Behavior: io.deephaven.client.impl.DeephavenTargetNameResolverProviders (default ports)
    pub fn port_or_default(&self) -> u32 {
        self.port.unwrap_or(if self.secure {
            DEFAULT_TLS_PORT
        } else {
            DEFAULT_PLAINTEXT_PORT
        })
    }

    /// The target as a URI.
    pub fn to_uri(&self) -> Uri {
        Uri::parse(&self.to_string()).expect("a target's URI string was checked when it was made")
    }
}

impl fmt::Display for DeephavenTarget {
    /// Behavior: io.deephaven.uri.DeephavenTarget#toString
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let scheme = if self.secure {
            SECURE_SCHEME
        } else {
            PLAINTEXT_SCHEME
        };
        match self.port {
            Some(port) => write!(f, "{scheme}://{}:{port}", self.host),
            None => write!(f, "{scheme}://{}", self.host),
        }
    }
}

fn check_name(kind: &str, name: &str) -> Result<String, UriError> {
    if is_uri_safe(name) {
        Ok(name.to_owned())
    } else {
        invalid(format!("Invalid {kind} '{name}'"))
    }
}

/// A variable of the query scope: `dh:///scope/<name>`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct QueryScopeUri {
    variable_name: String,
}

impl QueryScopeUri {
    /// The URI of `variable_name`, which must be URL-safe (`[A-Za-z0-9.*_-]*`).
    ///
    /// Behavior: io.deephaven.uri.QueryScopeUri#checkVariableName
    pub fn new(variable_name: &str) -> Result<QueryScopeUri, UriError> {
        Ok(QueryScopeUri {
            variable_name: check_name("variable name", variable_name)?,
        })
    }

    /// Behavior: io.deephaven.uri.QueryScopeUri#isWellFormed
    pub fn is_well_formed(uri: &Uri) -> bool {
        is_local_scheme(uri.scheme())
            && is_local_path(uri)
            && uri.path().is_some_and(|p| match_scope(&p).is_some())
    }

    /// Behavior: io.deephaven.uri.QueryScopeUri#of(java.net.URI)
    pub fn from_uri(uri: &Uri) -> Result<QueryScopeUri, UriError> {
        if !Self::is_well_formed(uri) {
            return invalid(format!("Invalid query scope URI '{uri}'"));
        }
        let path = uri.path().unwrap_or_default();
        Self::new(match_scope(&path).unwrap_or_default())
    }

    /// Parses `s` as a URI, then as a query scope URI.
    pub fn parse(s: &str) -> Result<QueryScopeUri, UriError> {
        Self::from_uri(&Uri::parse(s)?)
    }

    pub fn variable_name(&self) -> &str {
        &self.variable_name
    }
}

impl fmt::Display for QueryScopeUri {
    /// Behavior: io.deephaven.uri.QueryScopeUri#toString
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{LOCAL_SCHEME}:///scope/{}", self.variable_name)
    }
}

/// A field of an application: `dh:///app/<application id>/field/<field name>`.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ApplicationUri {
    application_id: String,
    field_name: String,
}

impl ApplicationUri {
    /// Both parts must be URL-safe; the application id is checked first.
    ///
    /// Behavior: io.deephaven.uri.ApplicationUri#checkApplicationId,
    /// io.deephaven.uri.ApplicationUri#checkFieldName
    pub fn new(application_id: &str, field_name: &str) -> Result<ApplicationUri, UriError> {
        Ok(ApplicationUri {
            application_id: check_name("application id", application_id)?,
            field_name: check_name("field name", field_name)?,
        })
    }

    /// Behavior: io.deephaven.uri.ApplicationUri#isWellFormed
    pub fn is_well_formed(uri: &Uri) -> bool {
        is_local_scheme(uri.scheme())
            && is_local_path(uri)
            && uri.path().is_some_and(|p| match_app(&p).is_some())
    }

    /// Behavior: io.deephaven.uri.ApplicationUri#of(java.net.URI)
    pub fn from_uri(uri: &Uri) -> Result<ApplicationUri, UriError> {
        if !Self::is_well_formed(uri) {
            return invalid(format!("Invalid application URI '{uri}'"));
        }
        let path = uri.path().unwrap_or_default();
        let (app, field) = match_app(&path).unwrap_or_default();
        Self::new(app, field)
    }

    /// Parses `s` as a URI, then as an application URI.
    pub fn parse(s: &str) -> Result<ApplicationUri, UriError> {
        Self::from_uri(&Uri::parse(s)?)
    }

    pub fn application_id(&self) -> &str {
        &self.application_id
    }

    pub fn field_name(&self) -> &str {
        &self.field_name
    }
}

impl fmt::Display for ApplicationUri {
    /// Behavior: io.deephaven.uri.ApplicationUri#toString
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "{LOCAL_SCHEME}:///app/{}/field/{}",
            self.application_id, self.field_name
        )
    }
}

/// A field without its application: `dh:///field/<name>`. It resolves only inside a
/// [`RemoteUri`], where the target's host is the application id.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct FieldUri {
    field_name: String,
}

impl FieldUri {
    /// Behavior: io.deephaven.uri.FieldUri#checkFieldName
    pub fn new(field_name: &str) -> Result<FieldUri, UriError> {
        Ok(FieldUri {
            field_name: check_name("field name", field_name)?,
        })
    }

    /// Behavior: io.deephaven.uri.FieldUri#isWellFormed
    pub fn is_well_formed(uri: &Uri) -> bool {
        is_local_scheme(uri.scheme())
            && is_local_path(uri)
            && uri.path().is_some_and(|p| match_field(&p).is_some())
    }

    /// Behavior: io.deephaven.uri.FieldUri#of(java.net.URI)
    pub fn from_uri(uri: &Uri) -> Result<FieldUri, UriError> {
        if !Self::is_well_formed(uri) {
            return invalid(format!("Invalid field URI '{uri}'"));
        }
        let path = uri.path().unwrap_or_default();
        Self::new(match_field(&path).unwrap_or_default())
    }

    /// Parses `s` as a URI, then as a field URI.
    pub fn parse(s: &str) -> Result<FieldUri, UriError> {
        Self::from_uri(&Uri::parse(s)?)
    }

    pub fn field_name(&self) -> &str {
        &self.field_name
    }
}

impl fmt::Display for FieldUri {
    /// Behavior: io.deephaven.uri.FieldUri#toString
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{LOCAL_SCHEME}:///field/{}", self.field_name)
    }
}

/// Any URI with a scheme other than `dh` and `dh+plain`, for example
/// `parquet:///data/t.parquet`; resolvers outside this crate give it meaning.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CustomUri {
    uri: Uri,
}

impl CustomUri {
    /// True for any scheme but the Deephaven ones; a URI without a scheme is not custom.
    ///
    /// Behavior: io.deephaven.uri.CustomUri#isValidScheme
    pub fn is_valid_scheme(scheme: Option<&str>) -> bool {
        scheme.is_some() && !is_deephaven_scheme(scheme)
    }

    /// Behavior: io.deephaven.uri.CustomUri#checkScheme
    pub fn new(uri: Uri) -> Result<CustomUri, UriError> {
        if !Self::is_valid_scheme(uri.scheme()) {
            return invalid(format!("Invalid custom URI '{uri}'"));
        }
        Ok(CustomUri { uri })
    }

    /// Parses `s` as a URI, then as a custom URI.
    pub fn parse(s: &str) -> Result<CustomUri, UriError> {
        Self::new(Uri::parse(s)?)
    }

    pub fn uri(&self) -> &Uri {
        &self.uri
    }
}

impl fmt::Display for CustomUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.uri.fmt(f)
    }
}

/// Any structured URI: what a [`RemoteUri`] wraps.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum StructuredUri {
    QueryScope(QueryScopeUri),
    Application(ApplicationUri),
    Field(FieldUri),
    Remote(Box<RemoteUri>),
    Custom(CustomUri),
}

impl StructuredUri {
    /// This URI, resolved through `target`.
    ///
    /// Behavior: io.deephaven.uri.StructuredUri#target
    pub fn target(self, target: DeephavenTarget) -> RemoteUri {
        RemoteUri::new(target, self)
    }
}

impl fmt::Display for StructuredUri {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StructuredUri::QueryScope(u) => u.fmt(f),
            StructuredUri::Application(u) => u.fmt(f),
            StructuredUri::Field(u) => u.fmt(f),
            StructuredUri::Remote(u) => u.fmt(f),
            StructuredUri::Custom(u) => u.fmt(f),
        }
    }
}

impl From<QueryScopeUri> for StructuredUri {
    fn from(u: QueryScopeUri) -> Self {
        StructuredUri::QueryScope(u)
    }
}

impl From<ApplicationUri> for StructuredUri {
    fn from(u: ApplicationUri) -> Self {
        StructuredUri::Application(u)
    }
}

impl From<FieldUri> for StructuredUri {
    fn from(u: FieldUri) -> Self {
        StructuredUri::Field(u)
    }
}

impl From<RemoteUri> for StructuredUri {
    fn from(u: RemoteUri) -> Self {
        StructuredUri::Remote(Box::new(u))
    }
}

impl From<CustomUri> for StructuredUri {
    fn from(u: CustomUri) -> Self {
        StructuredUri::Custom(u)
    }
}

/// A resource on a remote server: a [`DeephavenTarget`] and the URI to resolve there.
///
/// `dh://host/scope/t`, `dh://host/app/a/field/f` and `dh://host/field/f` wrap the local
/// forms. `dh://gateway?uri=<inner>` wraps any other URI to be resolved by the gateway: a
/// remote Deephaven URI as it is, anything else URL-encoded.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RemoteUri {
    target: DeephavenTarget,
    uri: StructuredUri,
}

impl RemoteUri {
    pub fn new(target: DeephavenTarget, uri: StructuredUri) -> RemoteUri {
        RemoteUri { target, uri }
    }

    /// True for `dh` and `dh+plain`.
    ///
    /// Behavior: io.deephaven.uri.RemoteUri#isValidScheme
    pub fn is_valid_scheme(scheme: Option<&str>) -> bool {
        is_deephaven_scheme(scheme)
    }

    /// Behavior: io.deephaven.uri.RemoteUri#isWellFormed
    pub fn is_well_formed(uri: &Uri) -> bool {
        Self::is_valid_scheme(uri.scheme())
            && ((is_remote_path(uri)
                && uri.path().is_some_and(|p| {
                    match_app(&p).is_some()
                        || match_field(&p).is_some()
                        || match_scope(&p).is_some()
                }))
                || (is_remote_query(uri) && uri.query().is_some_and(|q| match_proxy(&q).is_some())))
    }

    /// Reads a remote URI. The forms are tried in Java's order: application field, field,
    /// query scope, proxied.
    ///
    /// Behavior: io.deephaven.uri.RemoteUri#of(java.net.URI)
    pub fn from_uri(uri: &Uri) -> Result<RemoteUri, UriError> {
        if !Self::is_valid_scheme(uri.scheme()) {
            return invalid(format!("Invalid remote Deephaven URI '{uri}'"));
        }
        if is_remote_path(uri) {
            let path = uri.path().unwrap_or_default();
            let inner: Option<Result<StructuredUri, UriError>> =
                if let Some((a, f)) = match_app(&path) {
                    Some(ApplicationUri::new(a, f).map(Into::into))
                } else if let Some(f) = match_field(&path) {
                    Some(FieldUri::new(f).map(Into::into))
                } else {
                    match_scope(&path).map(|n| QueryScopeUri::new(n).map(Into::into))
                };
            if let Some(inner) = inner {
                return Ok(RemoteUri::new(DeephavenTarget::from_parts(uri)?, inner?));
            }
        }
        if is_remote_query(uri) {
            let query = uri.query().unwrap_or_default();
            if let Some(inner) = match_proxy(&query) {
                return Self::proxied(uri, inner);
            }
        }
        invalid(format!("Invalid remote Deephaven URI '{uri}'"))
    }

    /// Behavior: io.deephaven.uri.RemoteProxiedUri#of
    fn proxied(uri: &Uri, inner: &str) -> Result<RemoteUri, UriError> {
        let inner = Uri::parse(inner)?;
        let inner = if Self::is_well_formed(&inner) {
            StructuredUri::from(Self::from_uri(&inner)?)
        } else {
            StructuredUri::from(CustomUri::new(inner)?)
        };
        Ok(RemoteUri::new(DeephavenTarget::from_parts(uri)?, inner))
    }

    /// Parses `s` as a URI, then as a remote URI.
    pub fn parse(s: &str) -> Result<RemoteUri, UriError> {
        Self::from_uri(&Uri::parse(s)?)
    }

    pub fn target(&self) -> &DeephavenTarget {
        &self.target
    }

    /// The inner URI, to be resolved on the target.
    pub fn uri(&self) -> &StructuredUri {
        &self.uri
    }
}

impl fmt::Display for RemoteUri {
    /// Behavior: io.deephaven.uri.RemoteUri#toString
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let target = &self.target;
        match &self.uri {
            StructuredUri::QueryScope(u) => write!(f, "{target}/scope/{}", u.variable_name),
            StructuredUri::Application(u) => write!(
                f,
                "{target}/app/{}/field/{}",
                u.application_id, u.field_name
            ),
            StructuredUri::Field(u) => write!(f, "{target}/field/{}", u.field_name),
            StructuredUri::Remote(u) => write!(f, "{target}?uri={u}"),
            StructuredUri::Custom(u) => write!(f, "{target}?uri={}", url_encode(u.uri.as_str())),
        }
    }
}
