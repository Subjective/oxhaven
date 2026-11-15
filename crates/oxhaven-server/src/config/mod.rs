//! The config service: what a client may know before and after it authenticates.
//!
//! Behavior: `io.deephaven.server.config.ConfigServiceGrpcImpl`

use crate::console::Console;
use crate::session::{current_session, AuthHandler, AuthHandlers, PSK_AUTH_TYPE};
use oxhaven_proto::grpc::config_service_server::ConfigService;
use oxhaven_proto::grpc::config_value::Kind;
use oxhaven_proto::grpc::{
    AuthenticationConstantsRequest, AuthenticationConstantsResponse, ConfigValue,
    ConfigurationConstantsRequest, ConfigurationConstantsResponse,
};
use std::collections::HashMap;
use tonic::{Request, Response, Status};

/// The Deephaven release whose protocol and behavior this server reproduces; see
/// `oracle/REFERENCE.md`. Clients read it as `deephaven.version`.
pub const DEEPHAVEN_VERSION: &str = "42.9";

/// The version of the Barrage protocol library that release ships, which clients read as
/// `barrage.version`.
pub const BARRAGE_VERSION: &str = "1.0.0";

/// The class name Java's `AuthHandlers` property uses for anonymous authentication.
const ANONYMOUS_HANDLER_CLASS: &str = "io.deephaven.auth.AnonymousAuthenticationHandler";

fn values(pairs: impl IntoIterator<Item = (&'static str, String)>) -> HashMap<String, ConfigValue> {
    pairs
        .into_iter()
        .map(|(key, value)| {
            let value = ConfigValue {
                kind: Some(Kind::StringValue(value)),
            };
            (key.to_string(), value)
        })
        .collect()
}

/// The config service. Its answers are fixed when the server starts.
pub struct ConfigGrpc {
    authentication: HashMap<String, ConfigValue>,
    configuration: HashMap<String, ConfigValue>,
}

impl ConfigGrpc {
    /// `token_expire_ms` is the lifetime of a session token, which clients read as
    /// `http.session.durationMs` to decide how often to refresh theirs.
    ///
    /// The keys are those of the reference server's `authentication.client.configuration.list`
    /// and `client.configuration.list` in `dh-defaults.prop`, with the values of that file
    /// for the settings of the web client. `java.version` and `groovy.version` are left
    /// out, as the reference server leaves out a key it has no value for: this server runs
    /// neither. `python.version` is added by [`ConfigGrpc::with_python_version`].
    pub fn new(auth_handlers: &AuthHandlers, token_expire_ms: i64) -> Self {
        let handlers = auth_handlers
            .iter()
            .map(|handler| match handler {
                AuthHandler::Anonymous => ANONYMOUS_HANDLER_CLASS,
                AuthHandler::Psk(_) => PSK_AUTH_TYPE,
            })
            .collect::<Vec<_>>()
            .join(",");
        Self {
            authentication: values([("AuthHandlers", handlers)]),
            configuration: values([
                ("deephaven.version", DEEPHAVEN_VERSION.to_string()),
                ("barrage.version", BARRAGE_VERSION.to_string()),
                ("http.session.durationMs", token_expire_ms.to_string()),
                ("file.separator", std::path::MAIN_SEPARATOR.to_string()),
                ("web.storage.layout.directory", "/layouts".to_string()),
                ("web.storage.notebook.directory", "/notebooks".to_string()),
                ("web.webgl", "true".to_string()),
                ("web.webgl.editable", "true".to_string()),
                ("web.flattenViewports", "false".to_string()),
            ]),
        }
    }

    /// Adds `python.version`, the version of the interpreter of the Python console, or
    /// leaves the key out when there is none.
    ///
    /// Behavior: `io.deephaven.integrations.python.PythonDeephavenSession#setPythonVersion`
    pub fn with_python_version(mut self, version: Option<String>) -> Self {
        self.configuration
            .extend(values(version.map(|version| ("python.version", version))));
        self
    }
}

/// The version of the Python that `console` runs, as `platform.python_version()` gives it;
/// `None` when the server has no Python console, or the interpreter does not say.
///
/// Behavior: `io.deephaven.integrations.python.PythonDeephavenSession#setPythonVersion`
pub fn python_version(console: Option<&Console>) -> Option<String> {
    #[cfg(feature = "python")]
    if console.is_some_and(|console| console.console_type() == "python") {
        return match crate::console::python::python_version() {
            Ok(version) => Some(version),
            Err(error) => {
                tracing::warn!("Unable to retrieve python version: {error}");
                None
            }
        };
    }
    let _ = console;
    None
}

#[tonic::async_trait]
impl ConfigService for ConfigGrpc {
    /// Answers without a session: a client asks this to learn how to authenticate.
    ///
    /// Behavior: `ConfigServiceGrpcImpl#getAuthenticationConstants`
    async fn get_authentication_constants(
        &self,
        _request: Request<AuthenticationConstantsRequest>,
    ) -> Result<Response<AuthenticationConstantsResponse>, Status> {
        Ok(Response::new(AuthenticationConstantsResponse {
            config_values: self.authentication.clone(),
        }))
    }

    /// Behavior: `ConfigServiceGrpcImpl#getConfigurationConstants`
    async fn get_configuration_constants(
        &self,
        request: Request<ConfigurationConstantsRequest>,
    ) -> Result<Response<ConfigurationConstantsResponse>, Status> {
        current_session(&request)?;
        Ok(Response::new(ConfigurationConstantsResponse {
            config_values: self.configuration.clone(),
        }))
    }
}

#[cfg(test)]
mod tests;
