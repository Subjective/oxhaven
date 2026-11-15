use super::*;
use crate::test_support::{with_auth, TestServer};
use oxhaven_proto::grpc::config_service_client::ConfigServiceClient;
use tonic::Code;

fn strings(values: &HashMap<String, ConfigValue>) -> Vec<(&str, &str)> {
    let mut pairs: Vec<(&str, &str)> = values
        .iter()
        .map(|(key, value)| match &value.kind {
            Some(Kind::StringValue(text)) => (key.as_str(), text.as_str()),
            None => (key.as_str(), "<unset>"),
        })
        .collect();
    pairs.sort();
    pairs
}

#[tokio::test]
async fn authentication_constants_need_no_session_and_name_the_handlers() {
    let server = TestServer::start().await;
    let response = ConfigServiceClient::new(server.channel.clone())
        .get_authentication_constants(AuthenticationConstantsRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        strings(&response.config_values),
        vec![(
            "AuthHandlers",
            "io.deephaven.authentication.psk.PskAuthenticationHandler,\
             io.deephaven.auth.AnonymousAuthenticationHandler"
        )]
    );
}

#[tokio::test]
async fn configuration_constants_need_a_session() {
    let server = TestServer::start().await;
    let mut client = ConfigServiceClient::new(server.channel.clone());
    let refused = client
        .get_configuration_constants(ConfigurationConstantsRequest {})
        .await
        .unwrap_err();
    assert_eq!(refused.code(), Code::Unauthenticated);

    let bearer = server.login().await;
    let response = client
        .get_configuration_constants(with_auth(ConfigurationConstantsRequest {}, &bearer))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        strings(&response.config_values),
        vec![
            ("barrage.version", "1.0.0"),
            ("deephaven.version", "42.9"),
            ("file.separator", "/"),
            ("http.session.durationMs", "300000"),
            ("web.flattenViewports", "false"),
            ("web.storage.layout.directory", "/layouts"),
            ("web.storage.notebook.directory", "/notebooks"),
            ("web.webgl", "true"),
            ("web.webgl.editable", "true"),
        ]
    );
}

#[test]
fn the_session_duration_is_the_token_lifetime() {
    let handlers = AuthHandlers::new(vec![AuthHandler::Anonymous]).unwrap();
    let config = ConfigGrpc::new(&handlers, 12_345);
    assert_eq!(
        config.configuration["http.session.durationMs"].kind,
        Some(Kind::StringValue("12345".to_string()))
    );
    assert_eq!(
        strings(&config.authentication),
        vec![(
            "AuthHandlers",
            "io.deephaven.auth.AnonymousAuthenticationHandler"
        )]
    );
}

#[test]
fn the_python_version_is_reported_only_when_there_is_one() {
    let handlers = AuthHandlers::new(vec![AuthHandler::Anonymous]).unwrap();
    let without = ConfigGrpc::new(&handlers, 1).with_python_version(None);
    assert!(!without.configuration.contains_key("python.version"));
    assert_eq!(without.configuration.len(), 9);

    let with = ConfigGrpc::new(&handlers, 1).with_python_version(Some("3.13.2".to_string()));
    assert_eq!(
        with.configuration["python.version"].kind,
        Some(Kind::StringValue("3.13.2".to_string()))
    );
    assert_eq!(with.configuration.len(), 10);

    // a server without a console has no Python to report
    assert_eq!(python_version(None), None);
}
