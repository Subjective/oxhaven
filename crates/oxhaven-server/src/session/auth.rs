//! Authentication handlers: anonymous and pre-shared key.

use tonic::Status;

/// The `authorization` header value prefix of a session token.
pub const BEARER_PREFIX: &str = "Bearer ";

/// The auth type a client names to select the pre-shared key handler.
pub const PSK_AUTH_TYPE: &str = "io.deephaven.authentication.psk.PskAuthenticationHandler";

/// The auth type a client names to select the anonymous handler.
pub const ANONYMOUS_AUTH_TYPE: &str = "Anonymous";

/// Who a session acts as.
///
/// Behavior: `io.deephaven.auth.AuthContext`
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthContext {
    SuperUser,
    Anonymous,
}

/// One way of turning credentials into an [`AuthContext`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthHandler {
    /// Accepts an empty payload.
    Anonymous,
    /// Accepts a payload equal to the pre-shared key.
    Psk(String),
}

impl AuthHandler {
    /// A pre-shared key handler. The key is trimmed and must be non-empty ASCII.
    ///
    /// Behavior: the static initializer of
    /// `io.deephaven.authentication.psk.PskAuthenticationHandler`, except that Java replaces
    /// an empty key with a random one; here the caller generates it (see [`random_psk`]).
    pub fn psk(key: &str) -> Result<Self, String> {
        let key = key.trim();
        if key.is_empty() {
            return Err("pre-shared key is empty".to_string());
        }
        if !key.is_ascii() {
            return Err("pre-shared key is not ASCII".to_string());
        }
        Ok(AuthHandler::Psk(key.to_string()))
    }

    /// Behavior: `io.deephaven.auth.AuthenticationRequestHandler#getAuthType`
    pub fn auth_type(&self) -> &'static str {
        match self {
            AuthHandler::Anonymous => ANONYMOUS_AUTH_TYPE,
            AuthHandler::Psk(_) => PSK_AUTH_TYPE,
        }
    }

    /// Login with the payload of a Flight `Handshake` message.
    ///
    /// Behavior: `AnonymousAuthenticationHandler#login(long, ByteBuffer, ..)`,
    /// `PskAuthenticationHandler#login(long, ByteBuffer, ..)`
    pub fn login_handshake(&self, payload: &[u8]) -> Option<AuthContext> {
        match self {
            AuthHandler::Anonymous => payload.is_empty().then_some(AuthContext::Anonymous),
            AuthHandler::Psk(key) => (payload == key.as_bytes()).then_some(AuthContext::SuperUser),
        }
    }

    /// Login with the part of an `authorization` header value after the auth type.
    ///
    /// Behavior: `AnonymousAuthenticationHandler#login(String, ..)`,
    /// `PskAuthenticationHandler#login(String, ..)`
    pub fn login_header(&self, payload: &str) -> Option<AuthContext> {
        self.login_handshake(payload.as_bytes())
    }
}

/// The configured handlers, looked up by auth type.
///
/// Behavior: `io.deephaven.server.auth.AuthContextModule#initializeAuthHandlers`
#[derive(Clone, Debug, Default)]
pub struct AuthHandlers(Vec<AuthHandler>);

impl AuthHandlers {
    /// Fails if two handlers have the same auth type.
    pub fn new(handlers: Vec<AuthHandler>) -> Result<Self, String> {
        for (i, handler) in handlers.iter().enumerate() {
            if handlers[..i]
                .iter()
                .any(|h| h.auth_type() == handler.auth_type())
            {
                return Err(format!(
                    "Multiple authentication handlers registered for type {}",
                    handler.auth_type()
                ));
            }
        }
        Ok(Self(handlers))
    }

    pub fn get(&self, auth_type: &str) -> Option<&AuthHandler> {
        self.0.iter().find(|h| h.auth_type() == auth_type)
    }

    pub fn iter(&self) -> impl Iterator<Item = &AuthHandler> {
        self.0.iter()
    }
}

/// A random key in the form Java generates when none is configured: a non-negative 63-bit
/// number in base 36.
///
/// Behavior: the static initializer of
/// `io.deephaven.authentication.psk.PskAuthenticationHandler`
pub fn random_psk() -> String {
    let mut n = (uuid::Uuid::new_v4().as_u128() as u64) >> 1;
    let mut digits = Vec::new();
    loop {
        digits.push(char::from_digit((n % 36) as u32, 36).expect("digit below 36"));
        n /= 36;
        if n == 0 {
            break;
        }
    }
    digits.iter().rev().collect()
}

/// The status of a call whose credentials were refused.
///
/// Behavior: `SessionServiceGrpcImpl.SessionServiceInterceptor#AUTHENTICATION_DETAILS_INVALID`
pub fn authentication_details_invalid() -> Status {
    Status::unauthenticated("Authentication details invalid")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anonymous_accepts_only_an_empty_payload() {
        let handler = AuthHandler::Anonymous;
        assert_eq!(handler.login_header(""), Some(AuthContext::Anonymous));
        assert_eq!(handler.login_header("x"), None);
        assert_eq!(handler.login_handshake(b""), Some(AuthContext::Anonymous));
        assert_eq!(handler.login_handshake(b" "), None);
    }

    #[test]
    fn psk_accepts_only_the_exact_key() {
        let handler = AuthHandler::psk("  s3cret \n").unwrap();
        assert_eq!(handler, AuthHandler::Psk("s3cret".to_string()));
        assert_eq!(handler.login_header("s3cret"), Some(AuthContext::SuperUser));
        assert_eq!(
            handler.login_handshake(b"s3cret"),
            Some(AuthContext::SuperUser)
        );
        for wrong in ["", "s3cre", "s3cret ", "S3CRET", "s3crett"] {
            assert_eq!(handler.login_header(wrong), None, "{wrong:?}");
            assert_eq!(handler.login_handshake(wrong.as_bytes()), None, "{wrong:?}");
        }
    }

    #[test]
    fn psk_rejects_empty_and_non_ascii_keys() {
        assert!(AuthHandler::psk("   ").is_err());
        assert!(AuthHandler::psk("clé").is_err());
    }

    #[test]
    fn handlers_are_found_by_type_and_types_are_unique() {
        let handlers =
            AuthHandlers::new(vec![AuthHandler::Anonymous, AuthHandler::psk("k").unwrap()])
                .unwrap();
        assert_eq!(handlers.get("Anonymous"), Some(&AuthHandler::Anonymous));
        assert!(matches!(
            handlers.get(PSK_AUTH_TYPE),
            Some(AuthHandler::Psk(_))
        ));
        assert_eq!(handlers.get("Basic"), None);
        assert!(AuthHandlers::new(vec![
            AuthHandler::psk("a").unwrap(),
            AuthHandler::psk("b").unwrap()
        ])
        .is_err());
    }

    #[test]
    fn random_psk_is_lowercase_base_36() {
        let key = random_psk();
        assert!(!key.is_empty() && key.len() <= 13, "{key}");
        assert!(
            key.chars()
                .all(|c| c.is_ascii_digit() || c.is_ascii_lowercase()),
            "{key}"
        );
        assert!(AuthHandler::psk(&key).is_ok());
    }
}
