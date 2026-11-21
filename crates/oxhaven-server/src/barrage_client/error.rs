//! Why a call of the Barrage client failed.

use oxhaven_contracts::table::TableFailure;
use tonic::Status;

/// A failure of the client: of the connection, of a call the server refused, of data the
/// server sent, or of something this client does not read.
#[derive(Clone, Debug)]
pub enum ClientError {
    /// The configuration cannot be used: a bad host, header, authority or root certificate.
    Config(String),
    /// The connection could not be made, or its TLS handshake failed.
    Connect(String),
    /// The server answered a call with this status.
    Status(Status),
    /// The server sent something that is not what the protocol says.
    Protocol(String),
    /// The table has a column, or the server uses an encoding, that this client does not
    /// read. The text names the Java reader that would.
    NotReadable(String),
    /// The session is closed.
    Closed,
    /// The table failed while the subscription was being set up.
    Table(TableFailure),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Config(problem) => write!(f, "invalid client configuration: {problem}"),
            ClientError::Connect(problem) => write!(f, "cannot connect: {problem}"),
            ClientError::Status(status) => {
                write!(f, "{:?}: {}", status.code(), status.message())
            }
            ClientError::Protocol(problem) => write!(f, "unexpected Barrage data: {problem}"),
            ClientError::NotReadable(problem) => f.write_str(problem),
            ClientError::Closed => f.write_str("the Barrage session is closed"),
            ClientError::Table(failure) => f.write_str(&failure.message),
        }
    }
}

impl std::error::Error for ClientError {}

impl From<Status> for ClientError {
    fn from(status: Status) -> Self {
        ClientError::Status(status)
    }
}

impl From<TableFailure> for ClientError {
    fn from(failure: TableFailure) -> Self {
        ClientError::Table(failure)
    }
}

pub(super) fn protocol(problem: impl std::fmt::Display) -> ClientError {
    ClientError::Protocol(problem.to_string())
}

/// Something Java's client reads and this one does not, named by the Java code that does.
pub(super) fn not_ported(java: &str, what: impl std::fmt::Display) -> ClientError {
    ClientError::NotReadable(format!("not ported yet: {java}: {what}"))
}
