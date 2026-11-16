//! Applications: values the server itself holds, published to every client as named fields.
//!
//! An application is how a table exists before any client connects. The process creates an
//! [`Application`] in the server's [`Applications`] and sets fields on it:
//!
//! ```text
//! let app = state.applications.create("my_app", "My application")?;
//! app.set_field("trades", ExportValue::Table(table))?;
//! ```
//!
//! From then on:
//!
//! - `ApplicationService/ListFields` lists the field to every client, and tells the clients
//!   that are already listening when a field is set, replaced or removed ([`ApplicationGrpc`]).
//! - The ticket `a/my_app/f/trades` resolves to the value wherever a ticket is accepted, for
//!   example as the source of `TableService/FetchTable` ([`ApplicationTicketResolver`]).
//!
//! A [`Scope`] holds the variables that tickets of the form `s/<name>` name and that a
//! formula may name ([`ScopedCompiler`]). On a server with a script session they are the
//! session's variables ([`ScopeHolder`]), and the console lists those a client can open
//! under the application id `scope`.
//!
//! [`script`] starts the applications that a directory of `.app` files describes: scripts
//! that the script session runs before the server accepts clients, and that set the fields
//! of their application through `deephaven.appmode`.
//!
//! [`demo`] is a built-in application with the two tables of the web IDE acceptance test,
//! for a server without a script session. `oracle/ide/app.d` is the same application as
//! scripts, which this server and the Java reference both load.

pub mod demo;
mod fields;
mod grpc;
mod properties;
mod resolver;
mod scope;
pub mod script;

#[cfg(test)]
mod script_tests;
#[cfg(test)]
mod tests;

pub use fields::{Application, ApplicationError, Applications, FieldUpdates};
pub use grpc::ApplicationGrpc;
pub use resolver::{application_field_ticket, ApplicationTicketResolver, APPLICATION_TICKET_ROUTE};
pub use scope::{
    item_function, item_function_parts, scope_ticket, FormulaValue, Scope, ScopeHolder,
    ScopeTicketResolver, ScopedCompiler, SCOPE_DESCRIPTOR_ROUTE, SCOPE_TICKET_ROUTE,
};
