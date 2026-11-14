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
//! A [`Scope`] holds the variables that tickets of the form `s/<name>` name. Nothing lists
//! them yet: Java lists the variables of its script session, and this server has none.
//!
//! [`demo`] is a built-in application with the two tables of the web IDE acceptance test.

pub mod demo;
mod fields;
mod grpc;
mod resolver;
mod scope;

#[cfg(test)]
mod tests;

pub use fields::{Application, ApplicationError, Applications, FieldUpdates};
pub use grpc::ApplicationGrpc;
pub use resolver::{application_field_ticket, ApplicationTicketResolver, APPLICATION_TICKET_ROUTE};
pub use scope::{scope_ticket, Scope, ScopeTicketResolver, SCOPE_TICKET_ROUTE};
