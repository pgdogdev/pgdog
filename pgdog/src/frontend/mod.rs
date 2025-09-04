//! pgDog frontend manages connections to clients.

pub mod buffered_query;
pub mod client;
pub mod client_request;
pub mod comms;
pub mod connected_client;
pub mod error;
pub mod listener;
pub mod logical_session;
pub mod logical_transaction;
pub mod prepared_statements;
#[cfg(debug_assertions)]
pub mod query_logger;
pub mod router;
pub mod stats;

pub(crate) use buffered_query::BufferedQuery;
pub(crate) use client::Client;
pub(crate) use client_request::ClientRequest;
pub(crate) use comms::Comms;
pub(crate) use connected_client::ConnectedClient;
pub use error::Error;
pub(crate) use prepared_statements::{PreparedStatements, Rewrite};
#[cfg(debug_assertions)]
pub(crate) use query_logger::QueryLogger;
pub(crate) use router::{Command, Router};
pub(crate) use router::{RouterContext, SearchPath};
pub(crate) use stats::Stats;
