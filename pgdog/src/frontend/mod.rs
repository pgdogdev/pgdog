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

pub use buffered_query::BufferedQuery;
pub use client::Client;
pub use client_request::ClientRequest;
pub use comms::{ClientComms, Comms};
pub use connected_client::ConnectedClient;
pub(crate) use error::Error;
pub use prepared_statements::{PreparedStatements, Rewrite};
#[cfg(debug_assertions)]
pub use query_logger::QueryLogger;
pub use router::{Command, Router, SetParam};
pub use router::{RouterContext, SearchPath};
pub use stats::Stats;
