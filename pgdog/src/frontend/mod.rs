//! pgDog frontend manages connections to clients.

pub mod buffer;
pub mod client;
pub mod comms;
pub mod connected_client;
pub mod error;
pub mod listener;
pub mod prepared_statements;
#[cfg(debug_assertions)]
pub mod query_logger;
pub mod router;
pub mod stats;

pub use buffer::Buffer;
pub use client::Client;
pub use comms::Comms;
pub use connected_client::ConnectedClient;
pub use error::Error;
pub use prepared_statements::{PreparedStatements, Rewrite};
#[cfg(debug_assertions)]
pub use query_logger::QueryLogger;
pub use router::{Command, Router};
pub use router::{RouterContext, SearchPath};
pub use stats::Stats;
