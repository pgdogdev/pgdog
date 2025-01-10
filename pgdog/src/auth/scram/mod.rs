//! SCRAM-SHA-256 authentication.
pub mod client;
pub mod error;
pub mod state;

pub use client::Client;
pub use error::Error;
