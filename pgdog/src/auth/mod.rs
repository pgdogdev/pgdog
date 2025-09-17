//! PostgreSQL authentication mechanisms.

pub mod error;
pub mod gssapi;
pub mod md5;
pub mod scram;

pub use error::Error;
pub use md5::Client;
