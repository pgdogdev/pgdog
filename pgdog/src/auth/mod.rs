//! PostgreSQL authentication mechanisms.

pub mod error;
pub mod md5;
pub mod scram;

pub(crate) use error::Error;
pub(crate) use md5::Client;
