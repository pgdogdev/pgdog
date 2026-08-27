//! PostgreSQL authentication mechanisms.

pub(crate) mod auth_result;
pub(crate) mod error;
pub(crate) mod md5;
pub(crate) mod scram;
pub(crate) mod vault;

pub(crate) use auth_result::AuthResult;
pub(crate) use error::Error;
