//! PostgreSQL authentication mechanisms.

pub mod auth_result;
pub mod error;
pub mod md5;
pub mod scram;
pub mod vault;

pub use auth_result::AuthResult;
pub use error::Error;
pub use md5::Client;
