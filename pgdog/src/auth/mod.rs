//! PostgreSQL authentication mechanisms.

pub mod auth_result;
pub mod error;
pub mod jwt;
pub mod md5;
pub mod scram;

pub use auth_result::AuthResult;
pub use error::Error;
pub use jwt::{Claims, JwtError, JwtValidator};
pub use md5::Client;
