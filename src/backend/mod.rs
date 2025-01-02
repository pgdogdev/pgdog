//! pgDog backend managers connections to PostgreSQL.

pub mod error;
pub mod server;

pub use error::Error;
pub use server::Server;

pub mod pool;
