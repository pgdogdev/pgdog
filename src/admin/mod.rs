//! Administer the pooler.

use async_trait::async_trait;

use crate::net::messages::Message;

pub mod backend;
pub mod error;
pub mod parser;
pub mod pause;
pub mod prelude;

pub use error::Error;

/// All pooler commands implement this trait.
#[async_trait]
pub trait Command: Sized {
    /// Execute the command and return results to the client.
    async fn execute(&self) -> Result<Vec<Message>, Error>;
    /// Command name.
    fn name(&self) -> String;
    /// Parse SQL and construct a command handler.
    fn parse(sql: &str) -> Result<Self, Error>;
}
