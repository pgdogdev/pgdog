//! Administer the pooler.

use async_trait::async_trait;

use crate::net::messages::Message;

pub mod backend;
pub mod error;
pub mod parser;
pub mod pause;
pub mod prelude;

pub use error::Error;

#[async_trait]
pub trait Command: Sized {
    async fn execute(&self) -> Result<Vec<Message>, Error>;
    fn name(&self) -> String;
    fn parse(sql: &str) -> Result<Self, Error>;
}
