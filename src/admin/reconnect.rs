//! Recreate all connections to all databases.

use crate::backend::databases::reconnect;

use super::prelude::*;

/// Recreate connections.
pub struct Reconnect;

#[async_trait]
impl Command for Reconnect {
    fn name(&self) -> String {
        "RECONNECT".into()
    }

    fn parse(sql: &str) -> Result<Self, Error> {
        match sql {
            "reconnect" => Ok(Reconnect),
            _ => Err(Error::Syntax),
        }
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        reconnect();
        Ok(vec![])
    }
}
