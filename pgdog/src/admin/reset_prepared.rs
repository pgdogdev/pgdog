//! RESET PREPARED.
use crate::config::config;
use crate::frontend::prepared_statements::PreparedStatements;

use super::prelude::*;

pub struct ResetPrepared;

#[async_trait]
impl Command for ResetPrepared {
    fn name(&self) -> String {
        "RESET PREPARED".into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        let config = config();
        PreparedStatements::global()
            .write()
            .close_unused(config.config.general.prepared_statements_limit);
        Ok(vec![])
    }
}
