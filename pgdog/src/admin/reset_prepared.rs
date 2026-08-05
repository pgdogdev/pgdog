//! RESET PREPARED.
use crate::frontend::prepared_statements::PreparedStatements;

use super::prelude::*;

/// The admin console spelling of this command. The limit-0 warning quotes
/// it, and a parser test keeps the advice parseable.
pub(super) const RESET_PREPARED: &str = "RESET PREPARED";

pub struct ResetPrepared;

#[async_trait]
impl Command for ResetPrepared {
    fn name(&self) -> String {
        RESET_PREPARED.into()
    }

    fn parse(_: &str) -> Result<Self, Error> {
        Ok(Self)
    }

    async fn execute(&self) -> Result<Vec<Message>, Error> {
        // Explicit 0: drop everything not in use, whatever the configured
        // limit is. With the default (unlimited) limit this would otherwise
        // be a no-op.
        PreparedStatements::global().write().close_unused(0);
        Ok(vec![])
    }
}
