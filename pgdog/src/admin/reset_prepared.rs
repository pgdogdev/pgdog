//! RESET PREPARED.
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
        // Deliberately not the configured limit: RESET clears everything
        // not in use regardless of it.
        PreparedStatements::global().write().close_unused(0);
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::messages::Parse;

    #[tokio::test]
    async fn reset_prepared_clears_released_statements() {
        let held;
        let released;
        {
            let global = PreparedStatements::global();
            let mut cache = global.write();
            held = cache.insert(&Parse::named("s", "SELECT 'rp_held'")).1;
            released = cache.insert(&Parse::named("s", "SELECT 'rp_released'")).1;
            cache.close(&released);
        }

        // Default prepared_statements_limit is unlimited;
        // the command must clear released statements anyway.
        ResetPrepared.execute().await.unwrap();

        let global = PreparedStatements::global();
        let cache = global.read();
        assert!(
            cache.parse(&released).is_none(),
            "released statement cleared"
        );
        assert!(cache.parse(&held).is_some(), "statement in use survives");
    }
}
