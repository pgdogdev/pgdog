use crate::config::PreparedStatements;

use super::*;

impl QueryEngine {
    /// Rewrite extended protocol messages.
    pub(super) fn rewrite_extended(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        for message in context.client_request.iter_mut() {
            if message.extended() {
                let level = context.prepared_statements.level;
                match (level, message.anonymous()) {
                    (PreparedStatements::ExtendedAnonymous, _)
                    | (PreparedStatements::Extended, false) => {
                        context.prepared_statements.maybe_rewrite(message)?
                    }
                    _ => (),
                }
            }
        }
        Ok(())
    }
}
