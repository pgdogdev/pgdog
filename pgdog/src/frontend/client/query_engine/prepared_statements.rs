use super::*;

impl QueryEngine {
    /// Rewrite extended protocol messages.
    pub(super) fn rewrite_extended(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        for message in context.client_request.iter_mut() {
            if message.extended() && context.prepared_statements.enabled {
                context.prepared_statements.maybe_rewrite(message)?;
            }
        }
        Ok(())
    }
}
