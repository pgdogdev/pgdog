use super::*;

impl QueryEngine {
    pub(super) fn check_error(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        error: &Message,
    ) -> Result<(), Error> {
        if let Some(state) = self.pending_explain.as_mut() {
            state.annotated = true;
        }
        self.pending_explain = None;

        // If the server indicated that the schema has changed,
        // we can attempt a retry.
        context.schema_changed = self.backend.schema_changed();
        Ok(())
    }

    pub(super) fn handle_error(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        if context.schema_changed {
            if self.backend.idle() {
                if let Some(close) = context
                    .client_request
                    .parse()?
                    .map(|parse| parse.new_close())
                {}
            }
        }

        Ok(())
    }
}
