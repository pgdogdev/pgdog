use crate::net::{Close, FromBytes, Protocol, ToBytes};

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

    /// Remove prepared statements from local cache
    /// that the client doesn't want anymore.
    pub(super) fn handle_close(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        for message in context.client_request.iter() {
            if message.code() == 'C' {
                let close = Close::from_bytes(message.to_bytes()?)?;
                if close.is_statement() {
                    context.prepared_statements.close(close.name());
                }
            }
        }

        Ok(())
    }
}
