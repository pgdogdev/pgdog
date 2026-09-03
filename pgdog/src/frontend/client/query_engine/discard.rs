use crate::net::{CommandComplete, Protocol, ReadyForQuery};

use super::*;

impl QueryEngine {
    /// Handle DISCARD commands whose session state PgDog tracks.
    pub(super) async fn discard(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        target: DiscardTarget,
        extended: bool,
    ) -> Result<(), Error> {
        let _extended = extended;

        if target == DiscardTarget::Temp && self.backend.connected() {
            self.execute(context, None).await?;

            if !context.in_error() {
                self.discard_temp_tables(context.in_transaction());
                self.cleanup_backend(context)?;
            }

            return Ok(());
        }

        if target == DiscardTarget::All {
            context.prepared_statements.close_all();
        }
        let bytes_sent = context
            .stream
            .send_many(&[
                CommandComplete::new("DISCARD").message(),
                ReadyForQuery::in_transaction(context.in_transaction()).message(),
            ])
            .await?;
        self.stats.sent(bytes_sent);
        Ok(())
    }
}
