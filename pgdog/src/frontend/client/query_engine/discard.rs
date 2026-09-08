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

        match target {
            DiscardTarget::Temp if self.backend.connected() => {
                self.execute(context, None).await?;

                if !context.in_error() {
                    self.temp_tables.discard(context.in_transaction());
                    self.check_lock();
                    // execute() attempted cleanup while the temporary tables were
                    // still tracked. Try again now that the backend is unpinned.
                    self.cleanup_backend(context)?;
                }

                return Ok(());
            }
            DiscardTarget::All => context.prepared_statements.close_all(),
            DiscardTarget::Plans | DiscardTarget::Sequences | DiscardTarget::Temp => {}
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
