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
            if self.backend.connected() {
                self.backend
                    .execute("SELECT pg_advisory_unlock_all()")
                    .await?;
            }

            self.advisory_locks.clear();
            context.prepared_statements.close_all();
            self.backend.unlisten_all();
            self.check_lock();
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
