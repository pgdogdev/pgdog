use crate::net::{CommandComplete, Protocol, ReadyForQuery};

use super::*;

impl QueryEngine {
    /// Handle DISCARD. Postgres `DISCARD ALL` deallocates prepared statements;
    /// PgDog intercepts DISCARD, so the client's cache and global use counts
    /// must be updated here instead of waiting for disconnect.
    pub(super) async fn discard(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        target: DiscardTarget,
        extended: bool,
    ) -> Result<(), Error> {
        let _extended = extended;
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
