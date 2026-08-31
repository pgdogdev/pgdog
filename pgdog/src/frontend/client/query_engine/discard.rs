use crate::net::{CommandComplete, Protocol, ReadyForQuery};

use super::*;

impl QueryEngine {
    /// Handle DISCARD. Postgres `DISCARD ALL` deallocates prepared statements;
    /// PgDog intercepts DISCARD, so the client's cache and global use counts
    /// must be updated here instead of waiting for disconnect.
    pub(super) async fn discard(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        extended: bool,
    ) -> Result<(), Error> {
        let _extended = extended;
        context.prepared_statements.close_all();
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
