use crate::net::{CommandComplete, Protocol, ReadyForQuery};

use super::*;

impl QueryEngine {
    /// Ignore DISCARD command.
    pub(super) async fn discard(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        extended: bool,
    ) -> Result<(), Error> {
        let _extended = extended;
        let bytes_sent = context
            .stream
            .send_many(&[
                CommandComplete::new("DISCARD").message()?.backend(),
                ReadyForQuery::in_transaction(context.in_transaction()).message()?,
            ])
            .await?;
        self.stats.sent(bytes_sent);
        Ok(())
    }
}
