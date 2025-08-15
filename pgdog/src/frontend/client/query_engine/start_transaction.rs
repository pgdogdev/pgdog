use crate::net::{CommandComplete, Protocol, ReadyForQuery};

use super::*;

impl QueryEngine {
    /// BEGIN
    pub(super) async fn start_transaction(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        begin: BufferedQuery,
    ) -> Result<(), Error> {
        context.in_transaction = true;

        let bytes_sent = context
            .stream
            .send_many(&[
                CommandComplete::new_begin().message()?,
                ReadyForQuery::in_transaction(context.in_transaction).message()?,
            ])
            .await?;

        self.stats.sent(bytes_sent);
        self.begin_stmt = Some(begin);

        Ok(())
    }
}
