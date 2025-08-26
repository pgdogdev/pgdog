use crate::{
    frontend::client::TransactionType,
    net::{CommandComplete, Protocol, ReadyForQuery},
};

use super::*;

impl QueryEngine {
    /// BEGIN
    pub(super) async fn start_transaction(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        begin: BufferedQuery,
        transaction_type: TransactionType,
    ) -> Result<(), Error> {
        context.transaction = Some(transaction_type);

        let bytes_sent = context
            .stream
            .send_many(&[
                CommandComplete::new_begin().message()?.backend(),
                ReadyForQuery::in_transaction(context.in_transaction()).message()?,
            ])
            .await?;

        self.stats.sent(bytes_sent);
        self.begin_stmt = Some(begin);

        Ok(())
    }
}
