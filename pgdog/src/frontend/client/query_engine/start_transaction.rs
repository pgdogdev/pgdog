use crate::frontend::client::{
    TransactionType,
    protocol::{Commands, Statement},
    transaction_type::Transaction,
};

use super::*;

impl QueryEngine {
    /// BEGIN
    pub(super) async fn start_transaction(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        // FIXME(sage): Remove mut
        client_request: &mut ClientRequest,
        begin: BufferedQuery,
        transaction_type: TransactionType,
    ) -> Result<(), Error> {
        let previous_transaction = context.transaction();
        context.transaction = Some(Transaction::new(transaction_type));

        self.backend
            .start_transaction(transaction_type.read_only(), begin)?;

        if self.backend.connected() {
            self.execute(context, client_request, None).await?;
        } else {
            let responder = Commands::builder()
                .maybe_transaction(previous_transaction)
                .pipeline(&context.pipeline)
                .statement(Statement::Begin)
                .build();
            let (bytes_sent, _) = responder
                .send_reply(client_request.messages.as_slice(), context.stream)
                .await?;

            self.stats.sent(bytes_sent);
        }

        Ok(())
    }
}
