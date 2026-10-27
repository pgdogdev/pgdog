use crate::frontend::client::{TransactionType, transaction_type::Transaction};

use super::fake::{FakeCommand, FakeResponse};
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
        context.transaction = Some(Transaction::started_at(
            transaction_type,
            context.statement_start,
        ));
        context.params.begin();

        self.backend
            .start_transaction(transaction_type.read_only(), begin)?;

        if self.backend.connected() {
            self.execute(context, client_request, None).await?;
        } else {
            self.fake_command_response(
                context,
                &client_request.messages,
                &FakeResponse::new([FakeCommand::new("BEGIN")]),
            )
            .await?;
        }

        Ok(())
    }
}
