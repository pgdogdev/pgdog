use super::fake::FakeResponse;
use crate::frontend::{
    client::Transaction, client::TransactionType, router::parameter_hints::PGDOG_PIN,
};

use super::*;

impl QueryEngine {
    /// Handle DISCARD commands whose session state PgDog tracks.
    pub(super) async fn discard(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        // FIXME(sage): Remove mut
        client_request: &mut ClientRequest,
        target: DiscardTarget,
        extended: bool,
    ) -> Result<(), Error> {
        let _extended = extended;

        match target {
            DiscardTarget::All if context.in_transaction() => {
                context.transaction = Some(Transaction::new(
                    match context
                        .transaction
                        .map(|transaction| transaction.transaction_type())
                    {
                        Some(TransactionType::ReadOnly | TransactionType::ErrorReadOnly) => {
                            TransactionType::ErrorReadOnly
                        }
                        _ => TransactionType::ErrorReadWrite,
                    },
                ));
                self.error_response(
                    context,
                    client_request,
                    ErrorResponse::discard_all_in_transaction(),
                )
                .await?;
                return Ok(());
            }
            DiscardTarget::Temp if self.backend.connected() => {
                self.execute(context, client_request, None).await?;
                if !context.in_error() {
                    self.temp_tables.discard(context.in_transaction());
                    self.check_lock();
                    // execute() cleaned up before temp tracking was cleared.
                    // Try again now that the backend is unpinned.
                    self.cleanup_backend(context).await?;
                }
                return Ok(());
            }
            DiscardTarget::All => {
                if self.backend.connected() {
                    self.backend
                        .execute("SELECT pg_advisory_unlock_all()")
                        .await?;
                }
                self.advisory_locks.clear();
                context.prepared_statements.close_all();
                self.backend.unlisten_all();
                self.reset_session_params(context);
                self.check_lock();
                self.cleanup_backend(context).await?;
            }
            DiscardTarget::Plans | DiscardTarget::Sequences | DiscardTarget::Temp => {}
        }

        self.fake_command_response(
            context,
            &client_request.messages,
            &FakeResponse::command("DISCARD"),
        )
        .await?;
        Ok(())
    }

    fn reset_session_params(&mut self, context: &mut QueryEngineContext<'_>) {
        context.params.restore_startup(context.startup_params);
        self.manual_lock = context
            .params
            .get(PGDOG_PIN)
            .and_then(|value| value.as_str())
            .map(|value| matches!(value, "true" | "t"))
            .unwrap_or_default();
        self.comms.update_params(context.params);
    }
}
