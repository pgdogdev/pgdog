use crate::frontend::{client::TransactionType, router::parameter_hints::PGDOG_PIN};
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
            DiscardTarget::All if context.in_transaction() => {
                context.transaction = Some(match context.transaction {
                    Some(TransactionType::ReadOnly | TransactionType::ErrorReadOnly) => {
                        TransactionType::ErrorReadOnly
                    }
                    _ => TransactionType::ErrorReadWrite,
                });
                self.error_response(context, ErrorResponse::discard_all_in_transaction())
                    .await?;
                return Ok(());
            }
            DiscardTarget::Temp if self.backend.connected() => {
                self.execute(context, None).await?;
                if !context.in_error() {
                    self.temp_tables.discard(context.in_transaction());
                    self.check_lock();
                    // execute() cleaned up before temp tracking was cleared.
                    // Try again now that the backend is unpinned.
                    self.cleanup_backend(context)?;
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
                self.reset_session_params(context).await?;
                self.check_lock();
            }
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

    async fn reset_session_params(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        context.params.restore_startup(context.startup_params);
        self.manual_lock = context
            .params
            .get(PGDOG_PIN)
            .and_then(|value| value.as_str())
            .map(|value| matches!(value, "true" | "t"))
            .unwrap_or_default();
        self.comms.update_params(context.params);

        // Parameters the client SET while holding this server were sent straight
        // through, and `link_client` only replays what it knows diverged. Reset the
        // server so the startup values are re-applied onto a clean session.
        if self.backend.connected() {
            self.backend.execute("RESET ALL").await?;
            self.backend
                .link_client(context.id, context.params, None)
                .await?;
        }

        Ok(())
    }
}
