use crate::frontend::{client::TransactionType, router::parameter_hints::PGDOG_PIN};
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
        if target == DiscardTarget::All && context.in_transaction() {
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

        if target == DiscardTarget::All {
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
