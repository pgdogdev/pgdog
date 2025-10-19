use crate::net::{CommandComplete, NoticeResponse, Protocol, ReadyForQuery};

use super::*;

impl QueryEngine {
    pub(super) async fn end_not_connected(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        rollback: bool,
        extended: bool,
    ) -> Result<(), Error> {
        let bytes_sent = if extended {
            self.extended_transaction_reply(context, false, rollback)
                .await?
        } else {
            let cmd = if rollback {
                CommandComplete::new_rollback()
            } else {
                CommandComplete::new_commit()
            };
            let mut messages = if !context.in_transaction() {
                vec![NoticeResponse::from(ErrorResponse::no_transaction()).message()?]
            } else {
                vec![]
            };
            messages.push(cmd.message()?.backend());
            messages.push(ReadyForQuery::idle().message()?);

            context.stream.send_many(&messages).await?
        };

        self.stats.sent(bytes_sent);
        self.begin_stmt = None;
        context.transaction = None; // Clear transaction state

        if rollback {
            self.notify_buffer.clear();
        }

        Ok(())
    }

    pub(super) async fn end_connected(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        route: &Route,
        rollback: bool,
        extended: bool,
    ) -> Result<(), Error> {
        let cluster = self.backend.cluster()?;

        // If we experienced an error and client
        // tries to commit transaction anyway,
        // we rollback to prevent cross-shard inconsistencies.
        if context.in_error() && !rollback {
            self.backend.execute("ROLLBACK").await?;

            // Update stats.
            self.stats.query();
            self.stats.transaction(true);

            // Disconnect from servers.
            self.cleanup_backend(context);

            // Tell client we finished the transaction.
            self.end_not_connected(context, true, extended).await?;

            return Ok(());
        }

        // 2pc is used only for writes and is not needed for rollbacks.
        let two_pc = cluster.two_pc_enabled()
            && route.is_write()
            && !rollback
            && context.transaction().map(|t| t.write()).unwrap_or(false);

        if two_pc {
            self.end_two_pc(false).await?;

            // Update stats.
            self.stats.query();
            self.stats.transaction(true);

            // Disconnect from servers.
            self.cleanup_backend(context);

            // Tell client we finished the transaction.
            self.end_not_connected(context, false, extended).await?;
        } else {
            if rollback {
                self.notify_buffer.clear();
            }
            context.rollback = rollback;
            self.execute(context, route).await?;
        }

        Ok(())
    }

    pub(super) async fn end_two_pc(&mut self, rollback: bool) -> Result<(), Error> {
        let cluster = self.backend.cluster()?;

        if rollback {
            self.backend.execute("ROLLBACK").await?;
            return Ok(());
        }

        let identifier = cluster.identifier();
        let name = self.two_pc.transaction().to_string();

        // If interrupted here, the transaction must be rolled back.
        let _guard_phase_1 = self.two_pc.phase_one(&identifier).await?;
        self.backend.two_pc(&name, TwoPcPhase::Phase1).await?;

        debug!("[2pc] phase 1 complete");

        // If interrupted here, the transaction must be committed.
        let _guard_phase_2 = self.two_pc.phase_two(&identifier).await?;
        self.backend.two_pc(&name, TwoPcPhase::Phase2).await?;

        debug!("[2pc] phase 2 complete");

        // Remove transaction from 2pc state manager.
        self.two_pc.done().await?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::frontend::client::TransactionType;
    use crate::net::Stream;

    #[tokio::test]
    async fn test_transaction_state_not_cleared() {
        // Create a test client with DevNull stream (doesn't require real I/O)
        let mut client = crate::frontend::Client::new_test(
            Stream::dev_null(),
            std::net::SocketAddr::from(([127, 0, 0, 1], 1234)),
        );
        client.transaction = Some(TransactionType::ReadWrite);

        // Create a default query engine (avoids backend connection)
        let mut engine = QueryEngine::default();
        // state copied from client
        let mut context = QueryEngineContext::new(&mut client);
        let result = engine.end_not_connected(&mut context, false, false).await;
        assert!(result.is_ok(), "end_transaction should succeed");

        assert_eq!(
            context.transaction, None,
            "Transaction state should be None, but is {:?}",
            context.transaction
        );
    }
}
