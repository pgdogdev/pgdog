use crate::net::{CommandComplete, NoticeResponse, Protocol, ReadyForQuery};

use super::*;

impl QueryEngine {
    pub(super) async fn end_not_connected(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        client_request: &ClientRequest,
        rollback: bool,
        extended: bool,
    ) -> Result<(), Error> {
        let executable = client_request.is_executable();

        let bytes_sent = if extended {
            self.extended_transaction_reply(
                context,
                &client_request.messages,
                !executable && context.in_transaction(), // Only tell client we are ending transaction if we actually are.
                rollback,
            )
            .await?
        } else {
            let cmd = if rollback {
                CommandComplete::new_rollback()
            } else {
                CommandComplete::new_commit()
            };
            let mut messages = if !context.in_transaction() {
                vec![NoticeResponse::from(ErrorResponse::no_transaction()).message()]
            } else {
                vec![]
            };
            messages.push(cmd.message());

            if context.pipeline.is_done() || !context.pipeline.is_simple() {
                messages.push(ReadyForQuery::idle().message());
            }

            context.stream.send_many(&messages).await?
        };

        self.stats.sent(bytes_sent);
        if executable {
            self.backend.end_transaction();
            context.transaction = None; // Clear transaction state
        }

        if rollback {
            self.notify_buffer.clear();
        }

        Ok(())
    }

    pub(super) async fn end_connected(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        // FIXME(sage): Remove mut
        client_request: &mut ClientRequest,
        rollback: bool,
        extended: bool,
    ) -> Result<(), Error> {
        self.backend.transaction_params_hook(rollback);
        let cluster = self.backend.cluster()?;

        // If we experienced an error and client
        // tries to commit transaction anyway,
        // we rollback to prevent cross-shard inconsistencies.
        if context.in_error() && !rollback {
            self.temp_tables.finish_transaction(true);
            self.backend.execute("ROLLBACK").await?;

            // Update stats.
            self.stats.query();
            self.stats.transaction(true);

            // Disconnect from servers.
            self.cleanup_backend(context).await?;

            // Tell client we finished the transaction.
            self.end_not_connected(context, client_request, true, extended)
                .await?;

            return Ok(());
        }

        // 2pc is used for cross-shard writes and is not needed for rollbacks.
        let two_pc = cluster.two_pc_enabled()
            && client_request.route().is_write()
            && !rollback
            && context.transaction().map(|t| t.write()).unwrap_or(false)
            && self.backend.connected_servers() > 1;

        self.temp_tables.finish_transaction(rollback);

        if two_pc {
            self.end_two_pc(false).await?;

            // Update stats.
            self.stats.query();
            self.stats.transaction(true);

            // Disconnect from servers.
            self.cleanup_backend(context).await?;

            // Tell client we finished the transaction.
            self.end_not_connected(context, client_request, false, extended)
                .await?;
        } else {
            if rollback {
                self.notify_buffer.clear();
            }
            context.rollback = rollback;
            self.execute(context, client_request, None).await?;
        }

        Ok(())
    }

    pub(super) async fn end_two_pc(&mut self, rollback: bool) -> Result<(), Error> {
        let cluster = self.backend.cluster()?;

        if rollback {
            self.backend.execute("ROLLBACK").await?;
            self.backend.end_transaction();
            return Ok(());
        }

        let identifier = cluster.identifier();
        let transaction = self.two_pc.transaction();

        // If interrupted here, the transaction must be rolled back.
        let _guard_phase_1 = self.two_pc.phase_one(&identifier).await?;
        self.backend
            .two_pc(transaction, TwoPcPhase::Phase1, false)
            .await?;

        debug!("[2pc] phase 1 complete");

        // If interrupted here, the transaction must be committed.
        let _guard_phase_2 = self.two_pc.phase_two(&identifier).await?;
        self.backend
            .two_pc(transaction, TwoPcPhase::Phase2, false)
            .await?;

        debug!("[2pc] phase 2 complete");

        // Remove transaction from 2pc state manager.
        self.two_pc.done().await?;
        self.backend.end_transaction();

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::load_test;
    use crate::frontend::client::{Transaction, TransactionType};
    use crate::net::{Query, Stream};

    #[tokio::test]
    async fn test_transaction_state_not_cleared() {
        load_test();

        // Create a test client with DevNull stream (doesn't require real I/O)
        let mut client =
            crate::frontend::Client::new_test(Stream::dev_null(), Parameters::default());
        client.transaction = Some(Transaction::new(TransactionType::ReadWrite));

        // Create a default query engine (avoids backend connection)
        let mut engine = QueryEngine::from_client(&client).unwrap();
        // state copied from client
        let (mut context, client_request) = QueryEngineContext::new(&mut client);
        client_request.messages.push(Query::new("COMMIT").into());

        let result = engine
            .end_not_connected(&mut context, client_request, false, false)
            .await;
        assert!(result.is_ok(), "end_transaction should succeed");

        assert_eq!(
            context
                .transaction
                .map(|transaction| transaction.transaction_type()),
            None,
            "transaction state should be cleared, but is {:?}",
            context
                .transaction
                .map(|transaction| transaction.transaction_type())
        );
    }
}
