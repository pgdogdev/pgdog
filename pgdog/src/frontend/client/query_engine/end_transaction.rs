use crate::net::{CommandComplete, NoticeResponse, Protocol, ReadyForQuery};

use super::*;

impl QueryEngine {
    pub(super) async fn end_not_connected(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        rollback: bool,
    ) -> Result<(), Error> {
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

        let bytes_sent = context.stream.send_many(&messages).await?;
        self.stats.sent(bytes_sent);
        self.begin_stmt = None;
        context.transaction = None; // Clear transaction state

        Ok(())
    }

    pub(super) async fn end_connected(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        route: &Route,
    ) -> Result<(), Error> {
        let two_pc = self.backend.cluster()?.two_pc_enabled();
        if two_pc {
            let name = self.two_pc.name().to_owned();
            self.two_pc.phase_one().await?;
            self.backend
                .execute(format!(r#"PREPARE TRANSACTION '{}'"#, name).as_str())
                .await?;
            self.two_pc.phase_two().await?;
            self.backend
                .execute(format!(r#"COMMIT PREPARED '{}'"#, name).as_str())
                .await?;
            self.two_pc.done().await?;

            // Tell client we finished the transaction.
            self.end_not_connected(context, false).await?;

            // Update stats.
            self.stats.query();
            self.stats.transaction();

            // Disconnect from servers.
            self.cleanup_backend(context);
        } else {
            self.execute(context, route).await?;
        }

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
            Stream::DevNull,
            std::net::SocketAddr::from(([127, 0, 0, 1], 1234)),
        );
        client.transaction = Some(TransactionType::ReadWrite);

        // Create a default query engine (avoids backend connection)
        let mut engine = QueryEngine::default();
        // state copied from client
        let mut context = QueryEngineContext::new(&mut client);
        let result = engine.end_not_connected(&mut context, false).await;
        assert!(result.is_ok(), "end_transaction should succeed");

        assert_eq!(
            context.transaction, None,
            "Transaction state should be None, but is {:?}",
            context.transaction
        );
    }
}
