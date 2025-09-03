use crate::net::{CommandComplete, NoticeResponse, Protocol, ReadyForQuery};

use super::*;

impl QueryEngine {
    pub(super) async fn end_transaction(
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
        let result = engine.end_transaction(&mut context, false).await;
        assert!(result.is_ok(), "end_transaction should succeed");

        assert_eq!(
            context.transaction, None,
            "Transaction state should be None, but is {:?}",
            context.transaction
        );
    }
}
