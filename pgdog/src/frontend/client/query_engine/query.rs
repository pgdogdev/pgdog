use tokio::time::timeout;

use crate::{
    frontend::client::TransactionType,
    net::{
        FromBytes, Message, Protocol, ProtocolMessage, Query, ReadyForQuery, ToBytes,
        TransactionState,
    },
    state::State,
};

use tracing::debug;

use super::*;

impl QueryEngine {
    /// Handle query from client.
    pub(super) async fn execute(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        route: &Route,
    ) -> Result<(), Error> {
        // Check that we're not in a transaction error state.
        if !self.transaction_error_check(context, route).await? {
            return Ok(());
        }

        // Check if we need to do 2pc automatically
        // for single-statement writes.
        self.two_pc_check(context, route);

        // We need to run a query now.
        if let Some(begin_stmt) = self.begin_stmt.take() {
            // Connect to one shard if not sharded or to all shards
            // for a cross-shard tranasction.
            if !self.connect_transaction(context, route).await? {
                return Ok(());
            }

            self.backend.execute(begin_stmt.query()).await?;
        } else if !self.connect(context, route).await? {
            return Ok(());
        }

        // Check we can run this query.
        if !self.cross_shard_check(context, route).await? {
            return Ok(());
        }

        // Set response format.
        for msg in context.client_request.messages.iter() {
            if let ProtocolMessage::Bind(bind) = msg {
                self.backend.bind(bind)?
            }
        }

        self.backend
            .handle_client_request(context.client_request, &mut self.router, self.streaming)
            .await?;

        while self.backend.has_more_messages()
            && !self.backend.copy_mode()
            && !self.streaming
            && !self.test_mode
        {
            let message = timeout(
                context.timeouts.query_timeout(&State::Active),
                self.backend.read(),
            )
            .await??;
            self.server_message(context, message).await?;
        }

        Ok(())
    }

    pub async fn server_message(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        message: Message,
    ) -> Result<(), Error> {
        self.streaming = message.streaming();

        let code = message.code();
        let mut message = message.backend();
        let has_more_messages = self.backend.has_more_messages();

        // Messages that we need to send to the client immediately.
        // ReadyForQuery (B) | CopyInResponse (B) | ErrorResponse(B) | NoticeResponse(B) | NotificationResponse (B)
        let flush = matches!(code, 'Z' | 'G' | 'E' | 'N' | 'A')
            || !has_more_messages
            || message.streaming();

        // Server finished executing a query.
        // ReadyForQuery (B)
        if code == 'Z' {
            self.stats.query();

            let two_pc = if self.two_pc.auto() && !context.in_error() {
                self.end_two_pc().await?;
                message = ReadyForQuery::in_transaction(false).message()?;
                true
            } else {
                false
            };

            let rfq = ReadyForQuery::from_bytes(message.to_bytes()?)?;
            let state = rfq.state()?;

            match state {
                TransactionState::Error => {
                    context.transaction = Some(TransactionType::Error);
                }

                TransactionState::Idle => {
                    context.transaction = None;
                }

                TransactionState::InTrasaction => {
                    if context.transaction.is_none() {
                        // Query parser is disabled, so the server is responsible for telling us
                        // we started a transaction.
                        context.transaction = Some(TransactionType::ReadWrite);
                    }
                }
            }

            self.stats.idle(context.in_transaction());

            if !context.in_transaction() {
                self.stats.transaction(two_pc);
            }
        }

        self.stats.sent(message.len());

        // Do this before flushing, because flushing can take time.
        self.cleanup_backend(context);

        if flush {
            context.stream.send_flush(&message).await?;
        } else {
            context.stream.send(&message).await?;
        }

        Ok(())
    }

    pub(super) fn cleanup_backend(&mut self, context: &mut QueryEngineContext<'_>) {
        if self.backend.done() {
            let changed_params = self.backend.changed_params();

            // Release the connection back into the pool before flushing data to client.
            // Flushing can take a minute and we don't want to block the connection from being reused.
            if self.backend.transaction_mode() && context.requests_left == 0 {
                self.backend.disconnect();
            }

            self.router.reset();

            debug!(
                "transaction finished [{:.3}ms]",
                self.stats.last_transaction_time.as_secs_f64() * 1000.0
            );

            // Update client params with values
            // sent from the server using ParameterStatus(B) messages.
            if !changed_params.is_empty() {
                for (name, value) in changed_params.iter() {
                    debug!("setting client's \"{}\" to {}", name, value);
                    context.params.insert(name.clone(), value.clone());
                }
                self.comms.update_params(context.params);
            }
        }
    }

    // Perform cross-shard check.
    async fn cross_shard_check(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        route: &Route,
    ) -> Result<bool, Error> {
        // Check for cross-shard queries.
        if context.cross_shard_disabled.is_none() {
            context.cross_shard_disabled = Some(
                self.backend
                    .cluster()
                    .map(|c| c.cross_shard_disabled())
                    .unwrap_or_default(),
            );
        }

        let cross_shard_disabled = context.cross_shard_disabled.unwrap_or_default();

        debug!("cross-shard queries disabled: {}", cross_shard_disabled,);

        if cross_shard_disabled
            && route.is_cross_shard()
            && !context.admin
            && context.client_request.executable()
        {
            let bytes_sent = context
                .stream
                .error(
                    ErrorResponse::cross_shard_disabled(),
                    context.in_transaction(),
                )
                .await?;
            self.stats.sent(bytes_sent);

            if self.backend.connected() && self.backend.done() {
                self.backend.disconnect();
            }
            Ok(false)
        } else {
            Ok(true)
        }
    }

    fn two_pc_check(&mut self, context: &mut QueryEngineContext<'_>, route: &Route) {
        let enabled = self
            .backend
            .cluster()
            .map(|c| c.two_pc_auto_enabled())
            .unwrap_or_default();

        if enabled
            && route.should_2pc()
            && self.begin_stmt.is_none()
            && context.client_request.executable()
            && !context.in_transaction()
        {
            debug!("[2pc] enabling automatic transaction");
            self.two_pc.set_auto();
            self.begin_stmt = Some(BufferedQuery::Query(Query::new("BEGIN")));
        }
    }

    async fn transaction_error_check(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        route: &Route,
    ) -> Result<bool, Error> {
        if context.in_error() && !context.rollback && route.is_cross_shard() {
            let bytes_sent = context
                .stream
                .error(
                    ErrorResponse::in_failed_transaction(),
                    context.in_transaction(),
                )
                .await?;
            self.stats.sent(bytes_sent);

            Ok(false)
        } else {
            Ok(true)
        }
    }
}
