use tokio::time::timeout;

use crate::{
    net::{Message, Protocol, ProtocolMessage},
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
        // Check for cross-shard quries.
        if context.cross_shard_disabled && route.is_cross_shard() {
            let bytes_sent = context
                .stream
                .error(
                    ErrorResponse::cross_shard_disabled(),
                    context.in_transaction(),
                )
                .await?;
            self.stats.sent(bytes_sent);
            return Ok(());
        }

        if !self.connect(context, route).await? {
            return Ok(());
        }

        // We need to run a query now.
        if context.buffer.executable() {
            if let Some(begin_stmt) = self.begin_stmt.take() {
                self.backend.execute(begin_stmt.query()).await?;
            }
        }

        // Set response format.
        for msg in context.buffer.iter() {
            if let ProtocolMessage::Bind(bind) = msg {
                self.backend.bind(bind)?
            }
        }

        self.backend
            .handle_buffer(context.buffer, &mut self.router, self.streaming)
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
        let message = message.backend();
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
            // TODO: This is messed up.
            //
            // 1. We're ignoring server-set transaction state. Client gets a ReadyForQuery with transaction state set to Idle even
            // if they sent a BEGIN statement to us already.
            // 2. We're sending non-data fetching statements to the server without starting a transacation, e.g. Parse, Describe, Sync.
            // 3. We're confusing the hell out of pretty much anyone reading this. I wrote the damn thing and I'm still confused.
            let in_transaction = message.in_transaction() || self.begin_stmt.is_some();
            if !in_transaction {
                context.transaction = None;
            }

            self.stats.idle(context.in_transaction());

            if !context.in_transaction() {
                self.stats.transaction();
            }
        }

        self.stats.sent(message.len());

        if self.backend.done() {
            let changed_params = self.backend.changed_params();

            // Release the connection back into the pool before flushing data to client.
            // Flushing can take a minute and we don't want to block the connection from being reused.
            if self.backend.transaction_mode() {
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
                self.comms.update_params(&context.params);
            }
        }

        if flush {
            context.stream.send_flush(&message).await?;
        } else {
            context.stream.send(&message).await?;
        }

        Ok(())
    }
}
