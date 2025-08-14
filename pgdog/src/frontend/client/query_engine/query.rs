use tokio::time::timeout;

use crate::{
    net::{Message, Protocol, ProtocolMessage},
    state::State,
};

use tracing::debug;

use super::*;

impl QueryEngine {
    /// Handle query from client.
    pub(super) async fn query(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        route: &Route,
    ) -> Result<(), Error> {
        if !self.connect(context, route).await? {
            return Ok(());
        }

        // We need to run a query now.
        if context.buffer.executable() {
            if let Some(begin_stmt) = self.begin_stmt.take() {
                context.backend.execute(begin_stmt.query()).await?;
            }
        }

        // Set response format.
        for msg in context.buffer.iter() {
            if let ProtocolMessage::Bind(bind) = msg {
                context.backend.bind(bind)?
            }
        }

        context
            .backend
            .handle_buffer(context.buffer, &mut self.router, self.streaming)
            .await?;

        while context.backend.has_more_messages() && !context.backend.copy_mode() && !self.streaming
        {
            let message = timeout(
                context.timeouts.query_timeout(&State::Active),
                context.backend.read(),
            )
            .await??;
            self.server_message(context, message).await?;
        }

        Ok(())
    }

    async fn server_message(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        message: Message,
    ) -> Result<(), Error> {
        self.streaming = message.streaming();

        let code = message.code();
        let message = message.backend();
        let has_more_messages = context.backend.has_more_messages();

        // Messages that we need to send to the client immediately.
        // ReadyForQuery (B) | CopyInResponse (B) | ErrorResponse(B) | NoticeResponse(B) | NotificationResponse (B)
        let flush = matches!(code, 'Z' | 'G' | 'E' | 'N' | 'A')
            || !has_more_messages
            || message.streaming();

        // Server finished executing a query.
        // ReadyForQuery (B)
        if code == 'Z' {
            context.stats.query();
            context.in_transaction = message.in_transaction();
            context.stats.idle(context.in_transaction);

            // Flush mirrors.
            if !context.in_transaction {
                context.backend.mirror_flush();
            }
        }

        context.stats.sent(message.len());

        if context.backend.done() {
            let changed_params = context.backend.changed_params();

            // Release the connection back into the pool before flushing data to client.
            // Flushing can take a minute and we don't want to block the connection from being reused.
            if context.backend.transaction_mode() {
                context.backend.disconnect();
            }

            context.stats.transaction();
            self.router.reset();

            debug!(
                "transaction finished [{:.3}ms]",
                context.stats.last_transaction_time.as_secs_f64() * 1000.0
            );

            // Update client params with values
            // sent from the server using ParameterStatus(B) messages.
            if !changed_params.is_empty() {
                for (name, value) in changed_params.iter() {
                    debug!("setting client's \"{}\" to {}", name, value);
                    context.params.insert(name.clone(), value.clone());
                }
                context.comms.update_params(&context.params);
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
