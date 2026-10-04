use super::*;
use crate::{
    frontend::client::protocol::{Commands, Statement},
    net::ProtocolMessage,
};

impl QueryEngine {
    pub(super) async fn listen(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        client_messages: &[ProtocolMessage],
        channel: &str,
        shard: Shard,
    ) -> Result<(), Error> {
        self.backend.listen(channel, shard).await?;
        let responder = Commands::builder()
            .maybe_transaction(context.transaction())
            .pipeline(&context.pipeline)
            .statement(Statement::Listen)
            .build();
        let (bytes_sent, _) = responder
            .send_reply(client_messages, context.stream)
            .await?;
        self.stats.sent(bytes_sent);

        Ok(())
    }

    pub(super) async fn notify(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        client_messages: &[ProtocolMessage],
        channel: &str,
        payload: &str,
        shard: &Shard,
    ) -> Result<(), Error> {
        if context.in_transaction() {
            // Buffer the NOTIFY command if we're in a transaction
            self.notify_buffer
                .add(channel.to_string(), payload.to_string(), shard.clone());
        } else {
            // Send immediately if not in transaction
            self.backend.notify(channel, payload, shard.clone()).await?;
        }
        let responder = Commands::builder()
            .maybe_transaction(context.transaction())
            .pipeline(&context.pipeline)
            .statement(Statement::Notify)
            .build();
        let (bytes_sent, _) = responder
            .send_reply(client_messages, context.stream)
            .await?;
        self.stats.sent(bytes_sent);
        Ok(())
    }

    pub(super) async fn unlisten(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        client_messages: &[ProtocolMessage],
        channel: &str,
    ) -> Result<(), Error> {
        self.backend.unlisten(channel);
        let responder = Commands::builder()
            .maybe_transaction(context.transaction())
            .pipeline(&context.pipeline)
            .statement(Statement::Unlisten)
            .build();
        let (bytes_sent, _) = responder
            .send_reply(client_messages, context.stream)
            .await?;
        self.stats.sent(bytes_sent);
        Ok(())
    }

    pub(super) async fn flush_notify(&mut self) -> Result<(), Error> {
        for notify_cmd in self.notify_buffer.drain() {
            self.backend
                .notify(&notify_cmd.channel, &notify_cmd.payload, notify_cmd.shard)
                .await?;
        }
        Ok(())
    }
}
