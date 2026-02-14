use crate::net::{BackendKeyData, CommandComplete, Protocol, ReadyForQuery};

use super::*;

impl QueryEngine {
    pub(super) async fn listen(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        channel: &str,
        shard: Shard,
    ) -> Result<(), Error> {
        self.backend.listen(channel, shard).await?;
        self.command_complete(context, "LISTEN").await?;

        Ok(())
    }

    pub(super) async fn notify(
        &mut self,
        context: &mut QueryEngineContext<'_>,
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
        self.command_complete(context, "NOTIFY").await?;
        Ok(())
    }

    pub(super) async fn unlisten(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        channel: &str,
    ) -> Result<(), Error> {
        self.backend.unlisten(channel);
        self.command_complete(context, "UNLISTEN").await?;
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

    async fn command_complete(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        command: &str,
    ) -> Result<(), Error> {
        let bytes_sent = context
            .stream
            .send_many(&[
                CommandComplete::new(command)
                    .message()?
                    .backend(BackendKeyData::default()),
                ReadyForQuery::in_transaction(context.in_transaction()).message()?,
            ])
            .await?;

        self.stats.sent(bytes_sent);

        Ok(())
    }
}
