use crate::net::{CommandComplete, Protocol, ReadyForQuery};

use super::*;

impl QueryEngine {
    pub(super) async fn listen(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        channel: &str,
        shard: Shard,
    ) -> Result<(), Error> {
        context.backend.listen(channel, shard).await?;
        Self::command_complete(context, "LISTEN").await?;

        Ok(())
    }

    pub(super) async fn notify(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        channel: &str,
        payload: &str,
        shard: &Shard,
    ) -> Result<(), Error> {
        context
            .backend
            .notify(channel, payload, shard.clone())
            .await?;
        Self::command_complete(context, "NOTIFY").await?;
        Ok(())
    }

    pub(super) async fn unlisten(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        channel: &str,
    ) -> Result<(), Error> {
        context.backend.unlisten(channel);
        Self::command_complete(context, "UNLISTEN").await?;
        Ok(())
    }

    async fn command_complete(
        context: &mut QueryEngineContext<'_>,
        command: &str,
    ) -> Result<(), Error> {
        let bytes_sent = context
            .stream
            .send_many(&[
                CommandComplete::new(command).message()?,
                ReadyForQuery::in_transaction(context.in_transaction()).message()?,
            ])
            .await?;

        context.stats.sent(bytes_sent);

        Ok(())
    }
}
