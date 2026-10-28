use super::*;
use crate::backend::Error as BackendError;
use crate::backend::pool::Error as PoolError;
use crate::frontend::client::query_engine::fake::FakeResponse;
use crate::net::ProtocolMessage;

impl QueryEngine {
    pub(super) async fn listen(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        client_messages: &[ProtocolMessage],
        channel: &str,
        shard: Shard,
    ) -> Result<(), Error> {
        let result = self.backend.listen(channel, shard).await;
        if self.pub_sub_error(context, result).await? {
            return Ok(());
        }
        self.fake_command_response(context, client_messages, &FakeResponse::command("LISTEN"))
            .await?;

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
        let result = if !context.in_transaction() {
            // Send immediately if not in transaction
            self.backend.notify(channel, payload, shard.clone()).await
        } else if config().pub_sub_enabled() {
            // Buffer the NOTIFY command if we're in a transaction
            self.notify_buffer
                .add(channel.to_string(), payload.to_string(), shard.clone());
            Ok(())
        } else {
            // Fail now: the buffer is flushed after COMMIT has already succeeded.
            Err(BackendError::Pool(PoolError::PubSubDisabled))
        };
        if self.pub_sub_error(context, result).await? {
            return Ok(());
        }
        self.fake_command_response(context, client_messages, &FakeResponse::command("NOTIFY"))
            .await?;
        Ok(())
    }

    pub(super) async fn unlisten(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        client_messages: &[ProtocolMessage],
        channel: &str,
    ) -> Result<(), Error> {
        self.backend.unlisten(channel);
        self.fake_command_response(context, client_messages, &FakeResponse::command("UNLISTEN"))
            .await?;
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

    /// Report an unsupported pub/sub request to the client as an `ERROR`,
    /// keeping the connection open. Returns `true` if an error was sent.
    async fn pub_sub_error(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        result: Result<(), BackendError>,
    ) -> Result<bool, Error> {
        match result {
            Ok(()) => Ok(false),
            Err(err) if err.is_unsupported() => {
                if let Some(transaction) = context.transaction.as_mut() {
                    transaction.set_error();
                }
                let mut error = ErrorResponse::unsupported(&err);
                error.hint = Some("set pub_sub_channel_size to enable LISTEN/NOTIFY".into());
                self.send_error_response(context, error).await?;
                Ok(true)
            }
            Err(err) => Err(err.into()),
        }
    }
}
