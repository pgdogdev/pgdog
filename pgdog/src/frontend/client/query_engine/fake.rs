use tokio::io::AsyncWriteExt;

use crate::net::{
    BindComplete, CommandComplete, ParameterDescription, ParseComplete, Protocol, ReadyForQuery,
    RowDescription,
};

use super::*;

impl QueryEngine {
    /// Respond to a command sent by the client
    /// in a way that won't make it suspicious.
    pub async fn fake_command_response(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        command: &str,
    ) -> Result<(), Error> {
        let mut sent = 0;
        for message in context.client_request.iter() {
            sent += match message.code() {
                'P' => context.stream.send(&ParseComplete).await?,
                'B' => context.stream.send(&BindComplete).await?,
                'D' => {
                    context
                        .stream
                        .send(&ParameterDescription::default())
                        .await?
                        + context.stream.send(&RowDescription::default()).await?
                }
                'E' => context.stream.send(&CommandComplete::new(command)).await?,
                'S' => {
                    context
                        .stream
                        .send(&ReadyForQuery::in_transaction(context.in_transaction()))
                        .await?
                }
                'Q' => {
                    context.stream.send(&CommandComplete::new(command)).await?
                        + context
                            .stream
                            .send(&ReadyForQuery::in_transaction(context.in_transaction()))
                            .await?
                }
                _ => 0,
            };
        }
        context.stream.flush().await?;
        self.stats.sent(sent);

        Ok(())
    }
}
