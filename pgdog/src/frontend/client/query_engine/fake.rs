use tokio::io::AsyncWriteExt;

use crate::net::{
    BindComplete, CommandComplete, NoData, ParameterDescription, ParseComplete, Protocol,
    ProtocolMessage, ReadyForQuery, RowDescription,
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
            sent += match message {
                ProtocolMessage::Parse(_) => context.stream.send(&ParseComplete).await?,
                ProtocolMessage::Bind(_) => context.stream.send(&BindComplete).await?,
                ProtocolMessage::Describe(describe) => {
                    if describe.is_statement() {
                        context
                            .stream
                            .send(&ParameterDescription::default())
                            .await?
                            + context.stream.send(&RowDescription::default()).await?
                    } else {
                        context.stream.send(&NoData).await?
                    }
                }
                ProtocolMessage::Execute(_) => {
                    context.stream.send(&CommandComplete::new(command)).await?
                }
                ProtocolMessage::Sync(_) => {
                    context
                        .stream
                        .send(&ReadyForQuery::in_transaction(context.in_transaction()))
                        .await?
                }
                ProtocolMessage::Query(_) => {
                    context.stream.send(&CommandComplete::new(command)).await?
                        + context
                            .stream
                            .send(&ReadyForQuery::in_transaction(context.in_transaction()))
                            .await?
                }

                _ => 0,
            }
        }
        context.stream.flush().await?;
        self.stats.sent(sent);

        Ok(())
    }
}
