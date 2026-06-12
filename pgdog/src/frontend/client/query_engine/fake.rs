use tokio::io::AsyncWriteExt;

use crate::net::{
    BindComplete, CloseComplete, CommandComplete, DataRow, Field, NoData, ParameterDescription,
    ParseComplete, ProtocolMessage, ReadyForQuery, RowDescription, parameter::ParameterValue,
};

use super::*;

impl QueryEngine {
    /// Respond to a command sent by the client
    /// in a way that won't make it suspicious.
    pub(crate) async fn fake_command_response(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        command: &str,
        return_value: Option<impl IntoIterator<Item = Option<&'_ ParameterValue>> + Clone>,
    ) -> Result<(), Error> {
        let mut sent = 0;
        let return_fields = return_value
            .clone()
            .into_iter()
            .flatten()
            .map(|_| Field::text(""))
            .collect::<Vec<_>>();
        let row_description = RowDescription::new(&return_fields);
        let data_row = return_value.map(|return_value| {
            let mut row = DataRow::new();
            for val in return_value {
                row.add(val);
            }
            row
        });
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
                            + context.stream.send(&row_description).await?
                    } else {
                        context.stream.send(&NoData).await?
                    }
                }
                ProtocolMessage::Execute(_) => {
                    (if let Some(row) = data_row.as_ref() {
                        context.stream.send(row).await?
                    } else {
                        0
                    }) + context.stream.send(&CommandComplete::new(command)).await?
                }
                ProtocolMessage::Sync(_) => {
                    context
                        .stream
                        .send(&ReadyForQuery::in_transaction(context.in_transaction()))
                        .await?
                }
                ProtocolMessage::Query(_) => {
                    (if let Some(row) = data_row.as_ref() {
                        context.stream.send(&row_description).await?
                            + context.stream.send(row).await?
                    } else {
                        0
                    }) + context.stream.send(&CommandComplete::new(command)).await?
                        + context
                            .stream
                            .send(&ReadyForQuery::in_transaction(context.in_transaction()))
                            .await?
                }
                // TODO(lev): Elixir closes the statement it just asked us to prepare.
                // That's very memory-conscious of it, and we appreciate it.
                //
                // Add Elixir back to our CI.
                ProtocolMessage::Close(_) => context.stream.send(&CloseComplete).await?,

                _ => 0,
            }
        }
        context.stream.flush().await?;
        self.stats.sent(sent);

        Ok(())
    }
}
