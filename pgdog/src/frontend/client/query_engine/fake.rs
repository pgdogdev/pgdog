use itertools::Itertools;
use tokio::io::AsyncWriteExt;

use crate::net::{
    BindComplete, CloseComplete, CommandComplete, DataRow, Field, NoData, ParameterDescription,
    ParseComplete, ProtocolMessage, ReadyForQuery, RowDescription, parameter::ParameterValue,
};

use super::*;

#[derive(Debug, Clone)]
pub(super) struct FakeResponse {
    pub(super) row_description: RowDescription,
    pub(super) row: DataRow,
}

impl FakeResponse {
    pub(super) fn new_params<'a>(
        columns: &[&str],
        values: impl IntoIterator<Item = Option<&'a ParameterValue>> + Clone,
    ) -> Self {
        let row_description =
            RowDescription::new(&columns.iter().map(|col| Field::text(col)).collect_vec());

        let mut row = DataRow::new();
        for val in values {
            row.add(val);
        }

        Self {
            row_description,
            row,
        }
    }
}

impl QueryEngine {
    /// Respond to a command sent by the client
    /// in a way that won't make it suspicious.
    pub(super) async fn fake_command_response(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        command: &str,
        fake_response: Option<FakeResponse>,
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
                            + if let Some(fake_response) = fake_response.as_ref() {
                                context.stream.send(&fake_response.row_description).await?
                            } else {
                                context.stream.send(&NoData).await?
                            }
                    } else {
                        context.stream.send(&NoData).await?
                    }
                }
                ProtocolMessage::Execute(_) => {
                    (if let Some(fake_response) = fake_response.as_ref() {
                        context.stream.send(&fake_response.row).await?
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
                    (if let Some(fake_response) = fake_response.as_ref() {
                        context.stream.send(&fake_response.row_description).await?
                            + context.stream.send(&fake_response.row).await?
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
