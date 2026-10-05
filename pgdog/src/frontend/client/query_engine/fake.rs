use itertools::Itertools;
use tokio::io::AsyncWriteExt;

use crate::net::{
    BindComplete, CloseComplete, CommandComplete, DataRow, Field, NoData, NoticeResponse,
    ParameterDescription, ParseComplete, ProtocolMessage, ReadyForQuery, RowDescription,
    parameter::ParameterValue,
};

use super::*;

/// Messages this engine sends back for a command it handles itself.
#[derive(Debug)]
pub(super) struct FakeResponse {
    command: String,
    row_description: Option<RowDescription>,
    row: Option<DataRow>,
    notice: Option<NoticeResponse>,
}

impl FakeResponse {
    pub(super) fn command(command: impl Into<String>) -> Self {
        Self {
            command: command.into(),
            row_description: None,
            row: None,
            notice: None,
        }
    }

    pub(super) fn with_params<'a>(
        mut self,
        columns: &[&str],
        values: impl IntoIterator<Item = Option<&'a ParameterValue>>,
    ) -> Self {
        let row_description =
            RowDescription::new(&columns.iter().map(|col| Field::text(col)).collect_vec());

        let mut row = DataRow::new();
        for val in values {
            row.add(val);
        }

        self.row_description = Some(row_description);
        self.row = Some(row);
        self
    }

    pub(super) fn with_notice(mut self, notice: NoticeResponse) -> Self {
        self.notice = Some(notice);
        self
    }
}

impl QueryEngine {
    /// Respond to a command sent by the client
    /// in a way that won't make it suspicious.
    pub(super) async fn fake_command_response(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        client_messages: &[ProtocolMessage],
        response: &FakeResponse,
    ) -> Result<(), Error> {
        let mut sent = 0;

        for message in client_messages {
            sent += match message {
                ProtocolMessage::Parse(_) => context.stream.send(&ParseComplete).await?,
                ProtocolMessage::Bind(_) => context.stream.send(&BindComplete).await?,
                ProtocolMessage::Describe(describe) => {
                    if describe.is_statement() {
                        context
                            .stream
                            .send(&ParameterDescription::default())
                            .await?
                            + if let Some(row_description) = response.row_description.as_ref() {
                                context.stream.send(row_description).await?
                            } else {
                                context.stream.send(&NoData).await?
                            }
                    } else {
                        context.stream.send(&NoData).await?
                    }
                }
                ProtocolMessage::Execute(_) => {
                    (if let Some(notice) = response.notice.as_ref() {
                        context.stream.send(notice).await?
                    } else {
                        0
                    }) + (if let Some(row) = response.row.as_ref() {
                        context.stream.send(row).await?
                    } else {
                        0
                    }) + context
                        .stream
                        .send(&CommandComplete::new(&response.command))
                        .await?
                }
                ProtocolMessage::Sync(_) => {
                    context
                        .stream
                        .send(&ReadyForQuery::in_transaction(context.in_transaction()))
                        .await?
                }
                ProtocolMessage::Query(_) => {
                    (if let Some(notice) = response.notice.as_ref() {
                        context.stream.send(notice).await?
                    } else {
                        0
                    }) + (if let (Some(row_description), Some(row)) =
                        (response.row_description.as_ref(), response.row.as_ref())
                    {
                        context.stream.send(row_description).await?
                            + context.stream.send(row).await?
                    } else {
                        0
                    }) + context
                        .stream
                        .send(&CommandComplete::new(&response.command))
                        .await?
                        + if context.pipeline.is_simple() && !context.pipeline.is_done() {
                            // Don't send ReadyForQuery for intermediate queries in a simple query
                            // pipeline.
                            0
                        } else {
                            context
                                .stream
                                .send(&ReadyForQuery::in_transaction(context.in_transaction()))
                                .await?
                        }
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
