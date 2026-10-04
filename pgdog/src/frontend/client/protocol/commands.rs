//! Respond to any request with affirmation.
//! Do nothing otherwise.

use super::{ClientMessages, Statement};
use crate::{
    frontend::client::{Transaction, query_engine::Pipeline},
    net::*,
};

#[derive(Debug, Clone, Default)]
pub(in crate::frontend) struct Commands {
    transaction: Option<Transaction>,
    statement: Statement,
    simple_pipeline_end: bool,
    pub(super) row_description: Option<RowDescription>,
    pub(super) data_rows: Vec<DataRow>,
    pub(super) rows_affected: usize,
}

#[bon::bon]
impl Commands {
    #[builder(start_fn = builder, finish_fn = build)]
    pub(in crate::frontend) fn new(
        transaction: Option<Transaction>,
        pipeline: &Pipeline,
        statement: Statement,
        row_description: Option<RowDescription>,
        #[builder(default)] data_rows: Vec<DataRow>,
        #[builder(default)] rows_affected: usize,
    ) -> Self {
        Self {
            transaction,
            statement,
            simple_pipeline_end: pipeline.is_done() || !pipeline.is_simple(),
            row_description,
            data_rows,
            rows_affected,
        }
    }

    /// Send fake reply to stream, returning bytes sent and whether a statement executed.
    pub(in crate::frontend) async fn send_reply<'a>(
        &self,
        messages: impl Into<ClientMessages<'a>>,
        stream: &mut Stream,
    ) -> Result<(usize, bool), crate::net::Error> {
        let messages = messages.into();

        let reply = self.reply(messages);
        let sent = stream.send_many(&reply).await?;

        Ok((sent, messages.executes_statement()))
    }

    fn reply<'a>(&self, messages: ClientMessages<'a>) -> Vec<Message> {
        let mut reply = vec![];

        for message in messages.messages {
            match message.code() {
                'P' => reply.push(ParseComplete.message()),
                'B' => reply.push(BindComplete.message()),
                'C' => reply.push(CloseComplete.message()),
                'D' => {
                    if matches!(message, ProtocolMessage::Describe(d) if d.is_statement()) {
                        reply.push(ParameterDescription::empty().message());
                    }
                    if let Some(row_description) = &self.row_description {
                        reply.push(row_description.message());
                    } else {
                        reply.push(NoData.message());
                    }
                }
                'E' => {
                    self.notice_and_data(&mut reply);
                    reply.push(self.command_complete().message());
                }
                // Extended protocol ReadyForQuery
                // should acknowledge change of transaction state only if we
                // should actually do it.
                //
                // For example, a Parse("COMMIT"), Describe, Sync
                // inside a transaction will not actually commit that transaction.
                'S' => reply.push(
                    self.ready_for_query(messages.executes_statement())
                        .message(),
                ),
                'Q' => {
                    if let Some(row_description) = &self.row_description {
                        reply.push(row_description.message());
                    }
                    self.notice_and_data(&mut reply);
                    reply.push(self.command_complete().message());
                    if self.simple_pipeline_end {
                        reply.push(self.ready_for_query(true).message());
                    }
                }
                'H' => (),
                _ => (),
            }
        }

        reply
    }

    fn notice_and_data(&self, reply: &mut Vec<Message>) {
        if !self.data_rows.is_empty() {
            reply.extend(self.data_rows.iter().map(|row| row.message()));
        }
        if let Some(notice) = self.notice_response() {
            reply.push(notice.message());
        }
    }

    fn command_complete(&self) -> CommandComplete {
        match self.statement {
            Statement::Begin => CommandComplete::new_begin(),
            Statement::Rollback => CommandComplete::new_rollback(),
            Statement::Commit => CommandComplete::new_commit(),
            Statement::Listen => CommandComplete::new("LISTEN"),
            Statement::Notify => CommandComplete::new("NOTIFY"),
            Statement::Unlisten => CommandComplete::new("UNLISTEN"),
            Statement::Insert => CommandComplete::new(format!("INSERT 0 {}", self.rows_affected)),
            Statement::Update => CommandComplete::new(format!("UPDATE {}", self.rows_affected)),
            Statement::Delete => CommandComplete::new(format!("DELETE {}", self.rows_affected)),
            Statement::Select => CommandComplete::new(format!("SELECT {}", self.rows_affected)),
            Statement::Set => CommandComplete::new("SET"),
            Statement::Reset => CommandComplete::new("RESET"),
            Statement::Unknown => CommandComplete::new("UNKNOWN"),
        }
    }

    fn notice_response(&self) -> Option<NoticeResponse> {
        match self.statement {
            Statement::Commit => {
                if !self.in_transaction() {
                    return Some(NoticeResponse::from(ErrorResponse::no_transaction()));
                }
            }
            Statement::Begin => {
                if self.in_transaction() {
                    return Some(NoticeResponse::from(
                        ErrorResponse::transaction_already_started(),
                    ));
                }
            }

            Statement::Rollback => {
                if !self.in_transaction() {
                    return Some(NoticeResponse::from(ErrorResponse::no_transaction()));
                }
            }

            _ => (),
        }

        None
    }

    fn ready_for_query(&self, statement_executed: bool) -> ReadyForQuery {
        match self.statement {
            Statement::Begin => {
                ReadyForQuery::in_transaction(self.in_transaction() || statement_executed)
            }
            Statement::Commit | Statement::Rollback => {
                ReadyForQuery::in_transaction(self.in_transaction() && !statement_executed)
            }
            _ => ReadyForQuery::in_transaction(self.in_transaction()),
        }
    }

    fn in_transaction(&self) -> bool {
        self.transaction.is_some()
    }
}
