//! Respond to any request with affirmation.
//! Do nothing otherwise.

use super::ClientMessages;
use crate::{frontend::client::Transaction, net::*};

#[derive(Debug, Clone, PartialEq, Default)]
enum Statement {
    Commit,
    Rollback,
    Begin,
    Select,
    Update,
    Delete,
    Insert,
    Show,
    Notify,
    Listen,
    Unlisten,
    #[default]
    Unknown,
}

#[derive(Debug, Clone, Default)]
pub(in crate::frontend) struct ProtocolResponder {
    transaction: Option<Transaction>,
    statement: Statement,
    in_pipeline: bool,
}

impl ProtocolResponder {
    pub(in crate::frontend) fn new(transaction: Option<Transaction>, in_pipeline: bool) -> Self {
        Self {
            transaction,
            in_pipeline,
            ..Default::default()
        }
    }

    pub(in crate::frontend) fn commit(transaction: Option<Transaction>, in_pipeline: bool) -> Self {
        Self {
            statement: Statement::Commit,
            ..Self::new(transaction, in_pipeline)
        }
    }

    pub(in crate::frontend) fn rollback(
        transaction: Option<Transaction>,
        in_pipeline: bool,
    ) -> Self {
        Self {
            statement: Statement::Rollback,
            ..Self::new(transaction, in_pipeline)
        }
    }

    pub(in crate::frontend) fn begin(transaction: Option<Transaction>, in_pipeline: bool) -> Self {
        Self {
            statement: Statement::Begin,
            ..Self::new(transaction, in_pipeline)
        }
    }

    /// Send fake reply to stream, returning the number of bytes sent.
    pub(in crate::frontend) async fn send_reply<'a>(
        &self,
        messages: impl Into<ClientMessages<'a>>,
        stream: &mut Stream,
    ) -> Result<(usize, bool), crate::net::Error> {
        let messages = messages.into();

        let reply = self.reply(messages);
        let sent = stream.send_many(&reply).await?;

        Ok((sent, messages.actionable()))
    }

    fn reply<'a>(&self, messages: ClientMessages<'a>) -> Vec<Message> {
        let mut reply = vec![];

        for message in messages.messages {
            match message.code() {
                'P' => reply.push(ParseComplete.message()),
                'B' => reply.push(BindComplete.message()),
                'D' => {
                    if matches!(message, ProtocolMessage::Describe(d) if d.is_statement()) {
                        reply.push(ParameterDescription::empty().message());
                    }
                    reply.push(NoData.message());
                }
                'E' => {
                    if let Some(notice) = self.notice_response() {
                        reply.push(notice.message());
                    }
                    reply.push(self.command_complete().message());
                }
                // Extended protocol ReadyForQuery
                // should acknowledge change of transaction state only if we
                // should actually do it.
                //
                // For example, a Parse("COMMIT"), Describe, Sync
                // inside a transaction will not actually commit that transaction.
                'S' => reply.push(self.ready_for_query(messages.actionable()).message()),
                'Q' => {
                    if let Some(notice) = self.notice_response() {
                        reply.push(notice.message());
                    }
                    reply.push(self.command_complete().message());
                    if !self.in_pipeline {
                        reply.push(self.ready_for_query(true).message());
                    }
                }
                'H' => (),
                _ => (),
            }
        }

        reply
    }

    fn command_complete(&self) -> CommandComplete {
        match self.statement {
            Statement::Begin => CommandComplete::new_begin(),
            Statement::Rollback => CommandComplete::new_rollback(),
            Statement::Commit => CommandComplete::new_commit(),
            Statement::Select => CommandComplete::new("SELECT 0"),
            Statement::Delete => CommandComplete::new("DELETE 0"),
            Statement::Update => CommandComplete::new("UDPATE 0"),
            Statement::Insert => CommandComplete::new("INSERT 0 0"),
            Statement::Show => CommandComplete::new("SHOW"),
            Statement::Unknown => CommandComplete::new("UNKNOWN"),
            Statement::Listen => CommandComplete::new("LISTEN"),
            Statement::Notify => CommandComplete::new("NOTIFY"),
            Statement::Unlisten => CommandComplete::new("UNLISTEN"),
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

    fn ready_for_query(&self, actionable: bool) -> ReadyForQuery {
        match self.statement {
            Statement::Begin => ReadyForQuery::in_transaction(self.in_transaction() || actionable),
            Statement::Commit | Statement::Rollback => ReadyForQuery::in_transaction(actionable),
            _ => ReadyForQuery::in_transaction(self.in_transaction()),
        }
    }

    fn in_transaction(&self) -> bool {
        self.transaction.is_some()
    }
}
