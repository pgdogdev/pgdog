use crate::{
    frontend::client::{TransactionType, transaction_type::Transaction},
    net::{
        BindComplete, CommandComplete, NoData, NoticeResponse, ParameterDescription, ParseComplete,
        Protocol, ProtocolMessage, ReadyForQuery,
    },
};

use super::*;

impl QueryEngine {
    /// BEGIN
    pub(super) async fn start_transaction(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        // FIXME(sage): Remove mut
        client_request: &mut ClientRequest,
        begin: BufferedQuery,
        transaction_type: TransactionType,
        extended: bool,
    ) -> Result<(), Error> {
        context.transaction = Some(Transaction::new(transaction_type));

        self.backend
            .start_transaction(transaction_type.read_only(), begin)?;

        if self.backend.connected() {
            self.execute(context, client_request, None).await?;
        } else {
            let bytes_sent = if extended {
                self.extended_transaction_reply(context, &client_request.messages, true, false)
                    .await?
            } else {
                let mut messages = vec![CommandComplete::new_begin().message()];

                if context.pipeline.is_done() || !context.pipeline.is_simple() {
                    messages
                        .push(ReadyForQuery::in_transaction(context.in_transaction()).message());
                }

                context.stream.send_many(&messages).await?
            };

            self.stats.sent(bytes_sent);
        }

        Ok(())
    }

    pub(super) async fn extended_transaction_reply(
        &self,
        context: &mut QueryEngineContext<'_>,
        client_messages: &[ProtocolMessage],
        in_transaction: bool,
        rollback: bool,
    ) -> Result<usize, Error> {
        let mut reply = vec![];
        for message in client_messages {
            match message.code() {
                'P' => reply.push(ParseComplete.message()),
                'B' => reply.push(BindComplete.message()),
                'D' => {
                    if matches!(message, ProtocolMessage::Describe(d) if d.is_statement()) {
                        reply.push(ParameterDescription::empty().message());
                    }
                    reply.push(NoData.message());
                }
                'H' => (),
                'E' => reply.push(if in_transaction {
                    CommandComplete::new_begin().message()
                } else if !rollback {
                    CommandComplete::new_commit().message()
                } else {
                    CommandComplete::new_rollback().message()
                }),
                'S' => {
                    if rollback && !context.in_transaction() {
                        reply.push(NoticeResponse::from(ErrorResponse::no_transaction()).message());
                    }
                    reply.push(ReadyForQuery::in_transaction(in_transaction).message())
                }
                c => return Err(Error::UnexpectedMessage(c)),
            }
        }

        Ok(context.stream.send_many(&reply).await?)
    }
}
