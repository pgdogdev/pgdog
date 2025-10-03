use crate::{
    frontend::client::TransactionType,
    net::{BindComplete, CommandComplete, NoticeResponse, ParseComplete, Protocol, ReadyForQuery},
};

use super::*;

impl QueryEngine {
    /// BEGIN
    pub(super) async fn start_transaction(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        begin: BufferedQuery,
        transaction_type: TransactionType,
        extended: bool,
    ) -> Result<(), Error> {
        context.transaction = Some(transaction_type);

        let bytes_sent = if extended {
            self.extended_transaction_reply(context, true, false)
                .await?
        } else {
            context
                .stream
                .send_many(&[
                    CommandComplete::new_begin().message()?.backend(),
                    ReadyForQuery::in_transaction(context.in_transaction()).message()?,
                ])
                .await?
        };

        self.stats.sent(bytes_sent);
        self.begin_stmt = Some(begin);

        Ok(())
    }

    pub(super) async fn extended_transaction_reply(
        &self,
        context: &mut QueryEngineContext<'_>,
        in_transaction: bool,
        rollback: bool,
    ) -> Result<usize, Error> {
        let mut reply = vec![];
        for message in context.client_request.iter() {
            match message.code() {
                'P' => reply.push(ParseComplete.message()?),
                'B' => reply.push(BindComplete.message()?),
                'D' | 'H' => (),
                'E' => reply.push(if in_transaction {
                    CommandComplete::new_begin().message()?.backend()
                } else if !rollback {
                    CommandComplete::new_commit().message()?.backend()
                } else {
                    CommandComplete::new_rollback().message()?.backend()
                }),
                'S' => {
                    if rollback && !context.in_transaction() {
                        reply
                            .push(NoticeResponse::from(ErrorResponse::no_transaction()).message()?);
                    }
                    reply.push(ReadyForQuery::in_transaction(in_transaction).message()?)
                }
                c => return Err(Error::UnexpectedMessage(c)),
            }
        }

        Ok(context.stream.send_many(&reply).await?)
    }
}
