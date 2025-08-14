use crate::net::{CommandComplete, NoticeResponse, Protocol, ReadyForQuery};

use super::*;

impl QueryEngine {
    pub(super) async fn end_transaction(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        rollback: bool,
    ) -> Result<(), Error> {
        let cmd = if rollback {
            CommandComplete::new_rollback()
        } else {
            CommandComplete::new_commit()
        };
        let mut messages = if !context.in_transaction {
            vec![NoticeResponse::from(ErrorResponse::no_transaction()).message()?]
        } else {
            vec![]
        };
        messages.push(cmd.message()?.backend());
        messages.push(ReadyForQuery::idle().message()?);
        context.stream.send_many(&messages).await?;
        debug!("transaction ended");
        Ok(())
    }
}
