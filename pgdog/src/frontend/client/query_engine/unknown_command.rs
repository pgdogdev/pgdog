use super::*;

impl QueryEngine {
    pub(super) async fn unknown_command(
        &self,
        context: &mut QueryEngineContext<'_>,
        command: &Command,
    ) -> Result<(), Error> {
        let bytes_sent = context
            .stream
            .error(
                ErrorResponse::syntax(&format!("unknown command: {:?}", command)),
                context.in_transaction,
            )
            .await?;

        context.stats.sent(bytes_sent);

        Ok(())
    }
}
