use super::*;

impl QueryEngine {
    pub(super) async fn unknown_command(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        command: Command,
    ) -> Result<(), Error> {
        let bytes_sent = context
            .stream
            .error(
                ErrorResponse::syntax(&format!("unknown command: {:?}", command)),
                context.in_transaction(),
            )
            .await?;

        self.stats.sent(bytes_sent);

        Ok(())
    }
}
