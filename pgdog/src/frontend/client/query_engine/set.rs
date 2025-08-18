use crate::net::{parameter::ParameterValue, CommandComplete, Protocol, ReadyForQuery};

use super::*;

impl QueryEngine {
    pub(crate) async fn set(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        name: String,
        value: ParameterValue,
    ) -> Result<(), Error> {
        context.params.insert(name, value);
        self.comms.update_params(&context.params);

        let bytes_sent = context
            .stream
            .send_many(&vec![
                CommandComplete::from_str("SET").message()?,
                ReadyForQuery::in_transaction(context.in_transaction()).message()?,
            ])
            .await?;

        self.stats.sent(bytes_sent);

        Ok(())
    }
}
