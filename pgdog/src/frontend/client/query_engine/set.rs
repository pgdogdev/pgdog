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
        self.comms.update_params(context.params);

        let bytes_sent = context
            .stream
            .send_many(&[
                CommandComplete::from_str("SET").message()?.backend(),
                ReadyForQuery::in_transaction(context.in_transaction())
                    .message()?
                    .backend(),
            ])
            .await?;

        self.stats.sent(bytes_sent);

        Ok(())
    }

    pub(crate) async fn set_route(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        route: Route,
    ) -> Result<(), Error> {
        self.set_route = Some(route);

        let bytes_sent = context
            .stream
            .send_many(&[
                CommandComplete::from_str("SET").message()?.backend(),
                ReadyForQuery::in_transaction(context.in_transaction())
                    .message()?
                    .backend(),
            ])
            .await?;

        self.stats.sent(bytes_sent);

        Ok(())
    }
}
