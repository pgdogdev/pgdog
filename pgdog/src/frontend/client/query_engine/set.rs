use crate::net::{parameter::ParameterValue, CommandComplete, Protocol, ReadyForQuery};

use super::*;

impl QueryEngine {
    pub(crate) async fn set(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        name: String,
        value: ParameterValue,
        extended: bool,
        route: Route,
        local: bool,
    ) -> Result<(), Error> {
        if !local {
            context.params.insert(name, value.clone());
            self.comms.update_params(context.params);
        }

        if extended {
            // Re-enable cross-shard queries for this request.
            context.cross_shard_disabled = Some(false);
            self.execute(context, &route).await?;
        } else {
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
        }

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
