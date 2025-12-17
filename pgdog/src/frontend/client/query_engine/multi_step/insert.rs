use super::{CommandType, MultiServerState};
use crate::{
    frontend::{
        client::query_engine::{QueryEngine, QueryEngineContext},
        ClientRequest, Command, Router, RouterContext,
    },
    net::Protocol,
};

use super::super::Error;

#[derive(Debug)]
pub(crate) struct InsertMulti<'a> {
    /// Requests split by the rewrite engine.
    requests: Vec<ClientRequest>,
    /// Execution state.
    state: MultiServerState,
    /// Query engine.
    engine: &'a mut QueryEngine,
}

impl<'a> InsertMulti<'a> {
    /// Create multi-shard INSERT handler
    /// from query engine and a set of routed requests.
    pub(crate) fn from_engine(engine: &'a mut QueryEngine, requests: Vec<ClientRequest>) -> Self {
        Self {
            state: MultiServerState::new(requests.len()),
            requests,
            engine,
        }
    }

    /// Execute the multi-shard INSERT.
    pub(crate) async fn execute(
        &'a mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<bool, Error> {
        let cluster = self.engine.backend.cluster()?;
        for request in self.requests.iter_mut() {
            let context = RouterContext::new(
                request,
                cluster,
                context.params,
                context.transaction(),
                context.sticky,
            )?;
            let mut router = Router::new();
            let command = router.query(context)?;
            if let Command::Query(route) = command {
                request.route = Some(route.clone());
            } else {
                return Err(Error::NoRoute);
            }
        }

        if !self.engine.backend.is_multishard() {
            return Err(Error::MultiShardRequired);
        }

        for request in self.requests.iter() {
            self.engine.backend.send(request).await?;

            while self.engine.backend.has_more_messages() {
                let message = self.engine.read_server_message(context).await.unwrap();

                if self.state.forward(&message)? {
                    self.engine
                        .process_server_message(context, message)
                        .await
                        .unwrap();
                }
            }
        }

        if let Some(cc) = self.state.command_complete(CommandType::Insert) {
            self.engine
                .process_server_message(context, cc.message()?)
                .await
                .unwrap();
        }

        if let Some(rfq) = self.state.ready_for_query(context.in_transaction()) {
            self.engine
                .process_server_message(context, rfq.message()?)
                .await
                .unwrap();
        }

        Ok(self.state.error())
    }
}
