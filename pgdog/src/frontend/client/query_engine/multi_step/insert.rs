use super::{CommandType, MultiServerState};
use crate::{
    frontend::{
        ClientRequest, Command, Router, RouterContext,
        client::query_engine::{QueryEngine, QueryEngineContext},
        router::{
            Route,
            parser::route::{Shard, ShardWithPriority},
        },
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

    /// If every split routes to the same `Shard::Direct(n)`, return that shard
    /// number. Returns `None` when the splits span multiple shards or contain
    /// any non-direct routing.
    fn uniform_shard(&self) -> Option<usize> {
        let mut target: Option<usize> = None;
        for req in &self.requests {
            let n = match req.route.as_ref()?.shard() {
                Shard::Direct(n) => *n,
                _ => return None,
            };
            match target {
                None => target = Some(n),
                Some(t) if t != n => return None,
                _ => {}
            }
        }
        target
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

        // All tuples map to the same shard: send the original multi-row INSERT
        // as a single statement, skipping the multi-step path entirely.
        if let Some(shard_n) = self.uniform_shard() {
            context.client_request.route = Some(Route::write(ShardWithPriority::new_table(
                Shard::Direct(shard_n),
            )));
            self.engine
                .backend
                .handle_client_request(
                    context.client_request,
                    &mut self.engine.router,
                    self.engine.streaming,
                )
                .await?;
            while self.engine.backend.has_more_messages() {
                let message = self.engine.read_server_message().await?;
                self.engine.process_server_message(context, message).await?;
            }
            return Ok(false);
        }

        if !self.engine.backend.is_multishard() {
            return Err(Error::MultiShardRequired);
        }

        for request in self.requests.iter() {
            self.engine
                .backend
                .handle_client_request(request, &mut self.engine.router, self.engine.streaming)
                .await?;

            while self.engine.backend.has_more_messages() {
                let message = self.engine.read_server_message().await?;

                if self.state.forward(&message)? {
                    self.engine.process_server_message(context, message).await?;
                }
            }
        }

        if let Some(cc) = self.state.command_complete(CommandType::Insert) {
            self.engine
                .process_server_message(context, cc.message()?)
                .await?;
        }

        if let Some(rfq) = self.state.ready_for_query(context.in_transaction()) {
            self.engine
                .process_server_message(context, rfq.message()?)
                .await?;
        }

        Ok(self.state.error())
    }
}
