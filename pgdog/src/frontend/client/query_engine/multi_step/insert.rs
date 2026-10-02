use super::{CommandType, MultiServerState};
use crate::{
    backend::Cluster,
    frontend::{
        ClientRequest, Command, Router, RouterContext,
        client::query_engine::{QueryEngine, QueryEngineContext},
        router::parser::rewrite::statement::InsertSplitRewriteResult,
    },
    net::Protocol,
};

use super::super::Error;

#[derive(Debug)]
pub(crate) struct InsertMulti<'a> {
    /// Requests split by the rewrite engine.
    requests: InsertSplitRewriteResult,
    /// Execution state.
    state: MultiServerState,
    /// Query engine.
    engine: &'a mut QueryEngine,
}

impl<'a> InsertMulti<'a> {
    /// Create multi-shard INSERT handler
    /// from query engine and a set of routed requests.
    pub(crate) fn from_engine(
        engine: &'a mut QueryEngine,
        requests: InsertSplitRewriteResult,
    ) -> Self {
        Self {
            state: MultiServerState::new(requests.len()),
            requests,
            engine,
        }
    }

    /// Route each request in the split to its respective shard.
    ///
    /// This is implemented separately since we want to route the requests
    /// before we check out connections from the pools. If all inserts are sent
    /// to the same shard, we will only check out that shard's connection.
    pub(crate) fn route(
        requests: &mut InsertSplitRewriteResult,
        context: &QueryEngineContext<'_>,
        cluster: &Cluster,
    ) -> Result<(), Error> {
        for request in requests.iter_mut() {
            if request.route.is_some() {
                continue;
            }

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

        Ok(())
    }

    /// Execute the multi-shard INSERT.
    pub(crate) async fn execute(
        &'a mut self,
        context: &mut QueryEngineContext<'_>,
        client_request: &mut ClientRequest,
    ) -> Result<bool, Error> {
        let cluster = self.engine.backend.cluster()?;
        // TODO(lev): This is a no-op since we route requests in [`QueryEngine::parse_and_rewrite`].
        Self::route(&mut self.requests, context, cluster)?;

        // All tuples map to the same shard: send the original multi-row INSERT
        // as a single statement, skipping the multi-step path entirely.
        if self.requests.same_shard().is_some() {
            self.engine
                .backend
                .handle_client_request(
                    client_request,
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
                .process_server_message(context, cc.message())
                .await?;
        }

        if let Some(rfq) = self.state.ready_for_query(context.in_transaction()) {
            self.engine
                .process_server_message(context, rfq.message())
                .await?;
        }

        Ok(self.state.error())
    }
}
