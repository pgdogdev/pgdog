use pgdog_config::RewriteMode;

use crate::{
    frontend::{
        client::query_engine::{QueryEngine, QueryEngineContext},
        router::parser::rewrite::statement::ShardingKeyUpdate,
        ClientRequest, Command, Router, RouterContext,
    },
    net::ErrorResponse,
};

use super::super::Error;

#[derive(Debug)]
pub(crate) struct UpdateMulti<'a> {
    rewrite: ShardingKeyUpdate,
    engine: &'a mut QueryEngine,
}

impl<'a> UpdateMulti<'a> {
    /// Create new sharding key update handler.
    pub(crate) fn new(engine: &'a mut QueryEngine, rewrite: ShardingKeyUpdate) -> Self {
        Self { rewrite, engine }
    }

    /// Execute sharding key update, if needed.
    pub(crate) async fn execute(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        let mut check = self.rewrite.check.build_request(&context.client_request)?;
        self.route(&mut check, context)?;

        if self.is_same_shard(context)? {
            // Serve original request as-is.
            self.engine
                .backend
                .handle_client_request(
                    &context.client_request,
                    &mut self.engine.router,
                    self.engine.streaming,
                )
                .await?;

            while self.engine.backend.has_more_messages() {
                let message = self.engine.read_server_message(context).await?;
                self.engine.process_server_message(context, message).await?;
            }

            return Ok(());
        }

        if self.engine.backend.cluster()?.rewrite().shard_key == RewriteMode::Error {
            self.engine
                .error_response(
                    context,
                    ErrorResponse::from_err(&Error::ShardingKeyUpdateForbidden),
                )
                .await?;
            return Ok(());
        }

        if !self.engine.backend.is_multishard() {
            return Err(Error::MultiShardRequired);
        }

        Ok(())
    }

    /// Returns true if the new sharding key resides on the same shard
    /// as the old sharding key.
    ///
    /// This is an optimization to avoid doing a multi-shard UPDATE when
    /// we don't have to.
    pub(super) fn is_same_shard(&self, context: &QueryEngineContext<'_>) -> Result<bool, Error> {
        let mut check = self.rewrite.check.build_request(&context.client_request)?;
        self.route(&mut check, context)?;

        let new_shard = check.route().shard();
        let old_shard = context.client_request.route().shard();

        // The sharding key isn't actually being changed
        // or it maps to the same shard as before.
        Ok(new_shard == old_shard)
    }

    fn route(
        &self,
        request: &mut ClientRequest,
        context: &QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        let cluster = self.engine.backend.cluster()?;

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

        Ok(())
    }
}
