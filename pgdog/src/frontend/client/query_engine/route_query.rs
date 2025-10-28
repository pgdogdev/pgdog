use tracing::{error, trace};

use super::*;

impl QueryEngine {
    pub(super) async fn route_transaction(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<bool, Error> {
        // Admin doesn't have a cluster.
        let cluster = if let Ok(cluster) = self.backend.cluster() {
            cluster
        } else {
            return Ok(true);
        };

        let router_context = RouterContext::new(
            context.client_request,
            cluster,
            context.prepared_statements,
            context.params,
            context.transaction,
        )?;
        match self.router.query(router_context) {
            Ok(cmd) => {
                trace!(
                    "routing {:#?} to {:#?}",
                    context.client_request.messages,
                    cmd
                );
            }
            Err(err) => {
                error!("{:?} [{:?}]", err, context.stream.peer_addr());

                let error = ErrorResponse::syntax(err.to_string().as_str());

                self.hooks.on_engine_error(context, &error)?;

                let bytes_sent = context
                    .stream
                    .error(error, context.in_transaction())
                    .await?;
                self.stats.sent(bytes_sent);
                return Ok(false);
            }
        }

        Ok(true)
    }
}
