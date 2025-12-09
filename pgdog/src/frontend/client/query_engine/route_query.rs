use pgdog_config::PoolerMode;
use tracing::trace;

use super::*;

impl QueryEngine {
    pub(super) async fn route_transaction(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<bool, Error> {
        // Check that we can route this transaction at all.
        if self.backend.pooler_mode() == PoolerMode::Statement && context.client_request.is_begin()
        {
            self.error_response(context, ErrorResponse::transaction_statement_mode())
                .await?;
            return Ok(false);
        }

        // Admin doesn't have a cluster.
        let cluster = if let Ok(cluster) = self.backend.cluster() {
            if !context.in_transaction() && !cluster.online() {
                let identifier = cluster.identifier();

                // Reload cluster config.
                self.backend.safe_reload().await?;

                match self.backend.cluster() {
                    Ok(cluster) => cluster,
                    Err(_) => {
                        // Cluster is gone.
                        self.error_response(
                            context,
                            ErrorResponse::connection(&identifier.user, &identifier.database),
                        )
                        .await?;

                        return Ok(false);
                    }
                }
            } else {
                cluster
            }
        } else {
            return Ok(true);
        };

        let router_context = RouterContext::new(
            context.client_request,
            cluster,
            context.prepared_statements,
            context.params,
            context.transaction,
            context.sticky,
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
                self.error_response(context, ErrorResponse::syntax(err.to_string().as_str()))
                    .await?;

                return Ok(false);
            }
        }

        Ok(true)
    }
}
