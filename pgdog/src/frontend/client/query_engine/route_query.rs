use pgdog_config::PoolerMode;
use tracing::trace;

use crate::backend::Cluster;

use super::*;

impl QueryEngine {
    pub fn cluster(&self) -> Result<&Cluster, Error> {
        Ok(self.backend.cluster()?)
    }

    /// Check that cluster still exists.
    pub async fn ensure_cluster(&mut self, in_transaction: bool) -> Option<ErrorResponse> {
        if let Ok(cluster) = self.backend.cluster() {
            let identifier = cluster.identifier();

            if !in_transaction && !cluster.online() {
                // Reload cluster config.
                if self.backend.safe_reload().await.is_err() {
                    return Some(ErrorResponse::connection(
                        &identifier.user,
                        &identifier.database,
                    ));
                }

                if self.backend.cluster().is_err() {
                    return Some(ErrorResponse::connection(
                        &identifier.user,
                        &identifier.database,
                    ));
                }
            }
        }

        None
    }

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
            context.omni_sticky_index,
            context.ast.as_ref(),
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
