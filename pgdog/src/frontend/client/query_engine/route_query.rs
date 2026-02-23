use pgdog_config::PoolerMode;
use tokio::time::timeout;
use tracing::trace;

use super::*;

#[derive(Debug, Clone)]
pub enum ClusterCheck {
    Ok,
    Offline,
}

impl QueryEngine {
    /// Get mutable reference to the backend connection.
    pub fn backend(&mut self) -> &mut Connection {
        &mut self.backend
    }

    /// Check that the cluster is still valid and online.
    pub async fn cluster_check(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<ClusterCheck, Error> {
        // Admin doesn't have a cluster.
        let res = if let Ok(cluster) = self.backend.cluster() {
            if !context.in_transaction() && !cluster.online() {
                let identifier = cluster.identifier();

                // Reload cluster config.
                self.backend.safe_reload().await?;

                if self.backend.cluster().is_ok() {
                    Ok(ClusterCheck::Ok)
                } else {
                    self.error_response(
                        context,
                        ErrorResponse::connection(&identifier.user, &identifier.database),
                    )
                    .await?;
                    Ok(ClusterCheck::Offline)
                }
            } else {
                Ok(ClusterCheck::Ok)
            }
        } else {
            Ok(ClusterCheck::Ok)
        };

        if let Ok(ClusterCheck::Ok) = res {
            // Make sure schema is loaded before we throw traffic
            // at it. This matters for sharded deployments only.
            if let Ok(cluster) = self.backend.cluster() {
                timeout(
                    context.timeouts.query_timeout(&State::Active),
                    cluster.wait_schema_loaded(),
                )
                .await
                .map_err(|_| Error::SchemaLoad)?;
            }
            res
        } else {
            res
        }
    }

    pub(super) async fn route_query(
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

        let cluster = if let Ok(cluster) = self.backend.cluster() {
            cluster
        } else {
            return Ok(true);
        };

        let router_context = RouterContext::new(
            context.client_request,
            cluster,
            context.params,
            context.transaction,
            context.sticky,
        )?;

        match self.router.query(router_context) {
            Ok(command) => {
                context.client_request.route = Some(command.route().clone());
                trace!(
                    "routing {:#?} to {:#?}",
                    context.client_request.messages,
                    command,
                );

                // Apply post-parser rewrites, e.g. offset/limit.
                if let Some(ref rewrite_result) = &context.rewrite_result {
                    rewrite_result.apply_after_parser(context.client_request)?;
                }
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
