use pgdog_config::PoolerMode;
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
        if let Ok(cluster) = self.backend.cluster() {
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
        }
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

        let cluster = if let Ok(cluster) = self.backend.cluster() {
            cluster
        } else {
            return Ok(true);
        };

        let router_context = RouterContext::new(
            context.client_request,
            cluster,
            context.params.search_path(),
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
