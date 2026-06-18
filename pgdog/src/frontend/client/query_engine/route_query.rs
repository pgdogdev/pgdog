use pgdog_config::PoolerMode;
use tokio::time::timeout;
use tracing::trace;

use crate::backend::Cluster;

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
        let res = match self.backend.cluster() {
            Ok(cluster) => {
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
            }
            _ => Ok(ClusterCheck::Ok),
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

        let cluster = match self.backend.cluster() {
            Ok(cluster) => cluster,
            _ => {
                return Ok(true);
            }
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
                    context.client_request.messages, command,
                );

                // Apply post-parser rewrites, e.g. offset/limit.
                if let Some(rewrite_result) = &context.rewrite_result {
                    rewrite_result.apply_after_parser(context.client_request)?;
                }

                // Only validate shard placement for requests that actually execute
                // a query. Bare protocol-control batches (e.g. a lone Sync or Flush)
                // route to a default/cross-shard target but must still be forwarded
                // to the already-connected backend to finish the exchange.
                if context.client_request.is_executable() {
                    if Self::is_omnishard_unsafe(&self.backend, command, cluster) {
                        self.error_response(context, ErrorResponse::omni_in_direct_to_shard())
                            .await?;
                        return Ok(false);
                    }

                    if Self::is_shard_switch(command, &self.backend) {
                        self.error_response(context, ErrorResponse::direct_shard_mismatch())
                            .await?;
                        return Ok(false);
                    }
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

    // Make sure we don't send an omni write to a direct-to-shard route.
    // This will cause omni data inconsistency.
    fn is_omnishard_unsafe(backend: &Connection, command: &Command, cluster: &Cluster) -> bool {
        command.route().is_omnisharded()
            && command.route().is_write()
            && backend.connected() // FIXME(lev): I wish there was a way to say >0 and <n in one shot.
            && backend.connected_servers() < cluster.shards().len()
    }

    // Caller switched shards mid-transaction and the transaction is pinned
    // to one shard only.
    fn is_shard_switch(command: &Command, backend: &Connection) -> bool {
        if let Shard::Direct(shard) = command.route().shard() {
            // Round robin doesn't matter, any shard
            // can answer that query.
            if command
                .route()
                .shard_with_priority()
                .source()
                .is_round_robin()
            {
                return false;
            }
            // Session mode shouldn't trigger any checks,
            // you're on your own here.
            if backend.session_mode() {
                return false;
            }
            if let Some(connected_shard) = backend.direct_shard_number()
                && *shard != connected_shard
            {
                return true;
            }
        } else if let Command::Query(route) = command {
            // Tried to run a cross-shard query while connected to one shard only.
            if route.is_cross_shard() && backend.direct_shard_number().is_some() {
                return true;
            }
        }

        false
    }
}
