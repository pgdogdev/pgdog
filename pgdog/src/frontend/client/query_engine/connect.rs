use tokio::time::timeout;

use crate::frontend::router::parser::{ShardWithPriority, route::ShardSource};

use super::*;

use tracing::{error, trace};

impl QueryEngine {
    /// Connect to backend, if necessary.
    ///
    /// Return true if connected, false otherwise.
    ///
    /// # Arguments
    ///
    /// - context: Query engine context.
    /// - connect_route: Override which route to use for connecting to backend(s).
    ///   Used to connect to all shards for an explicit cross-shard transaction
    ///   started with `BEGIN`.
    ///
    pub(super) async fn connect(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        connect_route: Option<&Route>,
    ) -> Result<bool, Error> {
        if self.backend.connected() {
            self.debug_connected(context, true);
            return Ok(true);
        }

        let connect_route = connect_route.unwrap_or(context.client_request.route());

        let request = Request::new(context.id, connect_route.is_read());

        self.stats.waiting(request.created_at);
        self.comms.update_stats(self.stats);

        let connected = match self.backend.connect(&request, connect_route).await {
            Ok(_) => {
                self.stats.connected();
                self.debug_connected(context, false);

                let query_timeout = context.timeouts.query_timeout(&self.stats.state);
                let begin_stmt = self.begin_stmt.take();

                // We may need to sync params with the server and that reads from the socket.
                // If application_name_add_host is enabled, append client address to application_name.
                // This matches PgBouncer behavior: only applied at connection start, not on SET.
                if crate::config::config()
                    .config
                    .general
                    .application_name_add_host
                {
                    if let Some(addr) = *context.stream.peer_addr() {
                        let base = context
                            .params
                            .get_default("application_name", "PgDog")
                            .to_owned();
                        let with_host = format!("{} ({})", base, addr);
                        context
                            .params
                            .insert("application_name", with_host.as_str());
                    }
                }
                timeout(
                    query_timeout,
                    self.backend.link_client(
                        context.id,
                        context.params,
                        begin_stmt.as_ref().map(|stmt| stmt.query()),
                    ),
                )
                .await??;

                true
            }

            Err(err) => {
                self.stats.error();
                let can_recover = self
                    .backend
                    .cluster()
                    .map(|cluster| cluster.client_connection_recovery().can_recover())
                    .unwrap_or_default();

                if err.no_server() && can_recover {
                    error!("{} [{:?}]", err, context.stream.peer_addr());

                    let error = ErrorResponse::from_err(&err);

                    self.hooks.on_engine_error(context, &error)?;

                    let bytes_sent = context
                        .stream
                        .error(error, context.in_transaction())
                        .await?;

                    self.stats.sent(bytes_sent);
                    self.backend.disconnect();
                    self.router.reset();
                } else {
                    return Err(err.into());
                }

                false
            }
        };

        self.comms.update_stats(self.stats);

        Ok(connected)
    }

    /// Connect to serve a transaction.
    pub(super) async fn connect_transaction(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<bool, Error> {
        debug!("connecting to backend(s) to serve transaction");

        let route = self.transaction_route(context.client_request.route())?;

        trace!("transaction routing to {:#?}", route);

        self.connect(context, Some(&route)).await
    }

    /// Return a route for the transaction statement.
    ///
    /// This determines how many shards we connect to when the transaction is started.
    /// Typically, that's going to be all shards since we can't determine which shards we
    /// will need in advance _and_ we don't currently support lazy connect.
    ///
    /// TODO(lev): Add support for lazily connecting to shards as needed.
    ///
    /// Some notable exceptions:
    ///
    /// 1. Deployment is not sharded
    /// 2. Deployment uses schema-based sharding, and will talk to one shard per transaction, always
    /// 3. Caller used `SET` to specify shard/sharding key for the transaction
    ///
    pub(super) fn transaction_route(&mut self, route: &Route) -> Result<Route, Error> {
        let cluster = self.backend.cluster()?;

        if cluster.shards().len() == 1 {
            Ok(
                Route::write(ShardWithPriority::new_override_transaction(Shard::Direct(
                    0,
                )))
                .with_read(route.is_read()),
            )
        } else if route.is_search_path_driven()
            || route.shard_with_priority().source() == &ShardSource::Set
        {
            // - Schema-based routing will only go to one shard.
            // - SET is used to pick one shard, either with pgdog.shard or pgdog.sharding_key.
            Ok(route.clone())
        } else {
            Ok(
                Route::write(ShardWithPriority::new_override_transaction(Shard::All))
                    .with_read(route.is_read()),
            )
        }
    }

    fn debug_connected(&self, context: &QueryEngineContext<'_>, connected: bool) {
        if let Ok(addr) = self.backend.addr() {
            debug!(
                "{} [{}] using route [{}] [{:.4}ms]",
                if connected {
                    "already connected to"
                } else {
                    "client paired with"
                },
                addr.into_iter()
                    .map(|a| a.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
                context.client_request.route(),
                self.stats.wait_time.as_secs_f64() * 1000.0
            );
        }
    }
}
