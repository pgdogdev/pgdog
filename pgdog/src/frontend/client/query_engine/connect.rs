use crate::util::safe_timeout;

use super::*;

use tracing::error;

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
        connect_route: &Route,
    ) -> Result<bool, Error> {
        if self.backend.required_shards_connected(connect_route)? {
            self.debug_connected(connect_route, true);
            return Ok(true);
        }

        // Pass through the cluster's `read_only` flag (propagated from `User.read_only`)
        // to determine if we should exclude the primary from being allowed to read.
        let read_only = self.backend.cluster()?.read_only();
        let request = Request::new(context.id, connect_route.is_read(), read_only);

        self.stats.waiting(request.created_at);
        self.comms.update_stats(self.stats);

        let connected = match self.backend.connect(&request, connect_route).await {
            Ok(_) => {
                self.stats.connected();
                self.debug_connected(connect_route, false);

                let query_timeout = context.timeouts.query_timeout(&self.stats.state);

                // We may need to sync params with the server and that reads from the socket.
                safe_timeout(
                    query_timeout,
                    self.backend.link_client(context.id, context.params),
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
                    .unwrap_or_default()
                    && !context.in_transaction();

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

    fn debug_connected(&self, route: &Route, connected: bool) {
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
                route,
                self.stats.wait_time.as_secs_f64() * 1000.0
            );
        }
    }
}
