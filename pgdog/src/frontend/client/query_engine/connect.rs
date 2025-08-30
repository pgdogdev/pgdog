use tokio::time::timeout;

use super::*;

use tracing::{error, trace};

impl QueryEngine {
    /// Connect to backend, if necessary.
    ///
    /// Return true if connected, false otherwise.
    pub(super) async fn connect(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        route: &Route,
    ) -> Result<bool, Error> {
        if self.backend.connected() {
            return Ok(true);
        }

        let request = Request::new(self.client_id);

        self.stats.waiting(request.created_at);
        self.comms.stats(self.stats);

        let connected = match self.backend.connect(&request, route).await {
            Ok(_) => {
                self.stats.connected();
                self.stats.locked(route.lock_session());
                // This connection will be locked to this client
                // until they disconnect.
                //
                // Used in case the client runs an advisory lock
                // or another leaky transaction mode abstraction.
                self.backend.lock(route.lock_session());

                if let Ok(addr) = self.backend.addr() {
                    debug!(
                        "client paired with [{}] using route [{}] [{:.4}ms]",
                        addr.into_iter()
                            .map(|a| a.to_string())
                            .collect::<Vec<_>>()
                            .join(","),
                        route,
                        self.stats.wait_time.as_secs_f64() * 1000.0
                    );
                }

                let query_timeout = context.timeouts.query_timeout(&self.stats.state);
                // We may need to sync params with the server and that reads from the socket.
                timeout(query_timeout, self.backend.link_client(context.params)).await??;

                true
            }

            Err(err) => {
                self.stats.error();

                if err.no_server() {
                    error!("{} [{:?}]", err, context.stream.peer_addr());
                    let bytes_sent = context
                        .stream
                        .error(ErrorResponse::from_err(&err), context.in_transaction())
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

        self.comms.stats(self.stats);

        Ok(connected)
    }

    /// Connect to serve a transaction.
    pub(super) async fn connect_transaction(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        route: &Route,
    ) -> Result<bool, Error> {
        debug!("connecting to backend(s) to serve transaction");

        let route = self.transaction_route(route)?;

        trace!("transaction routing to {:#?}", route);

        self.connect(context, &route).await
    }

    pub(super) fn transaction_route(&self, route: &Route) -> Result<Route, Error> {
        let cluster = self.backend.cluster()?;

        if cluster.shards().len() == 1 {
            Ok(Route::write(Shard::Direct(0)).set_read(route.is_read()))
        } else {
            Ok(Route::write(Shard::All).set_read(route.is_read()))
        }
    }
}
