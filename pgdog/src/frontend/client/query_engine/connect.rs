use tokio::time::timeout;
use tracing::error;

use super::*;

impl QueryEngine {
    /// Connect to backend, if necessary.
    ///
    /// Return true if connected, false otherwise.
    pub(super) async fn connect(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<bool, Error> {
        if self.backend.connected() {
            return Ok(true);
        }

        let request = Request::new(self.client_id);
        let route = &context.client_request.route;

        self.stats.waiting(request.created_at);
        self.comms.stats(self.stats);

        let connected = match self.backend.connect(&request, &route).await {
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
                timeout(query_timeout, self.backend.link_client(&context.params)).await??;

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
}
