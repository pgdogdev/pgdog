use tokio::time::timeout;

use super::*;

use tracing::error;

impl QueryEngine {
    /// Connect to backend, if necessary.
    ///
    /// Return true if connected, false otherwise.
    pub(super) async fn connect(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        route: &Route,
    ) -> Result<bool, Error> {
        if context.backend.connected() {
            return Ok(true);
        }

        let request = Request::new(context.client_id);

        context.stats.waiting(request.created_at);
        context.comms.stats(*context.stats);

        let connected = match context.backend.connect(&request, &route).await {
            Ok(_) => {
                context.stats.connected();
                context.stats.locked(route.lock_session());
                // This connection will be locked to this client
                // until they disconnect.
                //
                // Used in case the client runs an advisory lock
                // or another leaky transaction mode abstraction.
                context.backend.lock(route.lock_session());

                if let Ok(addr) = context.backend.addr() {
                    debug!(
                        "client paired with [{}] using route [{}] [{:.4}ms]",
                        addr.into_iter()
                            .map(|a| a.to_string())
                            .collect::<Vec<_>>()
                            .join(","),
                        route,
                        context.stats.wait_time.as_secs_f64() * 1000.0
                    );
                }

                let query_timeout = context.timeouts.query_timeout(&context.stats.state);
                // We may need to sync params with the server and that reads from the socket.
                timeout(query_timeout, context.backend.link_client(&context.params)).await??;

                true
            }

            Err(err) => {
                context.stats.error();

                if err.no_server() {
                    error!("{} [{:?}]", err, context.stream.peer_addr());
                    let bytes_sent = context
                        .stream
                        .error(ErrorResponse::from_err(&err), context.in_transaction)
                        .await?;
                    context.stats.sent(bytes_sent);
                    context.backend.disconnect();
                    self.router.reset();
                } else {
                    return Err(err.into());
                }

                false
            }
        };

        context.comms.stats(*context.stats);

        Ok(connected)
    }
}
