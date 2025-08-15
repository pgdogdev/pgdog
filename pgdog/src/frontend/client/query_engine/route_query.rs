use crate::net::{EmptyQueryResponse, ReadyForQuery};
use tracing::error;

use super::*;

impl QueryEngine {
    pub(super) async fn route_transaction(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<bool, Error> {
        // Route request if we haven't already.
        if self.router.routed() {
            return Ok(true);
        }

        // Admin doesn't have a cluster.
        let cluster = if let Ok(cluster) = self.backend.cluster() {
            cluster
        } else {
            return Ok(true);
        };

        let router_context = RouterContext::new(
            context.buffer,
            cluster,
            context.prepared_statements,
            context.params,
            context.in_transaction,
        )?;
        match self.router.query(router_context) {
            Ok(_) => (),
            Err(err) => {
                if err.empty_query() {
                    let mut bytes_sent = context.stream.send(&EmptyQueryResponse).await?;
                    bytes_sent += context
                        .stream
                        .send_flush(&ReadyForQuery::in_transaction(context.in_transaction))
                        .await?;
                    self.stats.sent(bytes_sent);
                } else {
                    error!("{:?} [{:?}]", err, context.stream.peer_addr());
                    let bytes_sent = context
                        .stream
                        .error(
                            ErrorResponse::syntax(err.to_string().as_str()),
                            context.in_transaction,
                        )
                        .await?;
                    self.stats.sent(bytes_sent);
                }
                return Ok(false);
            }
        }

        Ok(true)
    }
}
