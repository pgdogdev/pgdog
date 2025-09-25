use tracing::{error, trace};

use super::*;
use crate::frontend::router::{parser::Error as ParserError, Error as RouterError};

impl QueryEngine {
    pub(super) async fn route_query(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<bool, Error> {
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
        )?;
        match self.router.query(router_context) {
            Ok(cmd) => {
                trace!(
                    "routing {:#?} to {:#?}",
                    context.client_request.messages,
                    cmd
                );
            }
            // Query intercepted by plugin.
            Err(RouterError::Parser(ParserError::ErrorResponse(err))) => {
                self.stats
                    .sent(context.stream.error(err, context.in_transaction()).await?);
                return Ok(false);
            }
            Err(err) => {
                error!("{:?} [{:?}]", err, context.stream.peer_addr());
                let bytes_sent = context
                    .stream
                    .error(
                        ErrorResponse::syntax(err.to_string().as_str()),
                        context.in_transaction(),
                    )
                    .await?;
                self.stats.sent(bytes_sent);
                return Ok(false);
            }
        }

        Ok(true)
    }
}
