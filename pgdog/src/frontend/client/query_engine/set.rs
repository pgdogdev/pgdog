use crate::frontend::SetParam;
use crate::frontend::client::protocol::{Commands, Statement, Statements};
use crate::frontend::router::parameter_hints::{PGDOG_PIN, PGDOG_SHARD, PGDOG_SHARDING_KEY};
use crate::net::ErrorResponse;

use super::*;

/// Shard-targeting parameters that pick the destination shard for a query.
/// Changing them after we've already connected to a server would let subsequent
/// queries route to a different shard than the one we're pinned to, so they may
/// only be set before any query connects to a backend.
const SHARD_TARGETING_PARAMS: [&str; 2] = [PGDOG_SHARD, PGDOG_SHARDING_KEY];

impl QueryEngine {
    /// Handle a `SET` statement or equivalent `SELECT set_config([...])` query.
    pub(crate) async fn set(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        // FIXME(sage): Remove mut
        client_request: &mut ClientRequest,
        params: &[SetParam],
        set_config: bool,
    ) -> Result<(), Error> {
        // Make sure client isn't changing route mid-transaction.
        if self
            .route_change_check(context, client_request, params)
            .await?
        {
            return Ok(());
        }

        let mut statement = Statement::Set;
        for param in params {
            let is_pin = param.name == PGDOG_PIN;

            if let Some(value) = param.value.clone() {
                if context.in_transaction() {
                    context
                        .params
                        .insert_transaction(&param.name, value, param.local);
                } else {
                    context.params.insert(&param.name, value);
                    if is_pin {
                        self.manual_lock = param
                            .value
                            .as_ref()
                            .and_then(|p| p.as_str())
                            .map(|p| matches!(p, "true" | "t"))
                            .unwrap_or_default();
                    }
                }
            } else {
                statement = Statement::Reset;
                context.params.reset(&param.name);
                if is_pin {
                    self.manual_lock = false;
                }
            }
        }

        if !context.in_transaction() {
            self.comms.update_params(context.params);
        }

        if self.backend.connected() {
            self.execute(context, client_request, None).await?;
        } else {
            let mut response = Statements::builder()
                .maybe_transaction(context.transaction())
                .pipeline(&context.pipeline)
                .statement(statement)
                .build();
            if set_config {
                response = response
                    .columns(&["set_config"])
                    .with_row(params.iter().map(|param| param.value.as_ref()));
            }
            let (bytes_sent, _) = response
                .send_reply(client_request.messages.as_slice(), context.stream)
                .await?;
            self.stats.sent(bytes_sent);
        }

        Ok(())
    }

    /// Make sure the client isn't changing the route mid-transaction
    /// by issuing a `SET pgdog.shard` or `SET pgdog.sharding_key` command.
    async fn route_change_check(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        client_request: &ClientRequest,
        params: &[SetParam],
    ) -> Result<bool, Error> {
        if !self.backend.connected() {
            return Ok(false);
        }

        let Some(param) = params.iter().find(|param| {
            SHARD_TARGETING_PARAMS
                .iter()
                .any(|name| param.name.eq_ignore_ascii_case(name))
        }) else {
            return Ok(false);
        };

        self.error_response(
            context,
            client_request,
            ErrorResponse::set_shard_after_connect(&param.name),
        )
        .await?;

        Ok(true)
    }

    pub(crate) async fn reset_all(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        // FIXME(sage): Remove mut
        client_request: &mut ClientRequest,
    ) -> Result<(), Error> {
        if context.in_transaction() || self.backend.connected() {
            context.params.reset_all();
        } else {
            context.params.restore_startup(context.startup_params);
            self.comms.update_params(context.params);
        }

        if self.backend.connected() {
            self.execute(context, client_request, None).await?;
        } else {
            let response = Commands::builder()
                .maybe_transaction(context.transaction())
                .pipeline(&context.pipeline)
                .statement(Statement::Reset)
                .build();
            let (bytes_sent, _) = response
                .send_reply(client_request.messages.as_slice(), context.stream)
                .await?;
            self.stats.sent(bytes_sent);
        }

        Ok(())
    }
}
