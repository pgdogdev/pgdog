use crate::frontend::SetParam;
use crate::frontend::client::query_engine::fake::{FakeCommand, FakeResponse};
use crate::frontend::router::parameter_hints::{PGDOG_PIN, PGDOG_SHARD, PGDOG_SHARDING_KEY};
use crate::net::messages::{ErrorResponse, NoticeResponse};
use crate::parameters::{ParameterNormalizer, ParameterValidator};

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

        // Validate and normalize before changing session state.
        let mut params = params.to_vec();
        let mut normalizer = ParameterNormalizer::new(context.params);
        let error = params
            .iter_mut()
            .filter(|param| context.in_transaction() || !param.local)
            .find_map(|param| {
                ParameterValidator::validate(&*param)
                    .or_else(|| normalizer.normalize_datestyle(param))
            });
        if let Some(error) = error {
            if let Some(transaction) = context.transaction.as_mut() {
                transaction.set_error();
            }
            self.send_error_response(context, error).await?;
            return Ok(());
        }

        let commands = params
            .iter()
            .map(|param| self.handle_parameter(context, param, set_config))
            .collect::<Vec<_>>();

        if !context.in_transaction() {
            self.comms.update_params(context.params);
        }

        if self.backend.connected() {
            self.execute(context, client_request, None).await?;
        } else {
            self.fake_command_response(
                context,
                &client_request.messages,
                &FakeResponse::new(commands),
            )
            .await?;
        }

        Ok(())
    }

    /// Apply a validated parameter and build its command response.
    fn handle_parameter(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        param: &SetParam,
        set_config: bool,
    ) -> FakeCommand {
        let tag = if set_config {
            "SELECT 1"
        } else if param.value.is_some() {
            "SET"
        } else {
            "RESET"
        };
        let mut command = FakeCommand::new(tag);
        if set_config {
            command = command.with_params(&["set_config"], [param.value.as_ref()]);
        }

        if !context.in_transaction() && param.local {
            return command.with_notice(NoticeResponse::from(
                ErrorResponse::set_local_outside_transaction(),
            ));
        }

        let previous = context.params.get(&param.name).cloned();
        let is_pin = param.name.eq_ignore_ascii_case(PGDOG_PIN);

        if let Some(value) = param.value.clone() {
            if param.local {
                context.params.set_local(&param.name, value);
            } else {
                context.params.set(&param.name, value);

                // Pin the backend to this client.
                if is_pin && !context.in_transaction() {
                    self.manual_lock = param
                        .value
                        .as_ref()
                        .and_then(|p| p.as_str())
                        .is_some_and(|p| matches!(p, "true" | "t"));
                }
            }
        } else {
            context.params.reset(&param.name);
            if is_pin {
                self.manual_lock = false;
            }
        }

        if let Some(status) =
            ParameterValidator::notify_client(param, previous.as_ref(), context.params)
        {
            command = command.with_parameter_status(status);
        }

        command
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
            (context.in_transaction() || !param.local)
                && SHARD_TARGETING_PARAMS
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
            context.params.restore_startup();
            self.comms.update_params(context.params);
        }

        if self.backend.connected() {
            self.execute(context, client_request, None).await?;
        } else {
            self.fake_command_response(
                context,
                &client_request.messages,
                &FakeResponse::command("RESET"),
            )
            .await?;
        }

        Ok(())
    }
}
