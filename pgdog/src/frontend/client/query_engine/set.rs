use crate::frontend::SetParam;
use crate::frontend::router::parameter_hints::{PGDOG_SHARD, PGDOG_SHARDING_KEY};
use crate::net::messages::ErrorResponse;

use super::*;

/// Shard-targeting parameters that pick the destination shard for a query.
/// Changing them after we've already connected to a server would let subsequent
/// queries route to a different shard than the one we're pinned to, so they may
/// only be set before any query connects to a backend.
const SHARD_TARGETING_PARAMS: [&str; 2] = [PGDOG_SHARD, PGDOG_SHARDING_KEY];

impl QueryEngine {
    pub(crate) async fn set(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        params: &[SetParam],
        behave_like_select: bool,
    ) -> Result<(), Error> {
        // Make sure client isn't changing route mid-transaction.
        if self.route_change_check(context, params).await? {
            return Ok(());
        }

        let mut fake_command = "SET";
        for param in params {
            if let Some(value) = param.value.clone() {
                if context.in_transaction() {
                    context
                        .params
                        .insert_transaction(&param.name, value, param.local);
                } else {
                    context.params.insert(&param.name, value);
                }
            } else {
                fake_command = "RESET";
                context.params.reset(&param.name);
            }
        }

        if !context.in_transaction() {
            self.comms.update_params(context.params);
        }

        if self.backend.connected() {
            self.execute(context).await?;
        } else {
            let values_to_return =
                behave_like_select.then(|| params.iter().map(|p| p.value.as_ref()));
            self.fake_command_response(context, fake_command, values_to_return)
                .await?;
        }

        Ok(())
    }

    /// Make sure the client isn't changing the route mid-transaction
    /// by issuing a `SET pgdog.shard` or `SET pgdog.sharding_key` command.
    async fn route_change_check(
        &mut self,
        context: &mut QueryEngineContext<'_>,
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

        self.error_response(context, ErrorResponse::set_shard_after_connect(&param.name))
            .await?;

        Ok(true)
    }

    pub(crate) async fn reset_all(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        context.params.reset_all();

        if self.backend.connected() {
            self.execute(context).await?;
        } else {
            self.fake_command_response(context, "RESET", None::<Option<_>>)
                .await?;
        }

        Ok(())
    }
}
