//! Cross-shard `HAVING`, evaluated by PostgreSQL after PgDog merges the groups.
//!
//! The shards return every group, with the `HAVING` operands in hidden
//! columns. Once the groups are merged, the operands are sent to one shard as
//! arrays, and the shard returns the positions of the groups that pass.

use std::collections::VecDeque;
use std::sync::Arc;

use crate::{
    backend::{
        Error as BackendError,
        pool::connection::{aggregate::Error as AggregateError, buffer::having::HavingInputs},
    },
    frontend::{
        ClientRequest,
        client::query_engine::{QueryEngine, QueryEngineContext},
        router::{
            Route,
            parser::{
                HavingRewritePlan, Shard, ShardWithPriority,
                rewrite::statement::{Error as RewriteError, projection::ProjectionRewritePlan},
            },
        },
    },
    net::{
        Bind, CommandComplete, ErrorResponse, Execute, FromBytes, Message, Parse, Protocol,
        ProtocolMessage, ToBytes, messages::Format,
    },
};

use super::Error;

pub(crate) struct HavingMulti<'a> {
    engine: &'a mut QueryEngine,
    plan: Arc<ProjectionRewritePlan>,
}

impl<'a> HavingMulti<'a> {
    pub(crate) fn new(engine: &'a mut QueryEngine, plan: Arc<ProjectionRewritePlan>) -> Self {
        Self { engine, plan }
    }

    pub(crate) async fn execute(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        client_request: &ClientRequest,
    ) -> Result<(), Error> {
        let engine = &mut *self.engine;
        engine
            .backend
            .handle_client_request(client_request, &mut engine.router, false)
            .await?;

        let mut messages = VecDeque::new();
        while engine.backend.has_more_messages() {
            match engine.read_server_message().await {
                Ok(message) => messages.push_back(message),
                Err(err) if err.is_unsupported() => {
                    if let Some(transaction) = context.transaction.as_mut() {
                        transaction.set_error();
                    }
                    return Ok(engine
                        .error_response(context, client_request, ErrorResponse::unsupported(&err))
                        .await?);
                }
                Err(err) => return Err(err.into()),
            }
        }

        let Some(inputs) = engine
            .backend
            .take_having_inputs()
            .filter(|inputs| inputs.rows() > 0)
        else {
            // No groups to filter, or the shards returned an error.
            for message in messages {
                engine.process_server_message(context, message).await?;
            }
            return Ok(());
        };

        let rows = messages
            .iter()
            .filter(|message| message.code() == 'D')
            .count();
        if rows != inputs.rows() {
            return Err(invalid(format!(
                "expected {} rows, received {rows}",
                inputs.rows()
            )));
        }

        let having = self
            .plan
            .having
            .as_deref()
            .expect("HAVING execution requires a HAVING plan")
            .rewrite();
        let keep = match filter(self.engine, having, &inputs).await {
            Ok(keep) => keep,
            Err(Error::Execution(error)) => {
                return self.forward_error(context, messages, *error).await;
            }
            Err(Error::Rewrite(err @ RewriteError::UnsupportedHaving(_))) => {
                return self
                    .forward_error(context, messages, ErrorResponse::unsupported(&err))
                    .await;
            }
            Err(err) => return Err(err),
        };

        let kept = keep.iter().filter(|&&keep| keep).count();
        let mut keep = keep.into_iter();
        for message in messages {
            let message = match message.code() {
                'D' => {
                    if !keep.next().unwrap_or(false) {
                        continue;
                    }
                    message
                }
                'C' => CommandComplete::from_bytes(message.to_bytes())?
                    .rewrite(kept)?
                    .message(),
                _ => message,
            };
            self.engine.process_server_message(context, message).await?;
        }

        Ok(())
    }

    /// Return PostgreSQL's error from the filter query after the messages
    /// that precede the rows, so the client sees the statement fail.
    async fn forward_error(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        messages: VecDeque<Message>,
        error: ErrorResponse,
    ) -> Result<(), Error> {
        let engine = &mut *self.engine;
        for message in messages {
            if !matches!(message.code(), 'D' | 'C' | 'Z') {
                engine.process_server_message(context, message).await?;
            }
        }
        if let Some(transaction) = context.transaction.as_mut() {
            transaction.set_error();
        }
        engine.cleanup_backend(context).await?;
        Ok(engine.send_error_response(context, error).await?)
    }
}

/// Run the `HAVING` filter over the merged groups and return, for each
/// group in order, whether it passes.
async fn filter(
    engine: &mut QueryEngine,
    having: &HavingRewritePlan,
    inputs: &HavingInputs,
) -> Result<Vec<bool>, Error> {
    let mut data_types = Vec::with_capacity(inputs.columns().len());
    let mut bind = Bind::default();
    for column in inputs.columns() {
        let array_oid = column.array_oid().ok_or(RewriteError::UnsupportedHaving(
            "operands must have common built-in types",
        ))?;
        data_types.push(array_oid);
        let (array, format) = column.array();
        bind.push_param(array, format);
    }

    let shard = engine
        .backend
        .any_connected_shard()
        .ok_or(BackendError::NotConnected)?;
    let mut request = ClientRequest::from(vec![
        ProtocolMessage::Parse(
            Parse::new_anonymous(having.filter_sql()).with_data_types(&data_types),
        ),
        ProtocolMessage::Bind(bind),
        ProtocolMessage::Execute(Execute::new()),
        ProtocolMessage::Sync(Default::default()),
    ]);
    request.route = Some(Route::read(ShardWithPriority::new_table(Shard::Direct(
        shard,
    ))));

    let (rows, _) = super::fetch_rows(engine, &request).await?;
    let mut keep = vec![false; inputs.rows()];
    for row in rows {
        let passed = row
            .get::<i64>(0, Format::Text)
            .and_then(|position| usize::try_from(position).ok()?.checked_sub(1))
            .and_then(|index| keep.get_mut(index))
            .ok_or_else(|| invalid("invalid HAVING row number".into()))?;
        *passed = true;
    }
    Ok(keep)
}

fn invalid(reason: String) -> Error {
    BackendError::from(AggregateError::InvalidHaving(reason)).into()
}
