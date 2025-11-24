use tokio::time::timeout;

use crate::{
    frontend::{client::TransactionType, router::parser::explain_trace::ExplainTrace},
    net::{
        DataRow, FromBytes, Message, Protocol, ProtocolMessage, Query, ReadyForQuery,
        RowDescription, ToBytes, TransactionState,
    },
    state::State,
};

use tracing::{debug, error};

use super::*;

impl QueryEngine {
    /// Handle query from client.
    pub(super) async fn execute(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        route: &Route,
    ) -> Result<(), Error> {
        // Check that we're not in a transaction error state.
        if !self.transaction_error_check(context, route).await? {
            return Ok(());
        }

        // Check if we need to do 2pc automatically
        // for single-statement writes.
        self.two_pc_check(context, route);

        // We need to run a query now.
        if let Some(begin_stmt) = self.begin_stmt.take() {
            // Connect to one shard if not sharded or to all shards
            // for a cross-shard tranasction.
            if !self.connect_transaction(context, route).await? {
                return Ok(());
            }

            self.backend.execute(begin_stmt.query()).await?;
        } else if !self.connect(context, route).await? {
            return Ok(());
        }

        // Check we can run this query.
        if !self.cross_shard_check(context, route).await? {
            return Ok(());
        }

        if let Some(sql) = route.rewritten_sql() {
            match context.client_request.rewrite(&[Query::new(sql).into()]) {
                Ok(()) => (),
                Err(crate::net::Error::OnlySimpleForRewrites) => {
                    context.client_request.rewrite_prepared(
                        sql,
                        context.prepared_statements,
                        route.rewrite_plan(),
                    );
                }
                Err(err) => return Err(err.into()),
            }
        }

        // Set response format.
        for msg in context.client_request.messages.iter() {
            if let ProtocolMessage::Bind(bind) = msg {
                self.backend.bind(bind)?
            }
        }

        self.hooks.after_connected(context, &self.backend)?;

        self.backend
            .handle_client_request(context.client_request, &mut self.router, self.streaming)
            .await?;

        while self.backend.has_more_messages()
            && !self.backend.copy_mode()
            && !self.streaming
            && !self.test_mode
        {
            let message = match timeout(
                context.timeouts.query_timeout(&State::Active),
                self.backend.read(),
            )
            .await
            {
                Ok(response) => response?,
                Err(err) => {
                    // Close the conn, it could be stuck executing a query
                    // or dead.
                    self.backend.force_close();
                    return Err(err.into());
                }
            };
            self.server_message(context, message).await?;
        }

        Ok(())
    }

    pub async fn server_message(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        message: Message,
    ) -> Result<(), Error> {
        self.streaming = message.streaming();

        let code = message.code();
        let payload = if code == 'T' {
            Some(message.payload())
        } else {
            None
        };
        let mut message = message.backend();
        let has_more_messages = self.backend.has_more_messages();

        if let Some(bytes) = payload {
            if let Some(state) = self.pending_explain.as_mut() {
                if let Ok(row_description) = RowDescription::from_bytes(bytes) {
                    state.capture_row_description(row_description);
                } else {
                    state.annotated = true;
                }
            }
        }

        if code == 'C' {
            self.emit_explain_rows(context).await?;
        }

        if code == 'E' {
            if let Some(state) = self.pending_explain.as_mut() {
                state.annotated = true;
            }
            self.pending_explain = None;
        }

        // Messages that we need to send to the client immediately.
        // ReadyForQuery (B) | CopyInResponse (B) | ErrorResponse(B) | NoticeResponse(B) | NotificationResponse (B)
        let flush = matches!(code, 'Z' | 'G' | 'E' | 'N' | 'A')
            || !has_more_messages
            || message.streaming();

        // Server finished executing a query.
        // ReadyForQuery (B)
        if code == 'Z' {
            self.stats.query();

            let mut two_pc_auto = false;
            let state = ReadyForQuery::from_bytes(message.to_bytes()?)?.state()?;

            match state {
                TransactionState::Error => {
                    let error_state = match context.transaction {
                        Some(TransactionType::ReadOnly) => Some(TransactionType::ErrorReadOnly),
                        Some(TransactionType::ReadWrite) => Some(TransactionType::ErrorReadWrite),
                        _ => None,
                    };
                    context.transaction = error_state;
                    if self.two_pc.auto() {
                        self.end_two_pc(true).await?;
                        // TODO: this records a 2pc transaction in client
                        // stats anyway but not on the servers. Is this what we want?
                        two_pc_auto = true;
                    }
                }

                TransactionState::Idle => {
                    context.transaction = None;
                }

                TransactionState::InTrasaction => {
                    if self.two_pc.auto() {
                        self.end_two_pc(false).await?;
                        two_pc_auto = true;
                    }
                    match context.transaction {
                        // Query parser is disabled, so the server is responsible for telling us
                        // we started a transaction.
                        None => {
                            context.transaction = Some(TransactionType::ReadWrite);
                        }

                        // Restore transaction state after rollback to savepoint.
                        Some(TransactionType::ErrorReadOnly) => {
                            context.transaction = Some(TransactionType::ReadOnly);
                        }

                        Some(TransactionType::ErrorReadWrite) => {
                            context.transaction = Some(TransactionType::ReadWrite);
                        }

                        _ => (),
                    }
                }
            }

            if two_pc_auto {
                // In auto mode, 2pc transaction was started automatically
                // without the client's knowledge. We need to return a regular RFQ
                // message and close the transaction.
                context.transaction = None;
                message = ReadyForQuery::in_transaction(false).message()?;
            }

            self.stats.idle(context.in_transaction());

            if !context.in_transaction() {
                self.stats.transaction(two_pc_auto);
            }
        }

        self.stats.sent(message.len());

        // Do this before flushing, because flushing can take time.
        self.cleanup_backend(context);

        if flush {
            context.stream.send_flush(&message).await?;
        } else {
            context.stream.send(&message).await?;
        }

        if code == 'Z' {
            self.pending_explain = None;
        }
        self.hooks.on_server_message(context, &message)?;

        Ok(())
    }

    async fn emit_explain_rows(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        if let Some(state) = self.pending_explain.as_mut() {
            if !state.should_emit() {
                return Ok(());
            }

            if state.row_description.is_none() {
                return Ok(());
            }

            for line in state.lines.clone() {
                let mut row = DataRow::new();
                row.add(line);
                let message = row.message()?;
                let len = message.len();
                context.stream.send(&message).await?;
                self.stats.sent(len);
            }

            state.annotated = true;
        }

        Ok(())
    }

    pub(super) fn cleanup_backend(&mut self, context: &mut QueryEngineContext<'_>) {
        if self.backend.done() {
            let changed_params = self.backend.changed_params();

            // Release the connection back into the pool before flushing data to client.
            // Flushing can take a minute and we don't want to block the connection from being reused.
            if !self.backend.session_mode() && context.requests_left == 0 {
                self.backend.disconnect();
            }

            self.router.reset();

            debug!(
                "transaction finished [{:.3}ms]",
                self.stats.last_transaction_time.as_secs_f64() * 1000.0
            );

            // Update client params with values
            // sent from the server using ParameterStatus(B) messages.
            if !changed_params.is_empty() {
                for (name, value) in changed_params.iter() {
                    debug!("setting client's \"{}\" to {}", name, value);
                    context.params.insert(name.clone(), value.clone());
                }
                self.comms.update_params(context.params);
            }
        }
    }

    // Perform cross-shard check.
    async fn cross_shard_check(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        route: &Route,
    ) -> Result<bool, Error> {
        // Check for cross-shard queries.
        if context.cross_shard_disabled.is_none() {
            context.cross_shard_disabled = Some(
                self.backend
                    .cluster()
                    .map(|c| c.cross_shard_disabled())
                    .unwrap_or_default(),
            );
        }

        let cross_shard_disabled = context.cross_shard_disabled.unwrap_or_default();

        debug!("cross-shard queries disabled: {}", cross_shard_disabled,);

        if cross_shard_disabled
            && route.is_cross_shard()
            && !context.admin
            && context.client_request.executable()
        {
            let error = ErrorResponse::cross_shard_disabled();

            self.error_response(context, error).await?;

            if self.backend.connected() && self.backend.done() {
                self.backend.disconnect();
            }

            Ok(false)
        } else {
            Ok(true)
        }
    }

    fn two_pc_check(&mut self, context: &mut QueryEngineContext<'_>, route: &Route) {
        let enabled = self
            .backend
            .cluster()
            .map(|c| c.two_pc_auto_enabled())
            .unwrap_or_default();

        if enabled
            && route.should_2pc()
            && self.begin_stmt.is_none()
            && context.client_request.executable()
            && !context.in_transaction()
        {
            debug!("[2pc] enabling automatic transaction");
            self.two_pc.set_auto();
            self.begin_stmt = Some(BufferedQuery::Query(Query::new("BEGIN")));
        }
    }

    async fn transaction_error_check(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        route: &Route,
    ) -> Result<bool, Error> {
        let shards = if let Ok(shards) = self.backend.shards() {
            shards
        } else {
            return Ok(true);
        };
        if shards > 1 // This check only matters for cross-shard queries
            && context.in_error()
            && !context.rollback
            && context.client_request.executable()
            && !route.rollback_savepoint()
        {
            let error = ErrorResponse::in_failed_transaction();

            self.error_response(context, error).await?;

            Ok(false)
        } else {
            Ok(true)
        }
    }

    pub(super) async fn error_response(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        error: ErrorResponse,
    ) -> Result<(), Error> {
        error!("{:?} [{:?}]", error.message, context.stream.peer_addr());

        self.hooks.on_engine_error(context, &error)?;

        let bytes_sent = context
            .stream
            .error(error, context.in_transaction())
            .await?;
        self.stats.sent(bytes_sent);

        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
pub(super) struct ExplainResponseState {
    lines: Vec<String>,
    row_description: Option<RowDescription>,
    annotated: bool,
    supported: bool,
}

impl ExplainResponseState {
    pub fn new(trace: ExplainTrace) -> Self {
        Self {
            lines: trace.render_lines(),
            row_description: None,
            annotated: false,
            supported: false,
        }
    }

    pub fn capture_row_description(&mut self, row_description: RowDescription) {
        self.supported = row_description.fields.len() == 1
            && matches!(row_description.field(0).map(|f| f.type_oid), Some(25));
        if self.supported {
            self.row_description = Some(row_description);
        } else {
            self.annotated = true;
        }
    }

    pub fn should_emit(&self) -> bool {
        self.supported && !self.annotated
    }
}
