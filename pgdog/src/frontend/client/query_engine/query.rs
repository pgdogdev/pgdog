use tracing::{info, trace};

use crate::{
    frontend::{
        client::{Transaction, TransactionSource, TransactionType},
        router::parser::{explain_trace::ExplainTrace, rewrite::statement::plan::RewriteResult},
    },
    net::{
        DataRow, FromBytes, Message, Protocol, ProtocolMessage, Query, ReadyForQuery,
        RowDescription, ToBytes, TransactionState,
    },
    state::State,
    util::safe_timeout,
};

use tracing::{debug, error};

use super::hooks::schema::schema_changed;
use super::*;

impl QueryEngine {
    /// Handle query from client.
    pub(super) async fn execute(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        // Check that we're not in a transaction error state.
        if !self.transaction_error_check(context).await? {
            return Ok(());
        }

        // Check if we need to do 2pc automatically
        // for single-statement writes.
        self.auto_transaction_check(context);

        // We need to run a query now.
        if context.in_transaction() {
            // Connect to one shard if not sharded or to all shards
            // for a cross-shard tranasction.
            if !self.connect_transaction(context).await? {
                return Ok(());
            }
        } else if !self.connect(context, None).await? {
            return Ok(());
        }

        // Check we can run this query.
        if !self.cross_shard_check(context).await? {
            return Ok(());
        }

        self.hooks.after_connected(context, &self.backend)?;

        // Set response format.
        for msg in context.client_request.messages.iter() {
            if let ProtocolMessage::Bind(bind) = msg {
                self.backend.bind(bind)?
            }
        }

        let query_timeout = context.timeouts.query_timeout(&State::Active);
        let result = safe_timeout(query_timeout, self.client_server_exchange(context)).await;

        match result {
            Ok(response) => response?,
            Err(err) => {
                // Close the conn, it could be stuck executing a query
                // or dead.
                self.backend.force_close();
                return Err(err.into());
            }
        }

        Ok(())
    }

    async fn client_server_exchange(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        match context.rewrite_result.take() {
            Some(RewriteResult::InsertSplit(requests)) => {
                Box::pin(multi_step::InsertMulti::from_engine(self, requests).execute(context))
                    .await?;
            }

            Some(RewriteResult::InPlace { .. }) | None => {
                self.backend
                    .handle_client_request(context.client_request, &mut self.router, self.streaming)
                    .await?;

                while self.backend.has_more_messages()
                    && !self.backend.in_copy_mode()
                    && !self.streaming
                {
                    let message = self.read_server_message().await?;
                    self.process_server_message(context, message).await?;
                }
            }

            Some(RewriteResult::ShardingKeyUpdate(sharding_key_update)) => {
                Box::pin(multi_step::UpdateMulti::new(self, &sharding_key_update).execute(context))
                    .await?;
            }
        }

        Ok(())
    }

    pub async fn read_server_message(&mut self) -> Result<Message, Error> {
        Ok(self.backend.read().await?)
    }

    pub async fn process_server_message(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        mut message: Message,
    ) -> Result<(), Error> {
        self.streaming = message.streaming();

        let code = message.code();
        let payload = if code == 'T' {
            Some(message.payload())
        } else {
            None
        };
        let has_more_messages = self.backend.has_more_messages();

        if let Some(bytes) = payload
            && let Some(state) = self.pending_explain.as_mut()
        {
            match RowDescription::from_bytes(bytes) {
                Ok(row_description) => {
                    state.capture_row_description(row_description);
                }
                _ => {
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

            let mut replace_rfq = false;
            let state = ReadyForQuery::from_bytes(message.to_bytes())?.state()?;

            match state {
                TransactionState::Error => {
                    context.transaction = if let Some(transaction) = context.transaction {
                        match transaction.transaction_type {
                            TransactionType::ReadOnly => Some(Transaction {
                                source: transaction.source,
                                transaction_type: TransactionType::ErrorReadOnly,
                            }),
                            TransactionType::ReadWrite | TransactionType::Implicit => {
                                Some(Transaction {
                                    transaction_type: TransactionType::ErrorReadWrite,
                                    source: transaction.source,
                                })
                            }

                            _ => None,
                        }
                    } else {
                        None
                    };

                    let is_automatic = context
                        .transaction
                        .as_ref()
                        .map(|txn| txn.is_automatic())
                        .unwrap_or_default();

                    // Only automatically rollback transactions if they were started by the client.
                    if is_automatic {
                        let end_two_pc = self.two_pc.is_auto();
                        let end_multi_query = context.in_multi_query_request;

                        if end_two_pc {
                            self.end_two_pc(true).await?;
                            replace_rfq = true;
                        } else if end_multi_query {
                            self.backend.execute("ROLLBACK").await?;
                            replace_rfq = true;
                        }

                        if end_multi_query {
                            self.pipeline_error = true;
                        }
                    }
                }

                TransactionState::Idle => {
                    context.transaction = None;
                }

                TransactionState::InTrasaction => {
                    let end_two_pc = self.two_pc.is_auto()
                        && (!context.in_multi_query_request || !context.more_requests_pending);
                    let end_multi_query =
                        context.in_multi_query_request && !context.more_requests_pending;

                    if end_two_pc {
                        self.end_two_pc(false).await?;
                        replace_rfq = true;
                    } else if end_multi_query {
                        self.backend.execute("COMMIT").await?;
                        replace_rfq = true;
                    }

                    let source = if self.two_pc.is_auto() || context.in_multi_query_request {
                        TransactionSource::Automatic
                    } else {
                        TransactionSource::Client
                    };

                    match context.transaction.as_deref() {
                        // Query parser is disabled, so the server is responsible for telling us
                        // we started a transaction.
                        None => {
                            context.transaction = Some(Transaction {
                                transaction_type: TransactionType::ReadWrite,
                                source,
                            });
                        }

                        // Restore transaction state after rollback to savepoint.
                        Some(TransactionType::ErrorReadOnly) => {
                            context.transaction = Some(Transaction {
                                transaction_type: TransactionType::ReadOnly,
                                source,
                            });
                        }

                        Some(TransactionType::ErrorReadWrite) => {
                            context.transaction = Some(Transaction {
                                transaction_type: TransactionType::ReadWrite,
                                source,
                            });
                        }

                        _ => (),
                    }
                }
            }

            if replace_rfq {
                // A transaction was started automatically
                // without the client's knowledge. We need to return a regular RFQ
                // message instead.
                context.transaction = None;
                message = ReadyForQuery::in_transaction(false).message();
            }

            self.stats.idle(context.in_transaction());
            // N.B. Call this before self.cleanup_backend(), since `cleanup_backend()` resets
            // the router and the command state.
            self.advisory_locks
                .merge(self.router.command().route().advisory_locks());

            self.check_lock();

            if !context.in_transaction() {
                self.stats.transaction(replace_rfq);
            }
        }

        self.stats.sent(message.len());

        // Do this before flushing, because flushing can take time.
        self.cleanup_backend(context)?;

        let drop_message = matches!(code, 'Z')
            && context.in_multi_query_request
            && context.more_requests_pending
            && !self.pipeline_error;

        if !drop_message {
            trace!("{:#?} >>> {:?}", message, context.stream.peer_addr());

            if flush {
                context.stream.send_flush(&message).await?;
            } else {
                context.stream.send(&message).await?;
            }
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
                let message = row.message();
                let len = message.len();
                context.stream.send(&message).await?;
                self.stats.sent(len);
            }

            state.annotated = true;
        }

        Ok(())
    }

    pub(super) fn cleanup_backend(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        if self.backend.done() {
            let changed_params = self.backend.changed_params();

            // Release the connection back into the pool before flushing data to client.
            // Flushing can take a minute and we don't want to block the connection from being reused.
            if !self.backend.session_mode() && !context.more_requests_pending {
                self.backend.disconnect();
            }

            // Detect schema change and relaod the config so we get new schema.
            if self.router.schema_changed()
                && self
                    .backend
                    .cluster()
                    .map(|cluster| cluster.reload_schema())
                    .unwrap_or_default()
            {
                info!(
                    "schema change detected, reloading config [{}]",
                    self.backend.cluster()?.identifier(),
                );
                schema_changed()?;
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

        Ok(())
    }

    // Perform cross-shard check.
    async fn cross_shard_check(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<bool, Error> {
        // Admin database queries are not checked.
        if context.admin {
            return Ok(true);
        }

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

        debug!("cross-shard queries disabled: {}", cross_shard_disabled);

        // This check is disabled.
        if !cross_shard_disabled {
            return Ok(true);
        }
        let query_is_cross_shard = context.client_request.route().is_cross_shard();

        // The query is direct-to-shard, we're good.
        if !query_is_cross_shard {
            return Ok(true);
        }

        let connected_shards = self.backend.connected_servers();
        let is_executable = context.client_request.is_executable();

        // This is a Parse-only request, so it's safe
        // to route it to any shard - it won't do any damage
        // and we need a real response from a server.
        if !is_executable {
            return Ok(true);
        }

        // Only run check if we are not connected yet or we are actually connected
        // to more than one shard.
        //
        // The connected_shards > 1 check is only relevant for session mode - we stay connected
        // until client disconnects. We don't want this check to trigger on queries that we think
        // should be cross-shard (e.g. BEGIN, COMMIT) but aren't really.
        if connected_shards == 0 || connected_shards > 1 {
            let query = context.client_request.query()?;
            self.error_response(
                context,
                ErrorResponse::cross_shard_disabled(query.as_ref().map(|q| q.query())),
            )
            .await?;

            if self.backend.connected() && self.backend.done() {
                self.backend.disconnect();
            }

            return Ok(false);
        }

        Ok(true)
    }

    /// Check if we need to start a transaction automatically without the client knowing about it.
    ///
    /// The two conditions for this are:
    ///
    /// 1. `two_phase_commit_auto = true`
    /// 2. multi-query pipepline
    ///
    fn auto_transaction_check(&mut self, context: &mut QueryEngineContext<'_>) {
        let two_pc_enabled = self
            .backend
            .cluster()
            .map(|c| c.two_pc_auto_enabled())
            .unwrap_or_default();

        let two_pc_wants_transaction =
            two_pc_enabled && context.client_request.route().should_2pc();

        let no_transaction_already = !context.in_transaction() && self.begin_stmt.is_none();

        if (two_pc_wants_transaction || context.in_multi_query_request)
            && no_transaction_already
            && context.client_request.is_executable()
            && !self.backend.connected()
        {
            debug!("enabling automatic transaction");
            if two_pc_wants_transaction {
                self.two_pc.set_auto();
            }
            self.begin_stmt = Some(BufferedQuery::Query(Query::new("BEGIN")));
        }
    }

    async fn transaction_error_check(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<bool, Error> {
        let shards = match self.backend.shards() {
            Ok(shards) => shards,
            _ => {
                return Ok(true);
            }
        };
        if shards > 1 // This check only matters for cross-shard queries
            && context.in_error()
            && !context.rollback
            && context.client_request.is_executable()
            && !context.client_request.route().rollback_savepoint()
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
        mut error: ErrorResponse,
    ) -> Result<(), Error> {
        error!("{:?} [{:?}]", error.message, context.stream.peer_addr());

        // The rest of the pipeline should be ignored.
        if context.more_requests_pending {
            self.pipeline_error = true;
        }

        // Attach query context.
        if error.detail.is_none() {
            let query = context
                .client_request
                .query()?
                .map(|q| q.query().to_owned());
            error.detail = Some(query.unwrap_or_default());
        }

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
