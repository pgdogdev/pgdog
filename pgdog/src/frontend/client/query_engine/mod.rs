use crate::{
    backend::pool::{Connection, Request},
    config::config,
    frontend::{
        BufferedQuery, Client, ClientComms, ClientRequest, Command, DiscardTarget, Error, Router,
        RouterContext, Stats,
        client::query_engine::{hooks::QueryEngineHooks, route_query::ClusterCheck},
        router::{Route, parser::Shard},
    },
    net::{ErrorResponse, Message, Parameters},
    state::State,
};
use tokio_util::sync::CancellationToken;
use tracing::debug;

pub(crate) mod advisory_lock;
pub(crate) mod connect;
pub(crate) mod context;
pub(crate) mod deallocate;
pub(crate) mod discard;
pub(crate) mod end_transaction;
pub(crate) mod fake;
pub(crate) mod hooks;
pub(crate) mod incomplete_requests;
pub(crate) mod internal_values;
pub(crate) mod lock;
pub(crate) mod maintenance_mode;
pub(crate) mod multi_step;
pub(crate) mod notify_buffer;
pub(crate) mod pub_sub;
pub(crate) mod query;
mod query_log_stdout;
pub(crate) mod result;
pub(crate) mod rewrite;
pub(crate) mod route_query;
pub(crate) mod set;
pub(crate) mod split;
pub(crate) mod start_transaction;
mod temp_table;
#[cfg(test)]
mod test;
#[cfg(test)]
mod testing;
pub(crate) mod two_pc;

use self::query::ExplainResponseState;
use self::query_log_stdout::log_query_stdout;
pub(crate) use advisory_lock::AdvisoryLocks;
pub(crate) use context::QueryEngineContext;
use notify_buffer::NotifyBuffer;
pub(crate) use result::QueryEngineResult;
pub(crate) use split::Pipeline;
pub(in crate::frontend) use temp_table::TempTableChange;
use temp_table::TempTables;
use two_pc::TwoPc;
pub(crate) use two_pc::phase::TwoPcPhase;
pub(crate) use two_pc::*;

/// Implements the entire client/server message exchange.
/// State here is preserved between requests.
#[derive(Debug)]
pub(crate) struct QueryEngine {
    router: Router,
    comms: ClientComms,
    stats: Stats,
    backend: Connection,
    streaming: bool,
    two_pc: TwoPc,
    notify_buffer: NotifyBuffer,
    pending_explain: Option<ExplainResponseState>,
    hooks: QueryEngineHooks,
    advisory_locks: AdvisoryLocks,
    // The client requested we disable transaction mode temporarily.
    // They will remain pinned to their connection until they unpin manually
    // or disconnect.
    manual_lock: bool,
    temp_tables: TempTables,
}

impl QueryEngine {
    /// Create new query engine.
    pub(crate) fn new(
        params: &Parameters,
        comms: &ClientComms,
        admin: bool,
    ) -> Result<Self, Error> {
        let user = params.get_required("user")?;
        let database = params.get_default("database", user);

        let backend = Connection::new(user, database, admin)?;

        Ok(Self {
            backend,
            comms: comms.clone(),
            hooks: QueryEngineHooks::new(),
            stats: Stats::default(),
            streaming: bool::default(),
            two_pc: TwoPc::default(),
            notify_buffer: NotifyBuffer::default(),
            pending_explain: None,
            router: Router::default(),
            advisory_locks: AdvisoryLocks::default(),
            manual_lock: false,
            temp_tables: Default::default(),
        })
    }

    pub(crate) fn from_client(client: &Client) -> Result<Self, Error> {
        Self::new(&client.params, &client.comms, client.admin)
    }

    /// Token cancelled when an admin terminates this client's cluster (FORCE_RELOAD).
    pub(crate) fn cancellation_token(&self) -> CancellationToken {
        self.backend.cancellation_token()
    }

    /// Wait for an async message from the backend.
    pub(crate) async fn read_backend(&mut self) -> Result<Message, Error> {
        Ok(self.backend.read().await?)
    }

    /// Client can safely disconnect (no active backend connection or pending transaction).
    pub(crate) fn can_disconnect(&self) -> bool {
        !self.backend.in_buffered_transaction() && self.backend.done()
    }

    /// Current state.
    pub(crate) fn client_state(&self) -> State {
        self.stats.state
    }

    /// Handle client request.
    pub(crate) async fn handle(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        client_request: &mut ClientRequest,
    ) -> Result<QueryEngineResult, Error> {
        if let Some(result) = Self::check_extended_pipeline_rewrite(client_request)? {
            return Ok(result);
        }

        self.stats.received(client_request.total_message_len());
        self.set_state(State::Active); // Client is active.

        if self.in_extended_pipeline_error(context, client_request) {
            return Ok(QueryEngineResult::Done(context.transaction()));
        }

        log_query_stdout(context, client_request);

        // Rewrite prepared statements.
        self.rewrite_extended(context, &mut client_request.messages)?;

        if let ClusterCheck::Offline = self.cluster_check(context, client_request).await? {
            return Ok(QueryEngineResult::Done(context.transaction()));
        }

        if let Err(e) = self.parse_request(context, client_request) {
            self.error_response(
                context,
                client_request,
                ErrorResponse::syntax(e.to_string()),
            )
            .await?;
            return Ok(QueryEngineResult::Done(context.transaction()));
        }

        // Queue up request to mirrors, if any.
        // Do this before sending query to actual server
        // to have accurate timings between queries.
        self.backend.mirror(client_request);

        let rewrite_result = self.rewrite_request(context, client_request).await?;

        // Intercept commands we don't have to forward to a server.
        if self.intercept_incomplete(context, client_request).await? {
            self.update_stats(context);
            return Ok(QueryEngineResult::Done(context.transaction()));
        }

        // Route transaction to the right servers.
        if !self
            .route_query(context, client_request, rewrite_result.as_ref())
            .await?
        {
            self.update_stats(context);
            debug!("query has nowhere to go");
            return Ok(QueryEngineResult::Done(context.transaction()));
        }

        self.hooks.before_execution(context)?;

        self.pending_explain = None;

        // Check if we need to lock the backend in-place.
        // This is here because ROLLBACK and COMMIT
        // can be handled by a separate path than [`QueryEngine::execute`],
        // e.g., if using two-phase commit.
        self.check_lock();

        let command = self.router.command();

        if let Some(trace) = client_request
            .route // Admin commands don't have a route.
            .as_mut()
            .and_then(|route| route.take_explain())
            && config().config.general.expanded_explain
        {
            self.pending_explain = Some(ExplainResponseState::new(trace));
        }

        match command {
            Command::InternalField { name, value } => {
                self.show_internal_value(context, name.clone(), value.clone())
                    .await?
            }
            Command::UniqueId => self.unique_id(context).await?,
            Command::StartTransaction {
                query,
                transaction_type,
                extended,
                ..
            } => {
                self.start_transaction(
                    context,
                    client_request,
                    query.clone(),
                    *transaction_type,
                    *extended,
                )
                .await?
            }
            Command::CommitTransaction { extended, .. } => {
                if self.backend.connected() {
                    // Transaction control statements should be excluded from cross-shard checks.
                    context.cross_shard_disabled = Some(false);
                    self.end_connected(context, client_request, false, *extended)
                        .await?;
                } else {
                    self.end_not_connected(context, client_request, false, *extended)
                        .await?
                }

                if client_request.is_executable() && context.params.commit() {
                    self.comms.update_params(context.params);
                }
            }
            Command::RollbackTransaction { extended, .. } => {
                if self.backend.connected() {
                    // Transaction control statements should be excluded from cross-shard checks.
                    context.cross_shard_disabled = Some(false);
                    self.end_connected(context, client_request, true, *extended)
                        .await?;
                } else {
                    self.end_not_connected(context, client_request, true, *extended)
                        .await?
                }

                if client_request.is_executable() {
                    context.params.rollback();
                }
            }
            Command::Query(_) => {
                self.execute(context, client_request, rewrite_result)
                    .await?
            }
            Command::Listen { .. } | Command::Notify { .. } | Command::Unlisten(_)
                if self.backend.session_mode() =>
            {
                self.execute(context, client_request, rewrite_result)
                    .await?
            }
            Command::Listen { channel, shard } => {
                self.listen(
                    context,
                    &client_request.messages,
                    &channel.clone(),
                    shard.clone(),
                )
                .await?
            }
            Command::Notify {
                channel,
                payload,
                shard,
            } => {
                self.notify(
                    context,
                    &client_request.messages,
                    &channel.clone(),
                    &payload.clone(),
                    &shard.clone(),
                )
                .await?
            }
            Command::Unlisten(channel) => {
                self.unlisten(context, &client_request.messages, &channel.clone())
                    .await?
            }
            Command::Set {
                params, set_config, ..
            } => {
                let params = params.clone();
                self.set(context, client_request, &params, *set_config)
                    .await?;
            }
            Command::ResetAll => {
                self.reset_all(context, client_request).await?;
            }
            Command::Copy { .. } => {
                self.execute(context, client_request, rewrite_result)
                    .await?
            }
            Command::Deallocate => self.deallocate(context).await?,
            Command::Discard { target, extended } => {
                self.discard(context, client_request, *target, *extended)
                    .await?
            }
            Command::Split(queries) => return Ok(Self::build_simple_split(queries)),
        }

        self.hooks.after_execution(context)?;

        if context.in_error() {
            self.backend.mirror_clear();
            self.notify_buffer.clear();
        } else if !context.in_transaction() {
            self.backend.mirror_flush();
            self.flush_notify().await?;
        }

        self.update_stats(context);

        Ok(QueryEngineResult::Done(context.transaction()))
    }

    fn update_stats(&mut self, context: &mut QueryEngineContext<'_>) {
        let state = if self.backend.has_more_messages() {
            State::Active
        } else {
            match context.in_transaction() {
                true => State::IdleInTransaction,
                false => State::Idle,
            }
        };

        self.stats.state = state;

        self.stats
            .prepared_statements(context.prepared_statements.num_statements());
        self.stats.memory_used(context.memory_stats);

        self.comms.update_stats(self.stats);
    }

    /// Set client execution state and update
    /// it immediately in the global store.
    fn set_state(&mut self, state: State) {
        self.stats.state = state;
        self.comms.update_stats(self.stats);
    }
}
