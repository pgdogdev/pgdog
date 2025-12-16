use crate::{
    backend::pool::{Connection, Request},
    config::config,
    frontend::{
        client::query_engine::{hooks::QueryEngineHooks, route_query::ClusterCheck},
        router::{parser::Shard, Route},
        BufferedQuery, Client, ClientComms, Command, Error, Router, RouterContext, Stats,
    },
    net::{ErrorResponse, Message, Parameters},
    state::State,
};

use pgdog_config::Role;
use tracing::debug;

pub mod connect;
pub mod context;
pub mod deallocate;
pub mod discard;
pub mod end_transaction;
pub mod hooks;
pub mod incomplete_requests;
pub mod internal_values;
pub mod multi_step;
pub mod notify_buffer;
pub mod pub_sub;
pub mod query;
pub mod rewrite;
pub mod route_query;
pub mod set;
pub mod shard_key_rewrite;
pub mod start_transaction;
#[cfg(test)]
mod test;
#[cfg(test)]
mod testing;
pub mod two_pc;
pub mod unknown_command;

use self::query::ExplainResponseState;
pub use context::QueryEngineContext;
use notify_buffer::NotifyBuffer;
pub use two_pc::phase::TwoPcPhase;
use two_pc::TwoPc;

#[derive(Debug)]
pub struct TestMode {
    pub enabled: bool,
}

impl TestMode {
    pub fn new() -> Self {
        Self {
            #[cfg(test)]
            enabled: true,
            #[cfg(not(test))]
            enabled: false,
        }
    }
}

#[derive(Debug)]
pub struct QueryEngine {
    begin_stmt: Option<BufferedQuery>,
    router: Router,
    comms: ClientComms,
    stats: Stats,
    backend: Connection,
    streaming: bool,
    test_mode: TestMode,
    set_route: Option<Route>,
    two_pc: TwoPc,
    notify_buffer: NotifyBuffer,
    pending_explain: Option<ExplainResponseState>,
    hooks: QueryEngineHooks,
}

impl QueryEngine {
    /// Create new query engine.
    pub fn new(
        params: &Parameters,
        comms: &ClientComms,
        admin: bool,
        passthrough_password: &Option<String>,
    ) -> Result<Self, Error> {
        let user = params.get_required("user")?;
        let database = params.get_default("database", user);

        let backend = Connection::new(user, database, admin, passthrough_password)?;

        Ok(Self {
            backend,
            comms: comms.clone(),
            hooks: QueryEngineHooks::new(),
            test_mode: TestMode::new(),
            stats: Stats::default(),
            streaming: bool::default(),
            two_pc: TwoPc::default(),
            notify_buffer: NotifyBuffer::default(),
            pending_explain: None,
            begin_stmt: None,
            router: Router::default(),
            set_route: None,
        })
    }

    pub fn from_client(client: &Client) -> Result<Self, Error> {
        Self::new(
            &client.params,
            &client.comms,
            client.admin,
            &client.passthrough_password,
        )
    }

    /// Wait for an async message from the backend.
    pub async fn read_backend(&mut self) -> Result<Message, Error> {
        Ok(self.backend.read().await?)
    }

    /// Query engine finished executing.
    pub fn done(&self) -> bool {
        !self.backend.connected() && self.begin_stmt.is_none()
    }

    /// Current state.
    pub fn client_state(&self) -> State {
        self.stats.state
    }

    /// Handle client request.
    pub async fn handle(&mut self, context: &mut QueryEngineContext<'_>) -> Result<(), Error> {
        self.stats
            .received(context.client_request.total_message_len());
        self.set_state(State::Active); // Client is active.

        // Rewrite prepared statements.
        self.rewrite_extended(context)?;

        if let ClusterCheck::Offline = self.cluster_check(context).await? {
            return Ok(());
        }

        // Rewrite statement if necessary.
        if !self.parse_and_rewrite(context).await? {
            return Ok(());
        }

        // Intercept commands we don't have to forward to a server.
        if self.intercept_incomplete(context).await? {
            self.update_stats(context);
            return Ok(());
        }

        // Route transaction to the right servers.
        if !self.route_transaction(context).await? {
            self.update_stats(context);
            debug!("transaction has nowhere to go");
            return Ok(());
        }

        self.hooks.before_execution(context)?;

        // Queue up request to mirrors, if any.
        // Do this before sending query to actual server
        // to have accurate timings between queries.
        self.backend.mirror(context.client_request);

        self.pending_explain = None;

        let command = self.router.command();

        // Schema-sharding route persists until the end
        // of the transaction.
        if command.route().is_schema_path_driven() && self.set_route.is_none() {
            debug!("search_path route is set for transaction");
            self.set_route = Some(command.route().clone());
        }

        // If we have set a fixed route using the schema_path session variable,
        // keep it for the rest of the transaaction.
        let mut route = if let Some(ref route) = self.set_route {
            route.clone()
        } else {
            command.route().clone()
        };

        // Override read/write property based on target_session_attrs.
        match context.sticky.role {
            Some(Role::Replica) => route.set_read_mut(true),
            Some(Role::Primary) => route.set_read_mut(false),
            _ => (),
        }

        if let Some(trace) = route.take_explain() {
            if config().config.general.expanded_explain {
                self.pending_explain = Some(ExplainResponseState::new(trace));
            }
        }

        // FIXME, we should not to copy route twice.
        context.client_request.route = Some(route.clone());

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
            } => {
                self.start_transaction(context, query.clone(), *transaction_type, *extended)
                    .await?
            }
            Command::CommitTransaction { extended } => {
                self.set_route = None;

                if self.backend.connected() || *extended {
                    let extended = *extended;
                    let transaction_route = self.transaction_route(&route)?;
                    context.client_request.route = Some(transaction_route.clone());
                    context.cross_shard_disabled = Some(false);
                    self.end_connected(context, &transaction_route, false, extended)
                        .await?;
                } else {
                    self.end_not_connected(context, false, *extended).await?
                }

                context.params.commit();
            }
            Command::RollbackTransaction { extended } => {
                self.set_route = None;

                if self.backend.connected() || *extended {
                    let extended = *extended;
                    let transaction_route = self.transaction_route(&route)?;
                    context.client_request.route = Some(transaction_route.clone());
                    context.cross_shard_disabled = Some(false);
                    self.end_connected(context, &transaction_route, true, extended)
                        .await?;
                } else {
                    self.end_not_connected(context, true, *extended).await?
                }

                context.params.rollback();
            }
            Command::Query(_) => self.execute(context, &route).await?,
            Command::Listen { channel, shard } => {
                self.listen(context, &channel.clone(), shard.clone())
                    .await?
            }
            Command::Notify {
                channel,
                payload,
                shard,
            } => {
                self.notify(context, &channel.clone(), &payload.clone(), &shard.clone())
                    .await?
            }
            Command::Unlisten(channel) => self.unlisten(context, &channel.clone()).await?,
            Command::Set {
                name,
                value,
                route,
                extended,
                local,
            } => {
                let route = route.clone();
                if self.backend.connected() {
                    self.execute(context, &route).await?;
                } else {
                    let extended = *extended;
                    self.set(
                        context,
                        name.clone(),
                        value.clone(),
                        extended,
                        route,
                        *local,
                    )
                    .await?;
                }
            }
            Command::SetRoute(route) => {
                self.set_route(context, route.clone()).await?;
            }
            Command::Copy(_) => self.execute(context, &route).await?,
            Command::ShardKeyRewrite(plan) => {
                self.shard_key_rewrite(context, *plan.clone()).await?
            }
            Command::Deallocate => self.deallocate(context).await?,
            Command::Discard { extended } => self.discard(context, *extended).await?,
            command => self.unknown_command(context, command.clone()).await?,
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

        Ok(())
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
            .prepared_statements(context.prepared_statements.len_local());
        self.stats.memory_used(context.memory_stats);

        self.comms.update_stats(self.stats);
    }

    pub fn set_state(&mut self, state: State) {
        self.stats.state = state;
        self.comms.update_stats(self.stats);
    }

    pub fn get_state(&self) -> State {
        self.stats.state
    }
}
