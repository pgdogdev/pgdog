use crate::{
    backend::pool::{Connection, Request},
    frontend::{
        router::{parser::Shard, Route},
        BufferedQuery, Client, Command, Comms, Error, Router, RouterContext, Stats,
    },
    net::{BackendKeyData, ErrorResponse, Message, Parameters},
    state::State,
};

use tracing::debug;

pub mod connect;
pub mod context;
pub mod deallocate;
pub mod end_transaction;
pub mod incomplete_requests;
pub mod pub_sub;
pub mod query;
pub mod route_query;
pub mod set;
pub mod show_shards;
pub mod start_transaction;
pub mod unknown_command;

#[cfg(test)]
mod testing;

pub use context::QueryEngineContext;

#[derive(Default, Debug)]
pub struct QueryEngine {
    begin_stmt: Option<BufferedQuery>,
    router: Router,
    comms: Comms,
    stats: Stats,
    backend: Connection,
    streaming: bool,
    client_id: BackendKeyData,
    test_mode: bool,
}

impl<'a> QueryEngine {
    /// Create new query engine.
    pub fn new(
        params: &Parameters,
        comms: &Comms,
        admin: bool,
        passthrough_password: &Option<String>,
    ) -> Result<Self, Error> {
        let user = params.get_required("user")?;
        let database = params.get_default("database", user);

        let backend = Connection::new(user, database, admin, passthrough_password)?;

        Ok(Self {
            backend,
            client_id: comms.client_id(),
            comms: comms.clone(),
            #[cfg(test)]
            test_mode: true,
            #[cfg(not(test))]
            test_mode: false,
            ..Default::default()
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

        // Queue up request to mirrors, if any.
        // Do this before sending query to actual server
        // to have accurate timings between queries.
        self.backend.mirror(&context.client_request);

        let command = self.router.command();
        let route = command.route().clone();

        // FIXME, we should not to copy route twice.
        context.client_request.route = route.clone();

        match command {
            Command::Shards(shards) => self.show_shards(context, *shards).await?,
            Command::StartTransaction(begin) => {
                self.start_transaction(context, begin.clone()).await?
            }
            Command::CommitTransaction => {
                if self.backend.connected() {
                    self.execute(context, &route).await?
                } else {
                    self.end_transaction(context, false).await?
                }
            }
            Command::RollbackTransaction => {
                if self.backend.connected() {
                    self.execute(context, &route).await?
                } else {
                    self.end_transaction(context, true).await?
                }
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
            Command::Set { name, value } => {
                if self.backend.connected() {
                    self.execute(context, &route).await?
                } else {
                    self.set(context, name.clone(), value.clone()).await?
                }
            }
            Command::Copy(_) => self.execute(context, &route).await?,
            Command::Rewrite(query) => {
                context.client_request.rewrite(query)?;
                self.execute(context, &route).await?;
            }
            Command::Deallocate => self.deallocate(context).await?,
            command => self.unknown_command(context, command.clone()).await?,
        }

        if !context.in_transaction() {
            self.backend.mirror_flush();
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
        self.stats.memory_used(context.memory_usage);

        self.comms.stats(self.stats);
    }
}
