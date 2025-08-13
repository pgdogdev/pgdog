use crate::{
    backend::pool::{Connection, Request},
    config::config,
    frontend::{
        client::{
            query_engine::{
                begin::Begin,
                cleanup::Cleanup,
                commit::Commit,
                cross_shard_check::CrossShardCheck,
                empty_query::EmptyQuery,
                error_response::ErrorHandler,
                rollback::Rollback,
                server_response::{ServerResponse, ServerResponseResult},
            },
            timeouts::Timeouts,
            transaction::Transaction,
        },
        router::Route,
        ClientRequest, Command, Comms, Error, PreparedStatements, Router, RouterContext, Stats,
    },
    net::{BackendKeyData, Message, Parameters},
};

use tracing::error;

#[cfg(not(test))]
pub(super) type Stream = crate::net::Stream;
#[cfg(test)]
pub(super) type Stream = super::test::Stream;

#[derive(Debug)]
pub struct QueryEngine {
    /// Client's prepared statements cache.
    pub(super) prepared_statements: PreparedStatements,
    /// Client parameters.
    pub(super) params: Parameters,
    /// Transaction state.
    pub(super) transaction: Transaction,
    /// Connection the the backend.
    pub(super) backend: Connection,
    /// Query statistics.
    pub(super) stats: Stats,
    /// Client ID.
    pub(super) client_id: BackendKeyData,
    /// Timeouts
    pub(super) timeouts: Timeouts,
    /// Client comms.
    pub(super) comms: Comms,
    /// Cross-shard queries are disabled.
    pub(super) cross_shard_disabled: bool,
    /// Streaming messages.
    pub(super) streaming: bool,
}

pub enum EngineState<'a> {
    /// Engine finished.
    Done { in_transaction: bool },

    /// Query execution in progress.
    Run { command: &'a Command },
}

impl QueryEngine {
    /// Create the query engine.
    pub fn new(
        comms: Comms,
        params: Parameters,
        admin: bool,
        passthrough_password: &Option<String>,
    ) -> Result<Self, Error> {
        let user = params.get_required("user")?;
        let database = params.get_default("database", user);

        let backend = Connection::new(user, database, admin, passthrough_password)?;

        Ok(Self {
            client_id: comms.client_id(),
            comms,
            timeouts: Timeouts::default(),
            stats: Stats::new(),
            backend,
            transaction: Transaction::default(),
            params,
            prepared_statements: PreparedStatements::new(),
            cross_shard_disabled: false,
            streaming: false,
        })
    }

    /// Handle request to query engine and send backend response
    /// directly back to the client socket.
    ///
    /// # Arguments
    ///
    /// * `request` - The client request to be processed by the query engine
    /// * `client_socket` - Mutable reference to the client's network stream for sending responses
    ///
    pub async fn handle_request(
        &mut self,
        request: &ClientRequest,
        router: &mut Router,
        client_socket: &mut Stream,
    ) -> Result<bool, Error> {
        self.refresh_config();

        let state = self.route_request(request, router, client_socket).await?;

        match state {
            EngineState::Done { in_transaction } => return Ok(in_transaction),
            EngineState::Run { command } => {
                self.connect(command.route()).await?;

                if request.executable() {
                    if let Some(begin) = self.transaction.take_begin() {
                        self.backend.execute(&begin).await?;
                    }
                }

                // Queue up request on mirrors.
                self.backend.mirror(request);

                // Send request to backend.
                self.backend
                    .handle_request(request, router, self.streaming)
                    .await?;

                let handle_response = !self.backend.copy_mode() && !self.streaming;

                if handle_response {
                    let ServerResponseResult { done, streaming } = ServerResponse::new(
                        &mut self.backend,
                        &mut self.stats,
                        &mut self.transaction,
                        &mut self.timeouts,
                    )
                    .handle(client_socket)
                    .await?;
                    self.streaming = streaming;

                    if done {
                        Cleanup::new(
                            &mut self.backend,
                            &mut self.stats,
                            &mut self.params,
                            &mut self.comms,
                        )
                        .handle()?;

                        router.reset();
                    }
                }

                self.comms.update_stats(self.stats);

                Ok(self.transaction.started())
            }
        }
    }

    /// Read async message from backend, if any.
    ///
    /// Can await forever.
    pub async fn async_message(&mut self) -> Result<Message, Error> {
        Ok(self.backend.read().await?)
    }

    /// Connect to one or many servers.
    async fn connect(&mut self, route: &Route) -> Result<(), Error> {
        if !self.backend.needs_connect() {
            return Ok(());
        }

        let request = Request::new(self.client_id);
        self.stats.waiting(request.created_at);
        self.comms.update_stats(self.stats);

        match self.backend.connect(&request, route).await {
            Ok(()) => {
                self.stats.connected();
                self.stats.locked(route.lock_session());
                // This connection will be locked to this client
                // until they disconnect.
                //
                // Used in case the client runs an advisory lock
                // or another leaky transaction mode abstraction.
                self.backend.lock(route.lock_session());
                self.comms.update_stats(self.stats);
                Ok(())
            }
            Err(err) => {
                self.stats.error();
                Err(err.into())
            }
        }
    }

    async fn route_request<'a>(
        &mut self,
        request: &ClientRequest,
        router: &'a mut Router,
        client_socket: &mut Stream,
    ) -> Result<EngineState<'a>, Error> {
        let context = RouterContext::new(
            request,                       // Query and parameters.
            self.backend.cluster()?,       // Cluster configuration.
            &mut self.prepared_statements, // Prepared statements.
            &self.params,                  // Client connection parameters.
            self.transaction.started(),    // Client in explicitily started transaction.
        )?;

        match router.query(context) {
            Ok(command) => {
                match command {
                    // Transaction control statements.
                    Command::StartTransaction(_)
                    | Command::CommitTransaction
                    | Command::RollbackTransaction => {
                        return self.transaction_control(command, client_socket).await;
                    }

                    Command::Deallocate => todo!(),
                    Command::Listen { channel, shard } => todo!(),
                    Command::Notify {
                        channel,
                        payload,
                        shard,
                    } => todo!(),
                    Command::Unlisten(channel) => todo!(),
                    Command::Set { name, value } => todo!(),
                    Command::Shards(shards) => todo!(),
                    Command::Query(route) => {
                        let blocked = CrossShardCheck::new(
                            self.cross_shard_disabled,
                            route,
                            self.transaction.started(),
                        )
                        .handle(client_socket)
                        .await?;

                        if blocked {
                            Ok(EngineState::Done {
                                in_transaction: self.transaction.started(),
                            })
                        } else {
                            Ok(EngineState::Run { command })
                        }
                    }
                    Command::Copy(_) => Ok(EngineState::Run { command }),
                    cmd => {
                        error!("unexpected command: {:?}", cmd);
                        return Err(Error::UnexpectedCommand);
                    }
                }
            }
            Err(err) => {
                if err.empty_query() {
                    EmptyQuery::new(self.transaction.started())
                        .handle(client_socket)
                        .await?;
                } else {
                    ErrorHandler::new(self.transaction.started())
                        .handle(err, client_socket)
                        .await?;
                }
                return Ok(EngineState::Done {
                    in_transaction: self.transaction.started(),
                });
            }
        }
    }

    /// Handle transaction control command.
    async fn transaction_control<'a>(
        &mut self,
        command: &'a Command,
        client_socket: &mut Stream,
    ) -> Result<EngineState<'a>, Error> {
        match command {
            Command::StartTransaction(begin) => {
                Begin::new(self.transaction.started())
                    .handle(client_socket)
                    .await?;

                if !self.transaction.started() {
                    self.transaction.buffer(begin);
                }
            }

            Command::RollbackTransaction => {
                Rollback::new(self.transaction.started(), &mut self.backend)
                    .handle(client_socket)
                    .await?;
                self.transaction.finish();
            }

            Command::CommitTransaction => {
                Commit::new(&mut self.backend).handle(client_socket).await?;
                self.transaction.finish();
            }

            _ => unreachable!("transaction_control"),
        }

        Ok(EngineState::Done {
            in_transaction: self.transaction.started(),
        })
    }

    /// Get latest config values.
    fn refresh_config(&mut self) {
        let config = config();

        self.prepared_statements.enabled = config.prepared_statements();
        self.prepared_statements.capacity = config.config.general.prepared_statements_limit;
        self.timeouts = Timeouts::from_config(&config.config.general);
        self.cross_shard_disabled = config.config.general.cross_shard_disabled;
    }
}
