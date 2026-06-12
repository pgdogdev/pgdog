//! PostgreSQL client.
//!
//! Entrypoint for client/server interactions.
//!

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use pgdog_config::users::PasswordKind;
use timeouts::Timeouts;
use tokio::{select, spawn, time::timeout};
use tracing::{Level as LogLevel, debug, enabled, error, info, trace, warn};

use super::{ClientRequest, Error, PreparedStatements};
use crate::auth::AuthResult;
use crate::auth::{md5, scram::Server};
use crate::backend::maintenance_mode;
use crate::backend::pool::stats::MemoryStats;
use crate::backend::{
    databases,
    pool::{Connection, Request},
};
use crate::config::convert::user_from_params;
use crate::config::{self, AuthType, ConfigAndUsers, config};
use crate::frontend::ClientComms;
use crate::frontend::client::query_engine::{QueryEngine, QueryEngineContext};
use crate::net::messages::{
    Authentication, BackendKeyData, ErrorResponse, FromBytes, FrontendPid, Message, Password,
    Protocol, ProtocolVersion, ReadyForQuery, ToBytes,
};
use crate::net::{MessageBuffer, ProtocolMessage, Stream, parameter::Parameters};
use crate::state::State;
use crate::stats::memory::MemoryUsage;
use crate::util::user_database_from_params;

pub mod query_engine;
pub mod sticky;
pub mod timeouts;
pub mod transaction_type;

pub(crate) use sticky::Sticky;
pub use transaction_type::TransactionType;

/// PostgreSQL client.
///
/// It thinks it's talking to a real Postgres server, but actually it's talking to PgDog :-).
///
#[derive(Debug)]
pub struct Client {
    // Client IP.
    addr: SocketAddr,
    // Client socket.
    stream: Stream,
    // Client unique key. Randomly generated
    // for each client.
    key: BackendKeyData,
    // Client startup parameters. Keeps track of any parameters
    // the client changes at runtime with `SET` as well.
    params: Parameters,
    // Process-global communication primitives used for clients
    // to talk to each other, e.g. to track their own state.
    comms: ClientComms,
    // Client is connected to the admin database.
    admin: bool,
    // Client is streaming data via replication, and not running
    // regular queries. We skip all the fancy stuff here, i.e.,
    // no query parsing, routing, etc.
    //
    // Don't expect sharding to work if this is what the client is doing.
    streaming: bool,
    // Client prepared statements cache.
    prepared_statements: PreparedStatements,
    // Client transaction state.
    transaction: Option<TransactionType>,
    // Current timeouts to use for client/server communication.
    // These change based on client state, e.g. if client is running query,
    // the `query_timeout` is active, and if the client is idle, the `client_idle_timeout` is.
    timeouts: Timeouts,
    // Stateful buffer containing the current whole client request.
    // This can be a query or just a `Parse` and `Flush`, but in either case, the client
    // will expect a response immediately and we need to handle it.
    client_request: ClientRequest,
    // Raw buffer of messages the client sent. We keep them here to avoid memory allocations
    // down the line (using [`bytes::Bytes`]).
    stream_buffer: MessageBuffer,
    // Settings that override query routing behavior, e.g., client wants to talk
    // to replicas only.
    sticky: Sticky,
    /// Client database.
    database: String,
    /// Log queries to stdout.
    query_log_stdout: bool,
    /// Maximum query message size before a warning is logged.
    query_size_limit: Option<usize>,
}

impl Client {
    /// Create new frontend client from the a TCP socket.
    ///
    /// The client already sent a valid Startup message and negotiated TLS.
    ///
    /// # Parameters
    ///
    /// - `stream`: TCP stream.
    /// - `params`: Client parameters extracted from the [`crate::net::Startup`] message.
    /// - `addr`: TCP IP.
    /// - `config`: Currently loaded `pgdog.toml` and `users.toml`.
    /// - `protocol_version`: The version of the PostgreSQL protocol used by the client. This is typically 3.0, but can be 3.2
    ///   for more modern clients.
    ///
    pub async fn spawn(
        stream: Stream,
        params: Parameters,
        addr: SocketAddr,
        config: Arc<ConfigAndUsers>,
        protocol_version: ProtocolVersion,
    ) -> Result<(), Error> {
        let login_timeout = Duration::from_millis(config.config.general.client_login_timeout);

        match timeout(
            login_timeout,
            Self::login(stream, params, addr, config, protocol_version),
        )
        .await
        {
            Ok(Ok(Some(mut client))) => {
                if client.admin {
                    // Admin clients are not waited on during shutdown.
                    spawn(async move {
                        client.spawn_internal().await;
                    });
                } else {
                    client.spawn_internal().await;
                }

                Ok(())
            }
            Err(_) => {
                error!("client login timeout [{}]", addr);
                Ok(())
            }
            Ok(Ok(None)) => Ok(()),
            Ok(Err(err)) => Err(err),
        }
    }

    /// Authenticate a client against the configured password(s) using the
    /// requested authentication method.
    ///
    /// Returns `false` if no passwords are configured or the credentials the
    /// client provided don't match.
    async fn check_password(
        stream: &mut Stream,
        user: &str,
        auth_type: &AuthType,
        passwords: &[PasswordKind],
    ) -> Result<AuthResult, Error> {
        if passwords.is_empty() {
            return Ok(AuthResult::NoPasswordConfig);
        }

        let result = match auth_type {
            AuthType::Md5 => {
                let md5 = md5::Client::new(
                    user,
                    &passwords.iter().map(|s| s.to_string()).collect::<Vec<_>>(),
                );
                stream.send_flush(&md5.challenge()).await?;
                let password = Password::from_bytes(stream.read().await?.to_bytes())?;
                if let Password::PasswordMessage { response } = password {
                    if md5.check(&response) {
                        AuthResult::Ok
                    } else {
                        AuthResult::NoPasswordMatch
                    }
                } else {
                    AuthResult::NoPasswordMessage
                }
            }

            AuthType::Scram => {
                stream.send_flush(&Authentication::scram()).await?;

                let scram = Server::new(passwords);
                let res = scram.handle(stream).await;
                if matches!(res, Ok(true)) {
                    AuthResult::Ok
                } else {
                    AuthResult::NoPasswordMatch
                }
            }

            AuthType::Plain => {
                stream
                    .send_flush(&Authentication::ClearTextPassword)
                    .await?;
                let response = stream.read().await?;
                let response = Password::from_bytes(response.to_bytes())?;
                let is_match = response.password().is_some_and(|provided| {
                    passwords.iter().any(|p| {
                        crate::util::constant_time_eq(p.as_str().as_bytes(), provided.as_bytes())
                    })
                });

                if is_match {
                    AuthResult::Ok
                } else {
                    AuthResult::NoPasswordMatch
                }
            }

            AuthType::Trust => AuthResult::Ok,
        };

        Ok(result)
    }

    /// Create new frontend client from the given TCP stream.
    async fn login(
        mut stream: Stream,
        params: Parameters,
        addr: SocketAddr,
        config: Arc<ConfigAndUsers>,
        protocol_version: ProtocolVersion,
    ) -> Result<Option<Client>, Error> {
        // Bail immediately if TLS is required but the connection isn't using it.
        if config.config.general.tls_client_required && !stream.is_tls() {
            stream.fatal(ErrorResponse::tls_required()).await?;
            return Ok(None);
        }

        let (user, database) = user_database_from_params(&params);
        let admin = database == config.config.admin.name && config.config.admin.user == user;
        let admin_password = &config.config.admin.password;
        let auth_type = &config.config.general.auth_type;
        let passthrough = config.config.general.passthrough_auth();
        let id = FrontendPid::new();
        let key = BackendKeyData::new_frontend(protocol_version, id);
        let comms = ClientComms::new(id);
        let log_connections = config.config.general.log_connections;

        // Check if we need to ask the client for its password in plaintext
        // because we don't actually have it configured.
        //
        // This is likely because passthrough authentication is enabled.
        //
        let auth_result = if admin {
            // The admin database is virtual and never present in the cluster
            // map, so authenticate directly against the configured admin password.
            let passwords = [PasswordKind::Plain(admin_password.clone())];
            Self::check_password(&mut stream, user, auth_type, &passwords).await?
        } else if passthrough {
            // Get the password. We always need it because we need to check if
            // it's current and hasn't been changed.
            stream
                .send_flush(&Authentication::ClearTextPassword)
                .await?;
            let password = stream.read().await?;
            let password = Password::from_bytes(password.to_bytes())?;
            // Passthrough authentication assumes the client password is good
            // and lets Postgres perform the authentication instead. If Postgres
            // returns an error, the connection pool will be banned and the client
            // won't be able to run queries.
            let user = user_from_params(&params, &password).ok();
            if let Some(user) = user {
                databases::add(user)?
            } else {
                AuthResult::NoPassthroughNoUser
            }
        } else {
            match databases::databases().cluster((user, database)) {
                Ok(cluster) => {
                    if let Some(identity) = cluster.identity() {
                        // mTLS authentication: the client certificate identity
                        // must match the configured user identity.
                        if stream.tls_identity() == Some(identity) {
                            AuthResult::Ok
                        } else {
                            AuthResult::NoIdentity
                        }
                    } else {
                        // Password authentication.
                        Self::check_password(&mut stream, user, auth_type, cluster.passwords())
                            .await?
                    }
                }

                Err(_) => AuthResult::NoUserOrDatabase,
            }
        };

        if !auth_result.is_ok() {
            if log_connections {
                warn!(
                    r#"user "{}" and database "{}" auth error: {}"#,
                    user, database, auth_result
                );
            }
            stream.fatal(ErrorResponse::auth(user, database)).await?;
            return Ok(None);
        } else {
            stream.send(&Authentication::Ok).await?;
        }

        // Check if the pooler is shutting down.
        //
        // We do this late because we don't want to give away anything about the
        // database state to clients that haven't authenticated themselves.
        //
        // Admin connections are allowed to connect anyway.
        if comms.offline() && !admin {
            stream.fatal(ErrorResponse::shutting_down()).await?;
            return Ok(None);
        }

        let mut conn = match Connection::new(user, database, admin) {
            Ok(conn) => conn,
            Err(err) => {
                debug!("connection error: {}", err);
                stream.fatal(ErrorResponse::auth(user, database)).await?;
                return Ok(None);
            }
        };

        // Get connection parameters. These will be most likely cached,
        // unless the pool was just created.
        let server_params = match conn.parameters(&Request::unrouted(id)).await {
            Ok(params) => params,
            Err(err) => {
                if err.no_server() {
                    error!(
                        "aborting new client connection, connection pool is down [{}]",
                        addr
                    );
                    stream
                        .fatal(ErrorResponse::connection(user, database))
                        .await?;
                    return Ok(None);
                } else {
                    return Err(err.into());
                }
            }
        };

        for param in server_params {
            stream.send(&param).await?;
        }

        stream.send(&key).await?;
        stream.send_flush(&ReadyForQuery::idle()).await?;
        comms.connect(key.clone(), addr, &params);

        if config.config.general.log_connections {
            info!(
                r#"client "{}" connected to database "{}" [{}, auth: {}] {}"#,
                user,
                database,
                addr,
                if passthrough {
                    "passthrough".into()
                } else {
                    auth_type.to_string()
                },
                if stream.is_tls() { "🔒" } else { "" }
            );
        }

        debug!(
            "client \"{}\" startup parameters: {} [{}]",
            user, params, addr
        );

        Ok(Some(Self {
            addr,
            stream,
            key,
            comms,
            admin,
            streaming: false,
            params: params.clone(),
            prepared_statements: PreparedStatements::new(),
            transaction: None,
            timeouts: Timeouts::from_config(&config.config.general),
            client_request: ClientRequest::default(),
            stream_buffer: MessageBuffer::new(
                config.config.memory.message_buffer,
                config.config.general.frontend_query_size_limit_block(),
            ),
            sticky: Sticky::from_params(&params),
            database: database.to_string(),
            query_log_stdout: false,
            query_size_limit: None,
        }))
    }

    #[cfg(test)]
    pub fn new_test(stream: Stream, params: Parameters) -> Self {
        use crate::config::config;

        let mut connect_params = Parameters::default();
        connect_params.insert("user", "pgdog");
        connect_params.insert("database", "pgdog");
        connect_params.merge(params);

        let id = FrontendPid::new();
        let key = BackendKeyData::new_frontend(ProtocolVersion::V3_0, id);
        let mut prepared_statements = PreparedStatements::new();
        prepared_statements.level = config().config.general.prepared_statements;

        Self {
            stream,
            addr: SocketAddr::from(([127, 0, 0, 1], 1234)),
            key,
            comms: ClientComms::new(id),
            streaming: false,
            prepared_statements,
            admin: false,
            transaction: None,
            timeouts: Timeouts::from_config(&config().config.general),
            client_request: ClientRequest::default(),
            stream_buffer: MessageBuffer::new(
                4096,
                config().config.general.frontend_query_size_limit_block(),
            ),
            sticky: Sticky::from_params(&connect_params),
            params: connect_params,
            database: "pgdog".to_string(),
            query_log_stdout: false,
            query_size_limit: None,
        }
    }

    /// Run the client and log disconnect.
    async fn spawn_internal(&mut self) {
        match self.run().await {
            Ok(_) => {
                if config().config.general.log_disconnections {
                    let (user, database) = user_database_from_params(&self.params);
                    info!(
                        r#"client "{}" disconnected from database "{}" [{}]"#,
                        user, database, self.addr
                    )
                }
            }
            Err(err) => {
                let _ = self
                    .stream
                    .fatal(ErrorResponse::from_client_err(&err))
                    .await;
                if config().config.general.log_disconnections {
                    let (user, database) = user_database_from_params(&self.params);
                    error!(
                        r#"client "{}" disconnected from database "{}" with error [{}]: {}"#,
                        user, database, self.addr, err
                    )
                }
            }
        }
    }

    /// Run the client.
    async fn run(&mut self) -> Result<(), Error> {
        let shutdown = self.comms.shutting_down();
        let mut query_engine = QueryEngine::from_client(self)?;

        loop {
            // Check if we should be shutting down.
            let offline = self.comms.offline();
            // Check that there are no active transactions.
            let query_engine_done = query_engine.can_disconnect();

            // If query engine is idle and we requested shutdown, we're done.
            if query_engine_done && offline {
                // Send shutdown notification to client.
                self.stream
                    .send_flush(&ErrorResponse::shutting_down())
                    .await?;
                break;
            }

            let client_state = query_engine.client_state();

            select! {
                _ = shutdown.notified() => {
                    continue; // Wake up task.
                }

                // Async messages.
                message = query_engine.read_backend() => {
                    let message = message?;
                    self.server_message(&mut query_engine, message).await?;
                }

                buffer = self.buffer(client_state) => {
                    let event = buffer?;

                    // Only send requests to the backend if they are complete.
                    if self.client_request.is_complete()
                        && !self.client_request.messages.is_empty() {
                            self.client_messages(&mut query_engine).await?;
                        }

                    match event {
                        // Client disconnected, we're done.
                        BufferEvent::DisconnectAbrupt | BufferEvent::DisconnectGraceful => break,
                        BufferEvent::HaveRequest => (),
                    }
                }
            }
        }

        Ok(())
    }

    async fn server_message(
        &mut self,
        query_engine: &mut QueryEngine,
        message: Message,
    ) -> Result<(), Error> {
        let mut context = QueryEngineContext::new(self);
        query_engine
            .process_server_message(&mut context, message)
            .await?;
        self.transaction = context.transaction();

        Ok(())
    }

    /// Handle client messages.
    async fn client_messages(&mut self, query_engine: &mut QueryEngine) -> Result<(), Error> {
        // Check maintenance mode.
        if !self.in_transaction()
            && !self.admin
            && let Some(waiter) = maintenance_mode::waiter(&self.database)
        {
            let state = query_engine.get_state();
            query_engine.set_state(State::Waiting);
            waiter.await;
            query_engine.set_state(state);
        }

        // If client sent multiple requests, split them up and execute individually.
        let spliced = self.client_request.spliced()?;
        if spliced.is_empty() {
            let mut context = QueryEngineContext::new(self);
            query_engine.handle(&mut context).await?;
            self.transaction = context.transaction();
        } else {
            let total = spliced.len();
            let mut reqs = spliced.into_iter().enumerate();
            while let Some((num, mut req)) = reqs.next() {
                debug!("processing spliced request {}/{}", num + 1, total);
                let mut context = QueryEngineContext::new(self).spliced(&mut req, reqs.len());
                query_engine.handle(&mut context).await?;
                self.transaction = context.transaction();

                // If pipeline is aborted due to error, skip to Sync to complete the pipeline.
                // Postgres ignores all commands after an error until it receives Sync.
                if query_engine.out_of_sync() && !req.is_sync_only() {
                    debug!("pipeline aborted, skipping to Sync");
                    for (_, mut next_req) in reqs.by_ref() {
                        if next_req.is_sync_only() {
                            debug!("processing Sync to complete aborted pipeline");
                            let mut ctx = QueryEngineContext::new(self).spliced(&mut next_req, 0);
                            query_engine.handle(&mut ctx).await?;
                            self.transaction = ctx.transaction();
                            break;
                        }
                    }
                    break;
                }
            }
        }

        // Check buffer size once per request.
        self.stream_buffer.shrink_to_fit();

        Ok(())
    }

    /// Buffer extended protocol messages until client requests a sync.
    ///
    /// This ensures we don't check out a connection from the pool until the client
    /// sent a complete request.
    async fn buffer(&mut self, state: State) -> Result<BufferEvent, Error> {
        self.client_request.clear();

        // Only start timer once we receive the first message.
        let mut timer = None;

        // Check config once per request.
        let config = config::config();
        // Configure prepared statements cache.
        self.prepared_statements.level = config.prepared_statements();
        self.timeouts = Timeouts::from_config(&config.config.general);
        self.query_log_stdout = config.config.general.query_log_stdout;
        self.query_size_limit = config.config.general.query_size_limit;
        self.stream_buffer
            .set_size_limit_block(config.config.general.frontend_query_size_limit_block());

        while !self.client_request.is_complete() {
            let idle_timeout = self
                .timeouts
                .client_idle_timeout(&state, &self.client_request);

            let message =
                match timeout(idle_timeout, self.stream_buffer.read(&mut self.stream)).await {
                    Err(_) => {
                        self.stream
                            .fatal(ErrorResponse::client_idle_timeout(idle_timeout, &state))
                            .await?;
                        return Ok(BufferEvent::DisconnectAbrupt);
                    }

                    Ok(Ok(message)) => message.stream(self.streaming).frontend(),
                    Ok(Err(err)) => {
                        if let Some(response) = err.as_fatal_error_response() {
                            self.stream.fatal(response).await?;
                        }
                        return Ok(BufferEvent::DisconnectAbrupt);
                    }
                };

            if timer.is_none() {
                timer = Some(Instant::now());
            }

            // Terminate (B & F).
            if message.code() == 'X' {
                return Ok(BufferEvent::DisconnectGraceful);
            } else {
                let message = ProtocolMessage::from_bytes(message.to_bytes())?;
                self.client_request.push(message);
            }
        }

        if !enabled!(LogLevel::TRACE) {
            debug!(
                "request buffered [{:.4}ms] {:?}",
                timer.unwrap().elapsed().as_secs_f64() * 1000.0,
                self.client_request
                    .messages
                    .iter()
                    .map(|m| m.code())
                    .collect::<Vec<_>>(),
            );
        } else {
            trace!(
                "request buffered [{:.4}ms]\n{:#?}",
                timer.unwrap().elapsed().as_secs_f64() * 1000.0,
                self.client_request,
            );
        }

        Ok(BufferEvent::HaveRequest)
    }

    pub fn in_transaction(&self) -> bool {
        self.transaction.is_some()
    }

    /// Get client memory stats.
    pub fn memory_stats(&self) -> MemoryStats {
        MemoryStats {
            inner: pgdog_stats::MemoryStats {
                buffer: *self.stream_buffer.stats(),
                prepared_statements: self.prepared_statements.memory_used(),
                stream: self.stream.memory_usage(),
            },
        }
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.comms.disconnect();
        self.prepared_statements.close_all();
    }
}

#[cfg(test)]
impl Client {
    pub async fn spawn_test(mut self) {
        self.spawn_internal().await;
    }
}

impl MemoryUsage for Client {
    #[inline]
    fn memory_usage(&self) -> usize {
        std::mem::size_of::<SocketAddr>()
            + std::mem::size_of::<Stream>()
            + std::mem::size_of::<BackendKeyData>()
            + self.params.memory_usage()
            + std::mem::size_of::<ClientComms>()
            + std::mem::size_of::<bool>() * 5
            + self.prepared_statements.memory_used()
            + std::mem::size_of::<Timeouts>()
            + self.stream_buffer.capacity()
            + self.client_request.memory_usage()
    }
}

#[cfg(test)]
pub mod test;

#[derive(Copy, Clone, PartialEq, Debug)]
enum BufferEvent {
    DisconnectGraceful,
    DisconnectAbrupt,
    HaveRequest,
}
