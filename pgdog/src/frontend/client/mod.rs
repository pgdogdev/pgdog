//! Frontend client.

use std::net::SocketAddr;
use std::time::Instant;

use bytes::BytesMut;
use timeouts::Timeouts;
use tokio::time::timeout;
use tokio::{select, spawn};
use tracing::{debug, enabled, error, info, trace, Level as LogLevel};

use super::{ClientRequest, Comms, Error, PreparedStatements};
use crate::auth::{md5, rate_limit, scram::Server};
use crate::backend::maintenance_mode;
use crate::backend::{
    databases,
    pool::{Connection, Request},
};
use crate::config::{self, config, AuthType};
use crate::frontend::client::query_engine::{QueryEngine, QueryEngineContext};
use crate::net::messages::{
    Authentication, BackendKeyData, ErrorResponse, FromBytes, Message, Password, Protocol,
    ReadyForQuery, ToBytes,
};
use crate::net::ProtocolMessage;
use crate::net::{parameter::Parameters, Stream};
use crate::state::State;
use crate::stats::memory::MemoryUsage;
use crate::util::user_database_from_params;

// pub mod counter;
pub mod query_engine;
pub mod timeouts;

/// Frontend client.
pub struct Client {
    addr: SocketAddr,
    stream: Stream,
    id: BackendKeyData,
    #[allow(dead_code)]
    connect_params: Parameters,
    params: Parameters,
    comms: Comms,
    admin: bool,
    streaming: bool,
    shutdown: bool,
    prepared_statements: PreparedStatements,
    transaction: Option<TransactionType>,
    timeouts: Timeouts,
    client_request: ClientRequest,
    stream_buffer: BytesMut,
    passthrough_password: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TransactionType {
    ReadOnly,
    #[default]
    ReadWrite,
    Error,
}

impl TransactionType {
    pub fn read_only(&self) -> bool {
        matches!(self, Self::ReadOnly)
    }

    pub fn write(&self) -> bool {
        !self.read_only()
    }

    pub fn error(&self) -> bool {
        matches!(self, Self::Error)
    }
}

impl MemoryUsage for Client {
    #[inline]
    fn memory_usage(&self) -> usize {
        std::mem::size_of::<SocketAddr>()
            + std::mem::size_of::<Stream>()
            + std::mem::size_of::<BackendKeyData>()
            + self.connect_params.memory_usage()
            + self.params.memory_usage()
            + std::mem::size_of::<Comms>()
            + std::mem::size_of::<bool>() * 5
            + self.prepared_statements.memory_used()
            + std::mem::size_of::<Timeouts>()
            + self.stream_buffer.memory_usage()
            + self.client_request.memory_usage()
            + self
                .passthrough_password
                .as_ref()
                .map(|s| s.capacity())
                .unwrap_or(0)
    }
}

impl Client {
    /// Create new frontend client from the given TCP stream.
    pub async fn spawn(
        mut stream: Stream,
        params: Parameters,
        addr: SocketAddr,
        mut comms: Comms,
    ) -> Result<(), Error> {
        let (user, database) = user_database_from_params(&params);
        let config = config::config();

        let admin = database == config.config.admin.name && config.config.admin.user == user;
        let admin_password = &config.config.admin.password;
        let auth_type = &config.config.general.auth_type;

        let id = BackendKeyData::new();

        // Auto database.
        let exists = databases::databases().exists((user, database));
        let passthrough_password = if config.config.general.passthrough_auth() && !admin {
            let password = if auth_type.trust() {
                // Use empty password.
                // TODO: Postgres must be using "trust" auth
                // or some other kind of authentication that doesn't require a password.
                Password::new_password("")
            } else {
                // Get the password.
                stream
                    .send_flush(&Authentication::ClearTextPassword)
                    .await?;
                let password = stream.read().await?;
                Password::from_bytes(password.to_bytes()?)?
            };

            if !exists {
                let user = config::User::from_params(&params, &password).ok();
                if let Some(user) = user {
                    databases::add(user);
                }
            }
            password.password().map(|p| p.to_owned())
        } else {
            None
        };

        // Get server parameters and send them to the client.
        let mut conn = match Connection::new(user, database, admin, &passthrough_password) {
            Ok(conn) => conn,
            Err(_) => {
                stream.fatal(ErrorResponse::auth(user, database)).await?;
                return Ok(());
            }
        };

        if let Some(addr) = *stream.peer_addr() {
            // Apply rate limiting only if enabled in config
            if config.config.general.auth_rate_limit.is_some() && !rate_limit::check(addr.ip()) {
                error!(
                    "Authentication rate limit exceeded for IP: {}, user: \"{}\", database: \"{}\"",
                    addr.ip(),
                    user,
                    database
                );
                // Send generic auth error to prevent information leakage to attacker
                stream.fatal(ErrorResponse::auth(user, database)).await?;
                return Ok(());
            }
        }

        let password = if admin {
            admin_password
        } else {
            conn.cluster()?.password()
        };

        let auth_type = &config.config.general.auth_type;
        let auth_ok = match (auth_type, stream.is_tls()) {
            // TODO: SCRAM doesn't work with TLS currently because of
            // lack of support for channel binding in our scram library.
            // Defaulting to MD5.
            (AuthType::Scram, true) | (AuthType::Md5, _) => {
                let md5 = md5::Client::new(user, password);
                stream.send_flush(&md5.challenge()).await?;
                let password = Password::from_bytes(stream.read().await?.to_bytes()?)?;
                if let Password::PasswordMessage { response } = password {
                    md5.check(&response)
                } else {
                    false
                }
            }

            (AuthType::Scram, false) => {
                stream.send_flush(&Authentication::scram()).await?;

                let scram = Server::new(password);
                let res = scram.handle(&mut stream).await;
                matches!(res, Ok(true))
            }

            (AuthType::Trust, _) => true,
        };

        if !auth_ok {
            stream.fatal(ErrorResponse::auth(user, database)).await?;
            return Ok(());
        } else {
            stream.send(&Authentication::Ok).await?;
        }

        // Check if the pooler is shutting down.
        if comms.offline() && !admin {
            stream.fatal(ErrorResponse::shutting_down()).await?;
            return Ok(());
        }

        let server_params = match conn.parameters(&Request::new(id)).await {
            Ok(params) => params,
            Err(err) => {
                if err.no_server() {
                    error!("connection pool is down");
                    stream.fatal(ErrorResponse::connection()).await?;
                    return Ok(());
                } else {
                    return Err(err.into());
                }
            }
        };

        for param in server_params {
            stream.send(&param).await?;
        }

        stream.send(&id).await?;
        stream.send_flush(&ReadyForQuery::idle()).await?;
        comms.connect(&id, addr, &params);

        if config.config.general.log_connections {
            info!(
                r#"client "{}" connected to database "{}" [{}]"#,
                user, database, addr
            );
        }

        let mut client = Self {
            addr,
            stream,
            id,
            comms,
            admin,
            streaming: false,
            params: params.clone(),
            connect_params: params,
            prepared_statements: PreparedStatements::new(),
            transaction: None,
            timeouts: Timeouts::from_config(&config.config.general),
            client_request: ClientRequest::new(),
            stream_buffer: BytesMut::new(),
            shutdown: false,

            passthrough_password,
        };

        drop(conn);

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

    #[cfg(test)]
    pub fn new_test(stream: Stream, addr: SocketAddr) -> Self {
        use crate::{config::config, frontend::comms::comms};

        let mut connect_params = Parameters::default();
        connect_params.insert("user", "pgdog");
        connect_params.insert("database", "pgdog");

        Self {
            stream,
            addr,
            id: BackendKeyData::new(),
            comms: comms(),
            streaming: false,
            prepared_statements: PreparedStatements::new(),
            connect_params: connect_params.clone(),
            params: connect_params,
            admin: false,
            transaction: None,
            timeouts: Timeouts::from_config(&config().config.general),
            client_request: ClientRequest::new(),
            stream_buffer: BytesMut::new(),
            shutdown: false,
            passthrough_password: None,
        }
    }

    /// Get client's identifier.
    pub fn id(&self) -> BackendKeyData {
        self.id
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
                    .error(ErrorResponse::from_err(&err), false)
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
        let mut offline;
        let mut query_engine = QueryEngine::from_client(self)?;

        loop {
            offline = (self.comms.offline() && !self.admin || self.shutdown) && query_engine.done();
            if offline {
                break;
            }

            let client_state = query_engine.client_state();

            select! {
                _ = shutdown.notified() => {
                    if query_engine.done() {
                        continue; // Wake up task.
                    }
                }

                // Async messages.
                message = query_engine.read_backend() => {
                    let message = message?;
                    self.server_message(&mut query_engine, message).await?;
                }

                buffer = self.buffer(client_state) => {
                    let event = buffer?;
                    if !self.client_request.messages.is_empty() {
                        self.client_messages(&mut query_engine).await?;
                    }

                    match event {
                        BufferEvent::DisconnectAbrupt => break,
                        BufferEvent::DisconnectGraceful => {
                            let done = query_engine.done();

                            if done {
                                break;
                            }
                        }

                        BufferEvent::HaveRequest => (),
                    }
                }
            }
        }

        if offline && !self.shutdown {
            self.stream
                .send_flush(&ErrorResponse::shutting_down())
                .await?;
        }

        Ok(())
    }

    async fn server_message(
        &mut self,
        query_engine: &mut QueryEngine,
        message: Message,
    ) -> Result<(), Error> {
        let mut context = QueryEngineContext::new(self);
        query_engine.server_message(&mut context, message).await?;
        self.transaction = context.transaction();

        Ok(())
    }

    /// Handle client messages.
    async fn client_messages(&mut self, query_engine: &mut QueryEngine) -> Result<(), Error> {
        // Check maintenance mode.
        if !self.in_transaction() && !self.admin {
            if let Some(waiter) = maintenance_mode::waiter() {
                let state = query_engine.get_state();
                query_engine.set_state(State::Waiting);
                waiter.await;
                query_engine.set_state(state);
            }
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
            }
        }

        Ok(())
    }

    /// Buffer extended protocol messages until client requests a sync.
    ///
    /// This ensures we don't check out a connection from the pool until the client
    /// sent a complete request.
    async fn buffer(&mut self, state: State) -> Result<BufferEvent, Error> {
        self.client_request.messages.clear();

        // Only start timer once we receive the first message.
        let mut timer = None;

        // Check config once per request.
        let config = config::config();
        // Configure prepared statements cache.
        self.prepared_statements.enabled = config.prepared_statements();
        self.prepared_statements.capacity = config.config.general.prepared_statements_limit;
        self.timeouts = Timeouts::from_config(&config.config.general);

        while !self.client_request.full() {
            let idle_timeout = self
                .timeouts
                .client_idle_timeout(&state, &self.client_request);

            let message =
                match timeout(idle_timeout, self.stream.read_buf(&mut self.stream_buffer)).await {
                    Err(_) => {
                        self.stream
                            .fatal(ErrorResponse::client_idle_timeout(idle_timeout))
                            .await?;
                        return Ok(BufferEvent::DisconnectAbrupt);
                    }

                    Ok(Ok(message)) => message.stream(self.streaming).frontend(),
                    Ok(Err(_)) => return Ok(BufferEvent::DisconnectAbrupt),
                };

            if timer.is_none() {
                timer = Some(Instant::now());
            }

            // Terminate (B & F).
            if message.code() == 'X' {
                self.shutdown = true;
                return Ok(BufferEvent::DisconnectGraceful);
            } else {
                let message = ProtocolMessage::from_bytes(message.to_bytes()?)?;
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
}

impl Drop for Client {
    fn drop(&mut self) {
        self.comms.disconnect();
        self.prepared_statements.close_all();
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
