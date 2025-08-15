//! Frontend client.

use std::net::SocketAddr;
use std::time::Instant;

use bytes::BytesMut;
use timeouts::Timeouts;
use tokio::time::timeout;
use tokio::{select, spawn};
use tracing::{debug, enabled, error, info, trace, Level as LogLevel};

use super::{Buffer, Comms, Error, PreparedStatements};
use crate::auth::{md5, scram::Server};
use crate::backend::{
    databases,
    pool::{Connection, Request},
};
use crate::config::{self, AuthType};
use crate::frontend::client::query_engine::{QueryEngine, QueryEngineContext};
use crate::net::messages::{
    Authentication, BackendKeyData, ErrorResponse, FromBytes, Message, Password, Protocol,
    ReadyForQuery, ToBytes,
};
use crate::net::ProtocolMessage;
use crate::net::{parameter::Parameters, Stream};
use crate::state::State;
use crate::stats::memory::MemoryUsage;

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
    in_transaction: bool,
    timeouts: Timeouts,
    request_buffer: Buffer,
    stream_buffer: BytesMut,
    cross_shard_disabled: bool,
    passthrough_password: Option<String>,
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
            + self.request_buffer.memory_usage()
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
        let user = params.get_default("user", "postgres");
        let database = params.get_default("database", user);
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

        info!("client connected [{}]", addr,);

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
            in_transaction: false,
            timeouts: Timeouts::from_config(&config.config.general),
            request_buffer: Buffer::new(),
            stream_buffer: BytesMut::new(),
            shutdown: false,
            cross_shard_disabled: false,
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
            in_transaction: false,
            timeouts: Timeouts::from_config(&config().config.general),
            request_buffer: Buffer::new(),
            stream_buffer: BytesMut::new(),
            shutdown: false,
            cross_shard_disabled: false,
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
            Ok(_) => info!("client disconnected [{}]", self.addr),
            Err(err) => {
                let _ = self
                    .stream
                    .error(ErrorResponse::from_err(&err), false)
                    .await;
                error!("client disconnected with error [{}]: {}", self.addr, err)
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
                    if !self.request_buffer.is_empty() {
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
        self.in_transaction = context.in_transaction();

        Ok(())
    }

    /// Handle client messages.
    async fn client_messages(&mut self, query_engine: &mut QueryEngine) -> Result<(), Error> {
        let mut context = QueryEngineContext::new(self);
        query_engine.handle(&mut context).await?;
        self.in_transaction = context.in_transaction();
        return Ok(());
    }

    /// Buffer extended protocol messages until client requests a sync.
    ///
    /// This ensures we don't check out a connection from the pool until the client
    /// sent a complete request.
    async fn buffer(&mut self, state: State) -> Result<BufferEvent, Error> {
        self.request_buffer.clear();

        // Only start timer once we receive the first message.
        let mut timer = None;

        // Check config once per request.
        let config = config::config();
        // Configure prepared statements cache.
        self.prepared_statements.enabled = config.prepared_statements();
        self.prepared_statements.capacity = config.config.general.prepared_statements_limit;
        self.timeouts = Timeouts::from_config(&config.config.general);
        self.cross_shard_disabled = config.config.general.cross_shard_disabled;

        while !self.request_buffer.full() {
            let idle_timeout = self
                .timeouts
                .client_idle_timeout(&state, &self.request_buffer);

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
                if message.extended() && self.prepared_statements.enabled {
                    self.request_buffer
                        .push(self.prepared_statements.maybe_rewrite(message)?);
                } else {
                    self.request_buffer.push(message);
                }
            }
        }

        if !enabled!(LogLevel::TRACE) {
            debug!(
                "request buffered [{:.4}ms] {:?}",
                timer.unwrap().elapsed().as_secs_f64() * 1000.0,
                self.request_buffer
                    .iter()
                    .map(|m| m.code())
                    .collect::<Vec<_>>(),
            );
        } else {
            trace!(
                "request buffered [{:.4}ms]\n{:#?}",
                timer.unwrap().elapsed().as_secs_f64() * 1000.0,
                self.request_buffer,
            );
        }

        Ok(BufferEvent::HaveRequest)
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
