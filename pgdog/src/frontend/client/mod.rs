//! Frontend client.

use std::net::SocketAddr;
use std::time::Instant;

use bytes::BytesMut;
use engine::EngineContext;

use timeouts::Timeouts;
use tokio::time::timeout;
use tokio::{select, spawn};
use tracing::{debug, enabled, error, info, trace, Level as LogLevel};
use transaction::Transaction;

use super::{ClientRequest, Command, Comms, Error, PreparedStatements};
use crate::auth::{md5, scram::Server};
use crate::backend::{
    databases,
    pool::{Connection, Request},
};
use crate::config::{self, AuthType};
use crate::frontend::buffer::BufferedQuery;
#[cfg(debug_assertions)]
use crate::frontend::QueryLogger;
use crate::net::messages::{
    Authentication, BackendKeyData, CommandComplete, ErrorResponse, FromBytes, Message, Password,
    Protocol, ReadyForQuery, ToBytes,
};
use crate::net::ProtocolMessage;
use crate::net::{parameter::Parameters, Stream};
use crate::net::{DataRow, EmptyQueryResponse, Field, NoticeResponse, RowDescription};
use crate::state::State;
use crate::stats::memory::MemoryUsage;

pub mod counter;
pub mod engine;
pub mod inner;
pub mod query_engine;
pub mod timeouts;
pub mod transaction;

pub use engine::Engine;
use inner::{Inner, InnerBorrow};

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
    timeouts: Timeouts,
    request: ClientRequest,
    stream_buffer: BytesMut,
    cross_shard_disabled: bool,
    passthrough_password: Option<String>,
    replication_mode: bool,
    transaction: Transaction,
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
            + self.request.memory_usage()
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
        let replication_mode = params
            .get("replication")
            .map(|v| v.as_str() == Some("database"))
            .unwrap_or(false);
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
        let shard = params.shard();

        info!(
            "client connected [{}]{}",
            addr,
            if let Some(ref shard) = shard {
                format!(" (replication, shard {})", shard)
            } else {
                "".into()
            }
        );
        if replication_mode {
            debug!("replication mode [{}]", addr);
        }

        let mut prepared_statements = PreparedStatements::new();
        prepared_statements.enabled = config.prepared_statements();

        let mut client = Self {
            addr,
            stream,
            id,
            comms,
            admin,
            streaming: false,
            params: params.clone(),
            replication_mode,
            connect_params: params,
            prepared_statements: PreparedStatements::new(),
            timeouts: Timeouts::from_config(&config.config.general),
            request: ClientRequest::new(),
            stream_buffer: BytesMut::new(),
            shutdown: false,
            cross_shard_disabled: false,
            passthrough_password,
            transaction: Transaction::default(),
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
            timeouts: Timeouts::from_config(&config().config.general),
            request: ClientRequest::new(),
            stream_buffer: BytesMut::new(),
            shutdown: false,
            cross_shard_disabled: false,
            passthrough_password: None,
            replication_mode: false,
            transaction: Transaction::default(),
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
        let mut inner = Inner::new(self)?;
        let shutdown = self.comms.shutting_down();

        loop {
            let query_timeout = self.timeouts.query_timeout(&inner.stats.state);

            select! {
                _ = shutdown.notified() => {
                    if !inner.backend.connected() && inner.start_transaction.is_none() {
                        break;
                    }
                }

                // Async messages.
                message = timeout(query_timeout, inner.backend.read()) => {
                    let message = message??;
                    let disconnect = self.server_message(&mut inner.get(), message).await?;
                    if disconnect {
                        break;
                    }
                }

                buffer = self.buffer_request(&inner.stats.state) => {
                    let event = buffer?;
                    if !self.request.is_empty() {
                        let disconnect = self.client_messages(inner.get()).await?;

                        if disconnect {
                            break;
                        }
                    }

                    match event {
                        BufferEvent::DisconnectAbrupt => break,
                        BufferEvent::DisconnectGraceful => {
                            let connected = inner.get().backend.connected();

                            if !connected {
                                break;
                            }
                        }

                        BufferEvent::HaveRequest => (),
                    }
                }
            }
        }

        if inner.comms.offline() && !self.admin {
            self.stream
                .send_flush(&ErrorResponse::shutting_down())
                .await?;
        }

        Ok(())
    }

    /// Handle client messages.
    async fn client_messages(&mut self, mut inner: InnerBorrow<'_>) -> Result<bool, Error> {
        inner.stats.received(self.request.total_message_len());

        #[cfg(debug_assertions)]
        if let Some(query) = self.request.query()? {
            debug!(
                "{} [{}] (in transaction: {})",
                query.query(),
                self.addr,
                self.transaction.started()
            );
            QueryLogger::new(&self.request).log().await?;
        }

        let context = EngineContext::new(self, &inner);

        // Query execution engine.
        let mut engine = Engine::new(context);

        use engine::Action;

        match engine.execute().await? {
            Action::Intercept(msgs) => {
                self.stream.send_many(&msgs).await?;
                inner.done(self.transaction.started());
                self.update_stats(&mut inner);
                return Ok(false);
            }

            Action::Forward => (),
        };

        let connected = inner.connected();

        let command = match inner.command(
            &mut self.request,
            &mut self.prepared_statements,
            &self.params,
            self.transaction.started(),
        ) {
            Ok(command) => command,
            Err(err) => {
                if err.empty_query() {
                    self.stream.send(&EmptyQueryResponse).await?;
                    self.stream
                        .send_flush(&ReadyForQuery::in_transaction(self.transaction.started()))
                        .await?;
                } else {
                    error!("{:?} [{}]", err, self.addr);
                    self.stream
                        .error(
                            ErrorResponse::syntax(err.to_string().as_str()),
                            self.transaction.started(),
                        )
                        .await?;
                }
                inner.done(self.transaction.started());
                return Ok(false);
            }
        };

        match command {
            Some(Command::Query(route)) => {
                self.request.set_route(route);
                self.transaction.set_route(route);

                // Check if cross-shard transactions are disabled.
                if route.is_cross_shard() && self.cross_shard_disabled {
                    self.stream
                        .error(
                            ErrorResponse::cross_shard_disabled(),
                            self.transaction.started(),
                        )
                        .await?;
                    inner.done(self.transaction.started());
                    inner.reset_router();
                    return Ok(false);
                }
            }

            Some(Command::CommitTransaction) => {
                self.request.set_route(self.transaction.transaction_route());
            }

            Some(Command::Deallocate) => {
                self.finish_command(&mut inner, "DEALLOCATE").await?;
                inner.done(self.transaction.started());
                return Ok(false);
            }

            Some(Command::StartTransaction(query)) => {
                if let BufferedQuery::Query(_) = query {
                    self.start_transaction(query).await?;
                    inner.done(true);
                    return Ok(false);
                }
            }

            // TODO: Handling session variables requires a lot more work,
            // e.g. we need to track RESET as well.
            Some(Command::Set { name, value }) => {
                if !self.transaction.started() {
                    self.params.insert(name, value.clone());
                    self.set(inner).await?;
                    return Ok(false);
                }
            }

            Some(Command::RollbackTransaction) => {
                if self.transaction.started() && connected {
                    inner.backend.execute("ROLLBACK").await?;
                }
                self.end_transaction(true).await?;
                inner.done(false);
                return Ok(false);
            }

            Some(Command::Listen { channel, shard }) => {
                let channel = channel.clone();
                let shard = shard.clone();
                inner.backend.listen(&channel, shard).await?;

                self.finish_command(&mut inner, "LISTEN").await?;
                return Ok(false);
            }

            Some(Command::Notify {
                channel,
                payload,
                shard,
            }) => {
                let channel = channel.clone();
                let shard = shard.clone();
                let payload = payload.clone();
                inner.backend.notify(&channel, &payload, shard).await?;

                self.finish_command(&mut inner, "NOTIFY").await?;
                return Ok(false);
            }

            Some(Command::Unlisten(channel)) => {
                let channel = channel.clone();
                inner.backend.unlisten(&channel);

                self.finish_command(&mut inner, "UNLISTEN").await?;
                return Ok(false);
            }

            Some(Command::Shards(shards)) => {
                let rd = RowDescription::new(&[Field::bigint("shards")]);
                let mut dr = DataRow::new();
                dr.add(*shards as i64);
                let cc = CommandComplete::from_str("SHOW");
                let rfq = ReadyForQuery::in_transaction(self.transaction.started());
                self.stream
                    .send_many(&[rd.message()?, dr.message()?, cc.message()?, rfq.message()?])
                    .await?;
                inner.done(self.transaction.started());
                return Ok(false);
            }

            _ => (),
        }

        trace!("request buffered \n{:#?}", self.request);

        if !connected {
            // Grab a connection from the right pool.
            let request = Request::new(self.id);
            let route = self.transaction.transaction_route();

            match inner.connect(&request, route).await {
                Ok(()) => {
                    let query_timeout = self.timeouts.query_timeout(&inner.stats.state);
                    // We may need to sync params with the server
                    // and that reads from the socket.
                    timeout(query_timeout, inner.backend.link_client(&self.params)).await??;
                }
                Err(err) => {
                    if err.no_server() {
                        error!("{} [{}]", err, self.addr);
                        self.stream
                            .error(ErrorResponse::from_err(&err), self.transaction.started())
                            .await?;
                        // TODO: should this be wrapped in a method?
                        inner.disconnect();
                        inner.reset_router();
                        inner.done(self.transaction.started());
                        return Ok(false);
                    } else {
                        return Err(err.into());
                    }
                }
            };
        }

        // Start transaction if we haven't already.
        if !self.transaction.started() {
            self.transaction.server_start();
        }

        // We don't start a transaction on the servers until
        // a client is actually executing something.
        //
        // This prevents us holding open connections to multiple servers
        if self.request.executable() {
            if let Some(query) = self.transaction.take_begin() {
                inner.backend.execute(&query).await?;
            }
        }

        for msg in self.request.iter() {
            if let ProtocolMessage::Bind(bind) = msg {
                inner.backend.bind(bind)?
            }
        }

        // Queue up request to mirrors, if any.
        // Do this before sending query to actual server
        // to have accurate timings between queries.
        inner.backend.mirror(&self.request);

        // Send request to actual server.
        inner.handle_request(&self.request, self.streaming).await?;

        self.update_stats(&mut inner);

        #[cfg(test)]
        let handle_response = false;
        #[cfg(not(test))]
        let handle_response = !self.streaming && !self.replication_mode;

        if handle_response {
            let query_timeout = self.timeouts.query_timeout(&inner.stats.state);

            while inner.backend.has_more_messages() && !inner.backend.copy_mode() {
                let message = timeout(query_timeout, inner.backend.read()).await??;
                if self.server_message(&mut inner, message).await? {
                    return Ok(true);
                }
            }
        }

        Ok(false)
    }

    /// Handle message from server(s).
    async fn server_message(
        &mut self,
        inner: &mut InnerBorrow<'_>,
        message: Message,
    ) -> Result<bool, Error> {
        let code = message.code();
        let message = message.backend();
        let has_more_messages = inner.backend.has_more_messages();

        // Messages that we need to send to the client immediately.
        // ReadyForQuery (B) | CopyInResponse (B) | ErrorResponse(B) | NoticeResponse(B) | NotificationResponse (B)
        let flush = matches!(code, 'Z' | 'G' | 'E' | 'N' | 'A')
            || !has_more_messages
            || message.streaming();

        // Server finished executing a query.
        // ReadyForQuery (B)
        if code == 'Z' {
            inner.stats.query();
            if !message.in_transaction() {
                self.transaction.finish();
                inner.backend.mirror_flush();
            }
            inner.stats.idle(self.transaction.started());
        }

        inner.stats.sent(message.len());

        // Release the connection back into the pool
        // before flushing data to client.
        // Flushing can take a minute and we don't want to block
        // the connection from being reused.
        if inner.backend.done() {
            let changed_params = inner.backend.changed_params();
            if inner.transaction_mode() && !self.replication_mode {
                inner.disconnect();
            }
            inner.stats.transaction();
            inner.reset_router();
            debug!(
                "transaction finished [{:.3}ms]",
                inner.stats.last_transaction_time.as_secs_f64() * 1000.0
            );

            // Update client params with values
            // sent from the server using ParameterStatus(B) messages.
            if !changed_params.is_empty() {
                for (name, value) in changed_params.iter() {
                    debug!("setting client's \"{}\" to {}", name, value);
                    self.params.insert(name.clone(), value.clone());
                }
                inner.comms.update_params(&self.params);
            }
        }

        if flush {
            self.stream.send_flush(&message).await?;
        } else {
            self.stream.send(&message).await?;
        }

        // Pooler is offline or the client requested to disconnect and the transaction is done.
        if inner.backend.done() && (inner.comms.offline() || self.shutdown) && !self.admin {
            return Ok(true);
        }

        Ok(false)
    }

    /// Buffer extended protocol messages until client requests a sync.
    ///
    /// This ensures we don't check out a connection from the pool until the client
    /// sent a complete request.
    async fn buffer_request(&mut self, state: &State) -> Result<BufferEvent, Error> {
        self.request.clear();

        // Only start timer once we receive the first message.
        let mut timer = None;

        // Check config once per request.
        let config = config::config();
        // Configure prepared statements cache.
        self.prepared_statements.enabled = config.prepared_statements();
        self.prepared_statements.capacity = config.config.general.prepared_statements_limit;
        self.timeouts = Timeouts::from_config(&config.config.general);
        self.cross_shard_disabled = config.config.general.cross_shard_disabled;

        while !self.request.full() {
            let idle_timeout = self.timeouts.client_idle_timeout(state, &self.request);

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
                    self.request
                        .push(self.prepared_statements.maybe_rewrite(message)?);
                } else {
                    self.request.push(message);
                }
            }
        }

        if !enabled!(LogLevel::TRACE) {
            debug!(
                "request buffered [{:.4}ms] {:?}",
                timer.unwrap().elapsed().as_secs_f64() * 1000.0,
                self.request.iter().map(|m| m.code()).collect::<Vec<_>>(),
            );
        }

        Ok(BufferEvent::HaveRequest)
    }

    /// Tell the client we started a transaction.
    async fn start_transaction(&mut self, query: &BufferedQuery) -> Result<(), Error> {
        self.transaction.buffer(query);
        self.stream
            .send_many(&[
                CommandComplete::new_begin().message()?.backend(),
                ReadyForQuery::in_transaction(true).message()?,
            ])
            .await?;
        debug!("transaction started");
        Ok(())
    }

    /// Tell the client we finished a transaction (without doing any work).
    ///
    /// This avoids connecting to servers when clients start and commit transactions
    /// with no queries.
    async fn end_transaction(&mut self, rollback: bool) -> Result<(), Error> {
        let cmd = if rollback {
            CommandComplete::new_rollback()
        } else {
            CommandComplete::new_commit()
        };
        let mut messages = if !self.transaction.started() {
            vec![NoticeResponse::from(ErrorResponse::no_transaction()).message()?]
        } else {
            self.transaction.finish();
            vec![]
        };

        messages.push(cmd.message()?.backend());
        messages.push(ReadyForQuery::idle().message()?);
        self.stream.send_many(&messages).await?;
        debug!("transaction ended");
        Ok(())
    }

    /// Handle SET command.
    async fn set(&mut self, mut inner: InnerBorrow<'_>) -> Result<(), Error> {
        self.finish_command(&mut inner, "SET").await?;
        inner.comms.update_params(&self.params);
        inner.done(self.transaction.started());
        Ok(())
    }

    async fn finish_command(
        &mut self,
        inner: &mut InnerBorrow<'_>,
        command: &str,
    ) -> Result<(), Error> {
        self.stream
            .send_many(&[
                CommandComplete::from_str(command).message()?.backend(),
                ReadyForQuery::in_transaction(self.transaction.started()).message()?,
            ])
            .await?;
        inner.done(self.transaction.started());

        Ok(())
    }

    fn update_stats(&self, inner: &mut InnerBorrow<'_>) {
        inner
            .stats
            .prepared_statements(self.prepared_statements.len_local());
        inner.stats.memory_used(self.memory_usage());
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
