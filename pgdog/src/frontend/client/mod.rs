//! Frontend client.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::time::Instant;

use bytes::BytesMut;
use timeouts::Timeouts;
use tokio::time::timeout;
use tokio::{select, spawn};
use tracing::{debug, error, info, trace};

use super::{Buffer, Command, Comms, Error, PreparedStatements};
use crate::auth::{md5, scram::Server};
use crate::backend::{
    databases,
    pool::{Connection, Request},
    ProtocolMessage,
};
use crate::config::{self, AuthType};
use crate::frontend::buffer::BufferedQuery;
#[cfg(debug_assertions)]
use crate::frontend::QueryLogger;
use crate::net::messages::{
    Authentication, BackendKeyData, CommandComplete, ErrorResponse, FromBytes, Message, Password,
    Protocol, ReadyForQuery, ToBytes,
};
use crate::net::{parameter::Parameters, Stream};
use crate::net::{DataRow, EmptyQueryResponse, Field, NoticeResponse, RowDescription};

pub mod counter;
pub mod inner;
pub mod timeouts;

use inner::{Inner, InnerBorrow};

/// Frontend client.
#[allow(dead_code)]
pub struct Client {
    addr: SocketAddr,
    stream: Stream,
    id: BackendKeyData,
    connect_params: Parameters,
    params: Parameters,
    comms: Comms,
    admin: bool,
    streaming: bool,
    shutdown: bool,
    shard: Option<usize>,
    prepared_statements: PreparedStatements,
    in_transaction: bool,
    timeouts: Timeouts,
    request_buffer: Buffer,
    stream_buffer: BytesMut,
    message_buffer: VecDeque<ProtocolMessage>,
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
        if !exists && config.config.general.passthrough_auth() {
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
            let user = config::User::from_params(&params, &password).ok();
            if let Some(user) = user {
                databases::add(user);
            }
        }

        // Get server parameters and send them to the client.
        let mut conn = match Connection::new(user, database, admin) {
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

        let mut prepared_statements = PreparedStatements::new();
        prepared_statements.enabled = config.prepared_statements();

        let mut client = Self {
            addr,
            stream,
            id,
            comms,
            admin,
            streaming: false,
            shard,
            params: params.clone(),
            connect_params: params,
            prepared_statements: PreparedStatements::new(),
            in_transaction: false,
            timeouts: Timeouts::from_config(&config.config.general),
            request_buffer: Buffer::new(),
            stream_buffer: BytesMut::new(),
            message_buffer: VecDeque::new(),
            shutdown: false,
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
            shard: None,
            in_transaction: false,
            timeouts: Timeouts::from_config(&config().config.general),
            request_buffer: Buffer::new(),
            stream_buffer: BytesMut::new(),
            message_buffer: VecDeque::new(),
            shutdown: false,
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
                let _ = self.stream.error(ErrorResponse::from_err(&err)).await;
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
                    let disconnect = self.server_message(inner.get(), message).await?;
                    if disconnect {
                        break;
                    }
                }

                buffer = self.buffer() => {
                    let event = buffer?;
                    if !self.request_buffer.is_empty() {
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
        inner.stats.received(self.request_buffer.len());

        #[cfg(debug_assertions)]
        if let Some(query) = self.request_buffer.query()? {
            debug!(
                "{} [{}] (in transaction: {})",
                query.query(),
                self.addr,
                self.in_transaction
            );
            QueryLogger::new(&self.request_buffer).log().await?;
        }

        let connected = inner.connected();

        let command = match inner.command(
            &mut self.request_buffer,
            &mut self.prepared_statements,
            &self.params,
        ) {
            Ok(command) => command,
            Err(err) => {
                if err.empty_query() {
                    self.stream.send(&EmptyQueryResponse::default()).await?;
                    self.stream
                        .send_flush(&ReadyForQuery::in_transaction(self.in_transaction))
                        .await?;
                } else {
                    error!("{:?} [{}]", err, self.addr);
                    self.stream
                        .error(ErrorResponse::syntax(err.to_string().as_str()))
                        .await?;
                }
                inner.done(self.in_transaction);
                return Ok(false);
            }
        };

        self.streaming = matches!(command, Some(Command::StartReplication));

        if !connected {
            // Simulate transaction starting
            // until client sends an actual query.
            //
            // This ensures we:
            //
            // 1. Don't connect to servers unnecessarily.
            // 2. Can use the first query sent by the client to route the transaction
            //    to a shard.
            //
            match command {
                Some(Command::StartTransaction(query)) => {
                    if let BufferedQuery::Query(_) = query {
                        self.start_transaction().await?;
                        inner.start_transaction = Some(query.clone());
                        self.in_transaction = true;
                        inner.done(true);
                        return Ok(false);
                    }
                }
                Some(Command::RollbackTransaction) => {
                    inner.start_transaction = None;
                    self.end_transaction(true).await?;
                    self.in_transaction = false;
                    inner.done(false);
                    return Ok(false);
                }
                Some(Command::CommitTransaction) => {
                    inner.start_transaction = None;
                    self.end_transaction(false).await?;
                    self.in_transaction = false;
                    inner.done(false);
                    return Ok(false);
                }
                // How many shards are configured.
                Some(Command::Shards(shards)) => {
                    let rd = RowDescription::new(&[Field::bigint("shards")]);
                    let mut dr = DataRow::new();
                    dr.add(*shards as i64);
                    let cc = CommandComplete::from_str("SHOW");
                    let rfq = ReadyForQuery::in_transaction(self.in_transaction);
                    self.stream
                        .send_many(&[rd.message()?, dr.message()?, cc.message()?, rfq.message()?])
                        .await?;
                    inner.done(false);
                    return Ok(false);
                }
                // TODO: Handling session variables requires a lot more work,
                // e.g. we need to track RESET as well.
                Some(Command::Set { name, value }) => {
                    self.params.insert(name, value.clone());
                    self.set(inner).await?;
                    return Ok(false);
                }
                _ => (),
            };

            // Grab a connection from the right pool.
            let request = Request::new(self.id);
            match inner.connect(&request).await {
                Ok(()) => {
                    let query_timeout = self.timeouts.query_timeout(&inner.stats.state);
                    // We may need to sync params with the server
                    // and that reads from the socket.
                    timeout(query_timeout, inner.backend.link_client(&self.params)).await??;
                }
                Err(err) => {
                    if err.no_server() {
                        error!("{} [{}]", err, self.addr);
                        self.stream.error(ErrorResponse::from_err(&err)).await?;
                        // TODO: should this be wrapped in a method?
                        inner.disconnect();
                        inner.reset_router();
                        return Ok(false);
                    } else {
                        return Err(err.into());
                    }
                }
            };
        }

        // We don't start a transaction on the servers until
        // a client is actually executing something.
        //
        // This prevents us holding open connections to multiple servers
        if self.request_buffer.executable() {
            if let Some(query) = inner.start_transaction.take() {
                inner.backend.execute(&query).await?;
            }
        }

        for msg in self.request_buffer.iter() {
            if let ProtocolMessage::Bind(bind) = msg {
                inner.backend.bind(bind)?
            }
        }

        inner
            .handle_buffer(&self.request_buffer, self.streaming)
            .await?;

        inner.stats.memory_used(self.stream_buffer.capacity());

        // Send traffic to mirrors, if any.
        inner.backend.mirror(&self.request_buffer);

        Ok(false)
    }

    /// Handle message from server(s).
    async fn server_message(
        &mut self,
        mut inner: InnerBorrow<'_>,
        message: Message,
    ) -> Result<bool, Error> {
        let code = message.code();
        let message = message.backend();
        let has_more_messages = inner.backend.has_more_messages();

        // Messages that we need to send to the client immediately.
        // ReadyForQuery (B) | CopyInResponse (B) | ErrorResponse(B) | NoticeResponse(B)
        let flush =
            matches!(code, 'Z' | 'G' | 'E' | 'N') || !has_more_messages || message.streaming();

        // Server finished executing a query.
        // ReadyForQuery (B)
        if code == 'Z' {
            inner.stats.query();
            self.in_transaction = message.in_transaction();
            inner.stats.idle(self.in_transaction);
        }

        inner.stats.sent(message.len());

        // Release the connection back into the pool
        // before flushing data to client.
        // Flushing can take a minute and we don't want to block
        // the connection from being reused.
        if inner.backend.done() {
            let changed_params = inner.backend.changed_params();
            if inner.transaction_mode() {
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

        trace!("[{}] <- {:#?}", self.addr, message);

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
    async fn buffer(&mut self) -> Result<BufferEvent, Error> {
        self.request_buffer.clear();

        // Only start timer once we receive the first message.
        let mut timer = None;

        // Check config once per request.
        let config = config::config();
        self.prepared_statements.enabled = config.prepared_statements();
        self.timeouts = Timeouts::from_config(&config.config.general);

        while !self.request_buffer.full() {
            let message = match self.stream.read_buf(&mut self.stream_buffer).await {
                Ok(message) => message.stream(self.streaming).frontend(),
                Err(_) => {
                    return Ok(BufferEvent::DisconnectAbrupt);
                }
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

        trace!(
            "request buffered [{:.4}ms]\n{:#?}",
            timer.unwrap().elapsed().as_secs_f64() * 1000.0,
            self.request_buffer,
        );

        Ok(BufferEvent::HaveRequest)
    }

    /// Tell the client we started a transaction.
    async fn start_transaction(&mut self) -> Result<(), Error> {
        self.stream
            .send_many(&[
                CommandComplete::new_begin().message()?,
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
        let mut messages = if !self.in_transaction {
            vec![NoticeResponse::from(ErrorResponse::no_transaction()).message()?]
        } else {
            vec![]
        };
        messages.push(cmd.message()?);
        messages.push(ReadyForQuery::idle().message()?);
        self.stream.send_many(&messages).await?;
        debug!("transaction ended");
        Ok(())
    }

    /// Handle SET command.
    async fn set(&mut self, mut inner: InnerBorrow<'_>) -> Result<(), Error> {
        self.stream.send(&CommandComplete::new("SET")).await?;
        self.stream
            .send_flush(&ReadyForQuery::in_transaction(self.in_transaction))
            .await?;
        inner.done(self.in_transaction);
        inner.comms.update_params(&self.params);
        debug!("set");
        Ok(())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.comms.disconnect();
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
