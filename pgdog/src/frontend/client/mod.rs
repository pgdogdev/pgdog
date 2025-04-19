//! Frontend client.

use std::net::SocketAddr;
use std::time::Instant;

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
use crate::config;
use crate::frontend::buffer::BufferedQuery;
#[cfg(debug_assertions)]
use crate::frontend::QueryLogger;
use crate::net::messages::{
    Authentication, BackendKeyData, CommandComplete, ErrorResponse, FromBytes, Message, Password,
    Protocol, ReadyForQuery, ToBytes,
};
use crate::net::{parameter::Parameters, Stream};

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
    params: Parameters,
    comms: Comms,
    admin: bool,
    streaming: bool,
    shard: Option<usize>,
    prepared_statements: PreparedStatements,
    in_transaction: bool,
    timeouts: Timeouts,
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

        let id = BackendKeyData::new();

        // Auto database.
        let exists = databases::databases().exists((user, database));
        if !exists && config.config.general.passthrough_auth() {
            // Get the password.
            stream
                .send_flush(&Authentication::ClearTextPassword)
                .await?;
            let password = stream.read().await?;
            let password = Password::from_bytes(password.to_bytes()?)?;
            let user = config::User::from_params(&params, &password).ok();
            if let Some(user) = user {
                databases::add(&user);
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

        let auth_ok = if stream.is_tls() {
            let md5 = md5::Client::new(user, password);
            stream.send_flush(&md5.challenge()).await?;
            let password = Password::from_bytes(stream.read().await?.to_bytes()?)?;
            if let Password::PasswordMessage { response } = password {
                md5.check(&response)
            } else {
                false
            }
        } else {
            stream.send_flush(&Authentication::scram()).await?;

            let scram = Server::new(password);
            let res = scram.handle(&mut stream).await;
            matches!(res, Ok(true))
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
            params,
            prepared_statements: PreparedStatements::new(),
            in_transaction: false,
            timeouts: Timeouts::from_config(&config.config.general),
        };

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

                message = timeout(query_timeout, inner.backend.read()) => {
                    let message = message??;
                    let disconnect = self.server_message(inner.get(), message).await?;
                    if disconnect {
                        break;
                    }
                }

                buffer = self.buffer() => {
                    let buffer = buffer?;
                    if buffer.is_empty() {
                        break;
                    }

                    let disconnect = self.client_messages(inner.get(), buffer).await?;
                    if disconnect {
                        break;
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
    async fn client_messages(
        &mut self,
        mut inner: InnerBorrow<'_>,
        mut buffer: Buffer,
    ) -> Result<bool, Error> {
        inner.is_async = buffer.is_async();
        inner.stats.received(buffer.len());

        #[cfg(debug_assertions)]
        if let Some(query) = buffer.query()? {
            debug!(
                "{} [{}] (in transaction: {})",
                query.query(),
                self.addr,
                self.in_transaction
            );
            QueryLogger::new(&buffer).log().await?;
        }

        let connected = inner.connected();
        let command = match inner.command(&mut buffer, &mut self.prepared_statements) {
            Ok(command) => command,
            Err(err) => {
                self.stream
                    .error(ErrorResponse::syntax(err.to_string().as_str()))
                    .await?;
                return Ok(true);
            }
        };

        self.streaming = matches!(command, Some(Command::StartReplication));

        if !connected {
            match command {
                Some(Command::StartTransaction(query)) => {
                    if let BufferedQuery::Query(_) = query {
                        self.start_transaction().await?;
                        inner.start_transaction = Some(query.clone());
                        return Ok(false);
                    }
                }
                Some(Command::RollbackTransaction) => {
                    inner.start_transaction = None;
                    self.end_transaction(true).await?;
                    return Ok(false);
                }
                Some(Command::CommitTransaction) => {
                    inner.start_transaction = None;
                    self.end_transaction(false).await?;
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
                    timeout(
                        query_timeout,
                        inner
                            .backend
                            .link_client(&self.params, self.prepared_statements.enabled),
                    )
                    .await??;
                }
                Err(err) => {
                    if err.no_server() {
                        error!("connection pool is down [{}]", self.addr);
                        self.stream.error(ErrorResponse::connection()).await?;
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
        if buffer.executable() {
            if let Some(query) = inner.start_transaction.take() {
                inner.backend.execute(&query).await?;
            }
        }

        for msg in buffer.iter() {
            if let ProtocolMessage::Bind(bind) = msg {
                inner.backend.bind(bind)?
            }
        }

        // inner.backend.wait_in_sync().await;

        // Handle COPY subprotocol in a potentially sharded context.
        if buffer.copy() && !self.streaming {
            let rows = inner.router.copy_data(&buffer)?;
            if !rows.is_empty() {
                inner.backend.send_copy(rows).await?;
                inner
                    .backend
                    .send(buffer.without_copy_data().into())
                    .await?;
            } else {
                inner.backend.send(buffer.into()).await?;
            }
        } else {
            // Send query to server.
            inner.backend.send(buffer.into()).await?;
        }

        Ok(false)
    }

    /// Handle message from server(s).
    async fn server_message(
        &mut self,
        mut inner: InnerBorrow<'_>,
        message: Message,
    ) -> Result<bool, Error> {
        let len = message.len();
        let code = message.code();
        let message = message.backend();

        // ReadyForQuery (B) | CopyInResponse (B)
        let flush = matches!(code, 'Z' | 'G' | 'E' | 'N');
        // RowDescription (B) | NoData(B)
        let async_flush = matches!(code, 'T' | 'n') && inner.is_async;
        let streaming = message.streaming();

        if code == 'Z' {
            inner.stats.query();
            self.in_transaction = message.in_transaction();
            inner.stats.idle(self.in_transaction);
        }

        trace!("[{}] <- {:#?}", self.addr, message);

        if flush || async_flush || streaming {
            self.stream.send_flush(&message).await?;
            if async_flush {
                inner.is_async = false;
            }
        } else {
            self.stream.send(&message).await?;
        }

        inner.stats.sent(len);

        if inner.backend.done() {
            let changed_params = inner.backend.changed_params();
            if inner.transaction_mode() {
                inner.disconnect();
            }
            inner.stats.transaction();
            debug!(
                "transaction finished [{}ms]",
                inner.stats.last_transaction_time.as_secs_f64() * 1000.0
            );
            for (name, value) in changed_params.iter() {
                debug!("setting client's \"{}\" to '{}'", name, value);
                self.params.insert(name.clone(), value.clone());
            }
            if inner.comms.offline() && !self.admin {
                return Ok(true);
            }
        }

        Ok(false)
    }

    /// Buffer extended protocol messages until client requests a sync.
    ///
    /// This ensures we don't check out a connection from the pool until the client
    /// sent a complete request.
    async fn buffer(&mut self) -> Result<Buffer, Error> {
        let mut buffer = Buffer::new();
        // Only start timer once we receive the first message.
        let mut timer = None;

        // Check config once per request.
        let config = config();
        self.prepared_statements.enabled = config.prepared_statements();
        self.timeouts = Timeouts::from_config(&config.config.general);

        while !buffer.full() {
            let message = match self.stream.read().await {
                Ok(message) => message.stream(self.streaming).frontend(),
                Err(_) => {
                    return Ok(vec![].into());
                }
            };

            if timer.is_none() {
                timer = Some(Instant::now());
            }

            // Terminate (B & F).
            if message.code() == 'X' {
                return Ok(vec![].into());
            } else {
                if self.prepared_statements.enabled {
                    let message = ProtocolMessage::from_bytes(message.to_bytes()?)?;
                    if message.extended() {
                        buffer.push(self.prepared_statements.maybe_rewrite(message)?);
                    } else {
                        buffer.push(message);
                    }
                } else {
                    buffer.push(message.into())
                }
            }
        }

        trace!(
            "request buffered [{:.4}ms]\n{:#?}",
            timer.unwrap().elapsed().as_secs_f64() * 1000.0,
            buffer,
        );

        Ok(buffer)
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
        self.stream
            .send_many(&[cmd.message()?, ReadyForQuery::idle().message()?])
            .await?;
        debug!("transaction ended");
        Ok(())
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.comms.disconnect();
    }
}
