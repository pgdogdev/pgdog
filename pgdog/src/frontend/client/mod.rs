//! Frontend client.

use std::net::SocketAddr;
use std::time::Instant;

use tokio::{select, spawn};
use tracing::{debug, error, info, trace};

use super::{Buffer, Command, Comms, Error, PreparedStatements};
use crate::auth::{md5, scram::Server};
use crate::backend::pool::{Connection, Request};
use crate::config::config;
#[cfg(debug_assertions)]
use crate::frontend::QueryLogger;
use crate::net::messages::{
    Authentication, BackendKeyData, CommandComplete, ErrorResponse, FromBytes, Message,
    ParseComplete, Password, Protocol, ReadyForQuery, ToBytes,
};
use crate::net::{parameter::Parameters, Stream};

pub mod counter;
pub mod inner;

use inner::Inner;

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
        let config = config();

        let admin = database == config.config.admin.name;
        let admin_password = &config.config.admin.password;

        let id = BackendKeyData::new();

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
            stream.send_flush(md5.challenge()).await?;
            let password = Password::from_bytes(stream.read().await?.to_bytes()?)?;
            if let Password::PasswordMessage { response } = password {
                md5.check(&response)
            } else {
                false
            }
        } else {
            stream.send_flush(Authentication::scram()).await?;

            let scram = Server::new(password);
            let res = scram.handle(&mut stream).await;
            matches!(res, Ok(true))
        };

        if !auth_ok {
            stream.fatal(ErrorResponse::auth(user, database)).await?;
            return Ok(());
        } else {
            stream.send(Authentication::Ok).await?;
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
            stream.send(param).await?;
        }

        stream.send(id).await?;
        stream.send_flush(ReadyForQuery::idle()).await?;
        comms.connect(&id, addr);
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

        loop {
            select! {
                _ = inner.comms.shutting_down() => {
                    if !inner.backend.connected() {
                        break;
                    }
                }

                buffer = self.buffer() => {
                    let buffer = buffer?;
                    if buffer.is_empty() {
                        break;
                    }

                    let disconnect = self.client_messages(&mut inner, buffer).await?;
                    if disconnect {
                        break;
                    }
                }

                message = inner.backend.read() => {
                    let message = message?;
                    let disconnect = self.server_message(&mut inner, message).await?;
                    if disconnect {
                        break;
                    }
                }
            }
        }

        if inner.comms.offline() && !self.admin {
            self.stream
                .send_flush(ErrorResponse::shutting_down())
                .await?;
        }

        Ok(())
    }

    /// Handle client messages.
    async fn client_messages(
        &mut self,
        inner: &mut Inner,
        mut buffer: Buffer,
    ) -> Result<bool, Error> {
        inner.async_ = buffer.async_();
        inner.comms.stats(inner.stats.received(buffer.len()));

        #[cfg(debug_assertions)]
        if let Some(query) = buffer.query()? {
            debug!("{} [{}]", query.query(), self.addr);
            QueryLogger::new(&buffer).log().await?;
        }

        let connected = inner.connected();
        let command = match inner.command(&buffer) {
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
                    inner.start_transaction = Some(query.clone());
                    self.start_transaction().await?;
                    return Ok(false);
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
                    inner.backend.sync_params(&self.params).await?;
                }
                Err(err) => {
                    if err.no_server() {
                        error!("connection pool is down");
                        self.stream.error(ErrorResponse::connection()).await?;
                        return Ok(false);
                    } else {
                        return Err(err.into());
                    }
                }
            };

            // Simulate a transaction until the client
            // sends a query over. This ensures that we don't
            // connect to all shards for no reason.
            if let Some(query) = inner.start_transaction.take() {
                inner.backend.execute(&query).await?;
            }
        }

        // Handle any prepared statements.
        for request in self.prepared_statements.requests() {
            if let Err(err) = inner.backend.prepare(&request.name).await {
                self.stream.error(ErrorResponse::from_err(&err)).await?;
                return Ok(false);
            }

            if request.new {
                self.stream.send(ParseComplete).await?;
                buffer = buffer.without_parse();
            }
        }

        inner.counter.count(&buffer);

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
    async fn server_message(&mut self, inner: &mut Inner, message: Message) -> Result<bool, Error> {
        let len = message.len();
        let code = message.code();
        inner.counter.receive(&message);

        // ReadyForQuery (B) | CopyInResponse (B)
        let flush = matches!(code, 'Z' | 'G' | 'E' | 'N');
        // RowDescription (B) | ErrorResponse (B)
        let async_flush = matches!(code, 'T') && inner.async_;
        let streaming = message.streaming();

        if flush || async_flush || streaming {
            self.stream.send_flush(message).await?;
            if async_flush {
                inner.async_ = false;
            }
        } else {
            self.stream.send(message).await?;
        }

        inner.comms.stats(inner.stats.sent(len));

        if code == 'Z' {
            inner.comms.stats(inner.stats.query());
        }

        if inner.counter.done() {
            inner.reset_counter();
            if inner.transaction_mode() {
                inner.disconnect();
            }
            inner.comms.stats(inner.stats.transaction());
            trace!(
                "transaction finished [{}ms]",
                inner.stats.last_transaction_time.as_secs_f64() * 1000.0
            );
            inner.backend.changed_params().merge(&mut self.params);
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
                buffer.push(self.prepared_statements.maybe_rewrite(message)?);
            }
        }

        trace!(
            "request buffered [{:.4}ms]",
            timer.unwrap().elapsed().as_secs_f64() * 1000.0
        );

        Ok(buffer)
    }

    /// Tell the client we started a transaction.
    async fn start_transaction(&mut self) -> Result<(), Error> {
        self.stream
            .send_many(vec![
                CommandComplete::new_begin().message()?,
                ReadyForQuery::in_transaction().message()?,
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
            .send_many(vec![cmd.message()?, ReadyForQuery::idle().message()?])
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
