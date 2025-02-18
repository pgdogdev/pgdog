//! Frontend client.

use std::net::SocketAddr;
use std::time::Instant;

use tokio::{select, spawn};
use tracing::{debug, error, info, trace};

use super::{Buffer, Command, Comms, Error, PreparedStatements, Router, Stats};
use crate::auth::scram::Server;
use crate::backend::pool::Connection;
use crate::config::config;
use crate::net::messages::command_complete::CommandComplete;
use crate::net::messages::parse::Parse;
use crate::net::messages::{
    Authentication, BackendKeyData, Bind, Describe, ErrorResponse, FromBytes, Protocol,
    ReadyForQuery, ToBytes,
};
use crate::net::{parameter::Parameters, Stream};

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

        stream.send_flush(Authentication::scram()).await?;

        let scram = Server::new(password);
        if let Ok(true) = scram.handle(&mut stream).await {
            stream.send(Authentication::Ok).await?;
        } else {
            stream.fatal(ErrorResponse::auth(user, database)).await?;
            return Ok(());
        }

        // Check if the pooler is shutting down.
        if comms.offline() && !admin {
            stream.fatal(ErrorResponse::shutting_down()).await?;
            return Ok(());
        }

        let server_params = match conn.parameters(&id).await {
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
        let user = self.params.get_required("user")?;
        let database = self.params.get_default("database", user);

        let mut backend = Connection::new(user, database, self.admin)?;
        let mut router = Router::new();
        let mut stats = Stats::new();
        let mut async_ = false;
        let mut start_transaction = None;
        let comms = self.comms.clone();

        if self.shard.is_some() {
            if let Some(config) = backend.cluster()?.replication_sharding_config() {
                backend.replication_mode(self.shard, &config)?;
                router.replication_mode();
                debug!("logical replication sharding [{}]", self.addr);
            }
        }

        loop {
            select! {
                _ = comms.shutting_down() => {
                    if !backend.connected() {
                        break;
                    }
                }

                buffer = self.buffer() => {
                    let buffer = buffer?;
                    if buffer.is_empty() {
                        break;
                    }

                    async_ = buffer.async_();
                    comms.stats(stats.received(buffer.len()));

                    #[cfg(debug_assertions)]
                    if let Some(query) = buffer.query()? {
                        debug!("{} [{}]", query, self.addr);
                    }

                    let command = match backend.cluster().ok().map(|cluster| router.query(&buffer, cluster)).transpose() {
                        Ok(command) => command,
                        Err(err) => {
                            self.stream.error(ErrorResponse::syntax(err.to_string().as_str())).await?;
                            continue;
                        }
                    };

                    self.streaming = matches!(command, Some(Command::StartReplication));

                    if !backend.connected() {
                        match command {
                            Some(Command::StartTransaction(query)) => {
                                start_transaction = Some(query.clone());
                                self.start_transaction().await?;
                                continue;
                            },
                            Some(Command::RollbackTransaction) => {
                                start_transaction  = None;
                                self.end_transaction(true).await?;
                                continue;
                            },
                            Some(Command::CommitTransaction) => {
                                start_transaction = None;
                                self.end_transaction(false).await?;
                                continue;
                            },
                            _ => (),
                        };

                        // Grab a connection from the right pool.
                        comms.stats(stats.waiting());
                        match backend.connect(&self.id, &router.route()).await {
                            Ok(()) => (),
                            Err(err) => if err.no_server() {
                                error!("connection pool is down");
                                self.stream.error(ErrorResponse::connection()).await?;
                                comms.stats(stats.error());
                                continue;
                            } else {
                                return Err(err.into());
                            }
                        };
                        comms.stats(stats.connected());
                        if let Ok(addr) = backend.addr() {
                            let addrs = addr.into_iter().map(|a| a.to_string()).collect::<Vec<_>>().join(",");
                            debug!("client paired with {} [{:.4}ms]", addrs, stats.wait_time.as_secs_f64() * 1000.0);
                        }

                        // Simulate a transaction until the client
                        // sends a query over. This ensures that we don't
                        // connect to all shards for no reason.
                        if let Some(query) = start_transaction.take() {
                            backend.execute(&query).await?;
                        }
                    }

                    // Handle any prepared statements.
                    for statement in buffer.prepared_statements()? {
                        backend.prepare(&statement).await?;
                    }

                    // Handle COPY subprotocol in a potentially sharded context.
                    if buffer.copy() && !self.streaming {
                        let rows = router.copy_data(&buffer)?;
                        if !rows.is_empty() {
                            backend.send_copy(rows).await?;
                            backend.send(buffer.without_copy_data().into()).await?;
                        } else {
                            backend.send(buffer.into()).await?;
                        }
                    } else {
                        // Send query to server.
                        backend.send(buffer.into()).await?;
                    }
                }

                message = backend.read() => {
                    let message = message?;
                    let len = message.len();
                    let code = message.code();

                    // ReadyForQuery (B) | CopyInResponse (B) || RowDescription (B) | ErrorResponse (B)
                    let flush = matches!(code, 'Z' | 'G') || matches!(code, 'T' | 'E')  && async_ || message.streaming();
                    if flush {
                        self.stream.send_flush(message).await?;
                        async_ = false;
                    } else {
                        self.stream.send(message).await?;
                    }

                    comms.stats(stats.sent(len));

                    if code == 'Z' {
                        comms.stats(stats.query());
                    }

                    if backend.done() {
                        if backend.transaction_mode() {
                            backend.disconnect();
                        }
                        comms.stats(stats.transaction());
                        trace!("transaction finished [{}ms]", stats.last_transaction_time.as_secs_f64() * 1000.0);
                        if comms.offline() && !self.admin {
                            break;
                        }
                    }
                }
            }
        }

        if comms.offline() && !self.admin {
            self.stream
                .send_flush(ErrorResponse::shutting_down())
                .await?;
        }

        Ok(())
    }

    /// Buffer extended protocol messages until client requests a sync.
    ///
    /// This ensures we don't check out a connection from the pool until the client
    /// sent a complete request.
    async fn buffer(&mut self) -> Result<Buffer, Error> {
        let mut buffer = Buffer::new();
        let mut timer = None;

        while !buffer.full() {
            let message = match self.stream.read().await {
                Ok(message) => message.stream(self.streaming),
                Err(_) => {
                    return Ok(vec![].into());
                }
            };

            if timer.is_none() {
                timer = Some(Instant::now());
            }

            match message.code() {
                // Terminate (F)
                'X' => return Ok(vec![].into()),
                'P' => {
                    let parse = Parse::from_bytes(message.to_bytes()?)?;
                    if parse.anonymous() {
                        buffer.push(message);
                    } else {
                        buffer.push(self.prepared_statements.insert(parse).message()?);
                    }
                }
                'B' => {
                    let bind = Bind::from_bytes(message.to_bytes()?)?;
                    if bind.anonymous() {
                        buffer.push(message);
                    } else {
                        let counter = self
                            .prepared_statements
                            .name(&bind.statement)
                            .ok_or(Error::MissingPreparedStatement(bind.statement.clone()))?;
                        buffer.push(bind.rename(counter).message()?);
                    }
                }
                'D' => {
                    let describe = Describe::from_bytes(message.to_bytes()?)?;
                    if describe.anonymous() {
                        buffer.push(message);
                    } else {
                        let counter = self
                            .prepared_statements
                            .name(&describe.statement)
                            .ok_or(Error::MissingPreparedStatement(describe.statement.clone()))?;
                        buffer.push(describe.rename(counter).message()?);
                    }
                }
                _ => buffer.push(message),
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
        let cmd = CommandComplete {
            command: "BEGIN".into(),
        };
        let rfq = ReadyForQuery::in_transaction();
        self.stream
            .send_many(vec![cmd.message()?, rfq.message()?])
            .await?;
        debug!("transaction started");

        Ok(())
    }

    async fn end_transaction(&mut self, rollback: bool) -> Result<(), Error> {
        let cmd = if rollback {
            CommandComplete {
                command: "ROLLBACK".into(),
            }
        } else {
            CommandComplete {
                command: "COMMIT".into(),
            }
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
