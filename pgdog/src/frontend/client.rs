//! Frontend client.

use std::net::SocketAddr;
use std::time::Instant;

use tokio::{select, spawn};
use tracing::{debug, error, info, trace};

use super::{Buffer, Comms, Error, Router, Stats};
use crate::auth::scram::Server;
use crate::backend::pool::Connection;
use crate::config::config;
use crate::net::messages::{
    Authentication, BackendKeyData, ErrorResponse, Protocol, ReadyForQuery,
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

        for param in server_params {
            stream.send(param).await?;
        }

        stream.send(id).await?;
        stream.send_flush(ReadyForQuery::idle()).await?;
        comms.connect(&id, addr);

        info!("client connected [{}]", addr);

        let mut client = Self {
            addr,
            stream,
            id,
            params,
            comms,
            admin,
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
        let comms = self.comms.clone();

        loop {
            select! {
                _ = comms.shutting_down() => {
                    if !backend.connected() {
                        break;
                    }
                }

                buffer = self.buffer() => {
                    if buffer.is_empty() {
                        break;
                    }

                    async_ = buffer.async_();
                    comms.stats(stats.received(buffer.len()));

                    if !backend.connected() {
                        // Figure out where the query should go.
                        if let Ok(cluster) = backend.cluster() {
                            router.query(&buffer, cluster)?;
                        }

                        // Grab a connection from the right pool.
                        comms.stats(stats.waiting());
                        match backend.connect(&self.id, router.route()).await {
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
                    }

                    // Handle COPY subprotocol in a potentially sharded context.
                    if buffer.copy() {
                        let rows = router.copy_data(&buffer, backend.cluster()?)?;
                        if !rows.is_empty() {
                            backend.send_copy(rows).await?;
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
                    if matches!(code, 'Z' | 'G') || matches!(code, 'T' | 'E')  && async_ {
                        self.stream.send_flush(message).await?;
                        comms.stats(stats.query());
                        async_ = false;
                    }  else {
                        self.stream.send(message).await?;
                    }

                    if backend.done() {
                        if backend.transaction_mode() {
                            backend.disconnect();
                        }
                        comms.stats(stats.transaction());
                        trace!("transaction finished [{}ms]", stats.last_transaction_time.as_secs_f64() * 1000.0);
                        if comms.offline() {
                            break;
                        }
                    }

                    comms.stats(stats.sent(len));
                }
            }
        }

        if comms.offline() {
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
    async fn buffer(&mut self) -> Buffer {
        let mut buffer = Buffer::new();
        let mut timer = None;

        while !buffer.full() {
            let message = match self.stream.read().await {
                Ok(message) => message,
                Err(_) => {
                    return vec![].into();
                }
            };

            if timer.is_none() {
                timer = Some(Instant::now());
            }

            match message.code() {
                // Terminate (F)
                'X' => return vec![].into(),
                _ => buffer.push(message),
            }
        }

        trace!(
            "request buffered [{:.4}ms]",
            timer.unwrap().elapsed().as_secs_f64() * 1000.0
        );

        buffer
    }
}

impl Drop for Client {
    fn drop(&mut self) {
        self.comms.disconnect();
    }
}
