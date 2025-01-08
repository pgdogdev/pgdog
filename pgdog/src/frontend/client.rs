//! Frontend client.

use std::net::SocketAddr;
use std::time::Instant;

use tokio::{select, spawn};
use tracing::{debug, error, info, trace};

use super::{Buffer, Comms, Error, Router};
use crate::backend::pool::Connection;
use crate::net::messages::{
    Authentication, BackendKeyData, ErrorResponse, Protocol, ReadyForQuery,
};
use crate::net::{parameter::Parameters, Stream};
use crate::state::State;
use crate::stats::ConnStats;

/// Frontend client.
#[allow(dead_code)]
pub struct Client {
    addr: SocketAddr,
    stream: Stream,
    comms: Comms,
    id: BackendKeyData,
    state: State,
    params: Parameters,
    stats: ConnStats,
}

impl Client {
    /// Create new frontend client from the given TCP stream.
    pub async fn new(
        mut stream: Stream,
        params: Parameters,
        addr: SocketAddr,
        comms: Comms,
    ) -> Result<Self, Error> {
        // TODO: perform authentication.
        let user = params.get_default("user", "postgres");
        let database = params.get_default("database", user);
        let admin = database == "admin";

        let id = BackendKeyData::new();

        // Get server parameters and send them to the client.
        {
            let mut conn = match Connection::new(user, database, admin) {
                Ok(conn) => conn,
                Err(_) => {
                    stream.fatal(ErrorResponse::auth(user, database)).await?;
                    return Self::disconnected(stream, addr, comms);
                }
            };

            stream.send(Authentication::Ok).await?;

            let params = match conn.parameters(&id).await {
                Ok(params) => params,
                Err(err) => {
                    if err.checkout_timeout() {
                        error!("connection pool is down");
                        stream.fatal(ErrorResponse::connection()).await?;
                        return Self::disconnected(stream, addr, comms);
                    } else {
                        return Err(err.into());
                    }
                }
            };

            for param in params {
                stream.send(param).await?;
            }
        }

        stream.send(id).await?;
        stream.send_flush(ReadyForQuery::idle()).await?;

        Ok(Self {
            addr,
            stream,
            comms,
            id,
            state: State::Idle,
            params,
            stats: ConnStats::default(),
        })
    }

    /// Disconnect user gracefully.
    fn disconnected(stream: Stream, addr: SocketAddr, comms: Comms) -> Result<Self, Error> {
        Ok(Self {
            addr,
            stream,
            comms,
            state: State::Disconnected,
            id: BackendKeyData::default(),
            params: Parameters::default(),
            stats: ConnStats::default(),
        })
    }

    /// Get client's identifier.
    pub fn id(&self) -> BackendKeyData {
        self.id
    }

    /// Handle the client.
    pub async fn spawn(mut self) {
        if self.state == State::Disconnected {
            return;
        }

        if self.admin() {
            spawn(async move {
                self.spawn_internal().await;
            });
        } else {
            self.spawn_internal().await
        }
    }

    async fn spawn_internal(&mut self) {
        match self.run().await {
            Ok(_) => info!("client disconnected [{}]", self.addr),
            Err(err) => error!("client disconnected with error [{}]: {}", self.addr, err),
        }
    }

    /// Run the client.
    async fn run(&mut self) -> Result<(), Error> {
        let user = self.params.get_required("user")?;
        let database = self.params.get_default("database", user);
        let admin = database == "admin";

        let mut backend = Connection::new(user, database, admin)?;
        let mut router = Router::new();
        let mut timer = Instant::now();
        let comms = self.comms.clone();

        self.state = State::Idle;

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

                    if !backend.connected() {
                        timer = Instant::now();

                        // Figure out where the query should go.
                        if let Ok(cluster) = backend.cluster() {
                            router.query(&buffer, cluster)?;
                        }

                        // Grab a connection from the right pool.
                        self.state = State::Waiting;
                        match backend.connect(&self.id, router.route()).await {
                            Ok(()) => (),
                            Err(err) => if err.checkout_timeout() {
                                error!("connection pool is down");
                                self.stream.error(ErrorResponse::connection()).await?;
                                self.state = State::Idle;
                                continue;
                            } else {
                                return Err(err.into());
                            }
                        };
                        self.state = State::Active;
                        debug!("client paired with {} [{:.4}ms]", backend.addr()?, timer.elapsed().as_secs_f64() * 1000.0);
                    }

                    // Send query to server.
                    backend.send(buffer.into()).await?;
                }

                message = backend.read() => {
                    let message = message?;
                    let len = message.len();

                    // ReadyForQuery (B) | CopyInResponse (B)
                    if matches!(message.code(), 'Z' | 'G') {
                        self.stream.send_flush(message).await?;
                        self.stats.queries += 1;
                    }  else {
                        self.stream.send(message).await?;
                    }

                    if backend.done() {
                        backend.disconnect();
                        self.stats.transactions += 1;
                        self.state = State::Idle;
                        trace!("transaction finished [{}ms]", timer.elapsed().as_secs_f64() * 1000.0);
                        if comms.offline() {
                            break;
                        }
                    }

                    self.stats.bytes_sent += len;
                }
            }
        }

        Ok(())
    }

    /// Buffer extended protocol messages until client requests a sync.
    ///
    /// This ensures we don't check out a connection from the pool until the client
    /// sent a complete request.
    async fn buffer(&mut self) -> Buffer {
        let mut buffer = Buffer::new();
        let timer = Instant::now();

        while !buffer.full() {
            let message = match self.stream.read().await {
                Ok(message) => message,
                Err(_) => {
                    self.state = State::Disconnected;
                    return vec![].into();
                }
            };

            self.stats.bytes_received += message.len();

            match message.code() {
                // Terminate (F)
                'X' => {
                    self.state = State::Disconnected;
                    return vec![].into();
                }
                _ => buffer.push(message),
            }
        }

        trace!(
            "request buffered [{:.4}ms]",
            timer.elapsed().as_secs_f64() * 1000.0
        );

        buffer
    }

    fn admin(&self) -> bool {
        self.params.get_default("database", "") == "admin"
    }
}
