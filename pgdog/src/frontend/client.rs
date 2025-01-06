//! Frontend client.

use tokio::select;
use tracing::{debug, error};

use super::{Buffer, Error, Router};
use crate::backend::pool::Connection;
use crate::net::messages::{
    Authentication, BackendKeyData, ErrorResponse, Protocol, ReadyForQuery, Terminate,
};
use crate::net::{parameter::Parameters, Stream};
use crate::state::State;
use crate::stats::ConnStats;

/// Frontend client.
#[allow(dead_code)]
pub struct Client {
    stream: Stream,
    id: BackendKeyData,
    state: State,
    params: Parameters,
    stats: ConnStats,
}

impl Client {
    /// Create new frontend client from the given TCP stream.
    pub async fn new(mut stream: Stream, params: Parameters) -> Result<Self, Error> {
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
                    return Self::auth_error(stream, user, database).await;
                }
            };

            stream.send(Authentication::Ok).await?;

            let params = match conn.parameters(&id).await {
                Ok(params) => params,
                Err(err) => {
                    if err.checkout_timeout() {
                        error!("connection pool is down");
                        stream.send(ErrorResponse::connection()).await?;
                        return Self::disconnect(stream).await;
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
            stream,
            id,
            state: State::Idle,
            params,
            stats: ConnStats::default(),
        })
    }

    async fn disconnect(mut stream: Stream) -> Result<Self, Error> {
        stream.send_flush(Terminate).await?;

        Ok(Self {
            stream,
            state: State::Disconnected,
            id: BackendKeyData::default(),
            params: Parameters::default(),
            stats: ConnStats::default(),
        })
    }

    async fn auth_error(mut stream: Stream, user: &str, database: &str) -> Result<Self, Error> {
        stream.send(ErrorResponse::auth(user, database)).await?;
        Self::disconnect(stream).await
    }

    /// Get client's identifier.
    pub fn id(&self) -> BackendKeyData {
        self.id
    }

    /// Run the client.
    pub async fn spawn(mut self) -> Result<Self, Error> {
        let user = self.params.get_required("user")?;
        let database = self.params.get_default("database", user);
        let admin = database == "admin";

        let mut backend = Connection::new(user, database, admin)?;
        let mut router = Router::new();
        let mut flush = false;

        self.state = State::Idle;

        loop {
            select! {
                buffer = self.buffer() => {
                    if buffer.is_empty() {
                        break;
                    }

                    flush = buffer.flush();

                    if !backend.connected() {
                        router.query(&buffer)?;

                        self.state = State::Waiting;

                        match backend.connect(&self.id, router.route()).await {
                            Ok(()) => (),
                            Err(err) => if err.checkout_timeout() {
                                error!("connection pool is down");
                                self.stream.send(ErrorResponse::connection()).await?;
                                self.stream.send_flush(ReadyForQuery::idle()).await?;
                                continue;
                            } else {
                                return Err(err.into());
                            }
                        };

                        self.state = State::Active;

                        debug!("client paired with {}", backend.addr()?);
                    }

                    backend.send(buffer.into()).await?;
                }

                message = backend.read() => {
                    let message = message?;
                    let len = message.len();

                    // ReadyForQuery (B) | CopyInResponse (B)
                    if matches!(message.code(), 'Z' | 'G') || flush {
                        self.stream.send_flush(message).await?;
                        flush = false;
                        self.stats.queries += 1;
                    }  else {
                        self.stream.send(message).await?;
                    }

                    if backend.done() {
                        backend.disconnect();
                        self.stats.transactions += 1;
                        self.state = State::Idle;
                    }

                    self.stats.bytes_sent += len;
                }
            }
        }

        Ok(self)
    }

    /// Buffer extended protocol messages until client requests a sync.
    ///
    /// This ensures we don't check out a connection from the pool until the client
    /// sent a complete request.
    async fn buffer(&mut self) -> Buffer {
        let mut buffer = Buffer::new();

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

        buffer
    }
}
