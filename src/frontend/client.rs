//! Frontend client.

use tokio::select;

use super::{Buffer, Error};
use crate::backend::pool::Connection;
use crate::net::messages::{
    Authentication, BackendKeyData, ParameterStatus, Protocol, ReadyForQuery,
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
        let user = params.get_required("user")?;
        let database = params.get_default("database", user);

        stream.send(Authentication::Ok).await?;

        let id = BackendKeyData::new();

        // Get server parameters and send them to the client.
        {
            let mut conn = Connection::new(user, database)?;
            for param in conn.parameters(&id).await? {
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

    /// Get client's identifier.
    pub fn id(&self) -> BackendKeyData {
        self.id
    }

    /// Run the client.
    pub async fn spawn(mut self) -> Result<Self, Error> {
        let user = self.params.get_required("user")?;
        let database = self.params.get_default("database", user);

        let mut server = Connection::new(user, database)?;
        let mut flush = false;

        loop {
            self.state = State::Idle;

            select! {
                buffer = self.buffer() => {
                    let buffer = match buffer {
                        Ok(buffer) => if buffer.is_empty() {
                            self.state = State::Disconnected;
                            break;
                        } else { buffer },
                        Err(_) => {
                            // IO error typically means the client disconnected
                            // abruptly.
                            self.state = State::Disconnected;
                            break;
                        },
                    };

                    flush = buffer.flush();

                    if !server.connected() {
                        self.state = State::Waiting;
                        server.connect(&self.id).await?;
                        self.state = State::Active;
                    }

                    server.send(buffer.into()).await?;
                }

                message = server.read() => {
                    let message = message?;

                    self.stats.bytes_sent += message.len();

                    // ReadyForQuery (B) | CopyInResponse (B)
                    if matches!(message.code(), 'Z' | 'G') || flush {
                        self.stream.send_flush(message).await?;
                        flush = false;
                        self.stats.queries += 1;
                    }  else {
                        self.stream.send(message).await?;
                    }

                    if server.done() {
                        self.stats.transactions += 1;
                        server.disconnect();
                    }
                }
            }
        }

        Ok(self)
    }

    /// Buffer extended protocol messages until client requests a sync.
    ///
    /// This ensures we don't check out a connection from the pool until the client
    /// sent a complete request.
    async fn buffer(&mut self) -> Result<Buffer, Error> {
        let mut buffer = Buffer::new();

        while !buffer.full() {
            let message = self.stream.read().await?;

            self.stats.bytes_received += message.len();

            match message.code() {
                // Terminate (F)
                'X' => return Ok(vec![].into()),
                _ => buffer.push(message),
            }
        }

        Ok(buffer)
    }
}
