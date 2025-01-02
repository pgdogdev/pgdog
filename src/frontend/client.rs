//! Frontend client.

use tokio::select;

use super::{Buffer, Error};
use crate::backend::pool::Connection;
use crate::backend::Server;
use crate::net::messages::{
    Authentication, BackendKeyData, Message, ParameterStatus, Protocol, ReadyForQuery,
};
use crate::net::Stream;
use crate::state::State;

/// Frontend client.
#[allow(dead_code)]
pub struct Client {
    stream: Stream,
    id: BackendKeyData,
    state: State,
    params: Vec<(String, String)>,
}

impl Client {
    /// Create new frontend client from the given TCP stream.
    pub async fn new(mut stream: Stream, params: Vec<(String, String)>) -> Result<Self, Error> {
        // TODO: perform authentication.
        stream.send(Authentication::Ok).await?;

        // TODO: fetch actual server params from the backend.
        let backend_params = ParameterStatus::fake();
        for param in backend_params {
            stream.send(param).await?;
        }

        let id = BackendKeyData::new();

        stream.send(id.clone()).await?;
        stream.send_flush(ReadyForQuery::idle()).await?;

        Ok(Self {
            stream,
            id,
            state: State::Idle,
            params,
        })
    }

    /// Get client's identifier.
    pub fn id(&self) -> BackendKeyData {
        self.id.clone()
    }

    /// Run the client.
    pub async fn spawn(mut self) -> Result<Self, Error> {
        let mut server = Connection::new();
        let mut flush = false;

        loop {
            self.state = State::Idle;

            select! {
                buffer = self.buffer() => {
                    let buffer = buffer?;

                    if buffer.is_empty() {
                        self.state = State::Disconnected;
                        break;
                    }

                    flush = buffer.flush();
                    self.state = State::Waiting;
                    server.connect().await?;
                    self.state = State::Active;

                    server.send(buffer.into()).await?;
                }

                message = server.read() => {
                    let message = message?;

                    // ReadyForQuery (B) | CopyInResponse (B)
                    if matches!(message.code(), 'Z' | 'G') || flush {
                        self.stream.send_flush(message).await?;
                        flush = false;
                    }  else {
                        self.stream.send(message).await?;
                    }

                    if server.done() {
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

            match message.code() {
                // Terminate (F)
                'X' => return Ok(vec![].into()),
                _ => buffer.push(message),
            }
        }

        Ok(buffer.into())
    }
}
