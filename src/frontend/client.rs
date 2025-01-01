//! Frontend client.

use tokio::select;

use super::Error;
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
        loop {
            select! {
                buffer = self.buffer() => {
                    let buffer = buffer?;

                    if buffer.is_empty() {
                        self.state = State::Disconnected;
                        break;
                    }

                    self.state = State::Active;
                }
            }
        }

        Ok(self)
    }

    /// Buffer extended protocol messages until client requests a sync.
    ///
    /// This ensures we don't check out a connection from the pool until the client
    /// sent a complete request.
    async fn buffer(&mut self) -> Result<Vec<Message>, Error> {
        let mut buffer = vec![];

        loop {
            let message = self.stream.read().await?;

            match message.code() {
                // Terminate (F)
                'X' => return Ok(vec![]),

                // Flush (F) | Sync (F) | Query (F)
                'H' | 'S' | 'Q' => {
                    buffer.push(message);
                    break;
                }

                _ => buffer.push(message),
            }
        }

        Ok(buffer)
    }
}
