//! Frontend client.

use super::Error;
use crate::net::messages::{BackendKeyData, Message, Protocol, ReadyForQuery, ToBytes};
use crate::net::Stream;

use tokio::select;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::spawn;

use tracing::debug;

/// Frontend client.
#[allow(dead_code)]
pub struct Client {
    stream: Stream,
    id: BackendKeyData,
}

impl Client {
    /// Create new frontend client from the given TCP stream.
    pub async fn new(mut stream: Stream) -> Result<Self, Error> {
        let id = BackendKeyData::new();

        stream.send(id.clone()).await?;
        stream.send_flush(ReadyForQuery::idle()).await?;

        Ok(Self { stream, id })
    }

    /// Get client's identifier.
    pub fn id(&self) -> BackendKeyData {
        self.id.clone()
    }

    /// Send a message to the client.
    pub async fn send(&mut self, message: impl ToBytes + Protocol) -> Result<(), Error> {
        self.stream.send(message).await?;

        Ok(())
    }

    /// Receive a message from a client.
    pub async fn recv(&mut self) -> Result<Message, Error> {
        let message = self.stream.read().await?;
        Ok(message)
    }
}
