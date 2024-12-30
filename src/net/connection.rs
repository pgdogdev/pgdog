//! Handle TCP connection.

use bytes::Bytes;

use tokio::io::AsyncWriteExt;
use tokio::select;
use tokio::sync::mpsc::{channel, Receiver, Sender};
use tokio::task::spawn;

use tracing::debug;

use super::messages::{Protocol, ToBytes};
use super::Error;
use crate::net::Stream;

/// Message received/sent from/to a connection.
#[derive(Debug, Clone)]
pub enum Message {
    /// Protocol message.
    Bytes(Bytes),
    /// Connection should be flushed.
    Flush,
    /// Connection is shutting down.
    Shutdown { error: bool },
}

/// Client connection.
pub struct Connection {
    stream: Stream,
}

impl Connection {
    /// Create new client connection from a network connection.
    ///
    /// # Arguments
    ///
    /// * `stream`: TCP connection socket.
    ///
    pub fn new(stream: Stream) -> Result<Self, Error> {
        Ok(Self { stream })
    }

    /// Send a message to the connection.
    pub async fn send(&mut self, message: impl ToBytes + Protocol) -> Result<(), Error> {
        let code = message.code();

        debug!("📡 <= {}", code);

        if let Err(_) = self.stream.write_all(&message.to_bytes()?).await {
            Err(Error::ConnectionDown)
        } else {
            Ok(())
        }
    }

    /// Request the connection flushes its internal buffers.
    pub async fn flush(&self) -> Result<(), Error> {
        if let Err(_) = self.tx.send(Message::Flush).await {
            Err(Error::ConnectionDown)
        } else {
            Ok(())
        }
    }

    /// Receive a message from the connection. Wait until a message is available.
    pub async fn recv(&mut self) -> Result<Bytes, Error> {
        loop {
            match self.rx.recv().await {
                Some(Message::Bytes(bytes)) => return Ok(bytes),
                Some(Message::Flush) => continue,
                _ => return Err(Error::ConnectionDown),
            }
        }
    }
}
