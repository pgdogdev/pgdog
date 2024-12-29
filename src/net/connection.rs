//! Handle TCP connection.

use bytes::Bytes;

use tokio::io::{AsyncWriteExt, BufStream};
use tokio::select;
use tokio::sync::broadcast::{channel, Receiver, Sender};
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
#[allow(dead_code)]
pub struct Connection {
    tx: Sender<Message>,
    rx: Receiver<Message>,
}

impl Clone for Connection {
    /// Create another listener/receiver for this connection.
    ///
    /// # Safety
    ///
    /// This method is **unsafe** if the connection is unsynchronized.
    /// Data loss will occur if a connection is cloned in the middle of
    /// an exchange.
    fn clone(&self) -> Self {
        Connection {
            tx: self.tx.clone(),
            rx: self.tx.subscribe(),
        }
    }
}

impl Connection {
    /// Create new client connection from a network connection.
    ///
    /// # Arguments
    ///
    /// * `stream`: TCP connection socket.
    ///
    pub fn new(mut stream: Stream) -> Result<Self, Error> {
        let (tx, rx) = channel::<Message>(4096);
        let (ttx, mut trx) = (tx.clone(), tx.subscribe());

        spawn(async move {
            loop {
                select! {
                    message = trx.recv() => {
                        match message {
                            Ok(Message::Bytes(bytes)) => {
                                if let Err(_err) = stream.write_all(&bytes).await {
                                    break;
                                }
                            }

                            Ok(Message::Flush) => {
                                if let Err(_err) = stream.flush().await {
                                    break;
                                }
                            }

                            Ok(Message::Shutdown { .. }) => break,

                            Err(_) => break,
                        }
                    }

                    message = stream.read() => {
                        if let Ok(message) = message {
                            let _ = ttx.send(Message::Bytes(message));
                        } else {
                            let _ = ttx.send(Message::Shutdown { error: true });
                            break;
                        }
                    }
                }
            }
        });

        Ok(Self { tx, rx })
    }

    /// Send a message to the connection.
    pub fn send(&self, message: impl ToBytes + Protocol) -> Result<(), Error> {
        let code = message.code();

        debug!("📡 <= {}", code);

        if let Err(_) = self.tx.send(Message::Bytes(message.to_bytes()?)) {
            Err(Error::ConnectionDown)
        } else {
            Ok(())
        }
    }

    /// Request the connection flushes its internal buffers.
    pub fn flush(&self) -> Result<(), Error> {
        if let Err(_) = self.tx.send(Message::Flush) {
            Err(Error::ConnectionDown)
        } else {
            Ok(())
        }
    }

    /// Receive a message from the connection. Wait until a message is available.
    pub async fn recv(&mut self) -> Result<Bytes, Error> {
        loop {
            match self.rx.recv().await {
                Ok(Message::Bytes(bytes)) => return Ok(bytes),
                Ok(Message::Flush) => continue,
                _ => return Err(Error::ConnectionDown),
            }
        }
    }
}
