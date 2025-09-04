//! Postgres server connection running in its own task.
//!
//! This allows to queue up messages across multiple instances
//! of this connection without blocking and while maintaining protocol integrity.
//!
use tokio::select;
use tokio::spawn;
use tokio::sync::{
    mpsc::{channel, Receiver, Sender},
    Notify,
};

use crate::{
    backend::Server,
    frontend::ClientRequest,
    net::{Message, ProtocolMessage},
};

use std::sync::Arc;

use super::super::Error;

// What we can send.
enum ParallelMessage {
    // Protocol message, e.g. Bind, Execute, Sync.
    ProtocolMessage(ProtocolMessage),
    // Flush the socket.
    Flush,
}

// What we can receive.
enum ParallelReply {
    // Message, e.g. RowDescription, DataRow, CommandComplete, etc.
    Message(Message),
    // The task gives back the server connection to the owner.
    // Preserve connections between parallel executions.
    Server(Box<Server>),
}

// Parallel Postgres server connection.
#[derive(Debug)]
pub(crate) struct ParallelConnection {
    tx: Sender<ParallelMessage>,
    rx: Receiver<ParallelReply>,
    stop: Arc<Notify>,
}

impl ParallelConnection {
    // Queue up message to server.
    pub(crate) async fn send_one(&mut self, message: &ProtocolMessage) -> Result<(), Error> {
        self.tx
            .send(ParallelMessage::ProtocolMessage(message.clone()))
            .await
            .map_err(|_| Error::ParallelConnection)?;

        Ok(())
    }

    // Queue up the contents of the buffer.
    pub(crate) async fn send(&mut self, client_request: &ClientRequest) -> Result<(), Error> {
        for message in client_request.messages.iter() {
            self.tx
                .send(ParallelMessage::ProtocolMessage(message.clone()))
                .await
                .map_err(|_| Error::ParallelConnection)?;
            self.tx
                .send(ParallelMessage::Flush)
                .await
                .map_err(|_| Error::ParallelConnection)?;
        }

        Ok(())
    }

    // Wait for a message from the server.
    pub(crate) async fn read(&mut self) -> Result<Message, Error> {
        let reply = self.rx.recv().await.ok_or(Error::ParallelConnection)?;
        match reply {
            ParallelReply::Message(message) => Ok(message),
            ParallelReply::Server(_) => Err(Error::ParallelConnection),
        }
    }

    // Request server connection performs socket flush.
    pub(crate) async fn flush(&mut self) -> Result<(), Error> {
        self.tx
            .send(ParallelMessage::Flush)
            .await
            .map_err(|_| Error::ParallelConnection)?;

        Ok(())
    }

    // Move server connection into its own Tokio task.
    pub(crate) fn new(server: Server) -> Result<Self, Error> {
        // Ideally we don't hardcode these. PgDog
        // can use a lot of memory if this is high.
        let (tx1, rx1) = channel(4096);
        let (tx2, rx2) = channel(4096);
        let stop = Arc::new(Notify::new());

        let listener = Listener {
            stop: stop.clone(),
            rx: rx1,
            tx: tx2,
            server: Some(Box::new(server)),
        };

        spawn(async move {
            listener.run().await?;

            Ok::<(), Error>(())
        });

        Ok(Self {
            tx: tx1,
            rx: rx2,
            stop,
        })
    }

    // Get the connection back from the async task. This will
    // only work if the connection is idle (ReadyForQuery received, no more traffic expected).
    pub(crate) async fn reattach(mut self) -> Result<Server, Error> {
        self.stop.notify_one();
        let server = self.rx.recv().await.ok_or(Error::ParallelConnection)?;
        match server {
            ParallelReply::Server(server) => Ok(*server),
            _ => Err(Error::ParallelConnection),
        }
    }
}

// Stop the background task and kill the connection.
// Prevents leaks in case the connection is not "reattached".
impl Drop for ParallelConnection {
    fn drop(&mut self) {
        self.stop.notify_one();
    }
}

// Background task performing the actual work of talking to Postgres.
struct Listener {
    rx: Receiver<ParallelMessage>,
    tx: Sender<ParallelReply>,
    server: Option<Box<Server>>,
    stop: Arc<Notify>,
}

impl Listener {
    // Send message to Postgres.
    async fn send(&mut self, message: ProtocolMessage) -> Result<(), Error> {
        if let Some(ref mut server) = self.server {
            server.send_one(&message).await?;
        }

        Ok(())
    }

    // Flush socket.
    async fn flush(&mut self) -> Result<(), Error> {
        if let Some(ref mut server) = self.server {
            server.flush().await?;
        }

        Ok(())
    }

    // Return server to parent task.
    async fn return_server(&mut self) -> Result<(), Error> {
        if let Some(server) = self.server.take() {
            if self.tx.is_closed() {
                drop(server);
            } else {
                let _ = self.tx.send(ParallelReply::Server(server)).await;
            }
        }

        Ok(())
    }

    // Run the background task.
    async fn run(mut self) -> Result<(), Error> {
        loop {
            select! {
                message = self.rx.recv() => {
                    if let Some(message) = message {
                        match message {
                            ParallelMessage::ProtocolMessage(message) => self.send(message).await?,
                            ParallelMessage::Flush => self.flush().await?,
                        }
                    } else {
                        self.return_server().await?;
                        break;
                    }
                }

                reply = self.server.as_mut().unwrap().read() => {
                    let reply = reply?;
                    self.tx.send(ParallelReply::Message(reply)).await.map_err(|_| Error::ParallelConnection)?;
                }

                _ = self.stop.notified() => {
                    self.return_server().await?;
                    break;
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use crate::{
        backend::server::test::test_server,
        net::{Parse, Protocol, Sync},
    };

    use super::*;

    #[tokio::test]
    async fn test_parallel_connection() {
        let server = test_server().await;
        let mut parallel = ParallelConnection::new(server).unwrap();

        parallel
            .send(
                &vec![
                    Parse::named("test", "SELECT $1::bigint").into(),
                    Sync.into(),
                ]
                .into(),
            )
            .await
            .unwrap();

        for c in ['1', 'Z'] {
            let msg = parallel.read().await.unwrap();
            assert_eq!(msg.code(), c);
        }

        let server = parallel.reattach().await.unwrap();
        assert!(server.in_sync());
    }
}
