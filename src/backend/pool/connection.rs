//! Server connection.

use tokio::time::sleep;

use crate::net::messages::{BackendKeyData, Message, Protocol};

use super::super::{
    pool::{pool, Guard},
    Error, Server,
};
use std::{ops::Deref, time::Duration};

/// Wrapper around a server connection.
pub struct Connection {
    server: Option<Guard>,
}

impl Connection {
    /// Create new server connection handler.
    pub fn new() -> Self {
        Self { server: None }
    }

    /// Check if the connection is available.
    pub fn connected(&self) -> bool {
        self.server.is_some()
    }

    /// Create a server connection if one doesn't exist already.
    pub async fn get(&mut self, id: &BackendKeyData) -> Result<(), Error> {
        if self.server.is_none() {
            let server = pool().get(id).await?;
            self.server = Some(server);
        }

        Ok(())
    }

    /// Disconnect from a server.
    pub fn disconnect(&mut self) {
        self.server = None;
    }

    /// Read a message from the server connection.
    pub async fn read(&mut self) -> Result<Message, Error> {
        if let Some(ref mut server) = self.server {
            let message = server.read().await?;
            Ok(message)
        } else {
            // Suspend the future until select! cancels it.
            loop {
                sleep(Duration::MAX).await;
            }
        }
    }

    /// Send messages to the server.
    pub async fn send(&mut self, messages: Vec<impl Protocol>) -> Result<(), Error> {
        if let Some(ref mut server) = self.server {
            server.send(messages).await
        } else {
            Err(Error::NotConnected)
        }
    }
}

impl Deref for Connection {
    type Target = Server;

    fn deref(&self) -> &Self::Target {
        self.server.as_ref().unwrap()
    }
}
