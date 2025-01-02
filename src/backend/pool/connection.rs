//! Server connection.

use tokio::time::sleep;

use crate::net::messages::{Message, Protocol};

use super::super::{Error, Server};
use std::{ops::Deref, time::Duration};

pub struct Connection {
    server: Option<Server>,
}

impl Connection {
    /// Create new server connection handler.
    pub fn new() -> Self {
        Self { server: None }
    }

    /// Create a server connection if one doesn't exist already.
    pub async fn get(&mut self) -> Result<(), Error> {
        if self.server.is_none() {
            self.server = Some(Server::connect("127.0.0.1:5432").await?);
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
            server.read().await
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

impl Drop for Connection {
    fn drop(&mut self) {
        if let Some(server) = self.server.take() {
            server.rollback();
        }
    }
}
