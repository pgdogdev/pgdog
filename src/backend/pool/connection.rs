//! Server connection.

use tokio::time::sleep;

use crate::{
    backend::databases::databases,
    net::messages::{BackendKeyData, Message, ParameterStatus, Protocol},
};

use super::{
    super::{pool::Guard, Error, Server},
    Cluster,
};
use std::{ops::Deref, time::Duration};

/// Wrapper around a server connection.
#[derive(Default)]
pub struct Connection {
    user: String,
    database: String,
    server: Option<Guard>,
    cluster: Option<Cluster>,
}

impl Connection {
    /// Create new server connection handler.
    pub fn new(user: &str, database: &str) -> Result<Self, Error> {
        let mut conn = Self {
            server: None,
            cluster: None,
            user: user.to_owned(),
            database: database.to_owned(),
        };

        conn.reload()?;

        Ok(conn)
    }

    /// Check if the connection is available.
    pub fn connected(&self) -> bool {
        self.server.is_some()
    }

    /// Create a server connection if one doesn't exist already.
    pub async fn connect(&mut self, id: &BackendKeyData) -> Result<(), Error> {
        if self.server.is_none() {
            let server = self.cluster()?.primary(0, id).await?;
            self.server = Some(server);
        }

        Ok(())
    }

    /// Get server parameters.
    pub async fn parameters(&mut self, id: &BackendKeyData) -> Result<Vec<ParameterStatus>, Error> {
        self.connect(id).await?;
        let params = self
            .params()
            .iter()
            .map(|p| ParameterStatus::from(p.clone()))
            .collect();
        self.disconnect();
        Ok(params)
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

    /// Fetch the cluster from the global database store.
    pub fn reload(&mut self) -> Result<(), Error> {
        let cluster = databases().cluster((self.user.as_str(), self.database.as_str()))?;
        self.cluster = Some(cluster);

        Ok(())
    }

    #[inline]
    fn cluster(&self) -> Result<&Cluster, Error> {
        Ok(self.cluster.as_ref().ok_or(Error::NotConnected)?)
    }
}

impl Deref for Connection {
    type Target = Server;

    fn deref(&self) -> &Self::Target {
        self.server.as_ref().unwrap()
    }
}
