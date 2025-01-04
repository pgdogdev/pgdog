//! Server connection.

use tokio::time::sleep;

use crate::{
    admin::backend::Backend,
    backend::databases::databases,
    net::messages::{BackendKeyData, Message, ParameterStatus, Protocol},
};

use super::{
    super::{pool::Guard, Error},
    Cluster,
};
use std::time::Duration;

/// Wrapper around a server connection.
#[derive(Default)]
pub struct Connection {
    user: String,
    database: String,
    server: Option<Guard>,
    cluster: Option<Cluster>,
    admin: Option<Backend>,
}

impl Connection {
    /// Create new server connection handler.
    pub fn new(user: &str, database: &str, admin: bool) -> Result<Self, Error> {
        let mut conn = Self {
            server: None,
            cluster: None,
            user: user.to_owned(),
            database: database.to_owned(),
            admin: if admin { Some(Backend::new()) } else { None },
        };

        if !admin {
            conn.reload()?;
        }

        Ok(conn)
    }

    /// Check if the connection is available.
    pub fn connected(&self) -> bool {
        self.server.is_some() || self.admin.is_some()
    }

    /// Create a server connection if one doesn't exist already.
    pub async fn connect(&mut self, id: &BackendKeyData) -> Result<(), Error> {
        if self.server.is_none() && self.admin.is_none() {
            let server = self.cluster()?.primary(0, id).await?;
            self.server = Some(server);
        }

        Ok(())
    }

    /// Get server parameters.
    pub async fn parameters(&mut self, id: &BackendKeyData) -> Result<Vec<ParameterStatus>, Error> {
        if self.admin.is_some() {
            Ok(ParameterStatus::fake())
        } else {
            self.connect(id).await?;
            let params = self
                .server()?
                .params()
                .iter()
                .map(|p| ParameterStatus::from(p.clone()))
                .collect();
            self.disconnect();
            Ok(params)
        }
    }

    /// Disconnect from a server.
    pub fn disconnect(&mut self) {
        self.server = None;
    }

    /// Read a message from the server connection.
    pub async fn read(&mut self) -> Result<Message, Error> {
        match (self.server.as_mut(), self.admin.as_mut()) {
            (Some(server), None) => Ok(server.read().await?),
            (None, Some(admin)) => Ok(admin.read().await?),
            (None, None) => {
                // Suspend the future until select! cancels it.
                loop {
                    sleep(Duration::MAX).await;
                }
            }
            (Some(_), Some(_)) => Err(Error::NotConnected),
        }
    }

    /// Send messages to the server.
    pub async fn send(&mut self, messages: Vec<impl Protocol>) -> Result<(), Error> {
        match (self.server.as_mut(), self.admin.as_mut()) {
            (Some(server), None) => server.send(messages).await,
            (None, Some(admin)) => Ok(admin.send(messages).await?),
            (None, None) | (Some(_), Some(_)) => Err(Error::NotConnected),
        }
    }

    /// Fetch the cluster from the global database store.
    pub fn reload(&mut self) -> Result<(), Error> {
        let cluster = databases().cluster((self.user.as_str(), self.database.as_str()))?;
        self.cluster = Some(cluster);

        Ok(())
    }

    /// We are done and can disconnect from this server.
    pub fn done(&self) -> bool {
        if let Some(ref server) = self.server {
            server.done()
        } else {
            true
        }
    }

    #[inline]
    fn cluster(&self) -> Result<&Cluster, Error> {
        Ok(self.cluster.as_ref().ok_or(Error::NotConnected)?)
    }

    /// Get server connection if we are connected, return an error
    /// otherwise.
    #[inline]
    pub fn server(&mut self) -> Result<&mut Guard, Error> {
        if let Some(ref mut server) = self.server {
            Ok(server)
        } else {
            Err(Error::NotConnected)
        }
    }
}
