//! Server connection requested by a frontend.

use pgdog_plugin::Route;
use tokio::time::sleep;

use crate::{
    admin::backend::Backend,
    backend::databases::databases,
    config::PoolerMode,
    net::messages::{BackendKeyData, Message, ParameterStatus, Protocol},
};

use super::{
    super::{pool::Guard, Error},
    Address, Cluster,
};

use std::time::Duration;

mod binding;
mod multi_shard;
use binding::Binding;
use multi_shard::MultiShard;

/// Wrapper around a server connection.
#[derive(Default)]
pub struct Connection {
    user: String,
    database: String,
    binding: Binding,
    cluster: Option<Cluster>,
}

impl Connection {
    /// Create new server connection handler.
    pub fn new(user: &str, database: &str, admin: bool) -> Result<Self, Error> {
        let mut conn = Self {
            binding: if admin {
                Binding::Admin(Backend::new())
            } else {
                Binding::Server(None)
            },
            cluster: None,
            user: user.to_owned(),
            database: database.to_owned(),
        };

        if !admin {
            conn.reload()?;
        }

        Ok(conn)
    }

    /// Check if the connection is available.
    pub fn connected(&self) -> bool {
        self.binding.connected()
    }

    /// Create a server connection if one doesn't exist already.
    pub async fn connect(&mut self, id: &BackendKeyData, route: &Route) -> Result<(), Error> {
        let connect = match &self.binding {
            Binding::Server(None) => true,
            Binding::MultiShard(shards, _) => shards.is_empty(),
            _ => false,
        };

        if connect {
            match self.try_conn(id, route).await {
                Ok(()) => (),
                Err(Error::Pool(super::Error::Offline)) => {
                    self.reload()?;
                    return self.try_conn(id, route).await;
                }
                Err(err) => return Err(err),
            }
        }

        Ok(())
    }

    /// Try to get a connection for the given route.
    async fn try_conn(&mut self, id: &BackendKeyData, route: &Route) -> Result<(), Error> {
        if let Some(shard) = route.shard() {
            let mut server = if route.is_read() {
                self.cluster()?.replica(shard, id).await?
            } else {
                self.cluster()?.primary(shard, id).await?
            };

            // Cleanup session mode connections when
            // they are done.
            if self.session_mode() {
                server.reset = true;
            }

            self.binding = Binding::Server(Some(server));
        } else if route.is_all_shards() {
            let mut shards = vec![];
            for shard in self.cluster()?.shards() {
                let mut server = if route.is_read() {
                    shard.replica(id).await?
                } else {
                    shard.primary(id).await?
                };

                if self.session_mode() {
                    server.reset = true;
                }

                shards.push(server);
            }
            let num_shards = shards.len();

            self.binding = Binding::MultiShard(shards, MultiShard::new(num_shards));
        }

        Ok(())
    }

    /// Get server parameters.
    pub async fn parameters(&mut self, id: &BackendKeyData) -> Result<Vec<ParameterStatus>, Error> {
        match &self.binding {
            Binding::Admin(_) => Ok(ParameterStatus::fake()),
            Binding::Server(_) | Binding::MultiShard(_, _) => {
                self.connect(id, &Route::write(0)).await?; // Get params from primary.
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
    }

    /// Disconnect from a server.
    pub fn disconnect(&mut self) {
        self.binding.disconnect();
    }

    /// Read a message from the server connection.
    ///
    /// Only await this future inside a `select!`. One of the conditions
    /// suspends this loop indefinitely and expects another `select!` branch
    /// to cancel it.
    pub async fn read(&mut self) -> Result<Message, Error> {
        self.binding.read().await
    }

    /// Send messages to the server.
    pub async fn send(&mut self, messages: Vec<impl Protocol>) -> Result<(), Error> {
        self.binding.send(messages).await
    }

    /// Fetch the cluster from the global database store.
    pub fn reload(&mut self) -> Result<(), Error> {
        match self.binding {
            Binding::Server(_) | Binding::MultiShard(_, _) => {
                let cluster = databases().cluster((self.user.as_str(), self.database.as_str()))?;
                self.cluster = Some(cluster);
            }

            _ => (),
        }

        Ok(())
    }

    /// We are done and can disconnect from this server.
    pub fn done(&self) -> bool {
        self.binding.done()
    }

    /// Get connected servers addresses.
    pub fn addr(&mut self) -> Result<Vec<&Address>, Error> {
        Ok(match self.binding {
            Binding::Server(Some(ref server)) => vec![server.addr()],
            Binding::MultiShard(ref servers, _) => servers.iter().map(|s| s.addr()).collect(),
            _ => return Err(Error::NotConnected),
        })
    }

    /// Get a connected server, if any. If multi-shard, get the first one.
    #[inline]
    fn server(&mut self) -> Result<&mut Guard, Error> {
        Ok(match self.binding {
            Binding::Server(ref mut server) => server.as_mut().ok_or(Error::NotConnected)?,
            Binding::MultiShard(ref mut servers, _) => {
                servers.first_mut().ok_or(Error::NotConnected)?
            }
            _ => return Err(Error::NotConnected),
        })
    }

    /// Get cluster if any.
    #[inline]
    pub fn cluster(&self) -> Result<&Cluster, Error> {
        self.cluster.as_ref().ok_or(Error::NotConnected)
    }

    /// This is an admin database connection.
    #[inline]
    pub fn admin(&self) -> bool {
        match self.binding {
            Binding::Admin(_) => true,
            _ => false,
        }
    }

    /// Transaction mode pooling.
    #[inline]
    pub fn transaction_mode(&self) -> bool {
        self.cluster()
            .map(|c| c.pooler_mode() == PoolerMode::Transaction)
            .unwrap_or(true)
    }

    /// Pooler is in session mod
    #[inline]
    pub fn session_mode(&self) -> bool {
        !self.transaction_mode()
    }
}
