//! Server connection requested by a frontend.

use tokio::time::sleep;

use crate::{
    admin::backend::Backend,
    backend::{
        databases::databases,
        replication::{Buffer, ReplicationConfig},
    },
    config::PoolerMode,
    frontend::router::{parser::Shard, CopyRow, Route},
    net::{
        messages::{Message, ParameterStatus, Protocol},
        parameter::Parameters,
    },
};

use super::{
    super::{pool::Guard, Error},
    Address, Cluster, Request, ShardingSchema,
};

use std::{mem::replace, time::Duration};

pub mod aggregate;
pub mod binding;
pub mod buffer;
pub mod multi_shard;

use aggregate::Aggregates;
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
    pub async fn connect(&mut self, request: &Request, route: &Route) -> Result<(), Error> {
        let connect = match &self.binding {
            Binding::Server(None) | Binding::Replication(None, _) => true,
            Binding::MultiShard(shards, _) => shards.is_empty(),
            _ => false,
        };

        if connect {
            match self.try_conn(request, route).await {
                Ok(()) => (),
                Err(Error::Pool(super::Error::Offline)) => {
                    self.reload()?;
                    return self.try_conn(request, route).await;
                }
                Err(err) => return Err(err),
            }
        }

        Ok(())
    }

    /// Set the connection into replication mode.
    pub fn replication_mode(
        &mut self,
        shard: Shard,
        replication_config: &ReplicationConfig,
        sharding_schema: &ShardingSchema,
    ) -> Result<(), Error> {
        self.binding = Binding::Replication(
            None,
            Buffer::new(shard, replication_config, sharding_schema),
        );
        Ok(())
    }

    /// Try to get a connection for the given route.
    async fn try_conn(&mut self, request: &Request, route: &Route) -> Result<(), Error> {
        if let Shard::Direct(shard) = route.shard() {
            let mut server = if route.is_read() {
                self.cluster()?.replica(*shard, request).await?
            } else {
                self.cluster()?.primary(*shard, request).await?
            };

            // Cleanup session mode connections when
            // they are done.
            if self.session_mode() {
                server.reset = true;
            }

            match &mut self.binding {
                Binding::Server(existing) => {
                    let _ = replace(existing, Some(server));
                }

                Binding::Replication(existing, _) => {
                    let _ = replace(existing, Some(server));
                }

                Binding::MultiShard(_, _) => {
                    self.binding = Binding::Server(Some(server));
                }

                _ => (),
            };
        } else {
            let mut shards = vec![];
            for (i, shard) in self.cluster()?.shards().iter().enumerate() {
                if let Shard::Multi(numbers) = route.shard() {
                    if !numbers.contains(&i) {
                        continue;
                    }
                };
                let mut server = if route.is_read() {
                    shard.replica(request).await?
                } else {
                    shard.primary(request).await?
                };

                if self.session_mode() {
                    server.reset = true;
                }

                shards.push(server);
            }
            let num_shards = shards.len();

            self.binding = Binding::MultiShard(shards, MultiShard::new(num_shards, route));
        }

        Ok(())
    }

    /// Get server parameters.
    pub async fn parameters(&mut self, request: &Request) -> Result<Vec<ParameterStatus>, Error> {
        match &self.binding {
            Binding::Admin(_) => Ok(ParameterStatus::fake()),
            _ => {
                self.connect(request, &Route::write(Some(0))).await?; // Get params from primary.
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

    /// Send COPY subprotocol data to the right shards.
    pub async fn send_copy(&mut self, rows: Vec<CopyRow>) -> Result<(), Error> {
        self.binding.send_copy(rows).await
    }

    /// Fetch the cluster from the global database store.
    pub fn reload(&mut self) -> Result<(), Error> {
        match self.binding {
            Binding::Server(_) | Binding::MultiShard(_, _) | Binding::Replication(_, _) => {
                let cluster = databases().cluster((self.user.as_str(), self.database.as_str()))?;
                self.cluster = Some(cluster);
            }

            _ => (),
        }

        Ok(())
    }

    /// Make sure a prepared statement exists on the connection.
    pub async fn prepare(&mut self, name: &str) -> Result<(), Error> {
        match self.binding {
            Binding::Server(Some(ref mut server)) => {
                server.prepare_statement(name).await?;
            }
            Binding::MultiShard(ref mut servers, _) => {
                for server in servers {
                    server.prepare_statement(name).await?;
                }
            }
            _ => (),
        }

        Ok(())
    }

    pub async fn describe(&mut self, name: &str) -> Result<Vec<Message>, Error> {
        match self.binding {
            Binding::Server(Some(ref mut server)) => Ok(server.describe_statement(name).await?),

            Binding::MultiShard(ref mut servers, _) => {
                let mut result: Option<Vec<Message>> = None;
                for server in servers {
                    let messages = server.describe_statement(name).await?;
                    if let Some(ref _res) = result {
                        // TODO: check for equivalency.
                    } else {
                        result = Some(messages);
                    }
                }

                if let Some(result) = result {
                    Ok(result)
                } else {
                    Err(Error::NotInSync)
                }
            }

            _ => Err(Error::NotInSync),
        }
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
        matches!(self.binding, Binding::Admin(_))
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

    /// Execute a query on the binding, if it's connected.
    pub async fn execute(&mut self, query: &str) -> Result<(), Error> {
        self.binding.execute(query).await
    }

    pub async fn sync_params(&mut self, params: &Parameters) -> Result<(), Error> {
        self.binding.sync_params(params).await
    }

    pub fn changed_params(&mut self) -> Parameters {
        self.binding.changed_params()
    }
}
