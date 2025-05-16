//! Server connection requested by a frontend.

use mirror::{MirrorHandler, MirrorRequest};
use tokio::time::sleep;
use tracing::debug;

use crate::{
    admin::backend::Backend,
    backend::{
        databases::databases,
        reload_notify,
        replication::{Buffer, ReplicationConfig},
    },
    config::PoolerMode,
    frontend::{
        router::{parser::Shard, CopyRow, Route},
        Router,
    },
    net::{Bind, Message, ParameterStatus, Parameters},
    state::State,
};

use super::{
    super::{pool::Guard, Error},
    Address, Cluster, Request, ShardingSchema,
};

use std::{mem::replace, time::Duration};

pub mod aggregate;
pub mod binding;
pub mod buffer;
pub mod mirror;
pub mod multi_shard;

use aggregate::Aggregates;
use binding::Binding;
use mirror::Mirror;
use multi_shard::MultiShard;

/// Wrapper around a server connection.
#[derive(Default, Debug)]
pub struct Connection {
    user: String,
    database: String,
    binding: Binding,
    cluster: Option<Cluster>,
    mirrors: Vec<MirrorHandler>,
}

impl Connection {
    /// Create new server connection handler.
    pub(crate) fn new(user: &str, database: &str, admin: bool) -> Result<Self, Error> {
        let mut conn = Self {
            binding: if admin {
                Binding::Admin(Backend::new())
            } else {
                Binding::Server(None)
            },
            cluster: None,
            user: user.to_owned(),
            database: database.to_owned(),
            mirrors: vec![],
        };

        if !admin {
            conn.reload()?;
        }

        Ok(conn)
    }

    /// Check if the connection is available.
    pub(crate) fn connected(&self) -> bool {
        self.binding.connected()
    }

    /// Create a server connection if one doesn't exist already.
    pub(crate) async fn connect(&mut self, request: &Request, route: &Route) -> Result<(), Error> {
        let connect = match &self.binding {
            Binding::Server(None) | Binding::Replication(None, _) => true,
            Binding::MultiShard(shards, _) => shards.is_empty(),
            _ => false,
        };

        if connect {
            debug!("connecting {}", route);
            match self.try_conn(request, route).await {
                Ok(()) => (),
                Err(Error::Pool(super::Error::Offline | super::Error::AllReplicasDown)) => {
                    // Wait to reload pools until they are ready.
                    if let Some(wait) = reload_notify::ready() {
                        wait.await;
                    }
                    self.reload()?;
                    return self.try_conn(request, route).await;
                }
                Err(err) => {
                    return Err(err);
                }
            }

            if !self.binding.state_check(State::Idle) {
                return Err(Error::NotInSync);
            }
        }

        Ok(())
    }

    /// Set the connection into replication mode.
    pub(crate) fn replication_mode(
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

    /// Send traffic to mirrors.
    pub(crate) fn mirror(&self, buffer: &crate::frontend::Buffer) {
        for mirror in &self.mirrors {
            let _ = mirror.tx.try_send(MirrorRequest::new(buffer));
        }
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
    pub(crate) async fn parameters(
        &mut self,
        request: &Request,
    ) -> Result<Vec<ParameterStatus>, Error> {
        match &self.binding {
            Binding::Admin(_) => Ok(ParameterStatus::fake()),
            _ => {
                // Try a replica. If not, try the primary.
                if self.connect(request, &Route::read(Some(0))).await.is_err() {
                    self.connect(request, &Route::write(Some(0))).await?;
                };
                let mut params = vec![];
                for param in self.server()?.params().iter() {
                    if let Some(value) = param.1.as_str() {
                        params.push(ParameterStatus::from((param.0.as_str(), value)));
                    }
                }
                self.disconnect();
                Ok(params)
            }
        }
    }

    /// Disconnect from a server.
    pub(crate) fn disconnect(&mut self) {
        self.binding.disconnect();
    }

    /// Close the connection without banning the pool.
    pub(crate) fn force_close(&mut self) {
        self.binding.force_close()
    }

    /// Read a message from the server connection.
    ///
    /// Only await this future inside a `select!`. One of the conditions
    /// suspends this loop indefinitely and expects another `select!` branch
    /// to cancel it.
    pub(crate) async fn read(&mut self) -> Result<Message, Error> {
        self.binding.read().await
    }

    /// Send messages to the server.
    pub(crate) async fn send(&mut self, messages: &crate::frontend::Buffer) -> Result<(), Error> {
        self.binding.send(messages).await
    }

    /// Send COPY subprotocol data to the right shards.
    pub(crate) async fn send_copy(&mut self, rows: Vec<CopyRow>) -> Result<(), Error> {
        self.binding.send_copy(rows).await
    }

    /// Send buffer in a potentially sharded context.
    pub(crate) async fn handle_buffer(
        &mut self,
        messages: &crate::frontend::Buffer,
        router: &mut Router,
        streaming: bool,
    ) -> Result<(), Error> {
        if messages.copy() && !streaming {
            let rows = router.copy_data(messages).unwrap();
            if !rows.is_empty() {
                self.send_copy(rows).await?;
                self.send(&messages.without_copy_data()).await?;
            } else {
                self.send(messages).await?;
            }
        } else {
            // Send query to server.
            self.send(messages).await?;
        }

        Ok(())
    }

    /// Fetch the cluster from the global database store.
    pub(crate) fn reload(&mut self) -> Result<(), Error> {
        match self.binding {
            Binding::Server(_) | Binding::MultiShard(_, _) | Binding::Replication(_, _) => {
                let databases = databases();
                let user = (self.user.as_str(), self.database.as_str());
                let cluster = databases.cluster(user)?;

                self.cluster = Some(cluster);
                self.mirrors = databases
                    .mirrors(user)?
                    .unwrap_or(&[])
                    .iter()
                    .map(Mirror::spawn)
                    .collect::<Result<Vec<_>, Error>>()?;
                debug!(
                    r#"database "{}" has {} mirrors"#,
                    self.cluster()?.name(),
                    self.mirrors.len()
                );
            }

            _ => (),
        }

        Ok(())
    }

    pub(crate) fn bind(&mut self, bind: &Bind) -> Result<(), Error> {
        match self.binding {
            Binding::MultiShard(_, ref mut state) => {
                state.set_context(bind);
                Ok(())
            }

            _ => Ok(()),
        }
    }

    /// We are done and can disconnect from this server.
    pub(crate) fn done(&self) -> bool {
        self.binding.done()
    }

    /// Get connected servers addresses.
    pub(crate) fn addr(&mut self) -> Result<Vec<&Address>, Error> {
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
    pub(crate) fn cluster(&self) -> Result<&Cluster, Error> {
        self.cluster.as_ref().ok_or(Error::NotConnected)
    }

    /// Transaction mode pooling.
    #[inline]
    pub(crate) fn transaction_mode(&self) -> bool {
        self.cluster()
            .map(|c| c.pooler_mode() == PoolerMode::Transaction)
            .unwrap_or(true)
    }

    /// Pooler is in session mod
    #[inline]
    pub(crate) fn session_mode(&self) -> bool {
        !self.transaction_mode()
    }

    /// Execute a query on the binding, if it's connected.
    pub(crate) async fn execute(&mut self, query: &str) -> Result<(), Error> {
        self.binding.execute(query).await
    }

    pub(crate) async fn link_client(&mut self, params: &Parameters) -> Result<usize, Error> {
        self.binding.link_client(params).await
    }

    pub(crate) fn changed_params(&mut self) -> Parameters {
        self.binding.changed_params()
    }
}
