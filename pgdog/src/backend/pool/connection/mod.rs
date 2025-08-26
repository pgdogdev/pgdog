//! Server connection requested by a frontend.

use mirror::MirrorHandler;
use tokio::{select, time::sleep};
use tracing::debug;

use crate::{
    admin::backend::Backend,
    backend::{
        databases::{self, databases},
        reload_notify, PubSubClient,
    },
    config::{config, PoolerMode, User},
    frontend::{
        router::{parser::Shard, CopyRow, Route},
        Router,
    },
    net::{Bind, Message, ParameterStatus, Protocol},
    state::State,
};

use super::{
    super::{pool::Guard, Error},
    Address, Cluster, Request,
};

use std::{
    mem::replace,
    ops::{Deref, DerefMut},
    time::Duration,
};

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
    passthrough_password: Option<String>,
    database: String,
    binding: Binding,
    cluster: Option<Cluster>,
    mirrors: Vec<MirrorHandler>,
    locked: bool,
    pub_sub: PubSubClient,
}

impl Connection {
    /// Create new server connection handler.
    pub(crate) fn new(
        user: &str,
        database: &str,
        admin: bool,
        passthrough_password: &Option<String>,
    ) -> Result<Self, Error> {
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
            locked: false,
            passthrough_password: passthrough_password.clone(),
            pub_sub: PubSubClient::new(),
        };

        if !admin {
            conn.reload()?;
        }

        Ok(conn)
    }

    /// Create a server connection if one doesn't exist already.
    pub(crate) async fn connect(&mut self, request: &Request, route: &Route) -> Result<(), Error> {
        let connect = match &self.binding {
            Binding::Server(None) => true,
            Binding::MultiShard(shards, _) => shards.is_empty(),
            _ => false,
        };

        if connect {
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

    /// Send client request to mirrors.
    pub fn mirror(&mut self, buffer: &crate::frontend::ClientRequest) {
        for mirror in &mut self.mirrors {
            mirror.send(buffer);
        }
    }

    /// Tell mirrors to flush buffered transaction.
    pub fn mirror_flush(&mut self) {
        for mirror in &mut self.mirrors {
            mirror.flush();
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

    /// Read a message from the server connection or a pub/sub channel.
    ///
    /// Only await this future inside a `select!`. One of the conditions
    /// suspends this loop indefinitely and expects another `select!` branch
    /// to cancel it.
    pub(crate) async fn read(&mut self) -> Result<Message, Error> {
        select! {
            notification = self.pub_sub.recv() => {
                Ok(notification.ok_or(Error::ProtocolOutOfSync)?.message()?)
            }

            message = self.binding.read() => {
                message
            }
        }
    }

    /// Subscribe to a channel.
    pub async fn listen(&mut self, channel: &str, shard: Shard) -> Result<(), Error> {
        let num = match shard {
            Shard::Direct(shard) => shard,
            _ => return Err(Error::ProtocolOutOfSync),
        };

        if let Some(shard) = self.cluster()?.shards().get(num) {
            let rx = shard.listen(channel).await?;
            self.pub_sub.listen(channel, rx);
        }

        Ok(())
    }

    /// Stop listening on a channel.
    pub fn unlisten(&mut self, channel: &str) {
        self.pub_sub.unlisten(channel);
    }

    /// Notify a channel.
    pub async fn notify(
        &mut self,
        channel: &str,
        payload: &str,
        shard: Shard,
    ) -> Result<(), Error> {
        let num = match shard {
            Shard::Direct(shard) => shard,
            _ => return Err(Error::ProtocolOutOfSync),
        };

        // Max two attempts.
        for _ in 0..2 {
            if let Some(shard) = self.cluster()?.shards().get(num) {
                match shard.notify(channel, payload).await {
                    Err(super::Error::Offline) => self.reload()?,
                    Err(err) => return Err(err.into()),
                    Ok(_) => break,
                }
            }
        }

        Ok(())
    }

    /// Send buffer in a potentially sharded context.
    pub(crate) async fn handle_client_request(
        &mut self,
        client_request: &crate::frontend::ClientRequest,
        router: &mut Router,
        streaming: bool,
    ) -> Result<(), Error> {
        if client_request.copy() && !streaming {
            let rows = router
                .copy_data(client_request)
                .map_err(|e| Error::Router(e.to_string()))?;
            if !rows.is_empty() {
                self.send_copy(rows).await?;
                self.send(&client_request.without_copy_data()).await?;
            } else {
                self.send(client_request).await?;
            }
        } else {
            // Send query to server.
            self.send(client_request).await?;
        }

        Ok(())
    }

    /// Fetch the cluster from the global database store.
    pub(crate) fn reload(&mut self) -> Result<(), Error> {
        match self.binding {
            Binding::Server(_) | Binding::MultiShard(_, _) => {
                let user = (self.user.as_str(), self.database.as_str());
                // Check passthrough auth.
                if config().config.general.passthrough_auth() && !databases().exists(user) {
                    if let Some(ref passthrough_password) = self.passthrough_password {
                        let new_user = User::new(&self.user, passthrough_password, &self.database);
                        databases::add(new_user);
                    }
                }

                let databases = databases();
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
        self.binding.done() && !self.locked
    }

    /// Lock this connection to the client, preventing it's
    /// release back into the pool.
    pub(crate) fn lock(&mut self, lock: bool) {
        self.locked = lock;
        if lock {
            self.binding.dirty();
        }
    }

    /// Get connected servers addresses.
    pub(crate) fn addr(&mut self) -> Result<Vec<&Address>, Error> {
        Ok(match self.binding {
            Binding::Server(Some(ref server)) => vec![server.addr()],
            Binding::MultiShard(ref servers, _) => servers.iter().map(|s| s.addr()).collect(),
            _ => {
                return Err(Error::NotConnected);
            }
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

    /// This is an admin DB connection.
    pub fn is_admin(&self) -> bool {
        matches!(self.binding, Binding::Admin(_))
    }
}

impl Deref for Connection {
    type Target = Binding;

    fn deref(&self) -> &Self::Target {
        &self.binding
    }
}

impl DerefMut for Connection {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.binding
    }
}
