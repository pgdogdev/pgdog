//! Server connection requested by a frontend.

use mirror::MirrorHandler;
use pgdog_config::users::PasswordKind;
use tokio::{select, time::sleep};
use tracing::debug;

use crate::{
    admin::server::AdminServer,
    backend::{
        databases::{self, databases},
        pool, reload_notify, PubSubClient,
    },
    config::{config, PoolerMode, User},
    frontend::{
        router::{parser::Shard, CopyRow, Route},
        ClientRequest, Router,
    },
    net::{Bind, Message, ParameterStatus, Protocol, ProtocolMessage},
    state::State,
};

use super::{
    super::{pool::Guard, Error},
    Address, Cluster, Request,
};

use std::{
    ops::{Deref, DerefMut},
    time::Duration,
};

pub mod aggregate;
pub mod binding;
#[cfg(test)]
pub mod binding_test;
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
    locked: bool,
    pub_sub: PubSubClient,
}

impl Connection {
    /// Create new server connection handler.
    pub(crate) fn new(user: &str, database: &str, admin: bool) -> Result<Self, Error> {
        let mut conn = Self {
            binding: if admin {
                Binding::Admin(AdminServer::new())
            } else {
                Binding::Direct(None)
            },
            cluster: None,
            user: user.to_owned(),
            database: database.to_owned(),
            mirrors: vec![],
            locked: false,
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
            Binding::Direct(None) => true,
            Binding::MultiShard(shards, _) => shards.is_empty(),
            _ => false,
        };

        if connect {
            match self.try_conn(request, route).await {
                Ok(()) => (),
                Err(Error::Pool(super::Error::Offline | super::Error::AllReplicasDown)) => {
                    debug!("detected configuration reload, reloading cluster");

                    // Wait to reload pools until they are ready.
                    self.safe_reload().await?;
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

    /// Remove transaction from mirrors buffers.
    pub fn mirror_clear(&mut self) {
        for mirror in &mut self.mirrors {
            mirror.clear();
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
                Binding::Direct(existing) => {
                    let _ = existing.replace(server);
                }

                Binding::MultiShard(_, _) => {
                    self.binding = Binding::Direct(Some(server));
                }

                _ => (),
            };
        } else {
            let mut shards = vec![];
            let mut shard_indices = vec![];
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
                shard_indices.push(i);
            }

            self.binding =
                Binding::MultiShard(shards, Box::new(MultiShard::new(shard_indices, route)));
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
                // Get params from the first database that answers.
                // Parameters are cached on the pool.
                for shard in self.cluster()?.shards() {
                    if let Ok(params) = shard.params(request).await {
                        let mut result = vec![];

                        for param in params.iter() {
                            if let Some(value) = param.1.as_str() {
                                result.push(ParameterStatus::from((param.0.as_str(), value)));
                            }
                        }

                        return Ok(result);
                    }
                }
                Err(Error::Pool(pool::Error::AllReplicasDown))
            }
        }
    }

    /// Read a message from the server connection or a pub/sub channel.
    ///
    /// Only await this future inside a `select!`. One of the conditions
    /// suspends this loop indefinitely and expects another `select!` branch
    /// to cancel it.
    ///
    pub(crate) async fn read(&mut self) -> Result<Message, Error> {
        select! {
            notification = self.pub_sub.recv() => {
                Ok(notification.ok_or(Error::ProtocolOutOfSync)?.message()?)
            }

            // This is cancel-safe.
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
        client_request: &ClientRequest,
        router: &mut Router,
        streaming: bool,
    ) -> Result<(), Error> {
        if client_request.is_copy() && !streaming {
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
            // We split up the extended protocol exhange as soon as we see
            // a Flush message. This means we can handle this:
            //
            // 1. Parse, Describe, Flush
            // 2. Bind, Execute, Sync
            //
            // without breaking the state by injecting the last Parse we saw into
            // the second request and ignoring ParseComplete from the server.
            //
            if let Some(ref parse) = client_request.last_parse {
                if client_request.needs_parse_injection() {
                    self.send_ignore(&ProtocolMessage::Parse(parse.clone()))
                        .await?;
                }
            }

            // Send query to server.
            self.send(client_request).await?;
        }

        Ok(())
    }

    /// Reload synchronized with partial config changes.
    pub async fn safe_reload(&mut self) -> Result<(), Error> {
        if let Some(wait) = reload_notify::ready() {
            wait.await;
        }

        self.reload()
    }

    /// Fetch the cluster from the global database store.
    fn reload(&mut self) -> Result<(), Error> {
        if matches!(self.binding, Binding::Admin(_)) {
            return Ok(());
        }

        let user = (self.user.as_str(), self.database.as_str());
        let config = config();

        // Check if we need re-configure passthrough auth using our existing password.
        //
        // This happens on configuration reload (RELOAD/sighup), because we
        // only load databases from the config. RELOAD effectively removes all passthrough
        // connection pools until a client needs to query it and we re-create it.
        //
        if config.config.general.passthrough_auth() && databases().passwords(user).is_none() {
            if let Some(ref cluster) = self.cluster {
                let mut user = User {
                    name: self.user.clone(),
                    database: self.database.clone(),
                    ..Default::default()
                };
                for pass in cluster.passwords() {
                    match pass {
                        PasswordKind::Hashed(hashed) => {
                            user.password_hash = Some(hashed.clone());
                        }

                        PasswordKind::Plain(plain) => {
                            user.passwords.push(plain.clone());
                        }
                    }
                }

                databases::add(user)?;
            }
        }

        let databases = databases();
        let cluster = databases.cluster(user)?;

        self.cluster = Some(cluster.clone());
        let source_db = cluster.name();
        self.mirrors = databases
            .mirrors(user)?
            .unwrap_or(&[])
            .iter()
            .map(|dest_cluster| {
                let mirror_config = databases.mirror_config(source_db, dest_cluster.name());
                Mirror::spawn(source_db, dest_cluster, mirror_config)
            })
            .collect::<Result<Vec<_>, Error>>()?;
        debug!(
            r#"database "{}" has {} mirrors"#,
            self.cluster()?.name(),
            self.mirrors.len()
        );

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

    /// Check if this connection is locked to a client.
    #[cfg(test)]
    pub(crate) fn locked(&self) -> bool {
        self.locked
    }

    /// Get connected servers addresses.
    pub(crate) fn addr(&self) -> Result<Vec<&Address>, Error> {
        Ok(match self.binding {
            Binding::Direct(Some(ref server)) => vec![server.addr()],
            Binding::MultiShard(ref servers, _) => servers.iter().map(|s| s.addr()).collect(),
            _ => {
                return Err(Error::NotConnected);
            }
        })
    }

    /// Get cluster if any.
    #[inline]
    pub(crate) fn cluster(&self) -> Result<&Cluster, Error> {
        self.cluster.as_ref().ok_or(Error::ClusterNotConnected)
    }

    /// Pooler is in session mode.
    #[inline]
    pub(crate) fn session_mode(&self) -> bool {
        self.cluster()
            .map(|c| c.pooler_mode() == PoolerMode::Session)
            .unwrap_or(true)
    }

    #[inline]
    pub(crate) fn pooler_mode(&self) -> PoolerMode {
        self.cluster().map(|c| c.pooler_mode()).unwrap_or_default()
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
