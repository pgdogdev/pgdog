//! Server connection requested by a frontend.

use futures::future::try_join_all;
use tokio::select;
use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::{
    admin::server::AdminServer,
    backend::{PubSubClient, pool},
    config::PoolerMode,
    frontend::{
        ClientRequest, Router,
        router::{CopyRow, Route, parser::Shard},
    },
    net::{Bind, Message, ParameterStatus, Protocol, ProtocolMessage, Query},
    state::State,
};

use super::{
    super::{Error, Server, pool::Guard},
    Address, Cluster, Request,
};

use std::{
    ops::{Deref, DerefMut},
    time::Duration,
};

pub(crate) mod aggregate;
pub(crate) mod binding;
#[cfg(test)]
pub(crate) mod binding_test;
pub(crate) mod buffer;
pub(crate) mod cluster_connection;
pub(crate) mod mirror;
pub(crate) mod multi_shard;

use aggregate::Aggregates;
use binding::Binding;
use cluster_connection::ClusterConnection;
use multi_shard::MultiBinding;

/// Wrapper around a server connection.
#[derive(Default, Debug)]
pub(crate) struct Connection {
    binding: Binding,
    cluster: ClusterConnection,
    pub_sub: PubSubClient,
}

impl Connection {
    /// Create new server connection handler.
    pub(crate) fn new(user: &str, database: &str, admin: bool) -> Result<Self, Error> {
        let mut conn = Self {
            binding: if admin {
                Binding::Admin(AdminServer::new())
            } else {
                Binding::NotConnected
            },
            cluster: ClusterConnection::new(user, database),
            pub_sub: PubSubClient::new(),
        };

        if !admin {
            conn.cluster.reload()?;
        }

        Ok(conn)
    }

    /// Create a server connection if one doesn't exist already.
    pub(crate) async fn connect(&mut self, request: &Request, route: &Route) -> Result<(), Error> {
        let connect = match &self.binding {
            Binding::NotConnected => true,
            Binding::MultiShard(shards) => shards.is_empty(),
            _ => false,
        };

        if connect {
            self.connect_internal(request, route).await?;

            if !self.binding.state_check(State::Idle) {
                return Err(Error::NotInSync);
            }
        }

        Ok(())
    }

    /// Send client request to mirrors.
    pub(crate) fn mirror(&mut self, buffer: &crate::frontend::ClientRequest) {
        for mirror in self.cluster.mirrors() {
            mirror.send(buffer);
        }
    }

    /// Tell mirrors to flush buffered transaction.
    pub(crate) fn mirror_flush(&mut self) {
        for mirror in self.cluster.mirrors() {
            mirror.flush();
        }
    }

    /// Remove transaction from mirrors buffers.
    pub(crate) fn mirror_clear(&mut self) {
        for mirror in self.cluster.mirrors() {
            mirror.clear();
        }
    }

    /// Try to get a connection for the given route.
    async fn connect_internal(&mut self, request: &Request, route: &Route) -> Result<(), Error> {
        if let Shard::Direct(shard) = route.shard() {
            let server = self
                .cluster
                .get_conn(request, *shard, route.is_read())
                .await?;

            self.binding = Binding::Direct(server, *shard);
        } else {
            let (shards, shard_indices) = self.cluster.get_conns(request, route).await?;

            self.binding = Binding::MultiShard(MultiBinding::new(shards, shard_indices, route));
        }

        Ok(())
    }

    /// Get server parameters.
    pub(crate) async fn parameters(
        &mut self,
        request: &Request,
    ) -> Result<Vec<ParameterStatus>, Error> {
        if matches!(self.binding, Binding::Admin(_)) {
            return Ok(ParameterStatus::fake());
        }

        match self.try_parameters(request).await {
            Ok(params) => Ok(params),
            // Configuration reload may have left the old pools offline before
            // the new ones were swapped in. Wait for the reload to settle and
            // retry once against the refreshed cluster.
            Err(Error::Pool(pool::Error::AllReplicasDown)) => {
                self.safe_reload().await?;
                self.try_parameters(request).await
            }
            Err(err) => Err(err),
        }
    }

    async fn try_parameters(&mut self, request: &Request) -> Result<Vec<ParameterStatus>, Error> {
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

    /// Read a message from the server connection or a pub/sub channel.
    ///
    /// Only await this future inside a `select!`. One of the conditions
    /// suspends this loop indefinitely and expects another `select!` branch
    /// to cancel it.
    ///
    pub(crate) async fn read(&mut self) -> Result<Message, Error> {
        select! {
            notification = self.pub_sub.recv() => {
                Ok(notification.ok_or(Error::ProtocolOutOfSync)?.message())
            }

            // This is cancel-safe.
            message = self.binding.read() => {
                message
            }
        }
    }

    /// Subscribe to a channel.
    pub(crate) async fn listen(&mut self, channel: &str, shard: Shard) -> Result<(), Error> {
        let num = match shard {
            Shard::Direct(shard) => shard,
            _ => return Err(Error::ProtocolOutOfSync),
        };

        if let Some(shard) = self.cluster()?.shards().get(num) {
            let listener = shard.listen(channel).await?;
            self.pub_sub.listen(channel, listener);
        }

        Ok(())
    }

    /// Stop listening on a channel.
    pub(crate) fn unlisten(&mut self, channel: &str) {
        self.pub_sub.unlisten(channel);
    }

    /// Stop listening on all channels.
    pub(crate) fn unlisten_all(&mut self) {
        self.pub_sub.unlisten_all();
    }

    /// Notify a channel.
    pub(crate) async fn notify(
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
                    Err(super::Error::Offline) => self.safe_reload().await?,
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
                .await
                .map_err(|e| Error::Router(e.to_string()))?;
            if !rows.is_empty() {
                self.send_copy(rows).await?;
            }
            // FIXME(lev): There is an assumption of protocol correctness here
            // from the client. If the client sends partial CopyData rows
            // and then sends CopyDone, we will send CopyDone to the shards,
            // causing the COPY to complete prematurely.
            //
            // We should assert here that the client request does not contain
            // _both_ CopyData and CopyDone messages.
            //
            self.send(&client_request.without_copy_data()).await?;
        } else {
            // We split up the extended protocol exhange as soon as we see
            // a Flush or Sync that doesn't actually execute anything. This
            // lets us handle drivers that prepare in one round-trip and run
            // in the next, e.g.:
            //
            // 1. Parse, Describe, Flush     (lib/pq uses Sync here)
            // 2. Bind, Execute, Sync
            //
            // without breaking the state by injecting the last Parse we saw
            // into the second request and ignoring ParseComplete from the
            // server. The injection has to follow the same route as the
            // request itself; sending it to extra shards would leave them
            // with a dangling Ignore expectation that hangs the read loop.
            if let Some(ref parse) = client_request.last_parse
                && client_request.needs_parse_injection()
            {
                self.send_ignore(
                    &ProtocolMessage::Parse(parse.clone()),
                    client_request.route(),
                )
                .await?;
            }

            // Send query to server.
            self.send(client_request).await?;
        }

        Ok(())
    }

    /// Reload synchronized with partial config changes.
    pub(crate) async fn safe_reload(&mut self) -> Result<(), Error> {
        if matches!(self.binding, Binding::Admin(_)) {
            return Ok(());
        }

        self.cluster.safe_reload().await
    }

    pub(crate) fn bind(&mut self, bind: &Bind) -> Result<(), Error> {
        match self.binding {
            Binding::MultiShard(ref mut servers) => {
                servers.state_mut().push_bind(bind);
                Ok(())
            }

            _ => Ok(()),
        }
    }

    /// Execute an internal query on all connected servers.
    pub(crate) async fn execute(
        &mut self,
        query: impl Into<Query> + Clone,
    ) -> Result<Vec<Message>, Error> {
        self.binding.execute(query).await
    }

    /// We are done and can disconnect from this server.
    pub(crate) fn done(&self) -> bool {
        self.binding.done() && !self.binding.is_locked()
    }

    /// Lock this connection to the client, preventing it's
    /// release back into the pool.
    pub(crate) fn lock(&mut self, lock: bool) {
        self.binding.set_locked(lock);
        if lock {
            self.binding.dirty();
        }
    }

    /// Check if any held server connection is currently locked to a client.
    #[cfg(test)]
    pub(crate) fn locked(&self) -> bool {
        self.binding.is_locked()
    }

    /// Get connected servers addresses.
    pub(crate) fn addr(&self) -> Result<Vec<&Address>, Error> {
        Ok(match self.binding {
            Binding::Direct(ref server, ..) => vec![server.addr()],
            Binding::MultiShard(ref servers) => servers.iter().map(|s| s.addr()).collect(),
            _ => {
                return Err(Error::NotConnected);
            }
        })
    }

    /// Cancel the query the server(s) are running for this client
    pub(crate) async fn cancel_query(&self) -> Result<(), Error> {
        let servers: Vec<&Guard> = match self.binding {
            Binding::Direct(ref server, ..) => vec![server],
            Binding::MultiShard(ref servers) => servers.iter().collect(),
            _ => return Ok(()),
        };

        try_join_all(
            servers
                .iter()
                .map(|server| Server::cancel(server.addr(), server.key().clone())),
        )
        .await?;

        Ok(())
    }

    /// Token cancelled when an admin terminates this connection's `Cluster`.
    pub(crate) fn cancellation_token(&self) -> CancellationToken {
        self.cluster.query_cancellation_token()
    }

    /// Get cluster if any.
    #[inline]
    pub(crate) fn cluster(&self) -> Result<&Cluster, Error> {
        self.cluster.cluster()
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
