//! Binding between frontend client and a connection on the backend.

use crate::{
    frontend::{
        ClientRequest,
        client::query_engine::{TwoPcPhase, two_pc::TwoPcTransaction},
    },
    net::{FrontendPid, ProtocolMessage, Query, parameter::Parameters},
    state::State,
};

use futures::future::join_all;

use super::*;
use crate::util::safe_sleep;
use multi_shard::MultiBinding;

/// The server(s) the client is connected to.
#[derive(Debug, Default)]
pub(crate) enum Binding {
    /// Transaction binding: BEGIN.
    Transaction(TransactionBinding),
    /// Direct-to-shard transaction.
    Direct(DirectBinding),
    /// Admin database connection.
    Admin(AdminServer),
    /// Multi-shard transaction.
    MultiShard(MultiBinding),
    /// Not connected.
    #[default]
    NotConnected,
}

impl Binding {
    /// Close all connections to all servers.
    pub(crate) fn disconnect(&mut self) {
        match self {
            Self::Admin(_) => (),
            _ => {
                *self = Binding::NotConnected;
            }
        }
    }

    /// Close connections and indicate to servers that
    /// they are probably broken and should not be re-used.
    pub(crate) fn force_close(&mut self) {
        match self {
            Binding::Direct(guard) => guard.stats_mut().state(State::ForceClose),
            Binding::MultiShard(servers) => {
                for guard in servers.iter_mut() {
                    guard.stats_mut().state(State::ForceClose);
                }
            }
            _ => (),
        }

        self.disconnect();
    }

    /// Are we connected to a backend?
    pub(crate) fn connected(&self) -> bool {
        match self {
            Binding::Direct(_) => true,
            Binding::MultiShard(servers) => !servers.is_empty(),
            Binding::Admin(_) => true,
            Binding::NotConnected => false,
            Binding::Transaction(_) => false,
        }
    }

    /// Number of PostgreSQL servers we are connected to.
    ///
    /// For direct-to-shard queries, that'll be 1. For cross-shard queries,
    /// that should be how many shards are configured, since we connect to all
    /// shards (no lazy shard loading yet).
    ///
    /// If we're not connected, e.g. [`Self::connected`] is false, then this returns 0.
    ///
    pub(crate) fn connected_servers(&self) -> usize {
        match self {
            Binding::Direct(_) => 1,
            Binding::MultiShard(servers) => servers.len(),
            Binding::Admin(_) => 1,
            _ => 0,
        }
    }

    pub(super) async fn read(&mut self) -> Result<Message, Error> {
        match self {
            Binding::Direct(guard) => guard.read().await,

            Binding::NotConnected | Binding::Transaction(_) => loop {
                safe_sleep(Duration::MAX).await
            },

            Binding::Admin(backend) => Ok(backend.read().await?),
            Binding::MultiShard(servers) => {
                if servers.is_empty() {
                    loop {
                        safe_sleep(Duration::MAX).await;
                    }
                } else {
                    if let Some(message) = servers.read().await? {
                        return Ok(message);
                    }

                    loop {
                        servers.query_complete();
                        safe_sleep(Duration::MAX).await;
                    }
                }
            }
        }
    }

    /// Send an entire buffer of messages to the servers(s).
    pub(crate) async fn send(&mut self, client_request: &ClientRequest) -> Result<(), Error> {
        match self {
            Binding::Admin(backend) => Ok(backend.send(client_request).await?),
            Binding::Direct(server) => server.send(client_request).await,
            Binding::NotConnected | Binding::Transaction(_) => Err(Error::NotConnected),
            Binding::MultiShard(servers) => servers.send(client_request).await,
        }
    }

    /// Send one message to the server(s) the upcoming request targets and
    /// ignore the response.
    ///
    /// This is only supported for extended protocol messages which usually
    /// have only one reply. The route must match the route of the request
    /// that follows — sending to extra shards leaves them with a dangling
    /// Ignore expectation that blocks the multi-shard read loop.
    pub(crate) async fn send_ignore(
        &mut self,
        message: &ProtocolMessage,
        route: &Route,
    ) -> Result<(), Error> {
        match self {
            Binding::Direct(server) => server.send_ignore(message).await,
            Binding::MultiShard(servers) => servers.send_ignore(message, route).await,
            _ => Err(Error::NotConnected),
        }
    }

    /// Send copy messages to shards they are destined to go.
    pub(crate) async fn send_copy(&mut self, rows: Vec<CopyRow>) -> Result<(), Error> {
        match self {
            Binding::MultiShard(servers) => servers.send_copy(rows).await,
            Binding::Direct(server, ..) => {
                for row in rows {
                    server
                        .send_one(&ProtocolMessage::from(row.message()))
                        .await?;
                }

                Ok(())
            }
            _ => Err(Error::CopyNotConnected),
        }
    }

    pub(super) fn done(&self) -> bool {
        match self {
            Binding::Admin(admin) => admin.done(),
            Binding::Direct(server) => server.done(),
            Binding::MultiShard(servers) => servers.iter().all(|s| s.done()),
            Binding::Transaction(_) => false,
            _ => true,
        }
    }

    pub(crate) fn has_more_messages(&self) -> bool {
        match self {
            Binding::Admin(admin) => !admin.done(),
            Binding::Direct(server) => server.has_more_messages(),
            Binding::MultiShard(servers) => servers.has_more_messages(),
            _ => false,
        }
    }

    /// Protocol is out of sync due to an error in extended protocol.
    pub(crate) fn out_of_sync(&self) -> bool {
        match self {
            Binding::Direct(server) => server.out_of_sync(),
            Binding::MultiShard(servers) => servers.iter().any(|s| s.out_of_sync()),
            _ => false,
        }
    }

    /// Execute a query on all servers.
    pub(crate) async fn execute(
        &mut self,
        query: impl Into<Query> + Clone,
    ) -> Result<Vec<Message>, Error> {
        let query: Query = query.into();
        let mut result = vec![];
        match self {
            Binding::Direct(server) => {
                result.extend(server.execute(query).await?);
            }

            Binding::MultiShard(servers) => {
                let futures = servers
                    .iter_mut()
                    .map(|server| server.execute(query.clone()));
                let results = join_all(futures).await;

                for server_result in results {
                    result.extend(server_result?);
                }
            }

            _ => (),
        }

        Ok(result)
    }

    /// Execute two-phase commit transaction control statements.
    pub(crate) async fn two_pc(
        &mut self,
        transaction: TwoPcTransaction,
        phase: TwoPcPhase,
        ignore_missing: bool,
    ) -> Result<(), Error> {
        match self {
            Binding::MultiShard(servers) => {
                servers.two_pc(transaction, phase, ignore_missing).await
            }

            _ => Err(Error::TwoPcMultiShardOnly),
        }
    }

    /// Link client to server.
    pub(crate) async fn link_client(
        &mut self,
        id: FrontendPid,
        params: &Parameters,
    ) -> Result<usize, Error> {
        match self {
            Binding::Direct(server, ..) => server.link_client(id, params).await,
            Binding::MultiShard(servers) => Ok(servers.link_client(id, params).await?),
            _ => Ok(0),
        }
    }

    /// Handle transaction end.
    pub(crate) fn transaction_params_hook(&mut self, rollback: bool) {
        match self {
            Binding::Direct(server, ..) => server.transaction_params_hook(rollback),
            Binding::MultiShard(servers) => servers
                .iter_mut()
                .for_each(|server| server.transaction_params_hook(rollback)),
            _ => (),
        }
    }

    pub(crate) fn changed_params(&mut self) -> Parameters {
        match self {
            Binding::Direct(server, ..) => server.changed_params().clone(),
            Binding::MultiShard(servers) => {
                if let Some(first) = servers.iter().next() {
                    first.changed_params().clone()
                } else {
                    Parameters::default()
                }
            }
            _ => Parameters::default(),
        }
    }

    pub(super) fn dirty(&mut self) {
        match self {
            Binding::Direct(server, ..) => server.mark_dirty(true),
            Binding::MultiShard(servers) => servers.iter_mut().for_each(|s| s.mark_dirty(true)),
            _ => (),
        }
    }

    /// Propagate the client's lock state to every held Guard so each pool's
    /// `sv_locked` reflects the pin.
    pub(super) fn set_locked(&mut self, locked: bool) {
        match self {
            Binding::Direct(server, ..) => server.set_locked(locked),
            Binding::MultiShard(servers) => {
                for server in servers.iter_mut() {
                    server.set_locked(locked);
                }
            }
            _ => (),
        }
    }

    /// Aggregate lock state across the held Guard(s). All shards in a
    /// multi-shard binding are set/cleared together via [`Self::set_locked`],
    /// so they should always agree; if they don't, warn and err on the side
    /// of "locked" so we don't recycle a pinned connection.
    pub(super) fn is_locked(&self) -> bool {
        match self {
            Binding::Direct(server, ..) => server.is_locked(),
            Binding::MultiShard(servers) => {
                debug_assert!(
                    servers.iter().all(|s| s.is_locked()) == servers.iter().any(|s| s.is_locked()),
                    "Shards disagree on lock status {servers:?}"
                );

                servers.iter().any(|s| s.is_locked())
            }
            _ => false,
        }
    }

    pub(crate) fn is_multishard(&self) -> bool {
        match self {
            Binding::MultiShard(servers) => !servers.is_empty(),
            _ => false,
        }
    }

    pub(crate) fn in_copy_mode(&self) -> bool {
        match self {
            Binding::Admin(_) => false,
            Binding::MultiShard(servers) => servers.iter().all(|s| s.in_copy_mode()),
            Binding::Direct(server) => server.in_copy_mode(),
            _ => false,
        }
    }
}
