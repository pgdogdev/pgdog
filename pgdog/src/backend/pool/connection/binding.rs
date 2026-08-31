//! Binding between frontend client and a connection on the backend.

use crate::{
    frontend::{
        ClientRequest,
        client::query_engine::{
            TwoPcPhase,
            two_pc::{TwoPcTransaction, statement::phase_control},
        },
    },
    net::{FrontendPid, ProtocolMessage, Query, parameter::Parameters},
    state::State,
};

use futures::future::join_all;

use super::*;
use crate::util::safe_sleep;

/// The server(s) the client is connected to.
#[derive(Debug, Default)]
pub(crate) enum Binding {
    /// Direct-to-shard transaction.
    Direct(Guard, usize),
    /// Admin database connection.
    Admin(AdminServer),
    /// Multi-shard transaction.
    MultiShard(Vec<Guard>, Box<MultiShard>),
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
            Binding::Direct(guard, _) => guard.stats_mut().state(State::ForceClose),
            Binding::MultiShard(guards, _) => {
                for guard in guards {
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
            Binding::Direct(_, _) => true,
            Binding::MultiShard(servers, _) => !servers.is_empty(),
            Binding::Admin(_) => true,
            Binding::NotConnected => false,
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
            Binding::Direct(_, _) => 1,
            Binding::MultiShard(servers, _) => servers.len(),
            Binding::Admin(_) => 1,
            _ => 0,
        }
    }

    /// Record a `client_idle_in_transaction_timeout` disconnect against
    /// every pool this client currently holds a checkout from.
    pub(crate) fn record_client_idle_xact_timeout(&self) {
        match self {
            Binding::Direct(guard, _) => guard.record_client_idle_xact_timeout(),
            Binding::MultiShard(guards, _) => {
                for guard in guards {
                    guard.record_client_idle_xact_timeout();
                }
            }
            Binding::Admin(_) | Binding::NotConnected => (),
        }
    }

    pub(super) async fn read(&mut self) -> Result<Message, Error> {
        match self {
            Binding::Direct(guard, _) => guard.read().await,

            Binding::NotConnected => loop {
                safe_sleep(Duration::MAX).await
            },

            Binding::Admin(backend) => Ok(backend.read().await?),
            Binding::MultiShard(shards, state) => {
                if shards.is_empty() {
                    loop {
                        safe_sleep(Duration::MAX).await;
                    }
                } else {
                    // Loop until we read a message from a shard
                    // or there are no more messages to be read.
                    loop {
                        // Return all sorted data rows if any.
                        if let Some(message) = state.get_server_message() {
                            return Ok(message);
                        }
                        let mut read = false;
                        for server in shards.iter_mut() {
                            if !server.has_more_messages() {
                                continue;
                            }

                            let message = server.read().await?;

                            read = true;
                            if let Some(message) = state.handle_server_message(message)? {
                                return Ok(message);
                            }
                        }

                        if !read {
                            break;
                        }
                    }

                    loop {
                        state.query_complete();
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

            Binding::Direct(server, _) => server.send(client_request).await,

            Binding::NotConnected => Err(Error::NotConnected),

            Binding::MultiShard(servers, state) => {
                let mut shards_sent = servers.len();
                let mut futures = Vec::new();

                for (position, server) in servers.iter_mut().enumerate() {
                    // Map positional index to actual shard number.
                    // When only a subset of shards is connected (Shard::Multi binding),
                    // positional indices don't match actual shard numbers.
                    let shard = state.shard_number(position);
                    let send = match client_request.route().shard() {
                        Shard::Direct(s) => {
                            shards_sent = 1;
                            *s == shard
                        }
                        Shard::Multi(shards) => {
                            shards_sent = shards.len();
                            shards.contains(&shard)
                        }
                        Shard::All => true,
                    };

                    if send {
                        futures.push(server.send(client_request));
                    }
                }

                let results = join_all(futures).await;

                for result in results {
                    result?;
                }

                // For Sync-only requests, update shards count but don't reset counters.
                // Sync needs correct shards for ReadyForQuery counting, but we must
                // preserve buffered CommandComplete from previous queries.
                if client_request.is_sync_only() {
                    state.update_shards(shards_sent);
                } else {
                    state.update(shards_sent, client_request.route());
                }

                Ok(())
            }
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
            Binding::Direct(server, ..) => {
                server.send_ignore(message).await?;
            }
            Binding::MultiShard(servers, state) => {
                if !servers.is_empty() {
                    let mut futures = Vec::new();
                    for (position, server) in servers.iter_mut().enumerate() {
                        let shard = state.shard_number(position);
                        let send = match route.shard() {
                            Shard::Direct(s) => *s == shard,
                            Shard::Multi(shards) => shards.contains(&shard),
                            Shard::All => true,
                        };
                        if send {
                            futures.push(server.send_ignore(message));
                        }
                    }
                    let results = join_all(futures).await;

                    for result in results {
                        result?;
                    }
                }
            }

            _ => return Err(Error::NotConnected),
        }

        Ok(())
    }

    /// Send copy messages to shards they are destined to go.
    pub(crate) async fn send_copy(&mut self, rows: Vec<CopyRow>) -> Result<(), Error> {
        match self {
            Binding::MultiShard(servers, state) => {
                for row in rows {
                    for (position, server) in servers.iter_mut().enumerate() {
                        let shard = state.shard_number(position);
                        match row.shard() {
                            Shard::Direct(row_shard) => {
                                if shard == *row_shard {
                                    server
                                        .send_one(&ProtocolMessage::from(row.message()))
                                        .await?;
                                }
                            }

                            Shard::All => {
                                server
                                    .send_one(&ProtocolMessage::from(row.message()))
                                    .await?;
                            }

                            Shard::Multi(multi) => {
                                if multi.contains(&shard) {
                                    server
                                        .send_one(&ProtocolMessage::from(row.message()))
                                        .await?;
                                }
                            }
                        }
                    }
                }
                Ok(())
            }

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
            Binding::Direct(server, ..) => server.done(),
            Binding::MultiShard(servers, _state) => servers.iter().all(|s| s.done()),
            _ => true,
        }
    }

    pub(crate) fn has_more_messages(&self) -> bool {
        match self {
            Binding::Admin(admin) => !admin.done(),
            Binding::Direct(server, ..) => server.has_more_messages(),
            Binding::MultiShard(servers, state) => {
                state.has_more_messages() || servers.iter().any(|s| s.has_more_messages())
            }
            _ => false,
        }
    }

    /// Protocol is out of sync due to an error in extended protocol.
    pub(crate) fn out_of_sync(&self) -> bool {
        match self {
            Binding::Direct(server, ..) => server.out_of_sync(),
            Binding::MultiShard(servers, _state) => servers.iter().any(|s| s.out_of_sync()),
            _ => false,
        }
    }

    pub(super) fn state_check(&self, state: State) -> bool {
        match self {
            Binding::Direct(server, ..) => {
                debug!(
                    "server is in \"{}\" state [{}]",
                    server.stats().get_state(),
                    server.addr()
                );
                server.stats().get_state() == state
            }
            Binding::MultiShard(servers, _) => servers.iter().all(|s| {
                debug!(
                    "server is in \"{}\" state [{}]",
                    s.stats().get_state(),
                    s.addr()
                );
                s.stats().get_state() == state
            }),
            _ => true,
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
            Binding::Direct(server, ..) => {
                result.extend(server.execute(query).await?);
            }

            Binding::MultiShard(servers, _) => {
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

    pub(crate) async fn two_pc_on_guards(
        servers: &mut [Guard],
        transaction: TwoPcTransaction,
        phase: TwoPcPhase,
        ignore_missing: bool,
    ) -> Result<(), Error> {
        let mut futures = Vec::new();
        for (shard, server) in servers.iter_mut().enumerate() {
            let query = phase_control(transaction, shard, phase);
            futures.push(server.execute(query));
        }

        let results = join_all(futures).await;

        for (shard, result) in results.into_iter().enumerate() {
            match result {
                Err(Error::ExecutionError(err)) => {
                    if !(ignore_missing && err.code == "42704") {
                        return Err(Error::ExecutionError(err));
                    }
                }
                Err(err) => return Err(err),
                Ok(_) => {
                    if phase == TwoPcPhase::Phase2 {
                        servers[shard].stats_mut().transaction_2pc();
                    }
                }
            }
        }

        Ok(())
    }

    /// Execute two-phase commit transaction control statements.
    pub(crate) async fn two_pc(
        &mut self,
        transaction: TwoPcTransaction,
        phase: TwoPcPhase,
        ignore_missing: bool,
    ) -> Result<(), Error> {
        match self {
            Binding::MultiShard(servers, _) => {
                Self::two_pc_on_guards(servers, transaction, phase, ignore_missing).await
            }

            _ => Err(Error::TwoPcMultiShardOnly),
        }
    }

    /// Link client to server.
    pub(crate) async fn link_client(
        &mut self,
        id: FrontendPid,
        params: &Parameters,
        transaction_start_stmt: Option<&str>,
    ) -> Result<usize, Error> {
        match self {
            Binding::Direct(server, ..) => {
                server.link_client(id, params, transaction_start_stmt).await
            }
            Binding::MultiShard(servers, _) => {
                let futures = servers
                    .iter_mut()
                    .map(|server| server.link_client(id, params, transaction_start_stmt));
                let results = join_all(futures).await;

                let mut max = 0;
                for result in results {
                    let synced = result?;
                    if max < synced {
                        max = synced;
                    }
                }
                Ok(max)
            }

            _ => Ok(0),
        }
    }

    /// Handle transaction end.
    pub(crate) fn transaction_params_hook(&mut self, rollback: bool) {
        match self {
            Binding::Direct(server, ..) => server.transaction_params_hook(rollback),
            Binding::MultiShard(servers, _) => servers
                .iter_mut()
                .for_each(|server| server.transaction_params_hook(rollback)),
            _ => (),
        }
    }

    pub(crate) fn changed_params(&mut self) -> Parameters {
        match self {
            Binding::Direct(server, ..) => server.changed_params().clone(),
            Binding::MultiShard(servers, _) => {
                if let Some(first) = servers.first() {
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
            Binding::MultiShard(servers, _state) => {
                servers.iter_mut().for_each(|s| s.mark_dirty(true))
            }
            _ => (),
        }
    }

    /// Propagate the client's lock state to every held Guard so each pool's
    /// `sv_locked` reflects the pin.
    pub(super) fn set_locked(&mut self, locked: bool) {
        match self {
            Binding::Direct(server, ..) => server.set_locked(locked),
            Binding::MultiShard(servers, _) => {
                for server in servers {
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
            Binding::MultiShard(servers, _) => {
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
            Binding::MultiShard(servers, _) => !servers.is_empty(),
            _ => false,
        }
    }

    /// If connected to one shard only, get that shard number.
    pub(crate) fn direct_shard_number(&self) -> Option<usize> {
        if let Self::Direct(_, shard) = self {
            Some(*shard)
        } else {
            None
        }
    }

    pub(crate) fn in_copy_mode(&self) -> bool {
        match self {
            Binding::Admin(_) => false,
            Binding::MultiShard(servers, _state) => servers.iter().all(|s| s.in_copy_mode()),
            Binding::Direct(server, ..) => server.in_copy_mode(),
            _ => false,
        }
    }

    /// Number of connected shards.
    pub(crate) fn shards(&self) -> Result<usize, Error> {
        Ok(match self {
            Binding::Admin(_) => 1,
            Binding::Direct(_, _) => 1,
            Binding::MultiShard(servers, _) => {
                if servers.is_empty() {
                    return Err(Error::MultiShardNotConnected);
                } else {
                    servers.len()
                }
            }
            _ => {
                return Err(Error::NotConnected);
            }
        })
    }
}
