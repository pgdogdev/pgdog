//! Binding between frontend client and a connection on the backend.

use crate::{
    frontend::{
        ClientRequest,
        client::query_engine::{
            TwoPcPhase,
            two_pc::{TwoPcTransaction, TwoPcTransactionOnShard, statement::phase_control},
        },
    },
    net::{DataRow, FrontendPid, ProtocolMessage, Query, parameter::Parameters},
    state::State,
    util::safe_identifier,
};

use futures::future::join_all;
use tracing::warn;

use super::*;

/// The server(s) the client is connected to.
#[derive(Debug, Default)]
pub enum Binding {
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
    pub fn disconnect(&mut self) {
        match self {
            Self::Admin(_) => (),
            _ => {
                *self = Binding::NotConnected;
            }
        }
    }

    /// Close connections and indicate to servers that
    /// they are probably broken and should not be re-used.
    pub fn force_close(&mut self) {
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
    pub fn connected(&self) -> bool {
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
    pub fn connected_servers(&self) -> usize {
        match self {
            Binding::Direct(_, _) => 1,
            Binding::MultiShard(servers, _) => servers.len(),
            Binding::Admin(_) => 1,
            _ => 0,
        }
    }

    pub(super) async fn read(&mut self) -> Result<Message, Error> {
        match self {
            Binding::Direct(guard, _) => guard.read().await,

            Binding::NotConnected => loop {
                debug!("binding suspended");
                sleep(Duration::MAX).await
            },

            Binding::Admin(backend) => Ok(backend.read().await?),
            Binding::MultiShard(shards, state) => {
                if shards.is_empty() {
                    loop {
                        debug!("multi-shard binding suspended");
                        sleep(Duration::MAX).await;
                    }
                } else {
                    // Loop until we read a message from a shard
                    // or there are no more messages to be read.
                    loop {
                        // Return all sorted data rows if any.
                        if let Some(message) = state.message() {
                            return Ok(message);
                        }
                        let mut read = false;
                        for server in shards.iter_mut() {
                            if !server.has_more_messages() {
                                continue;
                            }

                            let message = server.read().await?;

                            read = true;
                            if let Some(message) = state.forward(message)? {
                                return Ok(message);
                            }
                        }

                        if !read {
                            break;
                        }
                    }

                    loop {
                        state.reset();
                        debug!("multi-shard binding done");
                        sleep(Duration::MAX).await;
                    }
                }
            }
        }
    }

    /// Send an entire buffer of messages to the servers(s).
    pub async fn send(&mut self, client_request: &ClientRequest) -> Result<(), Error> {
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
                    let shard = state.shard_index(position);
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
    pub async fn send_ignore(
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
                        let shard = state.shard_index(position);
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
    pub async fn send_copy(&mut self, rows: Vec<CopyRow>) -> Result<(), Error> {
        match self {
            Binding::MultiShard(servers, state) => {
                for row in rows {
                    for (position, server) in servers.iter_mut().enumerate() {
                        let shard = state.shard_index(position);
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

    pub fn has_more_messages(&self) -> bool {
        match self {
            Binding::Admin(admin) => !admin.done(),
            Binding::Direct(server, ..) => server.has_more_messages(),
            Binding::MultiShard(servers, _state) => servers.iter().any(|s| s.has_more_messages()),
            _ => false,
        }
    }

    /// Protocol is out of sync due to an error in extended protocol.
    pub fn out_of_sync(&self) -> bool {
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
    pub async fn execute(
        &mut self,
        query: impl Into<Query> + Clone,
    ) -> Result<Vec<Message>, Error> {
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
        state: &MultiShard,
        transaction: TwoPcTransaction,
        phase: TwoPcPhase,
    ) -> Result<(), Error> {
        let skip_missing = matches!(phase, TwoPcPhase::Phase2 | TwoPcPhase::Rollback);

        let mut futures = Vec::new();
        for (position, server) in servers.iter_mut().enumerate() {
            // Map positional index to actual shard number.
            // When only a subset of shards is connected (Shard::Multi binding),
            // positional indices don't match actual shard numbers.
            let shard = state.shard_index(position);
            let query = phase_control(transaction, shard, phase);
            futures.push(server.execute(query));
        }

        let results = join_all(futures).await;

        for (position, result) in results.into_iter().enumerate() {
            match result {
                Err(Error::ExecutionError(err)) => {
                    if !(skip_missing && err.code == "42704") {
                        return Err(Error::ExecutionError(err));
                    }
                }
                Err(err) => return Err(err),
                Ok(_) => {
                    if phase == TwoPcPhase::Phase2 {
                        servers[position].stats_mut().transaction_2pc();
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
    ) -> Result<(), Error> {
        match self {
            Binding::MultiShard(servers, state) => {
                Self::two_pc_on_guards(servers, state, transaction, phase).await
            }

            _ => Err(Error::TwoPcMultiShardOnly),
        }
    }

    /// Resolve a two-phase transaction on every shard during cleanup
    /// and crash recovery.
    ///
    /// `prefix` is the coordinator GID prefix recorded when the
    /// transaction was created; combined with the transaction's numeric
    /// ID it names the exact prepared transaction on each shard, and the
    /// phase statement runs against the GIDs found in
    /// `pg_prepared_xacts`. An empty `prefix` means the transaction was
    /// restored from a WAL record that did not store it; matching then
    /// falls back to the durable numeric transaction ID and shard index.
    /// A shard with no matching GID is already resolved and is skipped.
    pub(crate) async fn two_pc_cleanup(
        &mut self,
        transaction: TwoPcTransaction,
        prefix: &str,
        phase: TwoPcPhase,
    ) -> Result<(), Error> {
        match self {
            Binding::MultiShard(servers, _) => {
                let futures = servers
                    .iter_mut()
                    .enumerate()
                    .map(|(shard, server)| async move {
                        let target = TwoPcTransactionOnShard::new(transaction, shard);
                        // A GID rendered from the recorded prefix is
                        // checked against the identifier alphabet before
                        // it is embedded in a quoted literal; a prefix
                        // outside the alphabet falls back to numeric-ID
                        // matching, which performs the same check.
                        let expected = if prefix.is_empty() {
                            None
                        } else {
                            let gid = target.gid(prefix);
                            if safe_identifier(&gid) {
                                Some(gid)
                            } else {
                                warn!(
                                    "[2pc] recorded gid {:?} contains characters \
                                     PgDog never generates; matching by numeric ID",
                                    gid
                                );
                                None
                            }
                        };
                        let rows: Vec<DataRow> = server
                            .fetch_all(
                                "SELECT gid FROM pg_prepared_xacts WHERE database = current_database()",
                            )
                            .await?;

                        for row in rows {
                            let Some(gid) = row.get_text(0) else {
                                continue;
                            };
                            let matched = match &expected {
                                Some(expected) => &gid == expected,
                                None => target.matches_gid(&gid),
                            };
                            if !matched {
                                continue;
                            }
                            // Both match paths guarantee the gid contains
                            // no characters that need quoting.
                            let statement = match phase {
                                TwoPcPhase::Phase2 => format!("COMMIT PREPARED '{gid}'"),
                                TwoPcPhase::Rollback => format!("ROLLBACK PREPARED '{gid}'"),
                                TwoPcPhase::Phase1 => {
                                    unreachable!("cleanup resolves transactions; it never prepares")
                                }
                            };
                            match server.execute(&statement[..]).await {
                                Ok(_) => {
                                    if phase == TwoPcPhase::Phase2 {
                                        server.stats_mut().transaction_2pc();
                                    }
                                }
                                // Insufficient privilege: the prepared transaction
                                // is owned by a role the configured user can't act
                                // for. Retrying can't succeed until an operator
                                // intervenes, so leave the transaction to them
                                // rather than retrying forever.
                                Err(Error::ExecutionError(err)) if err.code == "42501" => {
                                    warn!(
                                        "[2pc] insufficient privilege to run {}; \
                                         resolve the prepared transaction manually",
                                        statement
                                    );
                                }
                                Err(err) => return Err(err),
                            }
                        }

                        Ok::<(), Error>(())
                    });

                for result in join_all(futures).await {
                    result?;
                }

                Ok(())
            }

            _ => Err(Error::TwoPcMultiShardOnly),
        }
    }

    /// Link client to server.
    pub async fn link_client(
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
    pub fn transaction_params_hook(&mut self, rollback: bool) {
        match self {
            Binding::Direct(server, ..) => server.transaction_params_hook(rollback),
            Binding::MultiShard(servers, _) => servers
                .iter_mut()
                .for_each(|server| server.transaction_params_hook(rollback)),
            _ => (),
        }
    }

    pub fn changed_params(&mut self) -> Parameters {
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

    /// Reset changed params on all servers, disabling parameter tracking
    /// for this request.
    pub fn reset_changed_params(&mut self) {
        match self {
            Binding::Direct(server, ..) => server.reset_changed_params(),
            Binding::MultiShard(servers, _) => servers
                .iter_mut()
                .for_each(|server| server.reset_changed_params()),
            _ => (),
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

    pub fn is_multishard(&self) -> bool {
        match self {
            Binding::MultiShard(servers, _) => !servers.is_empty(),
            _ => false,
        }
    }

    pub fn is_direct(&self) -> bool {
        matches!(self, Binding::Direct(_, _))
    }

    /// If connected to one shard only, get that shard number.
    pub fn direct_shard_number(&self) -> Option<usize> {
        if let Self::Direct(_, shard) = self {
            Some(*shard)
        } else {
            None
        }
    }

    pub fn in_copy_mode(&self) -> bool {
        match self {
            Binding::Admin(_) => false,
            Binding::MultiShard(servers, _state) => servers.iter().all(|s| s.in_copy_mode()),
            Binding::Direct(server, ..) => server.in_copy_mode(),
            _ => false,
        }
    }

    /// Number of connected shards.
    pub fn shards(&self) -> Result<usize, Error> {
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
