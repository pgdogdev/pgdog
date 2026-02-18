//! Binding between frontend client and a connection on the backend.

use crate::{
    frontend::{client::query_engine::TwoPcPhase, ClientRequest},
    net::{parameter::Parameters, BackendKeyData, ProtocolMessage, Query},
    state::State,
};

use futures::future::join_all;

use super::*;

/// The server(s) the client is connected to.
#[derive(Debug)]
pub enum Binding {
    /// Direct-to-shard transaction.
    Direct(Option<Guard>),
    /// Admin database connection.
    Admin(AdminServer),
    /// Multi-shard transaction.
    MultiShard(Vec<Guard>, Box<MultiShard>),
}

impl Default for Binding {
    fn default() -> Self {
        Binding::Direct(None)
    }
}

impl Binding {
    /// Close all connections to all servers.
    pub fn disconnect(&mut self) {
        match self {
            Binding::Direct(guard) => drop(guard.take()),
            Binding::Admin(_) => (),
            Binding::MultiShard(guards, _) => guards.clear(),
        }
    }

    /// Close connections and indicate to servers that
    /// they are probably broken and should not be re-used.
    pub fn force_close(&mut self) {
        match self {
            Binding::Direct(Some(ref mut guard)) => guard.stats_mut().state(State::ForceClose),
            Binding::MultiShard(ref mut guards, _) => {
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
            Binding::Direct(server) => server.is_some(),
            Binding::MultiShard(servers, _) => !servers.is_empty(),
            Binding::Admin(_) => true,
        }
    }

    pub(super) async fn read(&mut self) -> Result<Message, Error> {
        match self {
            Binding::Direct(guard) => {
                if let Some(guard) = guard.as_mut() {
                    guard.read().await
                } else {
                    loop {
                        debug!("binding suspended");
                        sleep(Duration::MAX).await
                    }
                }
            }

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

            Binding::Direct(server) => {
                if let Some(server) = server {
                    server.send(client_request).await
                } else {
                    Err(Error::DirectToShardNotConnected)
                }
            }

            Binding::MultiShard(servers, state) => {
                let mut shards_sent = servers.len();
                let mut futures = Vec::new();

                for (shard, server) in servers.iter_mut().enumerate() {
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

    /// Send copy messages to shards they are destined to go.
    pub async fn send_copy(&mut self, rows: Vec<CopyRow>) -> Result<(), Error> {
        match self {
            Binding::MultiShard(servers, _state) => {
                for row in rows {
                    for (shard, server) in servers.iter_mut().enumerate() {
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

            Binding::Direct(Some(ref mut server)) => {
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
            Binding::Direct(Some(server)) => server.done(),
            Binding::MultiShard(servers, _state) => servers.iter().all(|s| s.done()),
            _ => true,
        }
    }

    pub fn has_more_messages(&self) -> bool {
        match self {
            Binding::Admin(admin) => !admin.done(),
            Binding::Direct(Some(server)) => server.has_more_messages(),
            Binding::MultiShard(servers, _state) => servers.iter().any(|s| s.has_more_messages()),
            _ => false,
        }
    }

    /// Protocol is out of sync due to an error in extended protocol.
    pub fn out_of_sync(&self) -> bool {
        match self {
            Binding::Direct(Some(server)) => server.out_of_sync(),
            Binding::MultiShard(servers, _state) => servers.iter().any(|s| s.out_of_sync()),
            _ => false,
        }
    }

    pub(super) fn state_check(&self, state: State) -> bool {
        match self {
            Binding::Direct(Some(server)) => {
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
            Binding::Direct(Some(ref mut server)) => {
                result.extend(server.execute(query).await?);
            }

            Binding::MultiShard(ref mut servers, _) => {
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
        name: &str,
        phase: TwoPcPhase,
    ) -> Result<(), Error> {
        let skip_missing = matches!(phase, TwoPcPhase::Phase2 | TwoPcPhase::Rollback);

        let mut futures = Vec::new();
        for (shard, server) in servers.iter_mut().enumerate() {
            let shard_name = format!("{}_{}", name, shard);

            let query = match phase {
                TwoPcPhase::Phase1 => format!("PREPARE TRANSACTION '{}'", shard_name),
                TwoPcPhase::Phase2 => format!("COMMIT PREPARED '{}'", shard_name),
                TwoPcPhase::Rollback => format!("ROLLBACK PREPARED '{}'", shard_name),
            };

            futures.push(server.execute(query));
        }

        let results = join_all(futures).await;

        for (shard, result) in results.into_iter().enumerate() {
            match result {
                Err(Error::ExecutionError(err)) => {
                    if !(skip_missing && err.code == "42704") {
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
    pub async fn two_pc(&mut self, name: &str, phase: TwoPcPhase) -> Result<(), Error> {
        match self {
            Binding::MultiShard(ref mut servers, _) => {
                Self::two_pc_on_guards(servers, name, phase).await
            }

            _ => Err(Error::TwoPcMultiShardOnly),
        }
    }

    /// Link client to server.
    pub async fn link_client(
        &mut self,
        id: &BackendKeyData,
        params: &Parameters,
        transaction_start_stmt: Option<&str>,
    ) -> Result<usize, Error> {
        match self {
            Binding::Direct(Some(ref mut server)) => {
                server.link_client(id, params, transaction_start_stmt).await
            }
            Binding::MultiShard(ref mut servers, _) => {
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
            Binding::Direct(Some(ref mut server)) => server.transaction_params_hook(rollback),
            Binding::MultiShard(ref mut servers, _) => servers
                .iter_mut()
                .for_each(|server| server.transaction_params_hook(rollback)),
            _ => (),
        }
    }

    pub fn changed_params(&mut self) -> Parameters {
        match self {
            Binding::Direct(Some(ref mut server)) => server.changed_params().clone(),
            Binding::MultiShard(ref mut servers, _) => {
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
            Binding::Direct(Some(ref mut server)) => server.reset_changed_params(),
            Binding::MultiShard(ref mut servers, _) => servers
                .iter_mut()
                .for_each(|server| server.reset_changed_params()),
            _ => (),
        }
    }

    pub(super) fn dirty(&mut self) {
        match self {
            Binding::Direct(Some(ref mut server)) => server.mark_dirty(true),
            Binding::MultiShard(ref mut servers, _state) => {
                servers.iter_mut().for_each(|s| s.mark_dirty(true))
            }
            _ => (),
        }
    }

    #[cfg(test)]
    pub fn is_dirty(&self) -> bool {
        match self {
            Binding::Direct(Some(ref server)) => server.dirty(),
            Binding::MultiShard(ref servers, _state) => servers.iter().any(|s| s.dirty()),
            _ => false,
        }
    }

    pub fn is_multishard(&self) -> bool {
        match self {
            Binding::MultiShard(ref servers, _) => !servers.is_empty(),
            _ => false,
        }
    }

    pub fn is_direct(&self) -> bool {
        matches!(self, Binding::Direct(Some(_)))
    }

    pub fn in_copy_mode(&self) -> bool {
        match self {
            Binding::Admin(_) => false,
            Binding::MultiShard(ref servers, _state) => servers.iter().all(|s| s.in_copy_mode()),
            Binding::Direct(Some(ref server)) => server.in_copy_mode(),
            _ => false,
        }
    }

    /// Number of connected shards.
    pub fn shards(&self) -> Result<usize, Error> {
        Ok(match self {
            Binding::Admin(_) => 1,
            Binding::Direct(Some(_)) => 1,
            Binding::MultiShard(ref servers, _) => {
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
