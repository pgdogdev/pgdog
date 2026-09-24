use std::{
    ops::{Deref, DerefMut},
    time::Duration,
};

use futures::future::join_all;
use tracing::debug;

use crate::{
    backend::Error,
    frontend::{
        ClientRequest,
        client::query_engine::{
            TwoPcPhase,
            two_pc::{TwoPcTransaction, statement::phase_control},
        },
        router::{CopyRow, Route, parser::Shard},
    },
    net::{FrontendPid, Message, ProtocolMessage, Query, parameter::Parameters},
    state::State,
    util::safe_sleep,
};

use super::super::{Guard, multi_shard::MultiShard};

#[derive(Debug)]
pub(crate) struct BindingMulti {
    conns: Vec<Guard>,
    state: MultiShard,
}

impl Deref for BindingMulti {
    type Target = MultiShard;

    fn deref(&self) -> &Self::Target {
        &self.state
    }
}

impl DerefMut for BindingMulti {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.state
    }
}

impl BindingMulti {
    pub(crate) fn new(conns: Vec<Guard>, route: &Route) -> Self {
        let state = MultiShard::new(conns.len(), route);
        Self { conns, state }
    }

    pub(in super::super::super::connection) fn servers(&self) -> &[Guard] {
        &self.conns
    }

    pub(crate) fn disconnect(&mut self) {
        self.conns.clear();
        self.state = MultiShard::default();
    }

    pub(crate) fn force_close(&mut self) {
        for server in &mut self.conns {
            server.force_close();
        }
        self.disconnect();
    }

    pub(crate) fn connected(&self) -> bool {
        !self.conns.is_empty()
    }

    pub(crate) fn connected_servers(&self) -> usize {
        self.conns.len()
    }

    pub(crate) async fn read(&mut self) -> Result<Message, Error> {
        if self.conns.is_empty() {
            loop {
                safe_sleep(Duration::MAX).await;
            }
        }

        loop {
            if let Some(message) = self.state.get_server_message() {
                return Ok(message);
            }

            let mut read = false;
            for server in &mut self.conns {
                if !server.has_more_messages() {
                    continue;
                }

                let message = server.read().await?;
                read = true;
                if let Some(message) = self.state.handle_server_message(message)? {
                    return Ok(message);
                }
            }

            if !read {
                break;
            }
        }

        loop {
            self.state.query_complete();
            safe_sleep(Duration::MAX).await;
        }
    }

    /// Send an entire buffer of messages to the targeted servers.
    pub(crate) async fn send(&mut self, client_request: &ClientRequest) -> Result<(), Error> {
        let mut shards_sent = self.conns.len();
        let mut futures = Vec::new();

        for server in &mut self.conns {
            // A subset of connections need not match positional shard indices.
            let shard = server.shard();
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

        for result in join_all(futures).await {
            result?;
        }

        // Sync must preserve counters and buffered CommandComplete messages.
        if client_request.is_sync_only() {
            self.state.update_shards(shards_sent);
        } else {
            self.state.update(shards_sent, client_request.route());
        }

        Ok(())
    }

    /// Ignore an extended protocol response only on the upcoming request's shards.
    pub(crate) async fn send_ignore(
        &mut self,
        message: &ProtocolMessage,
        route: &Route,
    ) -> Result<(), Error> {
        let mut futures = Vec::new();
        for server in &mut self.conns {
            let shard = server.shard();
            let send = match route.shard() {
                Shard::Direct(s) => *s == shard,
                Shard::Multi(shards) => shards.contains(&shard),
                Shard::All => true,
            };
            if send {
                futures.push(server.send_ignore(message));
            }
        }

        for result in join_all(futures).await {
            result?;
        }

        Ok(())
    }

    /// Send copy messages to their destination shards.
    pub(crate) async fn send_copy(&mut self, rows: Vec<CopyRow>) -> Result<(), Error> {
        for row in rows {
            for server in &mut self.conns {
                let shard = server.shard();
                let send = match row.shard() {
                    Shard::Direct(row_shard) => shard == *row_shard,
                    Shard::Multi(shards) => shards.contains(&shard),
                    Shard::All => true,
                };
                if send {
                    server
                        .send_one(&ProtocolMessage::from(row.message()))
                        .await?;
                }
            }
        }

        Ok(())
    }

    pub(in super::super::super::connection) fn done(&self) -> bool {
        self.conns.iter().all(|server| server.done())
    }

    pub(crate) fn has_more_messages(&self) -> bool {
        self.state.has_more_messages() || self.conns.iter().any(|server| server.has_more_messages())
    }

    /// Protocol is out of sync due to an error in extended protocol.
    pub(crate) fn out_of_sync(&self) -> bool {
        self.conns.iter().any(|server| server.out_of_sync())
    }

    pub(in super::super::super::connection) fn state_check(&self, state: State) -> bool {
        self.conns.iter().all(|server| {
            debug!(
                "server is in \"{}\" state [{}]",
                server.stats().get_state(),
                server.addr()
            );
            server.stats().get_state() == state
        })
    }

    /// Execute a query on all servers.
    pub(crate) async fn execute(
        &mut self,
        query: impl Into<Query> + Clone,
    ) -> Result<Vec<Message>, Error> {
        let query: Query = query.into();
        let futures = self
            .conns
            .iter_mut()
            .map(|server| server.execute(query.clone()));
        let mut messages = Vec::new();
        for result in join_all(futures).await {
            messages.extend(result?);
        }
        Ok(messages)
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
        Self::two_pc_on_guards(&mut self.conns, transaction, phase, ignore_missing).await
    }

    /// Link the client to every server, returning the maximum parameters synced.
    pub(crate) async fn link_client(
        &mut self,
        id: FrontendPid,
        params: &Parameters,
        transaction_start_stmt: Option<&str>,
    ) -> Result<usize, Error> {
        let futures = self
            .conns
            .iter_mut()
            .map(|server| server.link_client(id, params, transaction_start_stmt));
        let mut max = 0;
        for result in join_all(futures).await {
            max = max.max(result?);
        }
        Ok(max)
    }

    /// Handle transaction end on every server.
    pub(crate) fn transaction_params_hook(&mut self, rollback: bool) {
        for server in &mut self.conns {
            server.transaction_params_hook(rollback);
        }
    }

    pub(crate) fn changed_params(&mut self) -> Parameters {
        self.conns
            .first()
            .map(|server| server.changed_params().clone())
            .unwrap_or_default()
    }

    pub(in super::super::super::connection) fn mark_dirty(&mut self) {
        for server in &mut self.conns {
            server.mark_dirty(true);
        }
    }

    /// Propagate the client's lock state to every held guard.
    pub(in super::super::super::connection) fn set_locked(&mut self, locked: bool) {
        for server in &mut self.conns {
            server.set_locked(locked);
        }
    }

    pub(in super::super::super::connection) fn is_locked(&self) -> bool {
        debug_assert!(
            self.conns.iter().all(|server| server.is_locked())
                == self.conns.iter().any(|server| server.is_locked()),
            "shards disagree on lock status {:?}",
            self.conns
        );

        self.conns.iter().any(|server| server.is_locked())
    }

    pub(crate) fn is_multishard(&self) -> bool {
        !self.conns.is_empty()
    }

    pub(crate) fn direct_shard_number(&self) -> Option<usize> {
        None
    }

    pub(crate) fn in_copy_mode(&self) -> bool {
        self.conns.iter().all(|server| server.in_copy_mode())
    }

    /// Number of connected shards.
    pub(crate) fn shards(&self) -> Result<usize, Error> {
        if self.conns.is_empty() {
            Err(Error::MultiShardNotConnected)
        } else {
            Ok(self.conns.len())
        }
    }
}
