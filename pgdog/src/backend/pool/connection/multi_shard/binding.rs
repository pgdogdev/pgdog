use std::ops::{Deref, DerefMut};
use std::slice::{Iter, IterMut};

use futures::future::join_all;

use crate::backend::Error;
use crate::backend::pool::Request;
use crate::frontend::ClientRequest;
use crate::frontend::client::query_engine::{
    TwoPcPhase, TwoPcTransaction, statement::phase_control,
};
use crate::frontend::router::parser::Shard;
use crate::frontend::router::{CopyRow, Route};
use crate::net::{FrontendPid, Message, Parameters, ProtocolMessage};

use super::super::Guard;
use super::MultiShard;

#[derive(Debug)]
pub(crate) struct LinkedServer {
    server: Guard,
    // Shard number.
    shard: usize,
    // Parameters were sync'ed.
    linked: bool,
}

impl LinkedServer {
    pub(crate) fn shard(&self) -> usize {
        self.shard
    }
}

impl Deref for LinkedServer {
    type Target = Guard;

    fn deref(&self) -> &Self::Target {
        &self.server
    }
}

impl DerefMut for LinkedServer {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.server
    }
}

/// Handle talking to multiple servers for cross-shard queries.
#[derive(Debug)]
pub(crate) struct MultiBinding {
    servers: Vec<LinkedServer>,
    state: Box<MultiShard>,
}

impl MultiBinding {
    /// Iterate over the connected servers.
    pub(crate) fn iter(&self) -> Iter<'_, LinkedServer> {
        self.servers.iter()
    }

    /// Mutably iterate over the connected servers.
    pub(crate) fn iter_mut(&mut self) -> IterMut<'_, LinkedServer> {
        self.servers.iter_mut()
    }

    /// Returns true when binding is not connected to any servers.
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Number of connected servers.
    pub(crate) fn len(&self) -> usize {
        self.servers.len()
    }

    /// Create new multi-shard binding.
    pub(crate) fn new(servers: Vec<Guard>, shard_indices: Vec<usize>, route: &Route) -> Self {
        Self {
            state: Box::new(MultiShard::new(servers.len(), route)),
            servers: servers
                .into_iter()
                .zip(shard_indices.into_iter())
                .map(|(server, shard)| LinkedServer {
                    server,
                    shard,
                    linked: false,
                })
                .collect(),
        }
    }

    pub(crate) fn required_shards_connected(&self, route: &Route, shards: usize) -> bool {
        match route.shard() {
            Shard::Direct(shard) => self
                .servers
                .iter()
                .find(|server| server.shard == *shard)
                .is_some(),
            Shard::All => self.servers.len() == shards,
            Shard::Multi(shards) => shards
                .iter()
                .all(|shard| self.servers.iter().any(|server| server.shard == *shard)),
        }
    }

    #[allow(unused)]
    pub(crate) async fn ensure_connected(
        self,
        request: &Request,
        route: &Route,
    ) -> Result<Self, super::Error> {
        Ok(self)
    }

    pub(crate) async fn link_client(
        &mut self,
        client_id: FrontendPid,
        params: &Parameters,
        transaction_start_stmt: Option<&str>,
    ) -> Result<usize, Error> {
        let futures = self
            .servers
            .iter_mut()
            .filter(|server| !server.linked)
            .map(|server| server.link_client(client_id, params, transaction_start_stmt));
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

    /// Read-only handle to internal state.
    pub(crate) fn state(&self) -> &MultiShard {
        &self.state
    }

    /// Write-handle to internal state.
    ///
    /// BUG: This should not exist.
    pub(crate) fn state_mut(&mut self) -> &mut MultiShard {
        &mut self.state
    }

    /// Read 1 message from one of the shards.
    pub(crate) async fn read(&mut self) -> Result<Option<Message>, Error> {
        loop {
            // Return all sorted data rows if any.
            if let Some(message) = self.state.get_server_message() {
                return Ok(Some(message));
            }

            let mut read = false;
            for server in &mut self.servers {
                if !server.has_more_messages() {
                    continue;
                }

                let message = server.read().await?;
                read = true;

                if let Some(message) = self.state.handle_server_message(message)? {
                    return Ok(Some(message));
                }
            }

            if !read {
                break;
            }
        }

        Ok(None)
    }

    /// Send client request to the shards it should go to.
    pub(crate) async fn send(&mut self, client_request: &ClientRequest) -> Result<(), Error> {
        let mut shards_sent = self.servers.len();
        let mut futures = Vec::new();

        for server in self.servers.iter_mut() {
            // Map positional index to actual shard number.
            // When only a subset of shards is connected (Shard::Multi binding),
            // positional indices don't match actual shard numbers.
            let shard = server.shard;
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
            self.state.update_shards(shards_sent);
        } else {
            self.state.update(shards_sent, client_request.route());
        }

        Ok(())
    }

    /// Send a message the reply for which we will ignore to the shard(s) it needs to go to.
    pub(crate) async fn send_ignore(
        &mut self,
        message: &ProtocolMessage,
        route: &Route,
    ) -> Result<(), Error> {
        if self.servers.is_empty() {
            return Ok(());
        }

        let mut futures = Vec::new();
        for server in self.servers.iter_mut() {
            let shard = server.shard;
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

        Ok(())
    }

    /// Send COPY rows to all shards.
    pub(crate) async fn send_copy(&mut self, rows: Vec<CopyRow>) -> Result<(), Error> {
        for row in rows {
            for server in self.servers.iter_mut() {
                let shard = server.shard;
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

    /// Execute a 2pc exchange on all shards, given the 2pc transaction phase.
    pub(crate) async fn two_pc(
        &mut self,
        transaction: TwoPcTransaction,
        phase: TwoPcPhase,
        ignore_missing: bool,
    ) -> Result<(), Error> {
        let mut futures = Vec::new();
        for (shard, server) in self.servers.iter_mut().enumerate() {
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
                        self.servers[shard].stats_mut().transaction_2pc();
                    }
                }
            }
        }

        Ok(())
    }
}
