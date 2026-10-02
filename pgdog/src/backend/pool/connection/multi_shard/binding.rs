use std::slice::{Iter, IterMut};

use futures::future::join_all;

use crate::backend::Error;
use crate::frontend::client::query_engine::{
    TwoPcPhase, TwoPcTransaction, statement::phase_control,
};
use crate::frontend::{
    BufferedQuery, ClientRequest,
    router::{CopyRow, Route, parser::Shard},
};
use crate::net::{Bind, FrontendPid, Message, Parameters, ProtocolMessage};

use super::{
    super::{Guard, LinkedServer},
    MultiShard,
};

/// Handle talking to multiple servers for cross-shard queries.
#[derive(Debug)]
pub(crate) struct MultiBinding {
    // Actual server connections.
    pub(super) servers: Vec<LinkedServer>,
    // Handle all the cross-shard complexity: aggregates, sorting, deduping server messages, etc.
    pub(super) state: Box<MultiShard>,
    // Transaction statement, e.g., `BEGIN`, `BEGIN READ ONLY`, etc.
    pub(in crate::backend::pool::connection) transaction_stmt: Option<BufferedQuery>,
    // Is the transaction read-only (replicas) or write (primary)?
    pub(in crate::backend::pool::connection) is_read: bool,
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

    /// Sort servers in shard number order.
    pub(super) fn sort(&mut self) {
        self.servers.sort_by_key(|server| server.shard);
    }

    /// Get currently connected shard numbers for this multi-shard binding.
    pub(crate) fn connected_shards(&self) -> impl Iterator<Item = usize> + Clone {
        self.servers.iter().map(|server| server.shard)
    }

    /// Create new multi-shard binding.
    ///
    /// # Arguments
    ///
    /// - `servers`: Postgres connections.
    /// - `shard_indices: Which server is which shard.
    /// - `route`: Statement execution plan.
    /// - `transaction_stmt`: `BEGIN`, `BEGIN READ ONLY`, etc.
    /// - `is_read`: Are we reading from a replica or writing to a primary?
    ///
    pub(crate) fn new(
        servers: Vec<Guard>,
        shard_indices: Vec<usize>,
        route: &Route,
        transaction_stmt: Option<BufferedQuery>,
        is_read: bool,
    ) -> Self {
        debug_assert_eq!(servers.len(), shard_indices.len());

        Self {
            state: Box::new(MultiShard::new(servers.len(), route)),
            servers: servers
                .into_iter()
                .zip(shard_indices)
                .map(|(server, shard)| LinkedServer {
                    server,
                    shard,
                    linked: false,
                })
                .collect(),
            transaction_stmt,
            is_read,
        }
    }

    /// Given the execution plan in `route` and the total number of configured `shards`,
    /// do we have all the necessary connections to serve this request?
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

    /// Idempotently link the client to the connected servers. This sycnrhonizes parameters and
    /// starts a transaction on each server, if necessary.
    ///
    /// Each server is handled in parallel, so this is quick.
    ///
    pub(crate) async fn link_client(
        &mut self,
        client_id: FrontendPid,
        params: &Parameters,
    ) -> Result<usize, Error> {
        let transaction_start_stmt = self.transaction_stmt.as_ref().map(|q| q.query());

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

    /// Reset cross-shard state after a query finished executing.
    pub(in crate::backend::pool::connection) fn query_complete(&mut self) {
        self.state.query_complete();
    }

    /// Handle a [`Bind`] message received from the client. In cross-shard
    /// pipelines, we need to make sure we track these to know which parameters
    /// to decode for a given statement.
    pub(in crate::backend::pool::connection) fn bind(&mut self, bind: &Bind) {
        self.state.push_bind(bind);
    }

    /// Indicates that the backend(s) have more messages for the client
    /// so it should continue to pull them from the connection (until this returns false).
    pub(in crate::backend::pool::connection) fn has_more_messages(&self) -> bool {
        self.state.has_more_messages()
            || self.servers.iter().any(|server| server.has_more_messages())
    }

    /// Read 1 message from one of the shards, in the right order.
    ///
    /// This handles everything, incl. sorting, aggregation, etc.
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

    /// Send client request to the shard(s) it should go to.
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
        for (idx, server) in self.servers.iter_mut().enumerate() {
            let query = phase_control(transaction, server.shard, phase);
            futures.push(async move {
                server.execute(query).await?;
                Ok(idx)
            });
        }

        let results = join_all(futures).await;

        for result in results.into_iter() {
            match result {
                Err(Error::ExecutionError(err)) => {
                    if !(ignore_missing && err.code == "42704") {
                        return Err(Error::ExecutionError(err));
                    }
                }
                Err(err) => return Err(err),
                Ok(idx) => {
                    if phase == TwoPcPhase::Phase2 {
                        self.servers[idx].stats_mut().transaction_2pc();
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
pub(crate) mod test {
    use std::ops::{Deref, DerefMut};

    use crate::frontend::router::parser::ShardWithPriority;
    use crate::net::{Parameter, Query};

    use super::super::super::linked_server::test::TestLinkedServer;
    use super::*;

    pub(crate) struct TestMultiBinding {
        pub(crate) binding: MultiBinding,
        #[allow(unused)] // For the `Drop` trait.
        links: Vec<TestLinkedServer>,
    }

    impl Deref for TestMultiBinding {
        type Target = MultiBinding;

        fn deref(&self) -> &Self::Target {
            &self.binding
        }
    }

    impl DerefMut for TestMultiBinding {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.binding
        }
    }

    impl TestMultiBinding {
        pub(crate) async fn new(shards: usize, is_read: bool, in_transaction: bool) -> Self {
            let mut links = vec![];

            for shard in 0..shards {
                let link = TestLinkedServer::new(shard).await;
                links.push(link);
            }

            let servers = links
                .iter_mut()
                .map(|link| link.server.take().unwrap())
                .collect();

            let route = if is_read {
                Route::read(ShardWithPriority::new_table(Shard::All))
            } else {
                Route::write(ShardWithPriority::new_table(Shard::All))
            };

            let binding = MultiBinding {
                servers,
                state: MultiShard::new(shards, &route).boxed(),
                transaction_stmt: if in_transaction {
                    Some(BufferedQuery::Query(Query::new("BEGIN")))
                } else {
                    None
                },
                is_read,
            };

            Self { binding, links }
        }
    }

    #[tokio::test]
    async fn test_multi_binding() {
        let mut binding = TestMultiBinding::new(5, false, true).await;

        assert!(
            binding
                .servers
                .iter()
                .all(|server| !server.in_transaction()),
            "creating a binding does not start transaction"
        );

        assert_eq!(
            vec![0, 1, 2, 3, 4],
            binding.connected_shards().collect::<Vec<_>>(),
        );

        let params = Parameters::from(vec![Parameter::from((
            "application_name".to_string(),
            "test_multi_binding".to_string(),
        ))]);
        let pid = FrontendPid::new();

        assert_eq!(1, binding.link_client(pid, &params).await.unwrap());
        assert!(
            binding.servers.iter().all(|server| server.in_transaction()),
            "link_client starts transaction"
        );
        assert!(
            binding.servers.iter().all(|server| server.in_sync()),
            "link_client leaves all servers in-sync"
        );
        assert_eq!(
            0,
            binding.link_client(pid, &params).await.unwrap(),
            "link_params is idempotent"
        );

        assert!(
            binding.required_shards_connected(
                &Route::read(ShardWithPriority::new_table(Shard::All)),
                5
            ),
            "cross-shard query has coverage"
        );

        assert!(
            !binding.required_shards_connected(
                &Route::read(ShardWithPriority::new_table(Shard::All)),
                6
            ),
            "cross-shard query with more shards does not have coverage"
        );

        for shard in 0..5 {
            assert!(
                binding.required_shards_connected(
                    &Route::read(ShardWithPriority::new_table(Shard::Direct(shard))),
                    5
                ),
                "direct-to-shard has coverage"
            );
        }

        assert!(
            !binding.required_shards_connected(
                &Route::read(ShardWithPriority::new_table(Shard::Direct(5))),
                5
            ),
            "direct-to-shard with less shards does not have coverage"
        );
    }
}
