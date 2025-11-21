use rand::{thread_rng, Rng};

use crate::{
    backend::pool::{connection::mirror::Mirror, stats::MemoryStats},
    frontend::{
        client::{timeouts::Timeouts, TransactionType},
        Client, ClientRequest, PreparedStatements,
    },
    net::{BackendKeyData, Parameters, Stream},
};

#[allow(dead_code)]
/// Context passed to the query engine to execute a query.
pub struct QueryEngineContext<'a> {
    /// Client ID running the query.
    pub(super) id: &'a BackendKeyData,
    /// Prepared statements cache.
    pub(super) prepared_statements: &'a mut PreparedStatements,
    /// Client session parameters.
    pub(super) params: &'a mut Parameters,
    /// Request.
    pub(super) client_request: &'a mut ClientRequest,
    /// Request position in a splice.
    pub(super) requests_left: usize,
    /// Client's socket to send responses to.
    pub(super) stream: &'a mut Stream,
    /// Client in transaction?
    pub(super) transaction: Option<TransactionType>,
    /// Timeouts
    pub(super) timeouts: Timeouts,
    /// Cross shard  queries are disabled.
    pub(super) cross_shard_disabled: Option<bool>,
    /// Client memory usage.
    pub(super) memory_stats: MemoryStats,
    /// Is the client an admin.
    pub(super) admin: bool,
    /// Executing rollback statement.
    pub(super) rollback: bool,
    /// Omnisharded modulo.
    pub(super) omni_sticky_index: usize,
}

impl<'a> QueryEngineContext<'a> {
    pub fn new(client: &'a mut Client) -> Self {
        let memory_stats = client.memory_stats();

        Self {
            id: &client.id,
            prepared_statements: &mut client.prepared_statements,
            params: &mut client.params,
            client_request: &mut client.client_request,
            stream: &mut client.stream,
            transaction: client.transaction,
            timeouts: client.timeouts,
            cross_shard_disabled: None,
            memory_stats,
            admin: client.admin,
            requests_left: 0,
            rollback: false,
            omni_sticky_index: client.omni_sticky_index,
        }
    }

    pub fn spliced(mut self, req: &'a mut ClientRequest, request_left: usize) -> Self {
        self.client_request = req;
        self.requests_left = request_left;
        self
    }

    /// Create context from mirror.
    pub fn new_mirror(mirror: &'a mut Mirror, buffer: &'a mut ClientRequest) -> Self {
        Self {
            id: &mirror.id,
            prepared_statements: &mut mirror.prepared_statements,
            params: &mut mirror.params,
            client_request: buffer,
            stream: &mut mirror.stream,
            transaction: mirror.transaction,
            timeouts: mirror.timeouts,
            cross_shard_disabled: None,
            memory_stats: MemoryStats::default(),
            admin: false,
            requests_left: 0,
            rollback: false,
            omni_sticky_index: thread_rng().gen_range(1..usize::MAX),
        }
    }

    pub fn transaction(&self) -> Option<TransactionType> {
        self.transaction
    }

    pub fn in_transaction(&self) -> bool {
        self.transaction.is_some()
    }

    pub fn in_error(&self) -> bool {
        self.transaction.map(|t| t.error()).unwrap_or_default()
    }
}
