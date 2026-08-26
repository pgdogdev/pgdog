use crate::{
    backend::pool::{connection::mirror::Mirror, stats::MemoryStats},
    frontend::{
        Client, ClientRequest, PreparedStatements,
        client::{ClientRequestSettings, Sticky, TransactionType},
        router::parser::rewrite::statement::plan::RewriteResult,
    },
    net::{FrontendPid, Parameters, Stream},
};
use std::net::SocketAddr;

/// Context passed to the query engine to execute a query.
pub struct QueryEngineContext<'a> {
    /// Client ID running the query.
    pub(super) id: FrontendPid,
    /// Prepared statements cache.
    pub(super) prepared_statements: &'a mut PreparedStatements,
    /// Client session parameters.
    pub(super) params: &'a mut Parameters,
    /// Request.
    pub(super) client_request: &'a mut ClientRequest,
    /// How many requests are left to execute in an extended pipeline.
    pub(super) extended_pipeline_requests_left: usize,
    /// Client's socket to send responses to.
    pub(super) stream: &'a mut Stream,
    /// Client in transaction?
    pub(super) transaction: Option<TransactionType>,
    /// Per-request settings snapshot.
    pub(super) request_settings: ClientRequestSettings,
    /// Cross shard  queries are disabled.
    pub(super) cross_shard_disabled: Option<bool>,
    /// Client memory usage.
    pub(super) memory_stats: MemoryStats,
    /// Is the client an admin.
    pub(super) admin: bool,
    /// Executing rollback statement.
    pub(super) rollback: bool,
    /// Sticky config:
    pub(super) sticky: Sticky,
    /// Rewrite result.
    pub(super) rewrite_result: Option<RewriteResult>,
    /// Client TCP address, used for `application_name_add_host`.
    pub(super) client_addr: SocketAddr,
}

impl<'a> QueryEngineContext<'a> {
    pub fn new(client: &'a mut Client) -> Self {
        let memory_stats = client.memory_stats();

        Self {
            id: FrontendPid::from(&client.key),
            prepared_statements: &mut client.prepared_statements,
            params: &mut client.params,
            client_request: &mut client.client_request,
            stream: &mut client.stream,
            transaction: client.transaction,
            request_settings: client.request_settings,
            cross_shard_disabled: None,
            memory_stats,
            admin: client.admin,
            extended_pipeline_requests_left: 0,
            rollback: false,
            sticky: client.sticky,
            rewrite_result: None,
            client_addr: client.addr,
        }
    }

    /// The request is an extended protocol pipeline
    /// with a counter of how many requests are left to process.
    pub(crate) fn extended_pipeline(
        mut self,
        req: &'a mut ClientRequest,
        request_left: usize,
    ) -> Self {
        self.client_request = req;
        self.extended_pipeline_requests_left = request_left;
        self
    }

    /// Create context from mirror.
    pub fn new_mirror(mirror: &'a mut Mirror, buffer: &'a mut ClientRequest) -> Self {
        Self {
            id: mirror.id,
            prepared_statements: &mut mirror.prepared_statements,
            params: &mut mirror.params,
            client_request: buffer,
            stream: &mut mirror.stream,
            transaction: mirror.transaction,
            request_settings: ClientRequestSettings {
                timeouts: mirror.timeouts,
                ..ClientRequestSettings::default()
            },
            cross_shard_disabled: None,
            memory_stats: MemoryStats::default(),
            admin: false,
            extended_pipeline_requests_left: 0,
            rollback: false,
            sticky: Sticky::new(),
            rewrite_result: None,
            client_addr: SocketAddr::from(([0, 0, 0, 0], 0)),
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
