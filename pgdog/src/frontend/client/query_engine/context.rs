use crate::{
    backend::pool::{connection::mirror::Mirror, stats::MemoryStats},
    frontend::{
        Client, ClientRequest, PreparedStatements,
        client::{
            Sticky,
            timeouts::Timeouts,
            transaction_type::{QueryTimestamps, Transaction},
        },
    },
    net::{FrontendPid, Parameters, Stream},
};
use chrono::{DateTime, Utc};

use super::split::Pipeline;

/// Context passed to the query engine to execute a query.
pub(crate) struct QueryEngineContext<'a> {
    /// Client ID running the query.
    pub(super) id: FrontendPid,
    /// Prepared statements cache.
    pub(super) prepared_statements: &'a mut PreparedStatements,
    /// Client session parameters.
    pub(super) params: &'a mut Parameters,
    /// Parameters from the client's startup message.
    pub(super) startup_params: &'a Parameters,
    /// Request.
    pub(super) client_request: &'a mut ClientRequest,
    /// How many requests are left to execute in an extended pipeline.
    pub(super) pipeline: Pipeline,
    /// Client's socket to send responses to.
    pub(super) stream: &'a mut Stream,
    /// Client in transaction?
    pub(super) transaction: Option<Transaction>,
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
    /// Sticky config:
    pub(super) sticky: Sticky,
    /// Log queries to stdout.
    pub(super) query_log_stdout: bool,
    /// Maximum query message size before a warning is logged.
    pub(super) query_size_limit: Option<usize>,
    /// When we received the first message of the request.
    pub(super) statement_start: DateTime<Utc>,
}

impl<'a> QueryEngineContext<'a> {
    pub(crate) fn new(client: &'a mut Client) -> Self {
        let memory_stats = client.memory_stats();

        Self {
            id: FrontendPid::from(&client.key),
            prepared_statements: &mut client.prepared_statements,
            params: &mut client.params,
            startup_params: &client.startup_params,
            client_request: &mut client.client_request,
            stream: &mut client.stream,
            transaction: client.transaction,
            timeouts: client.timeouts,
            cross_shard_disabled: None,
            memory_stats,
            admin: client.admin,
            pipeline: Pipeline::None,
            rollback: false,
            sticky: client.sticky,
            query_log_stdout: client.query_log_stdout,
            query_size_limit: client.query_size_limit,
            statement_start: client.statement_start,
        }
    }

    /// The request is an extended protocol pipeline
    /// with a counter of how many requests are left to process.
    pub(crate) fn pipelined(mut self, req: &'a mut ClientRequest, pipeline: Pipeline) -> Self {
        self.client_request = req;
        self.pipeline = pipeline;
        self
    }

    /// Create context from mirror.
    pub(crate) fn new_mirror(mirror: &'a mut Mirror, buffer: &'a mut ClientRequest) -> Self {
        Self {
            id: mirror.id,
            prepared_statements: &mut mirror.prepared_statements,
            params: &mut mirror.params,
            startup_params: &mirror.startup_params,
            client_request: buffer,
            stream: &mut mirror.stream,
            transaction: mirror.transaction,
            timeouts: mirror.timeouts,
            cross_shard_disabled: None,
            memory_stats: MemoryStats::default(),
            admin: false,
            pipeline: Pipeline::None,
            rollback: false,
            sticky: Sticky::new(),
            query_log_stdout: false,
            query_size_limit: None,
            statement_start: Utc::now(),
        }
    }

    pub(crate) fn transaction(&self) -> Option<Transaction> {
        self.transaction
    }

    /// Request itself can start a transaction, so this is computed "on demand"
    pub(crate) fn timestamps(&self) -> QueryTimestamps {
        QueryTimestamps::new(self.transaction.as_ref(), self.statement_start)
    }

    pub(crate) fn in_transaction(&self) -> bool {
        self.transaction.is_some()
    }

    pub(crate) fn in_error(&self) -> bool {
        self.transaction.map(|t| t.error()).unwrap_or_default()
    }
}
