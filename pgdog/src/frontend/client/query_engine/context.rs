use crate::{
    frontend::{client::timeouts::Timeouts, Buffer, Client, PreparedStatements},
    net::{Parameters, Stream},
    stats::memory::MemoryUsage,
};

/// Context passed to the query engine to execute a query.
pub struct QueryEngineContext<'a> {
    /// Prepared statements cache.
    pub(super) prepared_statements: &'a mut PreparedStatements,
    /// Client session parameters.
    pub(super) params: &'a mut Parameters,
    /// Request
    pub(super) buffer: &'a mut Buffer,
    /// Client's socket to send responses to.
    pub(super) stream: &'a mut Stream,
    /// Client in transaction?
    pub(super) in_transaction: bool,
    /// Timeouts
    pub(super) timeouts: Timeouts,
    /// Cross shard  queries are disabled.
    pub(super) cross_shard_disabled: bool,
    /// Client memory usage.
    pub(super) memory_usage: usize,
}

impl<'a> QueryEngineContext<'a> {
    pub fn new(client: &'a mut Client) -> Self {
        let memory_usage = client.memory_usage();

        Self {
            prepared_statements: &mut client.prepared_statements,
            params: &mut client.params,
            buffer: &mut client.request_buffer,
            stream: &mut client.stream,
            in_transaction: client.in_transaction,
            timeouts: client.timeouts,
            cross_shard_disabled: client.cross_shard_disabled,
            memory_usage,
        }
    }

    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }
}
