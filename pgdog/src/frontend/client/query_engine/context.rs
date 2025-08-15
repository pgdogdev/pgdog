use crate::{
    frontend::{client::timeouts::Timeouts, Buffer, Client, PreparedStatements},
    net::{BackendKeyData, Parameters, Stream},
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
    /// Client ID.
    pub(super) client_id: BackendKeyData,
    /// Timeouts
    pub(super) timeouts: Timeouts,
}

impl<'a> QueryEngineContext<'a> {
    pub fn new(client: &'a mut Client) -> Self {
        Self {
            prepared_statements: &mut client.prepared_statements,
            params: &mut client.params,
            buffer: &mut client.request_buffer,
            stream: &mut client.stream,
            in_transaction: client.in_transaction,
            client_id: client.id,
            timeouts: client.timeouts,
        }
    }

    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }
}
