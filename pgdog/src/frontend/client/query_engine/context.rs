use crate::{
    backend::pool::Connection,
    frontend::{
        client::{inner::Inner, timeouts::Timeouts},
        Buffer, Client, Comms, PreparedStatements, Stats,
    },
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
    /// Client stats.
    pub(super) stats: &'a mut Stats,
    /// Backend connection.
    pub(super) backend: &'a mut Connection,
    /// Client ID.
    pub(super) client_id: BackendKeyData,
    /// Timeouts
    pub(super) timeouts: Timeouts,
    /// Client comms
    pub(super) comms: &'a mut Comms,
}

impl<'a> QueryEngineContext<'a> {
    pub fn new(client: &'a mut Client, inner: &'a mut Inner) -> Self {
        Self {
            prepared_statements: &mut client.prepared_statements,
            params: &mut client.params,
            buffer: &mut client.request_buffer,
            stream: &mut client.stream,
            in_transaction: client.in_transaction,
            backend: &mut inner.backend,
            stats: &mut inner.stats,
            client_id: client.id,
            timeouts: client.timeouts,
            comms: &mut client.comms,
        }
    }

    pub fn in_transaction(&self) -> bool {
        self.in_transaction
    }
}
