use crate::{
    frontend::{client::Inner, Client, ClientRequest, PreparedStatements},
    net::Parameters,
};

/// Context passed to the query execution engine.
pub struct EngineContext<'a> {
    /// Is the client connnected to a backend?
    pub(super) connected: bool,
    /// Client's prepared statements.
    pub(super) prepared_statements: &'a mut PreparedStatements,
    #[allow(dead_code)]
    /// Client parameters.
    pub(super) params: &'a Parameters,
    /// Is the client inside a transaction?
    pub(super) in_transaction: bool,
    /// Messages currently in client's buffer.
    pub(super) buffer: &'a ClientRequest,
}

impl<'a> EngineContext<'a> {
    pub fn new(client: &'a mut Client, inner: &Inner) -> Self {
        Self {
            prepared_statements: &mut client.prepared_statements,
            params: &client.params,
            in_transaction: client.transaction.started(),
            connected: inner.connected(),
            buffer: &client.request,
        }
    }
}
