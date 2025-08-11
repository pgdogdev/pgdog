use super::Error;
use crate::{
    backend::Cluster,
    frontend::{
        buffer::BufferedQuery, logical_transaction::LogicalTransaction, Buffer, PreparedStatements,
    },
    net::{Bind, Parameters},
};

#[derive(Debug)]
pub struct RouterContext<'a> {
    /// Prepared statements.
    pub prepared_statements: &'a mut PreparedStatements,
    /// Bound parameters to the query.
    pub bind: Option<&'a Bind>,
    /// Query we're looking it.
    pub query: Option<BufferedQuery>,
    /// Cluster configuration.
    pub cluster: &'a Cluster,
    /// Client parameters, e.g. search_path.
    pub params: &'a Parameters,
    /// Currently executing COPY statement.
    pub copy_mode: bool,
    /// Client's logical_transaction struct,
    pub logical_transaction: &'a LogicalTransaction,
}

impl<'a> RouterContext<'a> {
    pub fn new(
        buffer: &'a Buffer,
        cluster: &'a Cluster,
        stmt: &'a mut PreparedStatements,
        params: &'a Parameters,
        logical_transaction: &'a LogicalTransaction,
    ) -> Result<Self, Error> {
        let query = buffer.query()?;
        let bind = buffer.parameters()?;
        let copy_mode = buffer.copy();

        Ok(Self {
            query,
            bind,
            params,
            prepared_statements: stmt,
            logical_transaction,
            cluster,
            copy_mode,
        })
    }

    pub fn in_transaction(&self) -> bool {
        self.logical_transaction.in_transaction()
    }
}
