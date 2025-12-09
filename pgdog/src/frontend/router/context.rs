use super::Error;
use crate::{
    backend::Cluster,
    frontend::{
        client::{Sticky, TransactionType},
        BufferedQuery, ClientRequest, PreparedStatements,
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
    /// Client inside transaction,
    pub transaction: Option<TransactionType>,
    /// Currently executing COPY statement.
    pub copy_mode: bool,
    /// Do we have an executable buffer?
    pub executable: bool,
    /// Two-pc enabled
    pub two_pc: bool,
    /// Sticky omnisharded index.
    pub sticky: Sticky,
    /// Extended protocol.
    pub extended: bool,
}

impl<'a> RouterContext<'a> {
    pub fn new(
        buffer: &'a ClientRequest,
        cluster: &'a Cluster,
        stmt: &'a mut PreparedStatements,
        params: &'a Parameters,
        transaction: Option<TransactionType>,
        sticky: Sticky,
    ) -> Result<Self, Error> {
        let query = buffer.query()?;
        let bind = buffer.parameters()?;
        let copy_mode = buffer.copy();

        Ok(Self {
            bind,
            params,
            prepared_statements: stmt,
            cluster,
            transaction,
            copy_mode,
            executable: buffer.executable(),
            two_pc: cluster.two_pc_enabled(),
            sticky,
            extended: matches!(query, Some(BufferedQuery::Prepared(_))) || bind.is_some(),
            query,
        })
    }

    pub fn in_transaction(&self) -> bool {
        self.transaction.is_some()
    }

    pub fn transaction(&self) -> &Option<TransactionType> {
        &self.transaction
    }
}
