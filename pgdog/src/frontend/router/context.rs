use super::Error;
use crate::{
    backend::Cluster,
    frontend::{buffer::BufferedQuery, Buffer, PreparedStatements},
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
    pub in_transaction: bool,
}

impl<'a> RouterContext<'a> {
    pub fn new(
        buffer: &'a Buffer,
        cluster: &'a Cluster,
        stmt: &'a mut PreparedStatements,
        params: &'a Parameters,
        in_transaction: bool,
    ) -> Result<Self, Error> {
        let query = buffer.query()?;
        let bind = buffer.parameters()?;

        Ok(Self {
            query,
            bind,
            params,
            prepared_statements: stmt,
            cluster,
            in_transaction,
        })
    }
}
