use super::{Error, ParameterHints};
use crate::{
    backend::{Cluster, Schema},
    frontend::{
        BufferedQuery, ClientRequest, PreparedStatements,
        client::{Sticky, TransactionType},
        router::Ast,
        router::sharding::ResolvedLookups,
    },
    net::{Bind, Parameters},
};

#[derive(Debug)]
pub struct RouterContext<'a> {
    /// Bound parameters to the query.
    pub bind: Option<&'a Bind>,
    /// Query we're looking it.
    pub query: Option<BufferedQuery>,
    /// Cluster configuration.
    pub cluster: &'a Cluster,
    /// Client parameters, e.g. search_path.
    pub parameter_hints: ParameterHints<'a>,
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
    /// AST.
    pub ast: Option<Ast>,
    /// Schema.
    pub schema: Schema,
    /// Original client request.
    pub client_request: &'a ClientRequest,
    /// Sharding key translations resolved for this statement. Routing
    /// reads these before the lookup cache, so a second routing pass
    /// after resolving lookups can't miss.
    pub resolved_lookups: ResolvedLookups,
    /// Client's prepared statements, used to route `EXECUTE`
    /// based on the statement behind the name.
    pub prepared_statements: Option<&'a mut PreparedStatements>,
}

impl<'a> RouterContext<'a> {
    pub fn new(
        buffer: &'a ClientRequest,
        cluster: &'a Cluster,
        params: &'a Parameters,
        transaction: Option<TransactionType>,
        sticky: Sticky,
    ) -> Result<Self, Error> {
        let query = buffer.query()?;
        let bind = buffer.parameters()?;
        let copy_mode = buffer.is_copy();

        Ok(Self {
            bind,
            parameter_hints: ParameterHints::from(params),
            cluster,
            transaction,
            copy_mode,
            executable: buffer.is_executable(),
            two_pc: cluster.two_pc_enabled(),
            sticky,
            extended: matches!(query, Some(BufferedQuery::Prepared(_))) || bind.is_some(),
            query,
            ast: buffer.ast.clone(),
            schema: cluster.schema(),
            client_request: buffer,
            resolved_lookups: ResolvedLookups::default(),
            prepared_statements: None,
        })
    }

    /// Attach sharding key translations resolved for this statement.
    pub fn with_resolved_lookups(mut self, resolved: ResolvedLookups) -> Self {
        self.resolved_lookups = resolved;
        self
    }

    /// Give the router access to the client's prepared statements.
    pub fn with_prepared_statements(
        mut self,
        prepared_statements: &'a mut PreparedStatements,
    ) -> Self {
        self.prepared_statements = Some(prepared_statements);
        self
    }

    pub fn in_transaction(&self) -> bool {
        self.transaction.is_some()
    }

    pub fn transaction(&self) -> &Option<TransactionType> {
        &self.transaction
    }
}
