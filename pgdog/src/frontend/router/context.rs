use super::{Error, ParameterHints};
use crate::{
    backend::{Cluster, Schema},
    frontend::{
        BufferedQuery, ClientRequest,
        client::{Sticky, TransactionType},
        router::sharding::ResolvedLookups,
        router::{Ast, parser::StatementParameters},
    },
    net::Parameters,
};

#[derive(Debug)]
pub struct RouterContext<'a> {
    /// Bound parameters to the query.
    pub bind: Option<StatementParameters<'a>>,
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
        let bind = buffer.parameters()?.map(|bind| bind.into());
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
        })
    }

    /// Attach sharding key translations resolved for this statement.
    pub fn with_resolved_lookups(mut self, resolved: ResolvedLookups) -> Self {
        self.resolved_lookups = resolved;
        self
    }

    pub fn in_transaction(&self) -> bool {
        self.transaction.is_some()
    }

    pub fn transaction(&self) -> &Option<TransactionType> {
        &self.transaction
    }
}
