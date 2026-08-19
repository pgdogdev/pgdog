use super::{Error, ParameterHints};
use crate::{
    backend::{Cluster, Schema},
    frontend::{
        BufferedQuery, ClientRequest,
        client::{Sticky, TransactionType},
        router::{Ast, parser::StatementParameters, sharding::ResolvedLookups},
    },
    net::Parameters,
};

#[derive(Debug, Clone)]
pub struct RouterContext<'a> {
    /// Bound parameters to the query.
    pub(super) bind: Option<StatementParameters<'a>>,
    /// Query we're looking it.
    pub(super) query: Option<BufferedQuery>,
    /// Cluster configuration.
    pub(super) cluster: &'a Cluster,
    /// Client parameters, e.g. search_path.
    pub(super) parameter_hints: ParameterHints<'a>,
    /// Client inside transaction,
    pub(super) transaction: Option<TransactionType>,
    /// Currently executing COPY statement.
    pub(super) copy_mode: bool,
    /// Do we have an executable buffer?
    pub(super) executable: bool,
    /// Two-pc enabled
    pub(super) two_pc: bool,
    /// Sticky omnisharded index.
    pub(super) sticky: Sticky,
    /// AST.
    pub(super) ast: Option<&'a Ast>,
    /// Schema.
    pub(super) schema: Schema,
    /// Sharding key translations resolved for this statement. Routing
    /// reads these before the lookup cache, so a second routing pass
    /// after resolving lookups can't miss.
    pub(super) resolved_lookups: ResolvedLookups,
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
            query,
            ast: buffer.ast.as_ref(),
            schema: cluster.schema(),
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

    pub fn is_simple_protocol(&self) -> bool {
        self.query.as_ref().map(|q| q.simple()).unwrap_or_default()
    }
}
