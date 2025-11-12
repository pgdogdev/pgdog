use super::super::Error;
use crate::{
    backend::Cluster,
    frontend::{ClientRequest, PreparedStatements, RouterContext},
    net::{replication::TupleData, Bind, Parameters, Parse},
};

#[derive(Debug)]
pub struct StreamContext<'a> {
    request: ClientRequest,
    cluster: &'a Cluster,
    params: Parameters,
    prepared_statements: PreparedStatements,
    bind: Bind,
}

impl<'a> StreamContext<'a> {
    /// Construct new stream context.
    pub fn new(cluster: &'a Cluster, tuple: &TupleData, stmt: &Parse) -> Self {
        let bind = tuple.to_bind(stmt.name());
        let request = ClientRequest::from(vec![stmt.clone().into(), bind.clone().into()]);

        Self {
            request,
            cluster,
            prepared_statements: PreparedStatements::new(),
            params: Parameters::default(),
            bind,
        }
    }

    /// Get Bind message.
    pub fn bind(&self) -> &Bind {
        &self.bind
    }

    /// Construct router context.
    pub fn router_context(&'a mut self) -> Result<RouterContext<'a>, Error> {
        Ok(RouterContext::new(
            &self.request,
            self.cluster,
            &mut self.prepared_statements,
            &self.params,
            None,
        )?)
    }
}
