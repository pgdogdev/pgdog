//! Get and process query router command.

use crate::{
    backend::Cluster,
    frontend::{
        client::query_engine::engine_impl::QueryEngine, ClientRequest, Command, Error,
        PreparedStatements, Router, RouterContext,
    },
    net::Parameters,
};

pub struct RouterCommand<'a> {
    prepared_statements: &'a mut PreparedStatements,
    params: &'a Parameters,
    in_transaction: bool,
    cluster: &'a Cluster,
}

impl<'a> RouterCommand<'a> {
    /// Creates a new RouterCommand from a QueryEngine.
    ///
    /// # Arguments
    ///
    /// * `engine` - Mutable reference to the QueryEngine containing prepared statements,
    ///              parameters, and transaction state
    ///
    pub fn new(engine: &'a mut QueryEngine) -> Result<Self, Error> {
        Ok(Self {
            prepared_statements: &mut engine.prepared_statements,
            params: &engine.params,
            in_transaction: engine.transaction.started(),
            cluster: engine.backend.cluster()?,
        })
    }

    /// Routes a client request through the query router and returns the resulting command.
    ///
    /// # Arguments
    ///
    /// * `request` - The client request containing the query and parameters to be routed
    ///
    /// # Returns
    ///
    /// A reference to the Command that should be executed for this request.
    ///
    pub fn handle<'b>(
        &mut self,
        request: &ClientRequest,
        router: &'b mut Router,
    ) -> Result<&'b Command, Error> {
        let context = RouterContext::new(
            request,                       // Query and parameters.
            &self.cluster,                 // Cluster configuration.
            &mut self.prepared_statements, // Prepared statements.
            &self.params,                  // Client connection parameters.
            self.in_transaction,           // Client in explicitily started transaction.
        )?;
        Ok(router.query(context)?)
    }
}
