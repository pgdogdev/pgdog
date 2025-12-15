use super::{CommandType, Error, MultiServerState};
use crate::{
    backend::pool::{Connection, Request},
    frontend::{
        client::{
            query_engine::{QueryEngine, QueryEngineContext},
            Sticky, TransactionType,
        },
        router::{parser::Shard, Route},
        ClientRequest, Command, PreparedStatements, Router, RouterContext,
    },
    net::{parameter::ParameterValue, BackendKeyData, Protocol, Stream},
};

#[derive(Debug)]
pub(in crate::frontend::client) struct InsertMulti<'a> {
    /// Backend connections(s).
    connection: &'a mut Connection,
    /// Requests split by the rewrite engine.
    requests: &'a mut [ClientRequest],
    /// Client socket.
    stream: &'a mut Stream,
    /// Prepared statements.
    prepared_statements: &'a mut PreparedStatements,
    /// Client connection parameters.
    search_path: Option<&'a ParameterValue>,
    /// Transaction state.
    transaction: Option<TransactionType>,
    /// Sticky.
    sticky: Sticky,
    /// Client ID.
    client_id: BackendKeyData,
}

impl<'a> InsertMulti<'a> {
    pub(in crate::frontend::client) fn from_engine(
        engine: &'a mut QueryEngine,
        context: &'a mut QueryEngineContext<'a>,
        requests: &'a mut [ClientRequest],
    ) -> Self {
        Self {
            connection: &mut engine.backend,
            stream: context.stream,
            requests,
            search_path: context.params.search_path(),
            prepared_statements: context.prepared_statements,
            transaction: context.transaction,
            sticky: context.sticky,
            client_id: *context.id,
        }
    }

    async fn connect(&'a mut self) -> Result<(), Error> {
        if !self.connection.connected() {
            let request = Request::new(self.client_id);
            self.connection
                .connect(&request, &Route::write(Shard::All))
                .await?;
        }

        Ok(())
    }

    pub(in crate::frontend::client) async fn execute(&'a mut self) -> Result<(), Error> {
        let cluster = self.connection.cluster()?;
        for request in self.requests.iter_mut() {
            let context = RouterContext::new(
                request,
                &cluster,
                self.search_path,
                self.transaction,
                self.sticky,
            )?;
            let mut router = Router::new();
            let command = router.query(context)?;
            if let Command::Query(route) = command {
                request.route = Some(route.clone());
            } else {
                return Err(Error::NoRoute);
            }
        }

        let mut state = MultiServerState::new(self.requests.len());

        for request in self.requests.iter() {
            self.connection.send(request).await?;

            while self.connection.has_more_messages() {
                let reply = self.connection.read().await?;
                if state.forward(reply.code()) {
                    self.stream.send(&reply).await?;
                }
            }
        }

        if let Some(cc) = state.command_complete(CommandType::Insert) {
            self.stream.send(&cc).await?;
        }

        if let Some(rfq) = state.ready_for_query(false) {
            self.stream.send(&rfq).await?;
        }

        Ok(())
    }
}
