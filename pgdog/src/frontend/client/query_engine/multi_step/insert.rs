use super::{CommandType, Error, MultiServerState};
use crate::{
    backend::pool::Connection,
    frontend::{
        client::{
            query_engine::{QueryEngine, QueryEngineContext},
            Sticky, TransactionType,
        },
        ClientRequest, Command, Router, RouterContext,
    },
    net::{parameter::ParameterValue, Protocol, Stream},
};

#[derive(Debug)]
pub(crate) struct InsertMulti<'a> {
    /// Backend connections(s).
    connection: &'a mut Connection,
    /// Requests split by the rewrite engine.
    requests: Vec<ClientRequest>,
    /// Client socket.
    stream: &'a mut Stream,
    /// Client connection parameters.
    search_path: Option<&'a ParameterValue>,
    /// Transaction state.
    transaction: Option<TransactionType>,
    /// Sticky.
    sticky: Sticky,
}

impl<'a> InsertMulti<'a> {
    pub(crate) fn from_engine(
        engine: &'a mut QueryEngine,
        context: &'a mut QueryEngineContext<'_>,
        requests: Vec<ClientRequest>,
    ) -> Self {
        Self {
            connection: &mut engine.backend,
            stream: context.stream,
            requests,
            search_path: context.params.search_path(),
            transaction: context.transaction,
            sticky: context.sticky,
        }
    }

    pub(crate) async fn execute(&'a mut self) -> Result<(), Error> {
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

        if let Some(rfq) = state.ready_for_query(self.transaction.is_some()) {
            self.stream.send(&rfq).await?;
        }

        Ok(())
    }
}
