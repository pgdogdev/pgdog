// TODO: This could use more unit tests
// TODO: Can maybe use lifetimes to get rid of some .clone()s

use crate::frontend::client::query_engine::multi_step::error::{Error, UpdateError};
use crate::frontend::client::query_engine::multi_step::forward_check::ForwardCheck;
use crate::frontend::client::query_engine::multi_step::types::{
    QueryPlanner, QueryPlannerType, ResponseHistory, StatementRequest, StatementSource,
    StepProtocol, StepRequest, StepResponses,
};
use crate::frontend::client::query_engine::{QueryEngine, QueryEngineContext};
use crate::frontend::router::parser::rewrite::statement::Error as RewriteError;
use crate::frontend::router::{Ast, Route};
use crate::frontend::{BufferedQuery, ClientRequest, Command, Router, RouterContext};
use crate::net::{
    Bind, BindComplete, CommandComplete, DataRow, Describe, ErrorResponse, Execute, FromBytes,
    Message, Parse, ParseComplete, Protocol, Query, ReadyForQuery, RowDescription, Sync, ToBytes,
    TransactionState,
};
use bytes::Buf;
use indexmap::IndexSet;
use pg_raw_parse::{NodeMut, walk};

impl StatementRequest {
    /// Construct a `ClientRequest` based on statically/dynamically resolving the current `Step`
    /// based on prior `Step` responses. Handles both simple and extended protocol.
    pub(crate) fn assemble(&self, map: &ResponseHistory) -> Result<Option<ClientRequest>, Error> {
        let Some((parse, bind)) = self.source.resolve(map)? else {
            return Ok(None);
        };

        let mut request = ClientRequest::default();
        match self.protocol {
            StepProtocol::Simple => {
                request.push(Query::new(parse.query()).into());
            }
            StepProtocol::Extended => {
                let name = parse.name().to_owned();
                request.push(parse.into());
                // So we get both T and t.
                request.push(Describe::new_statement(&name).into());
                request.push(bind.into());
                request.push(Execute::new().into());
                request.push(Sync.into());
            }
        }
        request.route = Some(self.route.clone());
        request.ast = self.ast.clone();

        Ok(Some(request))
    }
}

/// Represents a statement that's fully known during planning. Needs no dynamic resolving.
#[derive(Debug, Clone)]
struct RewrittenStatement {
    parse: Parse,
    bind: Bind,
}

impl StatementSource for RewrittenStatement {
    fn resolve(&self, _map: &ResponseHistory) -> Result<Option<(Parse, Bind)>, Error> {
        Ok(Some((self.parse.clone(), self.bind.clone())))
    }
}

impl ResponseHistory {
    /// Protocol acks and the row set the client expects ahead of the final `CommandComplete`,
    /// composed from the given `steps` responses and filtered by what the client's request asked for.
    pub(crate) fn compose(
        steps: &[&StepResponses],
        client_request: &ClientRequest,
    ) -> Vec<Message> {
        let mut check = ForwardCheck::new(client_request);
        let mut messages = Vec::new();

        if check.forward('1') {
            messages.push(ParseComplete.message());
        }
        if let Some(parameter_description) = steps
            .iter()
            .find_map(|step| step.parameter_description.clone())
            && check.forward('t')
        {
            messages.push(parameter_description);
        }
        if let Some(row_description) = steps.iter().find_map(|step| step.row_description.clone())
            && check.forward('T')
        {
            messages.push(row_description.message());
        }
        if check.forward('2') {
            messages.push(BindComplete.message());
        }
        for step in steps {
            for row in &step.rows {
                if check.forward('D') {
                    messages.push(row.message());
                }
            }
        }

        messages
    }

    /// [`Self::compose`] for every `Step` (maintaining execution order)
    pub(crate) fn compose_all(&self, client_request: &ClientRequest) -> Vec<Message> {
        Self::compose(&self.steps().iter().collect::<Vec<_>>(), client_request)
    }
}

impl QueryEngine {
    /// Handles execution of all `Step`s in the `QueryPlanner` one-by-one, in a serial,
    /// sequential way. Handles dynamic resolving, executing returned `ClientRequest`s,
    /// checking to see if we should save Responses, and checking to see if we should forward.
    pub(crate) async fn run_steps(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        planner: &QueryPlanner,
    ) -> Result<(), crate::frontend::Error> {
        // TODO: I think there needs to be some work in regard to this; current functionality:
        //
        //       -  Aggregated plans buffer every step's responses and `ForwardToClient`
        //          composes the client's reply from them at the end
        //
        //       -  Normal plan streams its single Step responses.
        //
        //  If we consider a subquery, we may want to stream the outer query's Responses without
        //  waiting. Sage and I discussed briefly about potential Postgres protocol violation w.r.t.
        //  an Error occuring after Rows are sent back. Haven't tested yet, but thought I'd mention
        //  here as to future work.
        let mut map = ResponseHistory::default();
        let aggregate = planner.forward_to_client.is_some();

        // Iterate serially
        for step in &planner.steps {
            let assembled;
            let client_request = match &step.request {
                StepRequest::Raw => context.client_request,
                StepRequest::Statement(statement) => match statement.assemble(&map)? {
                    Some(request) => {
                        assembled = request;
                        &assembled
                    }
                    // The `Step` resolves to nothing
                    // (e.g. an INSERT whose source row doesn't exist), skip it here
                    None => continue,
                },
            };

            self.backend
                .handle_client_request(client_request, &mut self.router, self.streaming)
                .await?;

            let mut responses = StepResponses {
                key: step.save_key,
                ..Default::default()
            };
            let mut step_error = None;
            while self.backend.has_more_messages()
                && !self.backend.in_copy_mode()
                && !self.streaming
            {
                let message = self.read_server_message().await?;
                if aggregate && message.code() == 'E' {
                    step_error = Some(ErrorResponse::try_from(message)?);
                    continue;
                }

                // Prevent case where:
                // - A step failed.
                // - Trailing RFQ is held from the client,
                // Its aborted-transaction state should still be applied.
                // Otherwise, we think the transaction is healthy and COMMIT half-commits
                if step_error.is_some()
                    && message.code() == 'Z'
                    && ReadyForQuery::from_bytes(message.to_bytes())?.state()?
                        == TransactionState::Error
                {
                    context.set_transaction_error();
                    continue;
                }

                if aggregate {
                    match message.code() {
                        'T' => {
                            responses.row_description =
                                Some(RowDescription::from_bytes(message.to_bytes())?)
                        }
                        't' => responses.parameter_description = Some(message),
                        'D' => responses
                            .rows
                            .push(DataRow::from_bytes(message.to_bytes())?),
                        'C' => {
                            responses.command_complete =
                                Some(CommandComplete::from_bytes(message.to_bytes())?)
                        }
                        _ => (),
                    }
                } else {
                    self.process_server_message(context, message).await?;
                }
            }

            if let Some(error) = step_error {
                return Err(Error::Execution(Box::new(error)).into());
            }

            if aggregate {
                map.push(responses);
            }
        }

        // Forward whatever we need to the client at the end (in aggregate)
        if let Some(ftc) = &planner.forward_to_client {
            let messages = ftc.forward_to_client(context, map);
            for message_to_forward in messages {
                self.process_server_message(context, message_to_forward)
                    .await?;
            }
        }

        Ok(())
    }
}

impl QueryPlanner {
    pub(crate) async fn plan_query(
        request: &ClientRequest,
        engine: &mut QueryEngine,
        context: &mut QueryEngineContext<'_>,
        mut query_planner_type: QueryPlannerType,
    ) -> Result<Option<QueryPlanner>, Error> {
        if !request.is_executable() {
            query_planner_type = QueryPlannerType::Normal;
        };

        // Based on `QueryPlannerType`, plan out the `Steps` we should take.
        match query_planner_type {
            QueryPlannerType::InsertSplit => Self::plan_multi_insert(engine, context, request),
            QueryPlannerType::ShardingKeyUpdate => {
                let schema = engine.backend.cluster()?.sharding_schema();
                Self::plan_sharding_key_update(request, engine, context, schema).await
            }
            QueryPlannerType::Normal => Ok(None),
        }
    }

    /// Build a routed `StepRequest` from statement.
    /// Use the same protocol as the original statement.
    ///
    /// TODO: This is a lot of parameters passed in; move / turn it into a struct?
    pub(crate) fn build_request(
        engine: &QueryEngine,
        context: &QueryEngineContext<'_>,
        original: &ClientRequest,
        ast: &Ast,
        stmt: &str,
        params: &IndexSet<u16>,
        statement_name: Option<&str>,
    ) -> Result<StepRequest, Error> {
        let query = original.query()?.ok_or(RewriteError::EmptyQuery)?;
        let name = statement_name.unwrap_or_default();

        let (protocol, parse, bind) = match query {
            BufferedQuery::Query(_) => (
                StepProtocol::Simple,
                Parse::named(name, stmt),
                Bind::new_statement(name),
            ),
            BufferedQuery::Prepared(original_parse) => {
                let data_types = Self::rewrite_data_types(&original_parse, params);
                let bind = match original.parameters()? {
                    Some(bind) => Self::rewrite_bind(params, bind, name)?,
                    // This shouldn't really happen since we don't rewrite
                    // non-executable requests.
                    None => Bind::new_statement(name),
                };
                (
                    StepProtocol::Extended,
                    Parse::named(name, stmt).with_data_types(&data_types),
                    bind,
                )
            }
        };

        let mut statement = StatementRequest {
            source: Box::new(RewrittenStatement { parse, bind }),
            protocol,
            route: Route::default(),
            ast: Some(ast.clone()),
        };

        // Deliberately uses an empty history so we can route the solo request.
        let mut probe = statement
            .assemble(&ResponseHistory::default())?
            .ok_or(RewriteError::EmptyQuery)?;
        Self::route(engine, &mut probe, context)?;
        statement.route = probe.route.take().unwrap_or_default();

        Ok(StepRequest::Statement(Box::new(statement)))
    }

    fn rewrite_data_types(parse: &Parse, params: &IndexSet<u16>) -> Vec<u32> {
        let mut bytes = parse.data_types_ref();
        let count = bytes.get_i16().max(0) as usize;
        let declared: Vec<u32> = (0..count).map(|_| bytes.get_u32()).collect();

        params
            .iter()
            .map(|original| {
                declared
                    .get(*original as usize - 1)
                    .copied()
                    .unwrap_or_default()
            })
            .collect()
    }

    pub(crate) fn route(
        query_engine: &QueryEngine,
        request: &mut ClientRequest,
        context: &QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        let cluster = query_engine.backend.cluster()?;

        let context = RouterContext::new(
            request,
            cluster,
            context.params,
            context.transaction(),
            context.sticky,
        )?;
        let mut router = Router::new();
        let command = router.query(context)?;
        if let Command::Query(route) = command {
            request.route = Some(route.clone());
        } else {
            return Err(UpdateError::NoRoute.into());
        }

        Ok(())
    }

    /// Visit all ParamRef nodes in a ParseResult and renumber them sequentially.
    /// Returns a sorted list of the original parameter numbers.
    pub(crate) fn rewrite_params(node: NodeMut<'_, '_>) -> IndexSet<u16> {
        let mut params = IndexSet::new();
        walk::walk_mut(node, |node| {
            if let NodeMut::ParamRef(param) = node {
                params.insert(param.number as _);
                param.set_number(params.get_index_of(&(param.number as u16)).unwrap() as i32 + 1)
            }
        });
        params
    }

    /// Create new Bind message for the statement from original Bind.
    pub(crate) fn rewrite_bind(
        params: &IndexSet<u16>,
        bind: &Bind,
        statement_name: &str,
    ) -> Result<Bind, Error> {
        let mut new = Bind::new_statement(statement_name);
        for param in params {
            let param = bind
                .parameter(*param as usize - 1)?
                .ok_or(RewriteError::MissingParameter(*param))?;
            new.push_param(param.parameter().clone(), param.format());
        }

        Ok(new)
    }
}
