//! Sending and receiving logic for subqueries

use crate::{
    frontend::{
        ClientRequest, Router,
        client::query_engine::{
            QueryEngineContext,
            multi_step::{self, Error, SubqueryMulti},
        },
        router::parser::{
            AstContext, Cache,
            rewrite::statement::{plan::RewriteResult, projection},
        },
    },
    net::{
        Bind, DataRow, Describe, ErrorResponse, Parse, Protocol, ProtocolMessage, Query,
        RowDescription,
    },
};

impl<'a> SubqueryMulti<'a> {
    /// Send `query_string` and only return rows/RowDescription back if
    /// `is_subquery` is true. Else, return back directly to the client.
    pub(crate) async fn send_query_string(
        &mut self,
        original_request: &ClientRequest,
        query_string: String,
        context: &mut QueryEngineContext<'_>,
        is_subquery: bool,
    ) -> Result<Option<(Vec<DataRow>, Option<RowDescription>)>, Error> {
        let mut client_request: ClientRequest = self
            .form_client_request(query_string, original_request, context)
            .await?;

        multi_step::update::route(self.engine, &mut client_request, context)?;

        if is_subquery {
            let rewrite_result: Option<RewriteResult> = self
                .engine
                .rewrite_request(context, &mut client_request)
                .await?;

            match rewrite_result {
                Some(rewrite_result @ RewriteResult::InPlace { .. }) => {
                    let cluster = self.engine.backend.cluster()?;
                    projection::finalize_after_route(
                        &mut client_request,
                        &cluster.schema(),
                        rewrite_result.offset_plan(),
                    )?;
                    rewrite_result.apply_after_route(&mut client_request)?;

                    Ok(Some(self.send_request(&mut client_request).await?))
                }

                Some(RewriteResult::Subquery(subquery)) => {
                    // Pass the `client_request` we just made back through multi_step/subquery.rs's internal
                    // SQL response method (return new "outer" SQL to us instead of a response to client).
                    // In this case, the "outer" SQL is the evaluated subquery within this statement.
                    //
                    // Then, we send the re-written subquery here, and return the relevant DataRows / RD
                    let mut subquery_multi = SubqueryMulti {
                        subquery: &subquery,
                        engine: self.engine,
                        top_level_bind: self.top_level_bind,
                    };

                    let rewritten_subquery_sql = Box::pin(
                        subquery_multi
                            .evaluate_and_rewrite_outer_statement(context, &client_request),
                    )
                    .await?;

                    Box::pin(self.send_query_string(
                        original_request,
                        rewritten_subquery_sql,
                        context,
                        true,
                    ))
                    .await
                }

                // Seems unlikely we'll need to support INSERT split / Sharding Key Updates
                // as subqueries
                _ => panic!("Subqueries: unsupported"),
            }
        } else {
            // TODO(jkaczman): Is this full QueryEngine coverage? Can we reduce redundancy, perhaps by
            // extracting a full helper out of QueryEngine that doesn't return the response
            // to the Client when a flag is false?

            let rewrite_result = self
                .engine
                .rewrite_request(context, &mut client_request)
                .await?;

            // Need this to set route for stuff like INSERT split, where it relies on
            // "rewrite_result.apply_after_route(client_request)?;"
            // which sets the route of the outer statement if all tuples converge to the same shard.
            if !self
                .engine
                .route_query(context, &mut client_request, rewrite_result.as_ref())
                .await?
            {
                // TODO: if it fails, we need to handle this case.
                todo!()
            }

            // TODO(jkaczman): We rely on INSERT split for insert statements.
            // If a user has that rewrite disabled, but subqueries
            // enabled, then INSERTs will still fail with
            // INSERT SELECT FROM. Maybe we override INSERT split
            // if subqueries are enabled in that case
            Box::pin(
                self.engine
                    .execute(context, &mut client_request, rewrite_result),
            )
            .await
            .unwrap();

            Ok(None)
        }
    }

    async fn form_client_request(
        &self,
        mut query_string: String,
        original_request: &ClientRequest,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<ClientRequest, Error> {
        let is_extended_protocol = original_request
            .messages
            .iter()
            .find(|message| {
                matches!(
                    message,
                    ProtocolMessage::Parse(_) | ProtocolMessage::Bind(_)
                )
            })
            .is_some();

        let mut client_request = if is_extended_protocol {
            let mut bind = Bind::default();
            if let Some(top_level_bind) = self.top_level_bind {
                let mut actual_param_index = 1;
                let mut actual_param_index_str = format!("${actual_param_index}");

                for index in 1..=top_level_bind.params_raw().len() {
                    // If we have a Bind parameter within the subquery, then we should
                    // include it in our Bind message. This also means that we have to rename
                    // it to a local scheme to the subquery itself (e.g., if the first
                    // one we encounter in the subquery is $2, needs to be renamed to $1)
                    //
                    // TODO(jkaczman): we need to also update the original (top-lvl) ClientRequest's Bind
                    // to get rid of the evaluated params
                    let param_str = format!("${}", index);
                    if query_string.contains(param_str.as_str()) {
                        query_string = query_string
                            .replace(param_str.as_str(), actual_param_index_str.as_str());

                        actual_param_index += 1;
                        actual_param_index_str = format!("${}", actual_param_index);

                        let old_bind_param = top_level_bind.parameter(index - 1).unwrap().unwrap();

                        bind.push_param(
                            old_bind_param.parameter().clone(),
                            old_bind_param.format(),
                        );
                    }
                }
            }

            ClientRequest::from(vec![
                ProtocolMessage::Parse(Parse::new_anonymous(&query_string)),
                ProtocolMessage::Bind(bind),
                ProtocolMessage::Describe(Describe::new_portal("")),
                ProtocolMessage::Execute(Default::default()),
                ProtocolMessage::Sync(Default::default()),
            ])
        } else {
            ClientRequest::from(vec![ProtocolMessage::Query(Query::new(&query_string))])
        };

        let cluster = self.engine.backend.cluster()?;
        let ast_ctx = AstContext::from_cluster(cluster, context.params, context.timestamps());

        let query = crate::frontend::BufferedQuery::Query(Query::new(&query_string));
        let ast = Cache::get().query(&query, &ast_ctx, context.prepared_statements)?;

        client_request.ast = Some(ast.ast);
        client_request.cached = ast.cached;
        client_request.routing_comment = Some(ast.comment);

        Ok(client_request)
    }

    /// Send `client_request` and return the rows and RowDescription (if present) back.
    pub(crate) async fn send_request(
        &mut self,
        client_request: &mut ClientRequest,
    ) -> Result<(Vec<DataRow>, Option<RowDescription>), Error> {
        self.engine
            .backend
            .handle_client_request(client_request, &mut Router::default(), false)
            .await?;

        let mut row_description: Option<RowDescription> = None;
        let mut data_rows: Vec<DataRow> = Vec::new();

        while self.engine.backend.has_more_messages() {
            let message = self.engine.read_server_message().await?;
            match message.code() {
                'D' => {
                    data_rows.push(DataRow::try_from(message)?);
                }
                'T' => row_description = Some(RowDescription::try_from(message)?),
                'E' => return Err(ErrorResponse::try_from(message)?.into()),
                _ => (),
            }
        }

        Ok((data_rows, row_description))
    }
}
