use crate::{
    config::PreparedStatements,
    frontend::router::parser::{AstContext, Cache},
};

use super::*;

impl QueryEngine {
    /// Rewrite extended protocol messages.
    pub(super) fn rewrite_extended(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        for message in context.client_request.iter_mut() {
            if message.extended() {
                let level = context.prepared_statements.level;
                match (level, message.anonymous()) {
                    (PreparedStatements::ExtendedAnonymous, _)
                    | (PreparedStatements::Extended, false) => {
                        context.prepared_statements.maybe_rewrite(message)?
                    }
                    _ => (),
                }
            }
        }
        Ok(())
    }

    /// Parse client request and rewrite it, if necessary.
    pub(super) async fn parse_and_rewrite(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<bool, Error> {
        let use_parser = self
            .backend
            .cluster()
            .map(|cluster| cluster.use_query_parser())
            .unwrap_or(false);

        if !use_parser {
            return Ok(true);
        }

        let query = context.client_request.query()?;
        if let Some(query) = query {
            let cluster = self.backend.cluster()?;
            let ast_ctx = AstContext::from_cluster(cluster, context.params);
            let ast = match Cache::get().query(&query, &ast_ctx, context.prepared_statements) {
                Ok(ast) => ast,
                Err(err) => {
                    self.error_response(context, ErrorResponse::syntax(err.to_string().as_str()))
                        .await?;
                    return Ok(false);
                }
            };
            context.client_request.ast = Some(ast);
        }

        let plan = context
            .client_request
            .ast
            .as_ref()
            .map(|ast| ast.rewrite_plan.clone());

        if let Some(plan) = plan {
            context.rewrite_result = Some(plan.apply(context.client_request)?);
        }

        Ok(true)
    }
}
