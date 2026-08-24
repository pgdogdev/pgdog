use crate::frontend::router::parser::{AstContext, Cache};

use super::*;

impl QueryEngine {
    /// Rewrite extended protocol messages.
    pub(super) fn rewrite_extended(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        for message in context.client_request.iter_mut() {
            if message.is_extended() {
                let level = context.prepared_statements.level;
                if level.handles_extended() && (level.rewrite_anonymous() || !message.anonymous()) {
                    context.prepared_statements.maybe_rewrite(message)?;
                }
            }
        }
        Ok(())
    }

    /// Parse client request and rewrite it, if necessary.
    pub(super) fn parse_and_rewrite(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<bool, Error> {
        let use_parser = self
            .backend
            .cluster()
            .map(|cluster| cluster.use_query_parser(context.client_request))
            .unwrap_or(false);

        if !use_parser {
            return Ok(true);
        }

        let query = context.client_request.query()?;
        if let Some(query) = query {
            let cluster = self.backend.cluster()?;
            let ast_ctx = AstContext::from_cluster(cluster, context.params);
            let ast = Cache::get().query(&query, &ast_ctx, context.prepared_statements)?;

            context.rewrite_result = Some(ast.rewrite_plan.apply(context.client_request)?);
            context.client_request.ast = Some(ast);
        }

        Ok(true)
    }
}
