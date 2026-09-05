use super::*;
use crate::frontend::client::query_engine::multi_step::types::QueryPlanner;
use crate::frontend::router::parser::rewrite::statement::offset::OffsetPlan;
use crate::frontend::router::parser::{AstContext, Cache};

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
    pub(super) async fn parse_and_rewrite(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(Option<QueryPlanner>, Option<OffsetPlan>), Error> {
        let use_parser = self
            .backend
            .cluster()
            .map(|cluster| cluster.use_query_parser(context.client_request))
            .unwrap_or(false);

        if !use_parser {
            return Ok((None, None));
        }

        let query = context.client_request.query()?;
        if let Some(query) = query {
            let cluster = self.backend.cluster()?;
            let ast_ctx = AstContext::from_cluster(cluster, context.params);
            let ast = Cache::get().query(&query, &ast_ctx, context.prepared_statements)?;

            let rewrite_plan = ast.rewrite_plan.clone();
            context.client_request.ast = Some(ast);
            let rewrite_result = rewrite_plan.apply(self, context).await?;
            Ok((rewrite_result, rewrite_plan.offset))
        } else {
            Ok((None, None))
        }
    }
}
