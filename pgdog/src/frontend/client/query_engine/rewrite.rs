use super::*;
use crate::frontend::router::parser::rewrite::statement::plan::RewriteResult;
use crate::frontend::router::parser::{AstContext, Cache};
use crate::net::ProtocolMessage;

impl QueryEngine {
    /// Rewrite extended protocol messages.
    pub(super) fn rewrite_extended(
        &mut self,
        context: &mut QueryEngineContext<'_>,
        client_messages: &mut [ProtocolMessage],
    ) -> Result<(), Error> {
        for message in client_messages {
            if message.is_extended() {
                let level = context.prepared_statements.level;
                if level.handles_extended() && (level.rewrite_anonymous() || !message.anonymous()) {
                    context.prepared_statements.maybe_rewrite(message)?;
                }
            }
        }
        Ok(())
    }

    /// Parse client request
    pub(super) fn parse_request(
        &self,
        context: &mut QueryEngineContext<'_>,
        client_request: &mut ClientRequest,
    ) -> Result<(), Error> {
        let use_parser = self
            .backend
            .cluster()
            .map(|cluster| cluster.use_query_parser(client_request))
            .unwrap_or(false);

        if !use_parser {
            return Ok(());
        }

        if let Some(query) = client_request.query()? {
            let cluster = self.backend.cluster()?;
            let ast_ctx = AstContext::from_cluster(cluster, context.params, context.timestamps());
            let ast = Cache::get().query(&query, &ast_ctx, context.prepared_statements)?;
            client_request.ast = Some(ast);
        }
        Ok(())
    }

    pub(super) async fn rewrite_request(
        &self,
        context: &mut QueryEngineContext<'_>,
        client_request: &mut ClientRequest,
    ) -> Result<Option<RewriteResult>, Error> {
        let Some(ast) = client_request.ast.take() else {
            return Ok(None);
        };
        let cluster = self.backend.cluster()?;
        let ast_ctx = AstContext::from_cluster(cluster, context.params, context.timestamps());

        let rewrite_result = ast
            .rewrite_plan
            .apply(client_request, ast_ctx.timezone, ast_ctx.query_timestamps)
            .await?;
        client_request.ast = Some(ast);
        Ok(Some(rewrite_result))
    }

    #[cfg(test)]
    pub(super) async fn parse_and_rewrite(
        &self,
        context: &mut QueryEngineContext<'_>,
        client_request: &mut ClientRequest,
    ) -> Result<Option<RewriteResult>, Error> {
        self.parse_request(context, client_request)?;
        self.rewrite_request(context, client_request).await
    }
}
