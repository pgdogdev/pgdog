use crate::{config::PreparedStatements, frontend::router::parser::Cache};

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
    ) -> Result<(), Error> {
        let use_parser = self
            .backend
            .cluster()
            .map(|cluster| cluster.use_query_parser())
            .unwrap_or(false);

        if !use_parser {
            return Ok(());
        }

        let query = context.client_request.query()?;
        if let Some(query) = query {
            context.client_request.ast =
                Some(Cache::get().query(&query, &self.backend.cluster()?.sharding_schema())?);
        }

        let plan = context
            .client_request
            .ast
            .as_ref()
            .map(|ast| ast.rewrite_plan.clone());

        if let Some(plan) = plan {
            plan.apply(context.client_request)?;
        }

        Ok(())
    }
}
