use crate::config::PreparedStatements;

use super::*;

impl QueryEngine {
    /// Rewrite extended protocol messages.
    pub(super) fn rewrite_request(
        &mut self,
        context: &mut QueryEngineContext<'_>,
    ) -> Result<(), Error> {
        // Rewrite prepared statements to use global names.
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

        // Rewrite the statement itself.
        if let Ok(cluster) = self.backend.cluster() {
            if cluster.use_parser() && context.ast.is_none() {
                // Execute request rewrite, if needed.
                let mut rewrite = RewriteRequest::new(
                    context.client_request,
                    cluster,
                    context.prepared_statements,
                );
                context.ast = rewrite.execute()?;
            }
        }

        println!("req: {:#?}", context.client_request);

        Ok(())
    }
}
