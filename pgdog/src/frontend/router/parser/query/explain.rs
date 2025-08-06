use super::*;

impl QueryParser {
    pub(super) fn explain(
        &mut self,
        stmt: &ExplainStmt,
        context: &QueryParserContext,
    ) -> Result<Command, Error> {
        let query = stmt.query.as_ref().ok_or(Error::EmptyQuery)?;
        let node = query.node.as_ref().ok_or(Error::EmptyQuery)?;

        match node {
            NodeEnum::SelectStmt(ref stmt) => self.select(stmt, context),
            NodeEnum::InsertStmt(ref stmt) => Self::insert(stmt, context),
            NodeEnum::UpdateStmt(ref stmt) => Self::update(stmt, context),
            NodeEnum::DeleteStmt(ref stmt) => Self::delete(stmt, context),

            _ => {
                // For other statement types, route to all shards
                Ok(Command::Query(Route::write(None)))
            }
        }
    }
}
