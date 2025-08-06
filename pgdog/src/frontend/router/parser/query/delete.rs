use super::*;

impl QueryParser {
    pub(super) fn delete(
        stmt: &DeleteStmt,
        context: &QueryParserContext,
    ) -> Result<Command, Error> {
        let table = stmt.relation.as_ref().map(Table::from);
        let where_clause = WhereClause::new(table.map(|t| t.name), &stmt.where_clause);

        if let Some(where_clause) = where_clause {
            let shards = Self::where_clause(
                &context.sharding_schema,
                &where_clause,
                context.router_context.bind,
            )?;
            return Ok(Command::Query(Route::write(Self::converge(shards))));
        }

        Ok(Command::Query(Route::write(None)))
    }
}
