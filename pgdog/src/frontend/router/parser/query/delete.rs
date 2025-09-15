use crate::frontend::router::parser::where_clause::TablesSource;

use super::*;

impl QueryParser {
    pub(super) fn delete(
        stmt: &DeleteStmt,
        context: &QueryParserContext,
    ) -> Result<Command, Error> {
        let table = stmt.relation.as_ref().map(Table::from);

        if let Some(table) = table {
            let source = TablesSource::from(table);
            let where_clause = WhereClause::new(&source, &stmt.where_clause);

            if let Some(where_clause) = where_clause {
                let shards = Self::where_clause(
                    &context.sharding_schema,
                    &where_clause,
                    context.router_context.bind,
                )?;
                return Ok(Command::Query(Route::write(Self::converge(shards))));
            }
        }

        Ok(Command::Query(Route::write(None)))
    }
}
