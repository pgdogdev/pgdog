use pg_raw_parse::deparse;

use super::*;

impl QueryParser {
    /// Check if the statement contains multiple queries.
    ///
    /// If that's the case, return it back to the query engine for separate
    /// re-execution.
    pub(super) fn check_multi_statement(
        &self,
        ast: &Ast,
        context: &QueryParserContext<'_>,
    ) -> Result<Option<Command>, Error> {
        if ast.ast.stmts().count() <= 1 {
            return Ok(None);
        }

        let stmts = &ast.ast;

        match self.try_multi_set(&**stmts, context) {
            Ok(Some(set)) => Ok(Some(set)),
            Ok(None) => Ok(None),
            Err(Error::MultiStatementMixedSet) => {
                let queries = stmts
                    .stmts()
                    .map(|stmt| {
                        let query = deparse(stmt)?;
                        Ok::<_, Error>(query.as_str().to_owned())
                    })
                    .collect::<Result<Vec<_>, _>>()?;

                Ok(Some(Command::Split(queries)))
            }

            Err(err) => return Err(err),
        }
    }
}
