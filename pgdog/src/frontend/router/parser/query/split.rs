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
                if Self::no_transaction_safe(ast) {
                    let queries = stmts
                        .stmts()
                        .map(|stmt| {
                            let query = deparse(stmt)?;
                            Ok::<_, Error>(query.as_str().to_owned())
                        })
                        .collect::<Result<Vec<_>, _>>()?;

                    Ok(Some(Command::Split(queries)))
                } else {
                    Err(Error::MultiStatementMixedSet)
                }
            }

            Err(err) => Err(err),
        }
    }

    // Check that all statements in the request are safe to be executed
    // without a transactional guarantee.
    //
    // Implementation is simple: caller can execute as many `SET`, `RESET` and `SHOW`
    // commands as they want, but only _one_ real query, e.g., `SELECT`.
    //
    // The state mutations done by `SET` and `RESET` are tracked by the query engine, and we're
    // able to restore the client and server state back to what it was. `SHOW` is harmless.
    //
    // All other statements can modify the database and require transactional guarantees, so we
    // can only execute one of them at a time (for now).
    //
    // TODO(lev): start implicit transaction for multi-statement queries.
    //
    fn no_transaction_safe(ast: &Ast) -> bool {
        let mut stmts = 0;

        for stmt in ast.ast.stmts() {
            match stmt {
                Node::VariableSetStmt(_) => (),
                Node::VariableShowStmt(_) => (),
                _ => stmts += 1,
            }
        }

        stmts < 2
    }
}
