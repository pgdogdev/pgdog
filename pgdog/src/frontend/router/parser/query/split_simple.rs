use pg_raw_parse::{Error as PgParseRawError, deparse};

use super::*;

impl QueryParser {
    /// Handle the following conditions:
    ///
    /// 1. Multi-`SET` statements, which we can parse and execute.
    /// 2. Multi-query statement which we can split up and re-execute separately.
    ///
    /// # Example
    ///
    /// ```sql
    /// SET application_name TO 'pgdog'; SET statement_timeout TO 0;
    /// ```
    ///
    /// We extract the params and return a fake response to the client.
    ///
    /// ```sql
    /// SELECT 1; SELECT 2;
    /// ```
    ///
    /// We rewrite this into two separate queries and re-execute them
    /// through the query engine one by one.
    ///
    pub(super) fn split_simple_check(
        &self,
        context: &QueryParserContext<'_>,
        ast: &Ast,
    ) -> Result<Option<Command>, Error> {
        let stmts = &ast.ast;
        if stmts.len() > 1 {
            if let Ok(Some(command)) = self.try_multi_set(&**stmts, context) {
                return Ok(Some(command));
            } else if context.router_context.is_simple_protocol() {
                return Ok(Some(self.split_simple_queries(ast)?));
            }
        }

        Ok(None)
    }

    pub(super) fn split_simple_queries(&self, ast: &Ast) -> Result<Command, Error> {
        debug_assert!(ast.ast.stmts().count() > 1);

        let stmts = ast
            .ast
            .stmts()
            .map(|stmt| deparse(stmt))
            .collect::<Result<Vec<_>, PgParseRawError>>()?;
        let queries = stmts
            .into_iter()
            .map(|query| query.as_str().to_string())
            .collect::<Vec<_>>();

        Ok(Command::SimpleQuerySplit { queries })
    }
}
