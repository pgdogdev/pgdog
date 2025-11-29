//! EXECUTE statement rewriter.

use pg_query::NodeEnum;

use super::super::{Error, Input, RewriteModule};
use crate::frontend::PreparedStatements;

/// Rewriter for EXECUTE statements.
///
/// Renames the executed statement to use the globally cached name.
pub struct ExecuteRewrite<'a> {
    prepared_statements: &'a PreparedStatements,
}

impl<'a> ExecuteRewrite<'a> {
    pub fn new(prepared_statements: &'a PreparedStatements) -> Self {
        Self {
            prepared_statements,
        }
    }
}

impl RewriteModule for ExecuteRewrite<'_> {
    fn rewrite(&mut self, input: &mut Input<'_>) -> Result<(), Error> {
        if let Some(NodeEnum::ExecuteStmt(stmt)) = input
            .stmt()?
            .stmt
            .as_ref()
            .and_then(|stmt| stmt.node.as_ref())
        {
            let parse = self
                .prepared_statements
                .parse(&stmt.name)
                .ok_or_else(|| Error::PreparedStatementNotFound(stmt.name.clone()))?;

            let new_name = parse.name().to_string();

            if let Some(NodeEnum::ExecuteStmt(stmt)) = input
                .stmt_mut()?
                .stmt
                .as_mut()
                .and_then(|stmt| stmt.node.as_mut())
            {
                stmt.name = new_name;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::{super::PrepareRewrite, *};

    #[test]
    fn test_execute_rewrite() {
        let prepare_stmt = pg_query::parse("PREPARE test AS SELECT $1, $2, $3")
            .unwrap()
            .protobuf;
        let mut prepared_statements = PreparedStatements::default();

        // First prepare the statement
        let mut prepare_rewrite = PrepareRewrite::new(&mut prepared_statements);
        let mut input = Input::new(&prepare_stmt, None);
        prepare_rewrite.rewrite(&mut input).unwrap();

        // Now execute it
        let execute_stmt = pg_query::parse("EXECUTE test(1, 2, 3)").unwrap().protobuf;
        let mut execute_rewrite = ExecuteRewrite::new(&prepared_statements);
        let mut input = Input::new(&execute_stmt, None);
        execute_rewrite.rewrite(&mut input).unwrap();
        let output = input.build().unwrap();
        let query = output.query().unwrap();
        assert!(query.contains("__pgdog_"));
        assert!(!query.contains("EXECUTE test"));
    }

    #[test]
    fn test_execute_not_found() {
        let execute_stmt = pg_query::parse("EXECUTE nonexistent(1, 2, 3)")
            .unwrap()
            .protobuf;
        let prepared_statements = PreparedStatements::default();
        let mut execute_rewrite = ExecuteRewrite::new(&prepared_statements);
        let mut input = Input::new(&execute_stmt, None);
        let result = execute_rewrite.rewrite(&mut input);
        assert!(result.is_err());
    }
}
