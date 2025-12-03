//! PREPARE statement rewriter.

use pg_query::NodeEnum;

use super::super::{Context, Error, RewriteModule};
use crate::frontend::PreparedStatements;

/// Rewriter for PREPARE statements.
///
/// Renames the prepared statement to use a globally unique name from the cache.
pub struct PrepareRewrite<'a> {
    prepared_statements: &'a mut PreparedStatements,
}

impl<'a> PrepareRewrite<'a> {
    pub fn new(prepared_statements: &'a mut PreparedStatements) -> Self {
        Self {
            prepared_statements,
        }
    }
}

impl RewriteModule for PrepareRewrite<'_> {
    fn rewrite(&mut self, input: &mut Context<'_>) -> Result<(), Error> {
        if let Some(NodeEnum::PrepareStmt(stmt)) = input
            .stmt()?
            .stmt
            .as_ref()
            .and_then(|stmt| stmt.node.as_ref())
        {
            let statement = stmt
                .query
                .as_ref()
                .ok_or(Error::EmptyQuery)?
                .deparse()
                .map_err(|_| Error::EmptyQuery)?;

            let mut parse = crate::net::Parse::named(&stmt.name, &statement);
            self.prepared_statements.insert_anyway(&mut parse);
            let new_name = parse.name().to_string();

            if let Some(NodeEnum::PrepareStmt(stmt)) = input
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
    use super::*;

    #[test]
    fn test_prepare_rewrite() {
        let stmt = pg_query::parse("PREPARE test AS SELECT $1, $2, $3")
            .unwrap()
            .protobuf;
        let mut prepared_statements = PreparedStatements::default();
        let mut rewrite = PrepareRewrite::new(&mut prepared_statements);
        let mut input = Context::new(&stmt, None);
        rewrite.rewrite(&mut input).unwrap();
        let output = input.build().unwrap();
        let query = output.query().unwrap();
        assert_eq!(query, "PREPARE __pgdog_1 AS SELECT $1, $2, $3");
    }
}
