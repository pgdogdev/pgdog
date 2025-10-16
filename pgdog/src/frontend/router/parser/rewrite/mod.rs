use pg_query::{NodeEnum, ParseResult};

use super::{Command, Error};

mod shard_key;

use crate::frontend::PreparedStatements;
use crate::net::Parse;
pub use shard_key::{Assignment, AssignmentValue, ShardKeyRewritePlan};

#[derive(Debug, Clone)]
pub struct Rewrite<'a> {
    ast: &'a ParseResult,
}

impl<'a> Rewrite<'a> {
    pub fn new(ast: &'a ParseResult) -> Self {
        Self { ast }
    }

    /// Statement needs to be rewritten.
    pub fn needs_rewrite(&self) -> bool {
        for stmt in &self.ast.protobuf.stmts {
            if let Some(ref stmt) = stmt.stmt {
                if let Some(ref node) = stmt.node {
                    match node {
                        NodeEnum::PrepareStmt(_) => return true,
                        NodeEnum::ExecuteStmt(_) => return true,
                        NodeEnum::DeallocateStmt(_) => return true,
                        _ => (),
                    }
                }
            }
        }

        false
    }

    pub fn rewrite(&self, prepared_statements: &mut PreparedStatements) -> Result<Command, Error> {
        let mut ast = self.ast.protobuf.clone();

        for stmt in &mut ast.stmts {
            if let Some(ref mut stmt) = stmt.stmt {
                if let Some(ref mut node) = stmt.node {
                    match node {
                        NodeEnum::PrepareStmt(ref mut stmt) => {
                            let statement = stmt.query.as_ref().ok_or(Error::EmptyQuery)?;
                            let statement = statement.deparse().map_err(|_| Error::EmptyQuery)?;
                            let mut parse = Parse::named(&stmt.name, &statement);
                            prepared_statements.insert_anyway(&mut parse);
                            stmt.name = parse.name().to_string();
                        }

                        NodeEnum::ExecuteStmt(ref mut stmt) => {
                            let name = prepared_statements.name(&stmt.name);
                            if let Some(name) = name {
                                stmt.name = name.to_string();
                            }
                        }

                        NodeEnum::DeallocateStmt(_) => return Ok(Command::Deallocate),

                        _ => (),
                    }
                }
            }
        }

        Ok(Command::Rewrite(
            ast.deparse().map_err(|_| Error::EmptyQuery)?,
        ))
    }
}

#[cfg(test)]
mod test {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn test_rewrite_prepared() {
        let ast = pg_query::parse("BEGIN; PREPARE test AS SELECT $1, $2, $3; PREPARE test2 AS SELECT * FROM my_table WHERE id = $1; COMMIT;").unwrap();
        let rewrite = Rewrite::new(&ast);
        assert!(rewrite.needs_rewrite());
        let mut prepared_statements = PreparedStatements::new();
        let queries = rewrite.rewrite(&mut prepared_statements).unwrap();
        match queries {
            Command::Rewrite(queries) => assert_eq!(queries, "BEGIN; PREPARE __pgdog_1 AS SELECT $1, $2, $3; PREPARE __pgdog_2 AS SELECT * FROM my_table WHERE id = $1; COMMIT"),
            _ => panic!("not a rewrite"),
        }
    }

    #[test]
    fn test_deallocate() {
        for q in ["DEALLOCATE ALL", "DEALLOCATE test"] {
            let ast = pg_query::parse(q).unwrap();
            let ast = Arc::new(ast);
            let rewrite = Rewrite::new(&ast)
                .rewrite(&mut PreparedStatements::new())
                .unwrap();

            assert!(matches!(rewrite, Command::Deallocate));
        }
    }
}
