use pg_query::{Error as PgQueryError, NodeEnum};

use crate::frontend::PreparedStatements;
use crate::net::Parse;

use super::{Error, StatementRewrite};

/// Result of rewriting all PREPARE/EXECUTE statements in a query.
#[derive(Debug, Clone, Default)]
pub struct SimplePreparedResult {
    /// Whether any statement was rewritten.
    pub rewritten: bool,
    /// Prepared statements to prepend (name, statement) for EXECUTE rewrites.
    pub prepares: Vec<(String, String)>,
}

/// Result of rewriting a single PREPARE or EXECUTE SQL command.
#[derive(Debug, Clone)]
enum SimplePreparedRewrite {
    /// Node was not a PREPARE or EXECUTE statement.
    None,
    /// PREPARE statement was rewritten.
    Prepared,
    /// EXECUTE statement was rewritten. Contains the global name and statement
    /// needed to prepend a ProtocolMessage::Prepare.
    Executed { name: String, statement: String },
}

impl StatementRewrite<'_> {
    /// Rewrites all top-level `PREPARE` and `EXECUTE` SQL commands.
    ///
    /// # More details
    ///
    /// `PREPARE __stmt_1 AS SELECT $1` is rewritten as `PREPARE __pgdog_1 AS SELECT $1` and
    /// `SELECT $1` is stored in the global cache using `insert_anyway`.
    ///
    /// `EXECUTE __stmt_1(1)` is rewritten to `EXECUTE __pgdog_1(1)`. Additionally, the caller
    /// should prepend `ProtocolMessage::Prepare` to the client request using the returned
    /// name and statement.
    ///
    pub(super) fn rewrite_simple_prepared(&mut self) -> Result<SimplePreparedResult, Error> {
        let mut result = SimplePreparedResult::default();

        if !self.prepared_statements.level.full() {
            return Ok(result);
        }

        for stmt in &mut self.stmt.stmts {
            if let Some(ref mut node) = stmt.stmt {
                if let Some(ref mut inner) = node.node {
                    match rewrite_single_prepared(inner, self.prepared_statements)? {
                        SimplePreparedRewrite::Prepared => {
                            result.rewritten = true;
                        }
                        SimplePreparedRewrite::Executed { name, statement } => {
                            result.prepares.push((name, statement));
                            result.rewritten = true;
                        }
                        SimplePreparedRewrite::None => {}
                    }
                }
            }
        }

        Ok(result)
    }
}

/// Rewrites a single `PREPARE` or `EXECUTE` node.
fn rewrite_single_prepared(
    node: &mut NodeEnum,
    prepared_statements: &mut PreparedStatements,
) -> Result<SimplePreparedRewrite, Error> {
    match node {
        NodeEnum::PrepareStmt(stmt) => {
            let query = stmt
                .query
                .as_ref()
                .ok_or(Error::PgQuery(PgQueryError::Parse(
                    "missing query in PREPARE".into(),
                )))?
                .deparse()
                .map_err(Error::PgQuery)?;

            let mut parse = Parse::named(&stmt.name, &query);
            prepared_statements.insert_anyway(&mut parse);
            stmt.name = parse.name().to_string();

            Ok(SimplePreparedRewrite::Prepared)
        }

        NodeEnum::ExecuteStmt(stmt) => {
            let parse = prepared_statements.parse(&stmt.name);
            if let Some(parse) = parse {
                let global_name = parse.name().to_string();
                let statement = parse.query().to_string();
                stmt.name = global_name.clone();

                Ok(SimplePreparedRewrite::Executed {
                    name: global_name,
                    statement,
                })
            } else {
                Err(Error::PgQuery(PgQueryError::Parse(format!(
                    "prepared statement '{}' does not exist",
                    stmt.name
                ))))
            }
        }

        _ => Ok(SimplePreparedRewrite::None),
    }
}

#[cfg(test)]
mod tests {
    use super::super::{RewritePlan, StatementRewrite, StatementRewriteContext};
    use super::*;
    use crate::backend::replication::{ShardedSchemas, ShardedTables};
    use crate::backend::ShardingSchema;
    use crate::config::PreparedStatements as PreparedStatementsLevel;
    use pg_query::parse;
    use pg_query::protobuf::ParseResult;

    struct TestContext {
        ps: PreparedStatements,
        schema: ShardingSchema,
    }

    impl TestContext {
        fn new() -> Self {
            let mut ps = PreparedStatements::default();
            ps.set_level(PreparedStatementsLevel::Full);
            Self {
                ps,
                schema: ShardingSchema {
                    shards: 1,
                    tables: ShardedTables::default(),
                    schemas: ShardedSchemas::default(),
                },
            }
        }

        fn rewrite(&mut self, sql: &str) -> Result<(ParseResult, RewritePlan), Error> {
            let mut ast = parse(sql).unwrap().protobuf;
            let mut rewrite = StatementRewrite::new(StatementRewriteContext {
                stmt: &mut ast,
                extended: false,
                prepared: false,
                prepared_statements: &mut self.ps,
                schema: &self.schema,
            });
            let plan = rewrite.maybe_rewrite()?;
            Ok((ast, plan))
        }
    }

    #[test]
    fn test_rewrite_prepare() {
        let mut ctx = TestContext::new();
        let (ast, plan) = ctx.rewrite("PREPARE test_stmt AS SELECT $1, $2").unwrap();

        let sql = ast.deparse().unwrap();
        assert!(
            sql.contains("__pgdog_"),
            "PREPARE should be renamed to __pgdog_N, got: {sql}"
        );
        assert!(
            !sql.contains("test_stmt"),
            "original name should be replaced: {sql}"
        );
        assert!(plan.prepares.is_empty());
        assert!(plan.stmt.is_some());
    }

    #[test]
    fn test_rewrite_execute() {
        let mut ctx = TestContext::new();
        ctx.rewrite("PREPARE test_stmt AS SELECT 1").unwrap();
        let (ast, plan) = ctx.rewrite("EXECUTE test_stmt").unwrap();

        let sql = ast.deparse().unwrap();
        assert!(
            sql.contains("__pgdog_"),
            "EXECUTE should use global name, got: {sql}"
        );
        assert_eq!(plan.prepares.len(), 1);

        let (name, statement) = &plan.prepares[0];
        assert!(name.starts_with("__pgdog_"));
        assert_eq!(statement, "SELECT 1");
    }

    #[test]
    fn test_rewrite_execute_with_params() {
        let mut ctx = TestContext::new();
        ctx.rewrite("PREPARE test_stmt AS SELECT $1, $2").unwrap();
        let (ast, plan) = ctx.rewrite("EXECUTE test_stmt(1, 'hello')").unwrap();

        let sql = ast.deparse().unwrap();
        assert!(
            sql.contains("__pgdog_"),
            "EXECUTE should use global name, got: {sql}"
        );
        assert!(
            sql.contains("(1, 'hello')"),
            "EXECUTE params should be preserved, got: {sql}"
        );
        assert_eq!(plan.prepares.len(), 1);
    }

    #[test]
    fn test_execute_nonexistent_fails() {
        let mut ctx = TestContext::new();
        let result = ctx.rewrite("EXECUTE nonexistent_stmt");
        assert!(result.is_err());
    }

    #[test]
    fn test_no_rewrite_for_regular_select() {
        let mut ctx = TestContext::new();
        let (ast, plan) = ctx.rewrite("SELECT 1, 2, 3").unwrap();

        let sql = ast.deparse().unwrap();
        assert_eq!(sql, "SELECT 1, 2, 3");
        assert!(plan.prepares.is_empty());
        assert!(plan.stmt.is_none());
    }
}
