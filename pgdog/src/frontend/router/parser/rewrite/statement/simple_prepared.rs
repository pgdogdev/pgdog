use bytes::Bytes;
use pg_raw_parse::{NodeMut, make::MemoryToken};

use crate::{
    frontend::PreparedStatements,
    net::{PREPARE_TEMPLATE_NAME, Prepare},
};

use super::{Error, StatementRewrite};

#[derive(Debug, Clone)]
pub(crate) enum PrepareExecute {
    /// PREPARE statement sent by client
    Prepare(Prepare),
    /// EXECUTE statement sent by client and may require
    /// a PREPARE first.
    Execute(Prepare),
}

/// Result of rewriting all PREPARE/EXECUTE statements in a query.
#[derive(Debug, Clone, Default)]
pub(crate) struct SimplePreparedResult {
    /// Whether any statement was rewritten.
    pub(crate) rewritten: bool,
    /// Prepared statements to prepend (name, statement) for EXECUTE rewrites.
    pub(crate) rewrites: Vec<PrepareExecute>,
}

/// Result of rewriting a single PREPARE or EXECUTE SQL command.
#[derive(Debug, Clone)]
enum SimplePreparedRewrite {
    /// Node was not a PREPARE or EXECUTE statement.
    None,
    /// PREPARE statement was rewritten.
    Prepared { prepare: Prepare },
    /// EXECUTE statement was rewritten. Contains the global name and statement
    /// needed to prepend a ProtocolMessage::Prepare.
    Executed { prepare: Prepare },
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
    pub(super) fn rewrite_simple_prepared<'a>(
        &mut self,
        node: NodeMut<'a, '_>,
        mem: MemoryToken<'a>,
    ) -> Result<SimplePreparedResult, Error> {
        let mut result = SimplePreparedResult::default();

        if !self.prepared_statements.level.full() {
            return Ok(result);
        }

        match rewrite_single_prepared(node, mem, self.prepared_statements)? {
            SimplePreparedRewrite::Prepared { prepare } => {
                result.rewrites.push(PrepareExecute::Prepare(prepare));
                result.rewritten = true;
            }
            SimplePreparedRewrite::Executed { prepare } => {
                result.rewrites.push(PrepareExecute::Execute(prepare));
                result.rewritten = true;
            }
            SimplePreparedRewrite::None => {}
        }

        Ok(result)
    }
}

/// Rewrites a single `PREPARE` or `EXECUTE` node.
fn rewrite_single_prepared<'a>(
    node: NodeMut<'a, '_>,
    mem: MemoryToken<'a>,
    prepared_statements: &mut PreparedStatements,
) -> Result<SimplePreparedRewrite, Error> {
    match node {
        NodeMut::PrepareStmt(mut stmt) => {
            let client_name = stmt.name().expect("prepare must have a name").to_owned();

            // Create a globally unique key using the query text
            // with a hardcoded name.
            stmt.set_name(Some(mem.copy_string(PREPARE_TEMPLATE_NAME)));
            let query = Bytes::from(pg_raw_parse::deparse(&*stmt)?.as_str().to_owned());

            let prepare = prepared_statements.insert_prepare(&client_name, query);

            stmt.set_name(Some(mem.copy_string(prepare.name())));

            Ok(SimplePreparedRewrite::Prepared { prepare })
        }

        NodeMut::ExecuteStmt(mut stmt) => {
            let stmt_name = stmt.name().expect("EXECUTE always has name");
            let prepare = prepared_statements.prepare(stmt_name);
            if let Some(prepare) = prepare {
                stmt.set_name(Some(mem.copy_string(prepare.name())));
                Ok(SimplePreparedRewrite::Executed { prepare })
            } else {
                Err(Error::ExecuteMissingPrepare(stmt_name.to_owned()))
            }
        }

        _ => Ok(SimplePreparedRewrite::None),
    }
}

#[cfg(test)]
mod tests {
    use super::super::{RewritePlan, StatementRewrite, StatementRewriteContext};
    use super::*;
    use crate::backend::ShardingSchema;
    use crate::backend::schema::Schema;
    use crate::config::PreparedStatements as PreparedStatementsLevel;
    use pgdog_config::Rewrite;

    struct TestContext {
        ps: PreparedStatements,
        schema: ShardingSchema,
        db_schema: Schema,
    }

    impl TestContext {
        fn new() -> Self {
            let mut ps = PreparedStatements::default();
            ps.set_level(PreparedStatementsLevel::Full);
            Self {
                ps,
                schema: ShardingSchema {
                    shards: 1,
                    rewrite: Rewrite {
                        enabled: true,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                db_schema: Schema::default(),
            }
        }

        fn rewrite(&mut self, sql: &str) -> Result<(String, RewritePlan), Error> {
            let stmt = pg_raw_parse::parse(sql)?;
            let mut rewrite = StatementRewrite::new(StatementRewriteContext {
                extended: false,
                prepared: false,
                prepared_statements: &mut self.ps,
                schema: &self.schema,
                db_schema: &self.db_schema,
                user: "",
                search_path: None,
            });
            let mut plan = Default::default();
            let ast = pg_raw_parse::make::try_owned(|mem| {
                let mut copy = mem.make_unique(&*stmt.into_inner());
                plan = rewrite.maybe_rewrite(copy.as_mut().into_iter().next().unwrap(), mem)?;
                Ok::<_, Error>(copy)
            })?;
            let sql = pg_raw_parse::deparse_stmts(&*ast)?;
            Ok((sql, plan))
        }
    }

    #[test]
    fn test_rewrite_prepare() {
        let mut ctx = TestContext::new();
        let (sql, plan) = ctx.rewrite("PREPARE test_stmt AS SELECT $1, $2").unwrap();

        assert!(
            sql.contains("__pgdog_"),
            "PREPARE should be renamed to __pgdog_N, got: {sql}"
        );
        assert!(
            !sql.contains("test_stmt"),
            "original name should be replaced: {sql}"
        );
        assert!(plan.prepare_rewrites.is_empty());
        assert!(plan.stmt.is_some());
    }

    #[test]
    fn test_rewrite_execute() {
        let mut ctx = TestContext::new();
        ctx.rewrite("PREPARE test_stmt AS SELECT 1").unwrap();
        let (sql, plan) = ctx.rewrite("EXECUTE test_stmt").unwrap();

        assert!(
            sql.contains("__pgdog_"),
            "EXECUTE should use global name, got: {sql}"
        );
        assert_eq!(plan.prepare_rewrites.len(), 1);

        let prepare = &plan.prepare_rewrites[0];
        match prepare {
            PrepareExecute::Execute(prepare) => {
                assert!(prepare.name().starts_with("__pgdog_"));
                assert_eq!(prepare.query(), "SELECT 1");
            }

            _ => panic!("expected PrepareExecute::Execute"),
        }
    }

    #[test]
    fn test_rewrite_execute_with_params() {
        let mut ctx = TestContext::new();
        ctx.rewrite("PREPARE test_stmt AS SELECT $1, $2").unwrap();
        let (sql, plan) = ctx.rewrite("EXECUTE test_stmt(1, 'hello')").unwrap();

        assert!(
            sql.contains("__pgdog_"),
            "EXECUTE should use global name, got: {sql}"
        );
        assert!(
            sql.contains("(1, 'hello')"),
            "EXECUTE params should be preserved, got: {sql}"
        );
        assert_eq!(plan.prepare_rewrites.len(), 1);
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
        let (sql, plan) = ctx.rewrite("SELECT 1, 2, 3").unwrap();

        assert_eq!(sql, "SELECT 1, 2, 3");
        assert!(plan.prepare_rewrites.is_empty());
        assert!(plan.stmt.is_none());
    }
}
