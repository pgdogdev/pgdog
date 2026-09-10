use bytes::Bytes;
use pg_raw_parse::{
    ConstValue, NodeMut,
    make::MemoryToken,
    nodes::{ExecuteStmtMut, ParamRef, PrepareStmtMut},
};

use crate::{
    frontend::{
        PreparedStatements,
        router::parser::{Limit, rewrite::statement::offset::OffsetPlan},
    },
    net::{PREPARE_TEMPLATE_NAME, Prepare},
    unique_id::UniqueId,
};

use super::{Error, RewritePlan, StatementRewrite};

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
        plan: &mut RewritePlan,
    ) -> Result<SimplePreparedResult, Error> {
        let mut result = SimplePreparedResult::default();

        if !self.prepared_statements.level.full() {
            return Ok(result);
        }

        match rewrite_single_prepared(node, mem, self.prepared_statements, plan)? {
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
    plan: &mut RewritePlan,
) -> Result<SimplePreparedRewrite, Error> {
    match node {
        NodeMut::PrepareStmt(mut stmt) => {
            let client_name = stmt.name().expect("prepare must have a name").to_owned();

            // Create a globally unique key using the query text
            // with a hardcoded name.
            stmt.set_name(Some(mem.copy_string(PREPARE_TEMPLATE_NAME)));

            let original_query = Bytes::from(pg_raw_parse::deparse(&*stmt)?.as_str().to_owned());

            // Is the query a SELECT? Do we have both LIMIT and OFFSET in the SELECT?
            let offset_plan: Option<OffsetPlan> = create_offset_plan(mem, &mut stmt);

            let new_query = offset_plan
                .as_ref()
                .map(|_| {
                    pg_raw_parse::deparse(&*stmt)
                        .map(|deparse_result| Bytes::from(deparse_result.as_str().to_owned()))
                })
                .transpose()?;

            let prepare = prepared_statements.insert_prepare(
                &client_name,
                original_query,
                new_query,
                plan,
                offset_plan,
            );

            stmt.set_name(Some(mem.copy_string(prepare.name())));

            Ok(SimplePreparedRewrite::Prepared { prepare })
        }

        NodeMut::ExecuteStmt(mut stmt) => {
            let stmt_name = stmt.name().expect("EXECUTE always has name");

            if let Some((prepare, unique_ids, offset_plan)) =
                prepared_statements.prepare_and_unique_ids(stmt_name)
            {
                if let Some(mut offset_plan) = offset_plan {
                    // Note: This needs to be ordered before the offset_val/limit_val adjustment.
                    insert_offset_params(&mut stmt, mem, &offset_plan);
                    update_offset_plan_fields(&mut offset_plan, &mut stmt)?;

                    plan.offset = Some(offset_plan);
                }

                // Rewrite EXECUTE statement to match the rewrite
                // we did on the PREPARE statement.
                insert_unique_ids(&mut stmt, mem, unique_ids)?;

                stmt.set_name(Some(mem.copy_string(prepare.name())));
                Ok(SimplePreparedRewrite::Executed { prepare })
            } else {
                Err(Error::ExecuteMissingPrepare(stmt_name.to_owned()))
            }
        }

        _ => Ok(SimplePreparedRewrite::None),
    }
}

/// Helper method for `rewrite_single_prepared` for `ExecuteStatement`
/// Replace the cached OffsetPlan's un-resolved values
/// (originally `ParamRef` nodes; weren't `A_Const` nodes in `PrepareStmt`)
/// with the ones now provided within the `ExecuteStmt`
fn update_offset_plan_fields<'a>(
    offset_plan: &mut OffsetPlan,
    stmt: &mut ExecuteStmtMut<'a, '_>,
) -> Result<(), Error> {
    if offset_plan.limit.offset.is_none() {
        let pg_raw_parse::Node::A_Const(constant) = stmt
            .params()
            .get(offset_plan.offset_param - 1)
            .ok_or(Error::IncorrectExecuteParameters)?
        else {
            return Err(Error::IncorrectExecuteParameters);
        };

        offset_plan.limit.offset = Some(
            constant
                .val()
                .ok_or(Error::IncorrectExecuteParameters)?
                .numeric_value::<i32>()
                .ok_or(Error::IncorrectExecuteParameters)? as usize,
        );
    }

    if offset_plan.limit.limit.is_none() {
        let pg_raw_parse::Node::A_Const(constant) = stmt
            .params()
            .get(offset_plan.limit_param - 1)
            .ok_or(Error::IncorrectExecuteParameters)?
        else {
            return Err(Error::IncorrectExecuteParameters);
        };

        offset_plan.limit.limit = Some(
            constant
                .val()
                .ok_or(Error::IncorrectExecuteParameters)?
                .numeric_value::<i32>()
                .ok_or(Error::IncorrectExecuteParameters)? as usize,
        );
    }

    Ok(())
}

/// Helper method for `rewrite_single_prepared` for `PreparedStatement`
/// to create an `OffsetPlan` based on the SELECT query inside
/// of the `PreparedStatement`, which allows us to store
/// this `OffsetPlan` in the Prepared Statement cache
/// and reference later for re-writes.
fn create_offset_plan<'a>(
    mem: MemoryToken<'a>,
    stmt: &mut PrepareStmtMut<'a, '_>,
) -> Option<OffsetPlan> {
    // Inner query must be SELECT
    let pg_raw_parse::Node::SelectStmt(stmt_query) = stmt.query() else {
        return None;
    };

    // Must have both LIMIT and OFFSET
    if matches!(stmt_query.limit_count(), pg_raw_parse::Node::None)
        || matches!(stmt_query.limit_offset(), pg_raw_parse::Node::None)
    {
        return None;
    }

    // Count the `ParamRef` nodes in the query, so that we know what number to start at
    // if we need to add some more.
    let mut param_refs_count: usize = 0;
    pg_raw_parse::walk::walk(stmt_query.into(), |node| {
        if let pg_raw_parse::Node::ParamRef(_) = node {
            param_refs_count += 1;
        }
    });

    // Make a unique copy of the Client's statement to mutate
    let mut unique_stmt = mem.make_unique(stmt_query);
    let mut unique_stmt_mut = unique_stmt.as_mut();

    // Replace `A_Const` nodes with `ParamRef` nodes.
    // This allows us to dynamically change the LIMIT/OFFSET at execution time,
    // if we have a multi-shard query.
    let (limit_param, limit_val) =
        if let pg_raw_parse::Node::A_Const(limit_count) = stmt_query.limit_count() {
            param_refs_count += 1;

            let mut param_ref_count = mem.make_node::<ParamRef>();
            param_ref_count.as_mut().set_number(param_refs_count as i32);
            unique_stmt_mut.set_limit_count(param_ref_count.uncast());

            let limit_val = limit_count
                .val()
                .and_then(|limit_val| limit_val.numeric_value::<i32>())?;

            (param_refs_count, Some(limit_val as usize))
        } else if let pg_raw_parse::Node::ParamRef(param_ref) = stmt_query.limit_count() {
            (param_ref.number as usize, None)
        } else {
            return None;
        };

    let (offset_param, offset_val) =
        if let pg_raw_parse::Node::A_Const(limit_offset) = stmt_query.limit_offset() {
            param_refs_count += 1;

            let mut param_ref_offset = mem.make_node::<ParamRef>();
            param_ref_offset
                .as_mut()
                .set_number(param_refs_count as i32);
            unique_stmt_mut.set_limit_offset(param_ref_offset.uncast());

            let limit_offset_val = limit_offset
                .val()
                .and_then(|limit_offset_val| limit_offset_val.numeric_value::<i32>())?;

            (param_refs_count, Some(limit_offset_val as usize))
        } else if let pg_raw_parse::Node::ParamRef(param_ref) = stmt_query.limit_offset() {
            (param_ref.number as usize, None)
        } else {
            return None;
        };

    // Re-writes the Client's original statement with our version.
    stmt.set_query(unique_stmt.uncast());

    Some(OffsetPlan {
        limit: Limit {
            limit: limit_val,
            offset: offset_val,
        },
        limit_param,
        offset_param,
        prepare_execute: true,
    })
}

/// Helper method for `rewrite_single_prepared` to handle injecting
/// cached constants to an `ExecuteStmt`  which we previously stripped
/// from the `PrepareStmt`, so that we could dynamically re-write later
/// if `Route` resolves to multi-shard
fn insert_offset_params<'a>(
    stmt: &mut ExecuteStmtMut<'a, '_>,
    mem: MemoryToken<'a>,
    offset_plan: &OffsetPlan,
) {
    let offset_val = offset_plan.limit.offset;
    let limit_val = offset_plan.limit.limit;
    let offset_pos = offset_plan.offset_param;
    let limit_pos = offset_plan.limit_param;

    let mut params = stmt.params_mut();

    if let Some(offset_val) = offset_val
        && let Some(limit_val) = limit_val
    {
        // In this case, both nodes from the `PrepareStmt` were `A_Const`
        // Therefore, we need to use the cached values, and inject them
        // into the `ExecuteStmt`'s `params`.
        //
        // The if statements are to determine which one goes first in params,
        // which is based on the refs ($1, $2) we chose
        //
        // These are deterministically ordered (since we do them), but it's just one
        // extra check to do this, and doesn't try to enforce an invariant.

        let limit_val_node = mem
            .make_a_const(ConstValue::Integer(limit_val as i32))
            .uncast();
        let offset_val_node = mem
            .make_a_const(ConstValue::Integer(offset_val as i32))
            .uncast();

        let (first_node, second_node) = if offset_pos > limit_pos {
            (limit_val_node, offset_val_node)
        } else {
            (offset_val_node, limit_val_node)
        };

        params.push(mem, first_node);
        params.push(mem, second_node);
    } else if let Some(offset_val) = offset_val {
        // Only OFFSET was `A_Const`
        stmt.params_mut().push(
            mem,
            mem.make_a_const(ConstValue::Integer(offset_val as i32))
                .uncast(),
        );
    } else if let Some(limit_val) = limit_val {
        // Only LIMIT was `A_Const`
        stmt.params_mut().push(
            mem,
            mem.make_a_const(ConstValue::Integer(limit_val as i32))
                .uncast(),
        );
    }
}

fn insert_unique_ids<'a>(
    stmt: &mut ExecuteStmtMut<'a, '_>,
    mem: MemoryToken<'a>,
    num_unique_ids: u16,
) -> Result<(), Error> {
    for _ in 0..num_unique_ids {
        let unique_id = UniqueId::generator()?.next_id();
        stmt.params_mut().push(
            mem,
            mem.make_a_const(ConstValue::Float(&unique_id.to_string()))
                .uncast(),
        );
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::super::{RewritePlan, StatementRewrite, StatementRewriteContext};
    use super::*;
    use crate::backend::ShardingSchema;
    use crate::backend::schema::Schema;
    use crate::config::PreparedStatementsLevel;
    use crate::test_utils::set_env_var;
    use pg_raw_parse::Node;
    use pgdog_config::Rewrite;
    use std::collections::HashSet;

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

    fn apply_plan(sql: &str, plan: &RewritePlan) -> Result<String, Error> {
        let stmt = pg_raw_parse::parse(sql)?;
        let ast = pg_raw_parse::make::try_owned(|mem| {
            let mut copy = mem.make_unique(&*stmt.into_inner());
            let mut raw_stmt = copy
                .as_mut()
                .into_iter()
                .next()
                .expect("query must contain a statement");
            let NodeMut::ExecuteStmt(mut execute) = raw_stmt.stmt_mut() else {
                panic!("expected EXECUTE statement");
            };

            insert_unique_ids(&mut execute, mem, plan.unique_ids)?;
            Ok::<_, Error>(copy)
        })?;

        Ok(pg_raw_parse::deparse_stmts(&*ast)?)
    }

    #[test]
    fn test_apply_prepare_rewrite_plan_no_unique_ids() {
        let sql = apply_plan("EXECUTE stmt(1, 'hello')", &RewritePlan::default()).unwrap();

        assert_eq!(sql, "EXECUTE stmt(1, 'hello')");
    }

    #[test]
    fn test_apply_prepare_rewrite_plan_appends_unique_ids() {
        let _guard = set_env_var("NODE_ID", "pgdog-1");
        let plan = RewritePlan {
            unique_ids: 3,
            ..Default::default()
        };
        let sql = apply_plan("EXECUTE stmt(42)", &plan).unwrap();
        let ast = pg_raw_parse::parse(&sql).unwrap();
        let Node::ExecuteStmt(execute) = ast.stmts().next().unwrap() else {
            panic!("expected EXECUTE statement");
        };

        assert_eq!(execute.params().len(), 4);
        assert!(matches!(
            execute.params().first(),
            Some(Node::A_Const(value))
                if matches!(value.val(), Some(ConstValue::Integer(42)))
        ));

        let ids: HashSet<_> = execute
            .params()
            .iter()
            .skip(1)
            .map(|param| {
                let Node::A_Const(value) = param else {
                    panic!("expected unique ID to be a constant");
                };
                let Some(ConstValue::Float(value)) = value.val() else {
                    panic!("expected unique ID to be a numeric literal");
                };

                value.parse::<i64>().expect("unique ID must be an i64")
            })
            .collect();

        assert_eq!(ids.len(), 3, "all appended IDs should be unique");
    }

    /// Prepares two statements, both using LIMIT/OFFSET. One is re-written, one isn't.
    /// They should resolve to two different entries in the `GlobalCache` (two different `CacheKeys`)
    /// and their respective `CachedStmt` entries should contain the correct `OffsetPlan` information.
    /// Integration test `test_simple_prepared_limit` tests end-to-end functionality.
    #[test]
    fn test_rewrite_prepare_offset_limit_cache_differs() {
        let saved_first_time_sql;

        let mut ctx = TestContext::new();
        {
            let (sql, plan) = ctx
                .rewrite("PREPARE test_stmt AS SELECT * FROM sharded LIMIT 5 OFFSET 10")
                .unwrap();

            saved_first_time_sql = sql.clone();

            assert!(
                sql.contains("__pgdog_"),
                "PREPARE should be renamed to __pgdog_N, got: {sql}"
            );
            assert!(
                !sql.contains("test_stmt"),
                "original name should be replaced: {sql}"
            );
            assert_eq!(plan.prepare_rewrites.len(), 1);
            assert!(plan.stmt.is_some());

            let prepare = &plan.prepare_rewrites[0];
            match prepare {
                PrepareExecute::Prepare(prepare) => {
                    assert!(prepare.name().starts_with("__pgdog_"));
                    assert_eq!(
                        prepare.query(),
                        "PREPARE __pgdog_template_name AS SELECT * FROM sharded LIMIT $1 OFFSET $2"
                    );
                }

                _ => panic!("expected PrepareExecute::Prepare"),
            }

            // Verify 1 local, 1 global before we re-try with $1, $2 in the next block.
            assert_eq!(ctx.ps.local.len(), 1);
            assert_eq!(ctx.ps.global.read().len(), 1);

            // Verify the OffsetPlan is correct from the PreparedStatement name used.
            let (fetched_prepare, _, offset_plan) =
                ctx.ps.prepare_and_unique_ids("test_stmt").unwrap();
            let offset_plan = offset_plan.unwrap();
            assert_eq!(
                fetched_prepare.query,
                "PREPARE __pgdog_template_name AS SELECT * FROM sharded LIMIT $1 OFFSET $2"
            );
            assert!(offset_plan.prepare_execute);

            // Verifies these numbers were actually saved in the cache.
            // In the next block, verify these are NOT present (correctly creating a new global entry)
            assert_eq!(offset_plan.limit.limit, Some(5));
            assert_eq!(offset_plan.limit.offset, Some(10));
        }

        // LIMIT $1 OFFSET $2; assert OffsetPlan uses None instead of Some(5), Some(10)
        //
        {
            // Notice that this is the resolved statement the last segment was re-written to (for the cache key)
            // We're using a different name here to assert they resolve differently and to different global statements.
            let (sql, plan) = ctx
                .rewrite("PREPARE test_stmt2 AS SELECT * FROM sharded LIMIT $1 OFFSET $2")
                .unwrap();

            // Ensures that the global names differ. (diff global cache entries, diff OffsetPlans)
            // "PREPARE __pgdog_1 AS SELECT * FROM sharded LIMIT $1 OFFSET $2"
            // "PREPARE __pgdog_2 AS SELECT * FROM sharded LIMIT $1 OFFSET $2"
            assert_ne!(sql, saved_first_time_sql);

            assert!(
                sql.contains("__pgdog_"),
                "PREPARE should be renamed to __pgdog_N, got: {sql}"
            );
            assert!(
                !sql.contains("test_stmt2"),
                "original name should be replaced: {sql}"
            );
            assert_eq!(plan.prepare_rewrites.len(), 1);
            assert!(plan.stmt.is_some());

            let prepare = &plan.prepare_rewrites[0];
            match prepare {
                PrepareExecute::Prepare(prepare) => {
                    assert!(prepare.name().starts_with("__pgdog_"));
                    assert_eq!(
                        prepare.query(),
                        "PREPARE __pgdog_template_name AS SELECT * FROM sharded LIMIT $1 OFFSET $2"
                    );
                }

                _ => panic!("expected PrepareExecute::Prepare"),
            }

            // Now there are **TWO** local entries.
            assert_eq!(ctx.ps.local.len(), 2);
            // Now there are **TWO** global entries.
            assert_eq!(ctx.ps.global.read().len(), 2);

            // Verify the OffsetPlan is correct using the PreparedStatement name used.
            let (fetched_prepare, _, offset_plan) =
                ctx.ps.prepare_and_unique_ids("test_stmt2").unwrap();
            let offset_plan = offset_plan.unwrap();
            assert_eq!(
                fetched_prepare.query,
                "PREPARE __pgdog_template_name AS SELECT * FROM sharded LIMIT $1 OFFSET $2"
            );
            assert!(offset_plan.prepare_execute);

            // If we saw Some(5) and Some(10) here, they would have resolved to the last block (in the same context).
            // Since they don't, that means it's correctly using the pre-re-written `Query` as the `CacheKey`.
            // They're correctly not resolving to the same `CachedStmt`.
            assert_eq!(offset_plan.limit.limit, None);
            assert_eq!(offset_plan.limit.offset, None);
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
        assert_eq!(plan.prepare_rewrites.len(), 1);
        assert!(plan.stmt.is_some());

        let prepare = &plan.prepare_rewrites[0];
        match prepare {
            PrepareExecute::Prepare(prepare) => {
                assert!(prepare.name().starts_with("__pgdog_"));
                assert_eq!(
                    prepare.query(),
                    "PREPARE __pgdog_template_name AS SELECT $1, $2"
                );
            }

            _ => panic!("expected PrepareExecute::Prepare"),
        }
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
                assert_eq!(prepare.query(), "PREPARE __pgdog_template_name AS SELECT 1");
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
