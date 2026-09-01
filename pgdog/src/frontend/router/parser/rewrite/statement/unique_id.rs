use super::StatementRewrite;
use pg_raw_parse::{Node, make};

impl StatementRewrite<'_> {
    /// Attempt to rewrite a pgdog.unique_id() call.
    ///
    /// Returns `Ok(Some(replacement_node))` if the node is a unique_id call,
    /// `Ok(None)` otherwise. Increments `next_param` when in extended mode.
    pub(super) fn rewrite_unique_id<'mem>(
        node: Node<'_>,
        mem: make::MemoryToken<'mem>,
        extended: bool,
        next_param: &mut i32,
    ) -> Result<Option<make::Unique<'mem, Node<'mem>>>, super::Error> {
        if !Self::is_unique_id(node) {
            return Ok(None);
        }

        let replacement = if extended {
            let param_ref = mem.make_param_ref(*next_param);
            *next_param += 1;
            param_ref.uncast()
        } else {
            use pg_raw_parse::ConstValue;

            let unique_id = crate::unique_id::UniqueId::generator()?.next_id();
            mem.make_a_const(ConstValue::Float(&unique_id.to_string()))
                .uncast()
        };

        Ok(Some(
            mem.make_type_cast(
                replacement,
                mem.make_list(&[
                    mem.make_string(Some("pg_catalog")),
                    mem.make_string(Some("int8")),
                ]),
            )
            .uncast(),
        ))
    }

    /// Check if a node is a function call to pgdog.unique_id().
    fn is_unique_id(node: Node<'_>) -> bool {
        let Node::FuncCall(func) = node else {
            return false;
        };

        func.funcname()
            .iter()
            .filter_map(Node::as_str)
            .eq(["pgdog", "unique_id"])
    }
}

#[cfg(test)]
mod tests {
    use pgdog_config::Rewrite;

    use super::*;
    use crate::backend::ShardingSchema;
    use crate::backend::schema::Schema;
    use crate::frontend::PreparedStatements;
    use crate::frontend::router::parser::StatementRewriteContext;
    use crate::frontend::router::parser::rewrite::statement::RewritePlan;
    use crate::test_utils::set_env_var;
    use pg_raw_parse::{Owned, nodes};

    fn default_schema() -> ShardingSchema {
        ShardingSchema {
            shards: 1,
            rewrite: Rewrite {
                enabled: true,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn default_db_schema() -> Schema {
        Schema::default()
    }

    fn parse_first_target(sql: &str) -> Owned<nodes::ResTarget> {
        let ast = pg_raw_parse::parse(sql).unwrap();
        match ast.stmts().next().unwrap() {
            Node::SelectStmt(select) => {
                make::owned(|mem| mem.make_unique(select.target_list().first().unwrap()))
            }
            _ => panic!("expected SelectStmt"),
        }
    }

    #[test]
    fn test_is_unique_id_qualified() {
        let node = parse_first_target("SELECT pgdog.unique_id()");
        assert!(StatementRewrite::is_unique_id(node.val()));
    }

    #[test]
    fn test_is_unique_id_unqualified() {
        let node = parse_first_target("SELECT unique_id()");
        assert!(!StatementRewrite::is_unique_id(node.val()));
    }

    #[test]
    fn test_is_unique_id_wrong_schema() {
        let node = parse_first_target("SELECT other.unique_id()");
        assert!(!StatementRewrite::is_unique_id(node.val()));
    }

    #[test]
    fn test_is_unique_id_wrong_function() {
        let node = parse_first_target("SELECT pgdog.other_func()");
        assert!(!StatementRewrite::is_unique_id(node.val()));
    }

    #[test]
    fn test_is_unique_id_not_function() {
        let node = parse_first_target("SELECT 1");
        assert!(!StatementRewrite::is_unique_id(node.val()));
    }

    #[test]
    fn test_rewrite_select_extended_single() {
        let (sql, plan) = run_test("SELECT pgdog.unique_id()", true);

        assert_eq!(sql, "SELECT $1::bigint");
        assert_eq!(plan.params, 0);
        assert_eq!(plan.unique_ids, 1);
    }

    #[test]
    fn test_rewrite_select_extended_with_existing_params() {
        let (sql, plan) = run_test("SELECT pgdog.unique_id(), $1, $2", true);

        assert_eq!(sql, "SELECT $3::bigint, $1, $2");
        assert_eq!(plan.params, 2);
        assert_eq!(plan.unique_ids, 1);
    }

    #[test]
    fn test_rewrite_select_extended_multiple_unique_ids() {
        let (sql, plan) = run_test("SELECT pgdog.unique_id(), pgdog.unique_id()", true);

        assert_eq!(sql, "SELECT $1::bigint, $2::bigint");
        assert_eq!(plan.params, 0);
        assert_eq!(plan.unique_ids, 2);
    }

    #[test]
    fn test_rewrite_select_simple() {
        let _guard = set_env_var("NODE_ID", "pgdog-1");
        let (sql, plan) = run_test("SELECT pgdog.unique_id()", false);

        assert!(
            !sql.contains("pgdog.unique_id"),
            "Function should be replaced: {sql}"
        );
        assert_eq!(plan.params, 0);
        assert_eq!(plan.unique_ids, 1);
    }

    #[test]
    fn test_rewrite_select_simple_multiple_unique_ids() {
        let _guard = set_env_var("NODE_ID", "pgdog-1");
        let (sql, plan) = run_test("SELECT pgdog.unique_id(), pgdog.unique_id()", false);

        // Each unique_id call should get a different value
        assert!(
            !sql.contains("pgdog.unique_id"),
            "Functions should be replaced: {sql}"
        );
        assert_eq!(plan.unique_ids, 2);
    }

    #[test]
    fn test_rewrite_no_unique_id() {
        let (sql, plan) = run_test("SELECT 1, 2, 3", true);

        assert_eq!(sql, "SELECT 1, 2, 3");
        assert_eq!(plan.unique_ids, 0);
    }

    #[test]
    fn test_rewrite_insert_values() {
        let (sql, plan) = run_test(
            "INSERT INTO t (id, name) VALUES (pgdog.unique_id(), 'test')",
            true,
        );

        assert_eq!(sql, "INSERT INTO t (id, name) VALUES ($1::bigint, 'test')");
        assert_eq!(plan.unique_ids, 1);
    }

    #[test]
    fn test_rewrite_insert_multiple_rows() {
        let (sql, plan) = run_test(
            "INSERT INTO t (id) VALUES (pgdog.unique_id()), (pgdog.unique_id())",
            true,
        );

        assert_eq!(sql, "INSERT INTO t (id) VALUES ($1::bigint), ($2::bigint)");
        assert_eq!(plan.unique_ids, 2);
    }

    #[test]
    fn test_rewrite_insert_select() {
        let (sql, plan) = run_test("INSERT INTO t (id) SELECT pgdog.unique_id() FROM s", true);

        assert_eq!(sql, "INSERT INTO t (id) SELECT $1::bigint FROM s");
        assert_eq!(plan.unique_ids, 1);
    }

    #[test]
    fn test_rewrite_update_set() {
        let (sql, plan) = run_test(
            "UPDATE t SET id = pgdog.unique_id() WHERE name = 'test'",
            true,
        );

        assert_eq!(sql, "UPDATE t SET id = $1::bigint WHERE name = 'test'");
        assert_eq!(plan.unique_ids, 1);
    }

    #[test]
    fn test_rewrite_update_where() {
        let (sql, plan) = run_test(
            "UPDATE t SET name = 'new' WHERE id = pgdog.unique_id()",
            true,
        );

        assert_eq!(sql, "UPDATE t SET name = 'new' WHERE id = $1::bigint");
        assert_eq!(plan.unique_ids, 1);
    }

    #[test]
    fn test_rewrite_delete_where() {
        let (sql, plan) = run_test("DELETE FROM t WHERE id = pgdog.unique_id()", true);

        assert_eq!(sql, "DELETE FROM t WHERE id = $1::bigint");
        assert_eq!(plan.unique_ids, 1);
    }

    #[test]
    fn test_rewrite_insert_returning() {
        let (sql, plan) = run_test(
            "INSERT INTO t (id) VALUES (pgdog.unique_id()) RETURNING pgdog.unique_id()",
            true,
        );

        assert_eq!(
            sql,
            "INSERT INTO t (id) VALUES ($1::bigint) RETURNING $2::bigint"
        );
        assert_eq!(plan.unique_ids, 2);
    }

    #[test]
    fn test_rewrite_explain_insert_select() {
        let (sql, plan) = run_test(
            "EXPLAIN INSERT INTO t (id) SELECT pgdog.unique_id() FROM s",
            true,
        );

        assert_eq!(sql, "EXPLAIN INSERT INTO t (id) SELECT $1::bigint FROM s");
        assert_eq!(plan.unique_ids, 1);
    }

    #[test]
    fn test_rewrite_explain_select() {
        let (sql, plan) = run_test("EXPLAIN SELECT pgdog.unique_id()", true);

        assert_eq!(sql, "EXPLAIN SELECT $1::bigint");
        assert_eq!(plan.unique_ids, 1);
    }

    fn run_test(sql: &str, extended: bool) -> (String, RewritePlan) {
        let stmt = pg_raw_parse::parse(sql).unwrap();
        let mut ps = PreparedStatements::default();
        let schema = default_schema();
        let db_schema = default_db_schema();
        let mut rewrite = StatementRewrite::new(StatementRewriteContext {
            extended,
            prepared_statements: &mut ps,
            schema: &schema,
            db_schema: &db_schema,
            user: "",
            search_path: None,
        });
        let mut plan = Default::default();
        let ast = make::owned(|mem| {
            let mut copy = mem.make_unique(&*stmt.into_inner());
            let stmt = copy.as_mut().into_iter().next().unwrap();
            plan = rewrite.maybe_rewrite(stmt, mem).unwrap();
            copy
        });
        let sql = pg_raw_parse::deparse_stmts(&*ast).unwrap();
        (sql, plan)
    }
}
