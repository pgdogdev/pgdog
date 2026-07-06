use pg_query::protobuf::{AConst, ParamRef, String as PgString, TypeCast, TypeName, a_const::Val};
use pg_query::{Node, NodeEnum};

use super::StatementRewrite;

impl StatementRewrite<'_> {
    /// Attempt to rewrite a pgdog.unique_id() call.
    ///
    /// Returns `Ok(Some(replacement_node))` if the node is a unique_id call,
    /// `Ok(None)` otherwise. Increments `next_param` when in extended mode.
    pub(super) fn rewrite_unique_id(
        node: &Node,
        extended: bool,
        next_param: &mut i32,
    ) -> Result<Option<Node>, super::Error> {
        if !Self::is_unique_id(node) {
            return Ok(None);
        }

        let replacement = if extended {
            let param_num = *next_param;
            *next_param += 1;
            Self::param_bigint(param_num)
        } else {
            let unique_id = crate::unique_id::UniqueId::generator()?.next_id();
            Self::literal_bigint(unique_id)
        };

        Ok(Some(replacement))
    }

    /// Create a parameter reference cast to bigint: $N::bigint
    fn param_bigint(number: i32) -> Node {
        let param_ref = Node {
            node: Some(NodeEnum::ParamRef(ParamRef {
                number,
                ..Default::default()
            })),
        };

        Node {
            node: Some(NodeEnum::TypeCast(Box::new(TypeCast {
                arg: Some(Box::new(param_ref)),
                type_name: Some(Self::bigint_type()),
                ..Default::default()
            }))),
        }
    }

    /// Create a literal value cast to bigint: <value>::bigint
    fn literal_bigint(value: i64) -> Node {
        let literal = Node {
            node: Some(NodeEnum::AConst(AConst {
                val: Some(Val::Sval(PgString {
                    sval: value.to_string(),
                })),
                ..Default::default()
            })),
        };

        Node {
            node: Some(NodeEnum::TypeCast(Box::new(TypeCast {
                arg: Some(Box::new(literal)),
                type_name: Some(Self::bigint_type()),
                ..Default::default()
            }))),
        }
    }

    /// Create a TypeName for bigint (int8).
    fn bigint_type() -> TypeName {
        TypeName {
            names: vec![
                Node {
                    node: Some(NodeEnum::String(PgString {
                        sval: "pg_catalog".to_string(),
                    })),
                },
                Node {
                    node: Some(NodeEnum::String(PgString {
                        sval: "int8".to_string(),
                    })),
                },
            ],
            ..Default::default()
        }
    }

    /// Check if a node is a function call to pgdog.unique_id().
    fn is_unique_id(node: &Node) -> bool {
        let Some(NodeEnum::FuncCall(func)) = &node.node else {
            return false;
        };

        // Must have exactly 2 parts: schema "pgdog" and function "unique_id"
        if func.funcname.len() != 2 {
            return false;
        }

        let schema = func.funcname.first().and_then(|n| match &n.node {
            Some(NodeEnum::String(s)) => Some(s.sval.as_str()),
            _ => None,
        });

        let name = func.funcname.get(1).and_then(|n| match &n.node {
            Some(NodeEnum::String(s)) => Some(s.sval.as_str()),
            _ => None,
        });

        matches!((schema, name), (Some("pgdog"), Some("unique_id")))
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
    use pg_query::protobuf::ParseResult;

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

    fn parse_first_target(sql: &str) -> Node {
        let ast = pg_query::parse(sql).unwrap();
        let stmt = ast.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        match &stmt.node {
            Some(NodeEnum::SelectStmt(select)) => {
                let res_target = select.target_list.first().unwrap();
                match &res_target.node {
                    Some(NodeEnum::ResTarget(res)) => *res.val.as_ref().unwrap().clone(),
                    _ => panic!("expected ResTarget"),
                }
            }
            _ => panic!("expected SelectStmt"),
        }
    }

    #[test]
    fn test_is_unique_id_qualified() {
        let node = parse_first_target("SELECT pgdog.unique_id()");
        assert!(StatementRewrite::is_unique_id(&node));
    }

    #[test]
    fn test_is_unique_id_unqualified() {
        let node = parse_first_target("SELECT unique_id()");
        assert!(!StatementRewrite::is_unique_id(&node));
    }

    #[test]
    fn test_is_unique_id_wrong_schema() {
        let node = parse_first_target("SELECT other.unique_id()");
        assert!(!StatementRewrite::is_unique_id(&node));
    }

    #[test]
    fn test_is_unique_id_wrong_function() {
        let node = parse_first_target("SELECT pgdog.other_func()");
        assert!(!StatementRewrite::is_unique_id(&node));
    }

    #[test]
    fn test_is_unique_id_not_function() {
        let node = parse_first_target("SELECT 1");
        assert!(!StatementRewrite::is_unique_id(&node));
    }

    #[test]
    fn test_rewrite_select_extended_single() {
        let (ast, plan) = run_test("SELECT pgdog.unique_id()", true);

        let sql = ast.deparse().unwrap();
        assert_eq!(sql, "SELECT $1::bigint");
        assert_eq!(plan.params, 0);
        assert_eq!(plan.unique_ids, 1);
    }

    #[test]
    fn test_rewrite_select_extended_with_existing_params() {
        let (ast, plan) = run_test("SELECT pgdog.unique_id(), $1, $2", true);

        let sql = ast.deparse().unwrap();
        assert_eq!(sql, "SELECT $3::bigint, $1, $2");
        assert_eq!(plan.params, 2);
        assert_eq!(plan.unique_ids, 1);
    }

    #[test]
    fn test_rewrite_select_extended_multiple_unique_ids() {
        let (ast, plan) = run_test("SELECT pgdog.unique_id(), pgdog.unique_id()", true);

        let sql = ast.deparse().unwrap();
        assert_eq!(sql, "SELECT $1::bigint, $2::bigint");
        assert_eq!(plan.params, 0);
        assert_eq!(plan.unique_ids, 2);
    }

    #[test]
    fn test_rewrite_select_simple() {
        let _guard = set_env_var("NODE_ID", "pgdog-1");
        let (ast, plan) = run_test("SELECT pgdog.unique_id()", false);

        let sql = ast.deparse().unwrap();
        // Value should be a bigint literal cast
        assert!(
            sql.contains("::bigint"),
            "Expected ::bigint cast, got: {sql}"
        );
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
        let (ast, plan) = run_test("SELECT pgdog.unique_id(), pgdog.unique_id()", false);

        let sql = ast.deparse().unwrap();
        // Each unique_id call should get a different value
        assert!(
            !sql.contains("pgdog.unique_id"),
            "Functions should be replaced: {sql}"
        );
        assert_eq!(plan.unique_ids, 2);
    }

    #[test]
    fn test_rewrite_no_unique_id() {
        let (ast, plan) = run_test("SELECT 1, 2, 3", true);

        let sql = ast.deparse().unwrap();
        assert_eq!(sql, "SELECT 1, 2, 3");
        assert_eq!(plan.unique_ids, 0);
    }

    #[test]
    fn test_rewrite_insert_values() {
        let (ast, plan) = run_test(
            "INSERT INTO t (id, name) VALUES (pgdog.unique_id(), 'test')",
            true,
        );

        let sql = ast.deparse().unwrap();
        assert_eq!(sql, "INSERT INTO t (id, name) VALUES ($1::bigint, 'test')");
        assert_eq!(plan.unique_ids, 1);
    }

    #[test]
    fn test_rewrite_insert_multiple_rows() {
        let (ast, plan) = run_test(
            "INSERT INTO t (id) VALUES (pgdog.unique_id()), (pgdog.unique_id())",
            true,
        );

        let sql = ast.deparse().unwrap();
        assert_eq!(sql, "INSERT INTO t (id) VALUES ($1::bigint), ($2::bigint)");
        assert_eq!(plan.unique_ids, 2);
    }

    #[test]
    fn test_rewrite_insert_select() {
        let (ast, plan) = run_test("INSERT INTO t (id) SELECT pgdog.unique_id() FROM s", true);

        let sql = ast.deparse().unwrap();
        assert_eq!(sql, "INSERT INTO t (id) SELECT $1::bigint FROM s");
        assert_eq!(plan.unique_ids, 1);
    }

    #[test]
    fn test_rewrite_update_set() {
        let (ast, plan) = run_test(
            "UPDATE t SET id = pgdog.unique_id() WHERE name = 'test'",
            true,
        );

        let sql = ast.deparse().unwrap();
        assert_eq!(sql, "UPDATE t SET id = $1::bigint WHERE name = 'test'");
        assert_eq!(plan.unique_ids, 1);
    }

    #[test]
    fn test_rewrite_update_where() {
        let (ast, plan) = run_test(
            "UPDATE t SET name = 'new' WHERE id = pgdog.unique_id()",
            true,
        );

        let sql = ast.deparse().unwrap();
        assert_eq!(sql, "UPDATE t SET name = 'new' WHERE id = $1::bigint");
        assert_eq!(plan.unique_ids, 1);
    }

    #[test]
    fn test_rewrite_delete_where() {
        let (ast, plan) = run_test("DELETE FROM t WHERE id = pgdog.unique_id()", true);

        let sql = ast.deparse().unwrap();
        assert_eq!(sql, "DELETE FROM t WHERE id = $1::bigint");
        assert_eq!(plan.unique_ids, 1);
    }

    #[test]
    fn test_rewrite_insert_returning() {
        let (ast, plan) = run_test(
            "INSERT INTO t (id) VALUES (pgdog.unique_id()) RETURNING pgdog.unique_id()",
            true,
        );

        let sql = ast.deparse().unwrap();
        assert_eq!(
            sql,
            "INSERT INTO t (id) VALUES ($1::bigint) RETURNING $2::bigint"
        );
        assert_eq!(plan.unique_ids, 2);
    }

    #[test]
    fn test_rewrite_explain_insert_select() {
        let (ast, plan) = run_test(
            "EXPLAIN INSERT INTO t (id) SELECT pgdog.unique_id() FROM s",
            true,
        );

        let sql = ast.deparse().unwrap();
        assert_eq!(sql, "EXPLAIN INSERT INTO t (id) SELECT $1::bigint FROM s");
        assert_eq!(plan.unique_ids, 1);
    }

    #[test]
    fn test_rewrite_explain_select() {
        let (ast, plan) = run_test("EXPLAIN SELECT pgdog.unique_id()", true);

        let sql = ast.deparse().unwrap();
        assert_eq!(sql, "EXPLAIN SELECT $1::bigint");
        assert_eq!(plan.unique_ids, 1);
    }

    fn run_test(sql: &str, extended: bool) -> (ParseResult, RewritePlan) {
        let mut ast = pg_query::parse(sql).unwrap().protobuf;
        #[cfg(feature = "new_parser")]
        let stmt = pg_raw_parse::parse(sql).unwrap();
        let mut ps = PreparedStatements::default();
        let schema = default_schema();
        let db_schema = default_db_schema();
        let mut rewrite = StatementRewrite::new(StatementRewriteContext {
            stmt: &mut ast,
            extended,
            prepared: false,
            prepared_statements: &mut ps,
            schema: &schema,
            db_schema: &db_schema,
            user: "",
            search_path: None,
        });
        let plan = rewrite
            .maybe_rewrite(
                #[cfg(feature = "new_parser")]
                stmt.stmts().next().unwrap(),
            )
            .unwrap();
        (ast, plan)
    }
}
