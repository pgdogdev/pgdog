use std::collections::HashMap;

use pg_query::{
    protobuf::{
        AStar, ColumnRef, DeleteStmt, LimitOption, RangeVar, RawStmt, ResTarget, SelectStmt,
        SetOperation, UpdateStmt,
    },
    NodeEnum,
};

use crate::frontend::router::{
    parser::{rewrite::statement::visitor::visit_and_mutate_nodes, Column, Table},
    Ast,
};

use super::*;

#[derive(Debug, Clone)]
pub(crate) struct Statement {
    pub(crate) ast: Ast,
    pub(crate) stmt: String,
    pub(crate) params: Vec<u16>,
}

#[derive(Debug, Clone)]
pub(crate) struct ShardingKeyUpdate {
    pub(crate) select: Statement,
    pub(crate) delete: Statement,
}

impl StatementRewrite<'_> {
    pub(super) fn sharding_key_update(&mut self, plan: &mut RewritePlan) -> Result<(), Error> {
        let stmt = self
            .stmt
            .stmts
            .first()
            .map(|stmt| stmt.stmt.as_ref().map(|stmt| stmt.node.as_ref()))
            .flatten()
            .flatten();

        let stmt = if let Some(NodeEnum::UpdateStmt(stmt)) = stmt {
            stmt
        } else {
            return Ok(());
        };

        if !self.sharding_key_update_check(stmt) {
            return Ok(());
        }

        plan.sharding_key_update = Some(create_stmts(stmt)?);

        Ok(())
    }

    /// Check if the sharding key could be updated.
    fn sharding_key_update_check(&self, stmt: &UpdateStmt) -> bool {
        let table = if let Some(table) = stmt.relation.as_ref().map(Table::from) {
            table
        } else {
            return false;
        };

        let updating_sharding_key = stmt.target_list.iter().any(|column| {
            if let Ok(mut column) = Column::try_from(&column.node) {
                column.qualify(table);
                self.schema.tables().get_table(column).is_some()
            } else {
                false
            }
        });

        updating_sharding_key
    }
}

/// Visit all ParamRef nodes in a ParseResult and renumber them sequentially.
/// Returns a sorted list of the original parameter numbers.
fn rewrite_params(parse_result: &mut ParseResult) -> Result<Vec<u16>, Error> {
    let mut params = HashMap::new();

    visit_and_mutate_nodes(parse_result, |node| -> Result<Option<Node>, Error> {
        if let Some(NodeEnum::ParamRef(ref mut param)) = node.node {
            if let Some(existing) = params.get(&param.number) {
                param.number = *existing;
            } else {
                let number = params.len() as i32 + 1;
                params.insert(param.number, number);
                param.number = number;
            }
        }

        Ok(None)
    })?;

    let mut params: Vec<(i32, i32)> = params.into_iter().collect();
    params.sort_by(|a, b| a.1.cmp(&b.1));

    Ok(params
        .into_iter()
        .map(|(original, _)| original as u16)
        .collect())
}

fn create_stmts(stmt: &UpdateStmt) -> Result<ShardingKeyUpdate, Error> {
    let select = SelectStmt {
        target_list: vec![Node {
            node: Some(NodeEnum::ResTarget(Box::new(ResTarget {
                name: "".into(),
                val: Some(Box::new(Node {
                    node: Some(NodeEnum::ColumnRef(ColumnRef {
                        fields: vec![Node {
                            node: Some(NodeEnum::AStar(AStar {})),
                        }],
                        ..Default::default()
                    })),
                })),
                ..Default::default()
            }))),
        }],
        from_clause: vec![Node {
            node: Some(NodeEnum::RangeVar(stmt.relation.clone().unwrap())), // SAFETY: we checked the UPDATE stmt has a table name.
        }],
        limit_option: LimitOption::Default.try_into().unwrap(),
        where_clause: stmt.where_clause.clone(),
        op: SetOperation::SetopNone.try_into().unwrap(),
        ..Default::default()
    };

    let mut select = ParseResult {
        version: 170005,
        stmts: vec![RawStmt {
            stmt: Some(Box::new(Node {
                node: Some(NodeEnum::SelectStmt(Box::new(select))),
                ..Default::default()
            })),
            ..Default::default()
        }],
        ..Default::default()
    };

    let params = rewrite_params(&mut select)?;

    let select = pg_query::ParseResult::new(select, "".into());

    let select = Statement {
        stmt: select.deparse()?,
        ast: Ast::from_parse_result(select),
        params,
    };

    let delete = DeleteStmt {
        relation: stmt.relation.clone(),
        where_clause: stmt.where_clause.clone(),
        ..Default::default()
    };

    let mut delete = ParseResult {
        version: 170005,
        stmts: vec![RawStmt {
            stmt: Some(Box::new(Node {
                node: Some(NodeEnum::DeleteStmt(Box::new(delete))),
                ..Default::default()
            })),
            ..Default::default()
        }],
        ..Default::default()
    };

    let params = rewrite_params(&mut delete)?;

    let delete = pg_query::ParseResult::new(delete, "".into());

    let delete = Statement {
        stmt: delete.deparse()?.into(),
        ast: Ast::from_parse_result(delete),
        params,
    };

    Ok(ShardingKeyUpdate { select, delete })
}

#[cfg(test)]
mod test {
    use pg_query::parse;
    use pgdog_config::{Rewrite, ShardedTable};

    use crate::backend::{replication::ShardedSchemas, ShardedTables};

    use super::*;

    fn default_schema() -> ShardingSchema {
        ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    database: "pgdog".into(),
                    name: Some("sharded".into()),
                    column: "id".into(),
                    ..Default::default()
                }],
                vec![],
            ),
            schemas: ShardedSchemas::new(vec![]),
            rewrite: Rewrite {
                enabled: true,
                ..Default::default()
            },
        }
    }

    fn run_test(query: &str) -> Result<Option<ShardingKeyUpdate>, Error> {
        let mut stmt = parse(query)?;
        let schema = default_schema();
        let mut stmts = PreparedStatements::new();

        let ctx = StatementRewriteContext {
            stmt: &mut stmt.protobuf,
            schema: &schema,
            extended: true,
            prepared: false,
            prepared_statements: &mut stmts,
        };
        let mut plan = RewritePlan::default();
        StatementRewrite::new(ctx).sharding_key_update(&mut plan)?;
        Ok(plan.sharding_key_update)
    }

    #[test]
    fn test_sharding_key_update() {
        let stmt = run_test("UPDATE sharded SET id = $1 WHERE email = $2")
            .unwrap()
            .unwrap()
            .select
            .clone();
        println!("{:#?}", stmt);
    }

    #[test]
    fn test_select_basic_where_param() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2")
            .unwrap()
            .unwrap();

        // SELECT should have WHERE clause with param renumbered to $1
        assert_eq!(result.select.stmt, "SELECT * FROM sharded WHERE email = $1");
        assert_eq!(result.select.params, vec![2]);
    }

    #[test]
    fn test_select_multiple_where_params() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2 AND name = $3")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE email = $1 AND name = $2"
        );
        assert_eq!(result.select.params, vec![2, 3]);
    }

    #[test]
    fn test_select_non_sequential_params() {
        // Params in WHERE are $3 and $5, should be renumbered to $1 and $2
        let result = run_test(
            "UPDATE sharded SET id = $1, value = $2, other = $4 WHERE email = $3 AND name = $5",
        )
        .unwrap()
        .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE email = $1 AND name = $2"
        );
        assert_eq!(result.select.params, vec![3, 5]);
    }

    #[test]
    fn test_select_single_where_param() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2")
            .unwrap()
            .unwrap();

        assert_eq!(result.select.stmt, "SELECT * FROM sharded WHERE email = $1");
        assert_eq!(result.select.params, vec![2]);
    }

    #[test]
    fn test_delete_basic() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2")
            .unwrap()
            .unwrap();

        assert_eq!(result.delete.stmt, "DELETE FROM sharded WHERE email = $1");
    }

    #[test]
    fn test_delete_multiple_where_params() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2 AND name = $3")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.delete.stmt,
            "DELETE FROM sharded WHERE email = $1 AND name = $2"
        );
    }

    #[test]
    fn test_no_params_in_where() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = 'test@example.com'")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE email = 'test@example.com'"
        );
        assert_eq!(result.select.params, Vec::<u16>::new());
    }

    #[test]
    fn test_where_with_in_clause() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email IN ($2, $3, $4)")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE email IN ($1, $2, $3)"
        );
        assert_eq!(result.select.params, vec![2, 3, 4]);
    }

    #[test]
    fn test_where_with_comparison_operators() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE count > $2 AND count < $3")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE count > $1 AND count < $2"
        );
        assert_eq!(result.select.params, vec![2, 3]);
    }

    #[test]
    fn test_where_with_or_condition() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2 OR name = $3")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE email = $1 OR name = $2"
        );
        assert_eq!(result.select.params, vec![2, 3]);
    }

    #[test]
    fn test_high_param_numbers() {
        let result = run_test("UPDATE sharded SET id = $10 WHERE email = $20 AND name = $30")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE email = $1 AND name = $2"
        );
        assert_eq!(result.select.params, vec![20, 30]);
    }

    #[test]
    fn test_non_sharding_key_update_returns_none() {
        // Updating a non-sharding column should return None
        let result = run_test("UPDATE sharded SET email = $1 WHERE id = $2").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn test_where_with_like() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email LIKE $2")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE email LIKE $1"
        );
        assert_eq!(result.select.params, vec![2]);
    }

    #[test]
    fn test_where_with_is_null() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2 AND deleted_at IS NULL")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE email = $1 AND deleted_at IS NULL"
        );
        assert_eq!(result.select.params, vec![2]);
    }

    #[test]
    fn test_where_with_between() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE created_at BETWEEN $2 AND $3")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE created_at BETWEEN $1 AND $2"
        );
        assert_eq!(result.select.params, vec![2, 3]);
    }

    #[test]
    fn test_same_param_used_twice() {
        // Same parameter $2 used twice in WHERE clause
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2 OR name = $2")
            .unwrap()
            .unwrap();

        // Both occurrences should be renumbered to $1
        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE email = $1 OR name = $1"
        );
        // Only one unique param in the mapping
        assert_eq!(result.select.params, vec![2]);
    }

    #[test]
    fn test_same_param_used_multiple_times() {
        // $2 used three times
        let result = run_test("UPDATE sharded SET id = $1 WHERE a = $2 AND b = $2 AND c = $2")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE a = $1 AND b = $1 AND c = $1"
        );
        assert_eq!(result.select.params, vec![2]);
    }

    #[test]
    fn test_mixed_repeated_and_unique_params() {
        // $2 used twice, $3 used once
        let result = run_test("UPDATE sharded SET id = $1 WHERE a = $2 AND b = $3 AND c = $2")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE a = $1 AND b = $2 AND c = $1"
        );
        assert_eq!(result.select.params, vec![2, 3]);
    }

    #[test]
    fn test_repeated_params_in_in_clause() {
        // Same param repeated in IN clause (unusual but valid)
        let result = run_test("UPDATE sharded SET id = $1 WHERE email IN ($2, $3, $2)")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.select.stmt,
            "SELECT * FROM sharded WHERE email IN ($1, $2, $1)"
        );
        assert_eq!(result.select.params, vec![2, 3]);
    }

    #[test]
    fn test_delete_with_repeated_params() {
        let result = run_test("UPDATE sharded SET id = $1 WHERE email = $2 OR name = $2")
            .unwrap()
            .unwrap();

        assert_eq!(
            result.delete.stmt,
            "DELETE FROM sharded WHERE email = $1 OR name = $1"
        );
        assert_eq!(result.delete.params, vec![2]);
    }
}
