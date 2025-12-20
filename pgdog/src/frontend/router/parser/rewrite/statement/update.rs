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

    let mut params = HashMap::new();

    visit_and_mutate_nodes(&mut select, |node| -> Result<Option<Node>, Error> {
        if let Some(NodeEnum::ParamRef(ref mut param)) = node.node {
            let number = params.len() + 1;
            params.insert(param.number as u16, number);
            param.number = number as i32;
        }

        Ok(None)
    })?;

    let mut params: Vec<_> = params.keys().copied().collect();
    params.sort();

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

    let mut params = HashMap::new();

    visit_and_mutate_nodes(&mut delete, |node| -> Result<Option<Node>, Error> {
        if let Some(NodeEnum::ParamRef(ref mut param)) = node.node {
            let number = params.len() + 1;
            params.insert(param.number as u16, number);
            param.number = number as i32;
        }

        Ok(None)
    })?;

    let mut params: Vec<_> = params.keys().copied().collect();
    params.sort();

    let delete = pg_query::ParseResult::new(delete, "".into());

    let delete = Statement {
        stmt: delete.deparse()?.into(),
        ast: Ast::from_parse_result(delete),
        params: vec![],
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
}
