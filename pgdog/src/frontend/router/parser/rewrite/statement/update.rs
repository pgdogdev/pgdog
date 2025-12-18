use pg_query::protobuf::{DeleteStmt, ResTarget, SelectStmt, UpdateStmt};
use pg_query::{Node, NodeEnum};

use crate::frontend::router::parser::{Column, Table};
use crate::frontend::router::Ast;

use super::*;

#[derive(Debug, Clone)]
pub(crate) struct ShardingKeyStatement {
    /// The deparsed statement we can execute.
    pub(crate) stmt: String,
    /// The statement AST so we can parse it and route it.
    pub(crate) ast: Ast,
    /// The parameter positions in the Bind message,
    /// so we can construct a Bind message for this statement.
    pub(crate) params: Vec<u16>,
}

#[derive(Debug, Clone)]
pub(crate) struct ShardingKeyUpdate {
    pub(crate) select: ShardingKeyStatement,
    pub(crate) delete: ShardingKeyStatement,
}

impl StatementRewrite<'_> {
    /// Checks if the statement is updating the sharding key, e.g.:
    /// `UPDATE sharded SET id = [new value] WHERE cond = x`.
    ///
    /// If this is the case, it generates 2 statements:
    ///
    /// - `SELECT * FROM table WHERE [filters from the WHERE clause]`
    /// - `DELETE FROM table WHERE [same filters]
    ///
    /// Additionally, we'll need to generate an `INSERT` statement but that
    /// has to happen once we run the `SELECT` statement to discover all
    /// the columns.
    ///
    pub(super) fn sharding_key_update(&mut self, plan: &mut RewritePlan) -> Result<(), Error> {
        let stmt = self
            .stmt
            .stmts
            .first()
            .map(|stmt| stmt.stmt.as_ref())
            .flatten();
        let stmt = if let Some(NodeEnum::UpdateStmt(stmt)) =
            stmt.as_ref().map(|stmt| stmt.node).flatten()
        {
            stmt
        } else {
            return Ok(());
        };

        let Some(relation) = stmt.relation.as_ref() else {
            return Ok(());
        };

        let table = Table::from(relation);

        if !self.is_updating_sharding_key(&stmt, &table) {
            return Ok(());
        }

        let params = collect_where_params(&stmt.where_clause);

        let select = self.build_select_statement(&stmt, &params)?;
        let delete = self.build_delete_statement(&stmt, &params)?;

        plan.sharding_key_update = Some(ShardingKeyUpdate { select, delete });

        Ok(())
    }

    fn build_delete_statement(
        &self,
        update_stmt: &UpdateStmt,
        params: Vec<u16>,
    ) -> Result<ShardingKeyStatement, Error> {
        let delete_stmt = DeleteStmt {
            relation: update_stmt.relation.clone(),
            where_clause: update_stmt.where_clause.clone(),
            ..Default::default()
        };

        let stmt = NodeEnum::DeleteStmt(Box::new(delete_stmt))
            .deparse()
            .map_err(Error::PgQuery)?;
        let ast = Ast::new_record(&stmt).map_err(|e| Error::Cache(e.to_string()))?;

        Ok(ShardingKeyStatement { stmt, ast, params })
    }

    fn get_update_stmt(&self) -> Option<&UpdateStmt> {
        let stmt = self.stmt.stmts.first()?;
        let node = stmt.stmt.as_ref()?;

        if let Some(NodeEnum::UpdateStmt(ref update)) = node.node {
            Some(update)
        } else {
            None
        }
    }

    /// Check if the UPDATE query is updating the sharding key column.
    fn is_updating_sharding_key(&self, update_stmt: &UpdateStmt, table: &Table<'_>) -> bool {
        for target in &update_stmt.target_list {
            if let Some(NodeEnum::ResTarget(ref res)) = target.node {
                if let Some(column_name) = Self::get_target_column(res) {
                    let column = Column {
                        name: column_name,
                        table: Some(table.name),
                        schema: table.schema,
                    };

                    if self.schema.tables.get_table(column).is_some() {
                        return true;
                    }
                }
            }
        }

        false
    }

    fn get_target_column(res: &ResTarget) -> Option<&str> {
        if !res.name.is_empty() {
            return Some(&res.name);
        }

        if res.indirection.len() == 1 {
            if let Some(NodeEnum::String(ref value)) = res.indirection[0].node {
                return Some(&value.sval);
            }
        }

        None
    }
}

fn build_select_statement(
    update_stmt: &UpdateStmt,
    params: Vec<u16>,
) -> Result<ShardingKeyStatement, Error> {
    let relation = update_stmt.relation.clone();

    let select = SelectStmt {
        where_clause: update_stmt.where_clause.clone(),
        from_clause: vec![Node {
            node: Some(NodeEnum::RangeVar(relation.unwrap())),
        }],
        target_list: vec![],
        ..Default::default()
    };

    // Build a template SELECT * FROM table query
    let table_name = &relation.relname;
    let schema_prefix = if relation.schemaname.is_empty() {
        String::new()
    } else {
        format!("{}.", relation.schemaname)
    };
    let template = format!("SELECT * FROM {}{}", schema_prefix, table_name);

    // Parse the template to get properly structured AST nodes
    let parsed = pg_query::parse(&template).map_err(Error::PgQuery)?;
    let node = parsed
        .protobuf
        .stmts
        .first()
        .and_then(|s| s.stmt.as_ref())
        .ok_or_else(|| Error::Cache("failed to parse SELECT template".into()))?;

    let Some(NodeEnum::SelectStmt(ref select)) = node.node else {
        return Err(Error::Cache("parsed template is not SelectStmt".into()));
    };

    // Clone the parsed SELECT and add the WHERE clause from the UPDATE
    let mut new_select = (**select).clone();
    new_select.where_clause = update_stmt.where_clause.clone();

    let stmt = NodeEnum::SelectStmt(Box::new(new_select))
        .deparse()
        .map_err(Error::PgQuery)?;
    let ast = Ast::new_record(&stmt).map_err(|e| Error::Cache(e.to_string()))?;

    Ok(ShardingKeyStatement { stmt, ast, params })
}

fn collect_where_params(where_clause: &mut Option<Box<Node>>) -> Vec<u16> {
    use super::visitor::visit_and_mutate_children;

    let Some(ref mut where_node) = where_clause else {
        return Vec::new();
    };

    let mut params = Vec::new();

    // Check the root node
    if let Some(NodeEnum::ParamRef(ref param)) = where_node.node {
        if param.number > 0 {
            params.push((param.number - 1) as u16);
        }
    }

    // Use visitor to traverse all children
    if let Some(ref mut inner) = where_node.node {
        let _: Result<(), std::convert::Infallible> =
            visit_and_mutate_children(&mut inner, &mut |node| {
                if let Some(NodeEnum::ParamRef(ref param)) = node.node {
                    if param.number > 0 {
                        params.push((param.number - 1) as u16);
                    }
                }
                Ok(None)
            });
    }

    params
}

#[cfg(test)]
mod tests {
    use pg_query::protobuf::{AStar, ColumnRef, RangeVar, SelectStmt};
    use pgdog_config::{Rewrite, ShardedTable};

    use super::*;
    use crate::backend::replication::{ShardedSchemas, ShardedTables};
    use crate::backend::ShardingSchema;
    use crate::frontend::router::parser::StatementRewriteContext;
    use crate::frontend::PreparedStatements;

    fn sharding_schema_with_table(table_name: &str, column: &str) -> ShardingSchema {
        ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    name: Some(table_name.into()),
                    column: column.into(),
                    ..Default::default()
                }],
                vec![],
            ),
            schemas: ShardedSchemas::default(),
            rewrite: Rewrite {
                enabled: true,
                ..Default::default()
            },
        }
    }

    #[test]
    fn test_delete_stmt_deparse() {
        let parsed = pg_query::parse("UPDATE sharded SET id = 5 WHERE id = 1").unwrap();
        let stmt = parsed
            .protobuf
            .stmts
            .first()
            .unwrap()
            .stmt
            .as_ref()
            .unwrap();

        if let Some(NodeEnum::UpdateStmt(ref update)) = stmt.node {
            let delete_stmt = DeleteStmt {
                relation: update.relation.clone(),
                where_clause: update.where_clause.clone(),
                ..Default::default()
            };

            let sql = NodeEnum::DeleteStmt(Box::new(delete_stmt))
                .deparse()
                .expect("DELETE deparse should work");
            println!("DELETE SQL: {}", sql);
            assert!(sql.contains("DELETE"));
        }
    }

    #[test]
    fn test_select_stmt_deparse() {
        // Parse a real SELECT to get the structure we need
        let select_parsed = pg_query::parse("SELECT * FROM sharded").unwrap();
        let select_stmt_node = select_parsed
            .protobuf
            .stmts
            .first()
            .unwrap()
            .stmt
            .as_ref()
            .unwrap();

        if let Some(NodeEnum::SelectStmt(ref select)) = select_stmt_node.node {
            // Clone the select and modify where clause
            let update_parsed = pg_query::parse("UPDATE sharded SET id = 5 WHERE id = 1").unwrap();
            let update_stmt_node = update_parsed
                .protobuf
                .stmts
                .first()
                .unwrap()
                .stmt
                .as_ref()
                .unwrap();

            if let Some(NodeEnum::UpdateStmt(ref update)) = update_stmt_node.node {
                // Build select using the real select's target_list and from_clause structure
                let mut new_select = (**select).clone();
                new_select.where_clause = update.where_clause.clone();

                let sql = NodeEnum::SelectStmt(Box::new(new_select))
                    .deparse()
                    .expect("SELECT deparse should work");
                println!("SELECT SQL: {}", sql);
                assert!(sql.contains("SELECT"));
                assert!(sql.contains("WHERE"));
            }
        }
    }

    #[test]
    fn test_detects_sharding_key_update() {
        let mut ast = pg_query::parse("UPDATE sharded SET id = 5 WHERE id = 1")
            .unwrap()
            .protobuf;
        let mut prepared = PreparedStatements::default();
        let schema = sharding_schema_with_table("sharded", "id");

        let mut rewriter = StatementRewrite::new(StatementRewriteContext {
            stmt: &mut ast,
            extended: false,
            prepared: false,
            prepared_statements: &mut prepared,
            schema: &schema,
        });

        let mut plan = RewritePlan::default();
        rewriter.sharding_key_update(&mut plan).unwrap();

        assert!(
            plan.sharding_key_update.is_some(),
            "should detect sharding key update"
        );

        let update = plan.sharding_key_update.unwrap();
        assert!(
            update.select.stmt.contains("SELECT"),
            "should generate SELECT statement"
        );
        assert!(
            update.delete.stmt.contains("DELETE"),
            "should generate DELETE statement"
        );
    }

    #[test]
    fn test_ignores_non_sharding_key_update() {
        let mut ast = pg_query::parse("UPDATE sharded SET value = 'test' WHERE id = 1")
            .unwrap()
            .protobuf;
        let mut prepared = PreparedStatements::default();
        let schema = sharding_schema_with_table("sharded", "id");

        let mut rewriter = StatementRewrite::new(StatementRewriteContext {
            stmt: &mut ast,
            extended: false,
            prepared: false,
            prepared_statements: &mut prepared,
            schema: &schema,
        });

        let mut plan = RewritePlan::default();
        rewriter.sharding_key_update(&mut plan).unwrap();

        assert!(
            plan.sharding_key_update.is_none(),
            "should not detect sharding key update when not modifying sharding column"
        );
    }

    #[test]
    fn test_ignores_non_update_statement() {
        let mut ast = pg_query::parse("SELECT * FROM sharded WHERE id = 1")
            .unwrap()
            .protobuf;
        let mut prepared = PreparedStatements::default();
        let schema = sharding_schema_with_table("sharded", "id");

        let mut rewriter = StatementRewrite::new(StatementRewriteContext {
            stmt: &mut ast,
            extended: false,
            prepared: false,
            prepared_statements: &mut prepared,
            schema: &schema,
        });

        let mut plan = RewritePlan::default();
        rewriter.sharding_key_update(&mut plan).unwrap();

        assert!(
            plan.sharding_key_update.is_none(),
            "should not detect sharding key update for SELECT"
        );
    }

    #[test]
    fn test_examine_select_structure() {
        // Parse a SELECT * FROM schema.table to see the exact structure
        let parsed = pg_query::parse("SELECT * FROM myschema.mytable").unwrap();
        println!("Parsed SELECT structure:\n{:#?}", parsed.protobuf);
    }

    #[test]
    fn test_construct_select_from_rangevar() {
        // Parse UPDATE to get a RangeVar
        let update_parsed =
            pg_query::parse("UPDATE myschema.mytable SET id = 5 WHERE id = 1").unwrap();
        let update_node = update_parsed
            .protobuf
            .stmts
            .first()
            .unwrap()
            .stmt
            .as_ref()
            .unwrap();

        let Some(NodeEnum::UpdateStmt(ref update)) = update_node.node else {
            panic!("Expected UpdateStmt");
        };

        // Get the relation (RangeVar) from the UPDATE statement
        let relation = update.relation.clone();

        // Build a SELECT * FROM table using the RangeVar
        // We need to wrap the RangeVar in a Node for the from_clause
        let from_node = pg_query::Node {
            node: Some(NodeEnum::RangeVar(relation.unwrap())),
        };

        // Build AStar for SELECT *
        let a_star = AStar {};
        let column_ref = ColumnRef {
            fields: vec![pg_query::Node {
                node: Some(NodeEnum::AStar(a_star)),
            }],
            location: 0,
        };
        let res_target = ResTarget {
            name: String::new(),
            indirection: vec![],
            val: Some(Box::new(pg_query::Node {
                node: Some(NodeEnum::ColumnRef(Box::new(column_ref))),
            })),
            location: 0,
        };
        let target_node = pg_query::Node {
            node: Some(NodeEnum::ResTarget(Box::new(res_target))),
        };

        // Build the SelectStmt
        let select_stmt = SelectStmt {
            target_list: vec![target_node],
            from_clause: vec![from_node],
            where_clause: update.where_clause.clone(),
            ..Default::default()
        };

        // Try to deparse
        let sql = NodeEnum::SelectStmt(Box::new(select_stmt))
            .deparse()
            .expect("SELECT deparse should work");
        println!("Constructed SELECT SQL: {}", sql);
        assert!(sql.contains("SELECT"));
        assert!(sql.contains("myschema"));
        assert!(sql.contains("mytable"));
        assert!(sql.contains("WHERE"));
    }
}
