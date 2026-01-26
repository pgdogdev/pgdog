//! Auto-inject pgdog.unique_id() for missing BIGINT primary keys in INSERT statements.

use std::collections::HashSet;

use pg_query::protobuf::{FuncCall, ResTarget, String as PgString};
use pg_query::{Node, NodeEnum};
use pgdog_config::RewriteMode;

use super::{Error, RewritePlan, StatementRewrite};
use crate::frontend::router::parser::Table;

impl StatementRewrite<'_> {
    /// Handle missing BIGINT primary key columns in INSERT statements based on config.
    ///
    /// Behavior depends on `rewrite.primary_key` setting:
    /// - `ignore`: Do nothing
    /// - `error`: Return an error if a BIGINT primary key is missing
    /// - `rewrite`: Auto-inject pgdog.unique_id() for missing columns
    ///
    /// This runs before unique_id replacement so injected function calls
    /// will be processed by the unique_id rewriter.
    pub(super) fn inject_auto_id(&mut self, plan: &mut RewritePlan) -> Result<(), Error> {
        let mode = self.schema.rewrite.primary_key;

        if mode == RewriteMode::Ignore || self.schema.shards == 1 {
            return Ok(());
        }

        let Some(table) = self.get_insert_table() else {
            return Ok(());
        };

        let Some(relation) = self.db_schema.table(table, self.user, self.search_path) else {
            return Ok(());
        };

        // Get the columns specified in the INSERT (if any)
        let insert_columns = self.get_insert_column_names();

        // Find BIGINT primary key columns
        let bigint_pk_columns: Vec<&str> = relation
            .columns()
            .values()
            .filter(|col| col.is_primary_key && is_bigint_type(&col.data_type))
            .map(|col| col.column_name.as_str())
            .collect();

        if bigint_pk_columns.is_empty() {
            return Ok(());
        }

        // Find which PK columns are missing
        let missing_columns: Vec<&str> = bigint_pk_columns
            .into_iter()
            .filter(|pk_col| !insert_columns.contains(pk_col))
            .collect();

        if missing_columns.is_empty() {
            return Ok(());
        }

        match mode {
            RewriteMode::Error => {
                return Err(Error::MissingPrimaryKey {
                    table: table.name.to_string(),
                    columns: missing_columns.iter().map(|s| s.to_string()).collect(),
                });
            }
            RewriteMode::Rewrite => {
                for column in missing_columns {
                    self.inject_column_with_unique_id(column)?;
                    plan.auto_id_injected += 1;
                }
                self.rewritten = true;
            }
            RewriteMode::Ignore => unreachable!(),
        }

        Ok(())
    }

    /// Get the table from an INSERT statement.
    fn get_insert_table(&self) -> Option<Table<'_>> {
        let stmt = self.stmt.stmts.first()?;
        let node = stmt.stmt.as_ref()?;

        if let NodeEnum::InsertStmt(insert) = node.node.as_ref()? {
            let relation = insert.relation.as_ref()?;
            return Some(Table::from(relation));
        }

        None
    }

    /// Get the column names specified in the INSERT statement.
    fn get_insert_column_names(&self) -> HashSet<&str> {
        let Some(stmt) = self.stmt.stmts.first() else {
            return HashSet::new();
        };
        let Some(node) = stmt.stmt.as_ref() else {
            return HashSet::new();
        };
        let Some(NodeEnum::InsertStmt(insert)) = node.node.as_ref() else {
            return HashSet::new();
        };

        insert
            .cols
            .iter()
            .filter_map(|col| {
                if let Some(NodeEnum::ResTarget(res)) = &col.node {
                    if !res.name.is_empty() {
                        return Some(res.name.as_str());
                    }
                }
                None
            })
            .collect()
    }

    /// Inject a column with pgdog.unique_id() as the value.
    fn inject_column_with_unique_id(&mut self, column_name: &str) -> Result<(), Error> {
        let Some(stmt) = self.stmt.stmts.first_mut() else {
            return Ok(());
        };
        let Some(node) = stmt.stmt.as_mut() else {
            return Ok(());
        };
        let Some(NodeEnum::InsertStmt(insert)) = node.node.as_mut() else {
            return Ok(());
        };

        // Add the column to the column list
        let col_node = Node {
            node: Some(NodeEnum::ResTarget(Box::new(ResTarget {
                name: column_name.to_string(),
                ..Default::default()
            }))),
        };
        insert.cols.insert(0, col_node);

        // Add pgdog.unique_id() to each values list
        let Some(select) = insert.select_stmt.as_mut() else {
            return Ok(());
        };
        let Some(NodeEnum::SelectStmt(select_stmt)) = select.node.as_mut() else {
            return Ok(());
        };

        let unique_id_call = Self::unique_id_func_call();

        for values_node in &mut select_stmt.values_lists {
            if let Some(NodeEnum::List(list)) = &mut values_node.node {
                list.items.insert(0, unique_id_call.clone());
            }
        }

        Ok(())
    }

    /// Create a function call node for pgdog.unique_id().
    fn unique_id_func_call() -> Node {
        Node {
            node: Some(NodeEnum::FuncCall(Box::new(FuncCall {
                funcname: vec![
                    Node {
                        node: Some(NodeEnum::String(PgString {
                            sval: "pgdog".to_string(),
                        })),
                    },
                    Node {
                        node: Some(NodeEnum::String(PgString {
                            sval: "unique_id".to_string(),
                        })),
                    },
                ],
                args: vec![],
                func_variadic: false,
                ..Default::default()
            }))),
        }
    }
}

/// Check if a data type is a BIGINT variant.
fn is_bigint_type(data_type: &str) -> bool {
    matches!(
        data_type.to_lowercase().as_str(),
        "bigint" | "int8" | "bigserial" | "serial8"
    )
}

#[cfg(test)]
mod tests {
    use indexmap::IndexMap;
    use pgdog_config::Rewrite;
    use std::collections::HashMap;

    use super::*;
    use crate::backend::schema::columns::Column as SchemaColumn;
    use crate::backend::schema::{Relation, Schema};
    use crate::backend::ShardingSchema;
    use crate::frontend::router::parser::StatementRewriteContext;
    use crate::frontend::PreparedStatements;

    fn make_schema_with_bigint_pk() -> Schema {
        let mut columns = IndexMap::new();
        columns.insert(
            "id".to_string(),
            SchemaColumn {
                table_catalog: "test".into(),
                table_schema: "public".into(),
                table_name: "users".into(),
                column_name: "id".into(),
                column_default: String::new(),
                is_nullable: false,
                data_type: "bigint".into(),
                ordinal_position: 1,
                is_primary_key: true,
            },
        );
        columns.insert(
            "name".to_string(),
            SchemaColumn {
                table_catalog: "test".into(),
                table_schema: "public".into(),
                table_name: "users".into(),
                column_name: "name".into(),
                column_default: String::new(),
                is_nullable: true,
                data_type: "text".into(),
                ordinal_position: 2,
                is_primary_key: false,
            },
        );
        let relation = Relation::test_table("public", "users", columns);
        let relations = HashMap::from([(("public".into(), "users".into()), relation)]);
        Schema::from_parts(vec!["public".into()], relations)
    }

    fn make_schema_with_non_bigint_pk() -> Schema {
        let mut columns = IndexMap::new();
        columns.insert(
            "id".to_string(),
            SchemaColumn {
                table_catalog: "test".into(),
                table_schema: "public".into(),
                table_name: "users".into(),
                column_name: "id".into(),
                column_default: String::new(),
                is_nullable: false,
                data_type: "uuid".into(),
                ordinal_position: 1,
                is_primary_key: true,
            },
        );
        columns.insert(
            "name".to_string(),
            SchemaColumn {
                table_catalog: "test".into(),
                table_schema: "public".into(),
                table_name: "users".into(),
                column_name: "name".into(),
                column_default: String::new(),
                is_nullable: true,
                data_type: "text".into(),
                ordinal_position: 2,
                is_primary_key: false,
            },
        );
        let relation = Relation::test_table("public", "users", columns);
        let relations = HashMap::from([(("public".into(), "users".into()), relation)]);
        Schema::from_parts(vec!["public".into()], relations)
    }

    fn sharding_schema_with_mode(mode: RewriteMode) -> ShardingSchema {
        ShardingSchema {
            rewrite: Rewrite {
                primary_key: mode,
                ..Default::default()
            },
            ..Default::default()
        }
    }

    fn rewrite_sql_with_mode(
        sql: &str,
        db_schema: &Schema,
        mode: RewriteMode,
    ) -> Result<(String, RewritePlan), Error> {
        unsafe {
            std::env::set_var("NODE_ID", "pgdog-1");
        }
        let mut ast = pg_query::parse(sql).unwrap().protobuf;
        let mut prepared = PreparedStatements::default();
        let schema = sharding_schema_with_mode(mode);
        let mut rewriter = StatementRewrite::new(StatementRewriteContext {
            stmt: &mut ast,
            extended: false,
            prepared: false,
            prepared_statements: &mut prepared,
            schema: &schema,
            db_schema,
            user: "",
            search_path: None,
        });
        let plan = rewriter.maybe_rewrite()?;
        let result = if plan.stmt.is_some() {
            plan.stmt.clone().unwrap()
        } else {
            ast.deparse().unwrap()
        };
        Ok((result, plan))
    }

    #[test]
    fn test_rewrite_mode_injects_auto_id() {
        let db_schema = make_schema_with_bigint_pk();
        let (sql, plan) = rewrite_sql_with_mode(
            "INSERT INTO users (name) VALUES ('test')",
            &db_schema,
            RewriteMode::Rewrite,
        )
        .unwrap();

        assert_eq!(plan.auto_id_injected, 1);
        assert_eq!(plan.unique_ids, 1); // confirms unique_id was processed
        assert!(sql.contains("id"));
        // pgdog.unique_id() should be replaced with actual bigint value
        assert!(!sql.contains("pgdog.unique_id"));
        assert!(sql.contains("::bigint")); // value is cast to bigint
    }

    #[test]
    fn test_error_mode_returns_error() {
        let db_schema = make_schema_with_bigint_pk();
        let result = rewrite_sql_with_mode(
            "INSERT INTO users (name) VALUES ('test')",
            &db_schema,
            RewriteMode::Error,
        );

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("missing primary key"),
            "Expected MissingPrimaryKey error, got: {}",
            err
        );
    }

    #[test]
    fn test_ignore_mode_does_nothing() {
        let db_schema = make_schema_with_bigint_pk();
        let (sql, plan) = rewrite_sql_with_mode(
            "INSERT INTO users (name) VALUES ('test')",
            &db_schema,
            RewriteMode::Ignore,
        )
        .unwrap();

        assert_eq!(plan.auto_id_injected, 0);
        assert!(!sql.contains("id,"));
    }

    #[test]
    fn test_no_inject_when_pk_present() {
        let db_schema = make_schema_with_bigint_pk();
        let (sql, plan) = rewrite_sql_with_mode(
            "INSERT INTO users (id, name) VALUES (1, 'test')",
            &db_schema,
            RewriteMode::Rewrite,
        )
        .unwrap();

        assert_eq!(plan.auto_id_injected, 0);
        assert!(!sql.contains("pgdog.unique_id"));
    }

    #[test]
    fn test_no_inject_for_non_bigint_pk() {
        let db_schema = make_schema_with_non_bigint_pk();
        let (sql, plan) = rewrite_sql_with_mode(
            "INSERT INTO users (name) VALUES ('test')",
            &db_schema,
            RewriteMode::Rewrite,
        )
        .unwrap();

        assert_eq!(plan.auto_id_injected, 0);
        assert!(!sql.contains("pgdog.unique_id"));
    }

    #[test]
    fn test_no_inject_for_unknown_table() {
        let db_schema = Schema::default();
        let (_, plan) = rewrite_sql_with_mode(
            "INSERT INTO unknown (name) VALUES ('test')",
            &db_schema,
            RewriteMode::Rewrite,
        )
        .unwrap();

        assert_eq!(plan.auto_id_injected, 0);
    }

    #[test]
    fn test_inject_with_multi_row_insert() {
        let db_schema = make_schema_with_bigint_pk();
        let (sql, plan) = rewrite_sql_with_mode(
            "INSERT INTO users (name) VALUES ('a'), ('b')",
            &db_schema,
            RewriteMode::Rewrite,
        )
        .unwrap();

        assert_eq!(plan.auto_id_injected, 1);
        assert!(sql.contains("id"));
    }

    #[test]
    fn test_error_mode_ok_when_pk_present() {
        let db_schema = make_schema_with_bigint_pk();
        let result = rewrite_sql_with_mode(
            "INSERT INTO users (id, name) VALUES (1, 'test')",
            &db_schema,
            RewriteMode::Error,
        );

        assert!(result.is_ok());
    }
}
