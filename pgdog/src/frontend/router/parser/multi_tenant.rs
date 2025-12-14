use pg_query::{protobuf::Param, NodeEnum, ParseResult};

use super::Error;
use crate::{
    backend::Schema,
    config::MultiTenant,
    frontend::{
        router::parser::{where_clause::TablesSource, Table, WhereClause},
        SearchPath,
    },
    net::{parameter::ParameterValue, Parameters},
};

pub struct MultiTenantCheck<'a> {
    user: &'a str,
    config: &'a MultiTenant,
    schema: Schema,
    ast: &'a ParseResult,
    search_path: Option<&'a ParameterValue>,
}

impl<'a> MultiTenantCheck<'a> {
    pub fn new(
        user: &'a str,
        config: &'a MultiTenant,
        schema: Schema,
        ast: &'a ParseResult,
        search_path: Option<&'a ParameterValue>,
    ) -> Self {
        Self {
            config,
            schema,
            ast,
            search_path,
            user,
        }
    }

    pub fn run(&self) -> Result<(), Error> {
        let stmt = self
            .ast
            .protobuf
            .stmts
            .first()
            .and_then(|s| s.stmt.as_ref());

        match stmt.and_then(|n| n.node.as_ref()) {
            Some(NodeEnum::UpdateStmt(stmt)) => {
                let table = stmt.relation.as_ref().map(Table::from);

                if let Some(table) = table {
                    let source = TablesSource::from(table);
                    let where_clause = WhereClause::new(&source, &stmt.where_clause);
                    self.check(table, where_clause)?;
                }
            }
            Some(NodeEnum::SelectStmt(stmt)) => {
                let table = Table::try_from(&stmt.from_clause).ok();

                if let Some(table) = table {
                    let source = TablesSource::from(table);
                    let where_clause = WhereClause::new(&source, &stmt.where_clause);
                    self.check(table, where_clause)?;
                }
            }
            Some(NodeEnum::DeleteStmt(stmt)) => {
                let table = stmt.relation.as_ref().map(Table::from);

                if let Some(table) = table {
                    let source = TablesSource::from(table);
                    let where_clause = WhereClause::new(&source, &stmt.where_clause);
                    self.check(table, where_clause)?;
                }
            }

            _ => (),
        }
        Ok(())
    }

    fn check(&self, table: Table, where_clause: Option<WhereClause>) -> Result<(), Error> {
        let search_path = SearchPath::new(self.user, self.search_path, &self.schema);
        let schemas = search_path.resolve();

        for schema in schemas {
            let schema_table = self
                .schema
                .get(&(schema.to_owned(), table.name.to_string()));
            if let Some(schema_table) = schema_table {
                let has_tenant_id = schema_table.columns().contains_key(&self.config.column);
                if !has_tenant_id {
                    continue;
                }

                let check = where_clause
                    .as_ref()
                    .map(|w| !w.keys(Some(table.name), &self.config.column).is_empty());
                if let Some(true) = check {
                    return Ok(());
                } else {
                    return Err(Error::MultiTenantId);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::backend::schema::{columns::Column, Relation, Schema};
    use std::collections::HashMap;

    fn schema_with_tenant_column(column: &str) -> Schema {
        let mut columns = HashMap::new();
        columns.insert(
            column.to_string(),
            Column {
                table_catalog: "catalog".into(),
                table_schema: "public".into(),
                table_name: "accounts".into(),
                column_name: column.into(),
                column_default: String::new(),
                is_nullable: false,
                data_type: "bigint".into(),
            },
        );

        let relation = Relation::test_table("public", "accounts", columns);
        let mut relations = HashMap::new();
        relations.insert(("public".into(), "accounts".into()), relation);

        Schema::from_parts(vec!["$user".into(), "public".into()], relations)
    }

    #[test]
    fn multi_tenant_check_passes_with_matching_filter() {
        let schema = schema_with_tenant_column("tenant_id");
        let ast = pg_query::parse("SELECT * FROM accounts WHERE tenant_id = 1")
            .expect("parse select statement");
        let config = MultiTenant {
            column: "tenant_id".into(),
        };

        let check = MultiTenantCheck::new("alice", &config, schema, &ast, None);
        assert!(check.run().is_ok());
    }

    #[test]
    fn multi_tenant_check_requires_tenant_column_in_filter() {
        let schema = schema_with_tenant_column("tenant_id");
        let ast = pg_query::parse("SELECT * FROM accounts WHERE other_id = 1")
            .expect("parse select statement");
        let config = MultiTenant {
            column: "tenant_id".into(),
        };

        let check = MultiTenantCheck::new("alice", &config, schema, &ast, None);
        let err = check
            .run()
            .expect_err("expected tenant id validation error");
        matches!(err, Error::MultiTenantId)
            .then_some(())
            .expect("should return multi-tenant id error");
    }
}
