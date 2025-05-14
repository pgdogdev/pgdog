use std::sync::Arc;

use pg_query::{protobuf::UpdateStmt, NodeEnum, ParseResult};

use super::Error;
use crate::{
    backend::{schema::relation::TABLES, Schema},
    config::MultiTenant,
    frontend::router::parser::{Table, WhereClause},
};

pub struct MultiTenantCheck<'a> {
    config: &'a MultiTenant,
    schema: Schema,
    ast: &'a ParseResult,
}

impl<'a> MultiTenantCheck<'a> {
    pub fn new(config: &'a MultiTenant, schema: Schema, ast: &'a ParseResult) -> Self {
        Self {
            config,
            schema,
            ast,
        }
    }

    pub fn run(&self) -> Result<(), Error> {
        let stmt = self
            .ast
            .protobuf
            .stmts
            .first()
            .map(|s| s.stmt.as_ref())
            .flatten();

        match stmt.map(|n| n.node.as_ref()).flatten() {
            Some(NodeEnum::UpdateStmt(stmt)) => {
                let table = stmt
                    .relation
                    .as_ref()
                    .map(Table::from)
                    .ok_or(Error::MultiTenantId)?;
                let schema_table = self.schema.table(&table.name, Some("pgdog"));

                println!("{:?}, {:?}", schema_table, table);
                println!("{:?}", self.schema);

                if let Some(schema_table) = schema_table {
                    let has_tenant_id = schema_table.columns().contains_key(&self.config.column);
                    if !has_tenant_id {
                        return Ok(());
                    }
                    let where_clause = WhereClause::new(Some(&table.name), &stmt.where_clause)
                        .ok_or(Error::MultiTenantId)?;
                    let missing_column = where_clause
                        .keys(Some(&table.name), &self.config.column)
                        .is_empty();
                    return if !missing_column {
                        Ok(())
                    } else {
                        Err(Error::MultiTenantId)
                    };
                }
            }
            Some(NodeEnum::SelectStmt(stmt)) => {}
            Some(NodeEnum::DeleteStmt(stmt)) => {}

            _ => (),
        }
        Ok(())
    }
}
