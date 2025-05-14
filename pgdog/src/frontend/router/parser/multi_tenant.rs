use pg_query::{NodeEnum, ParseResult};

use super::Error;
use crate::{
    backend::Schema,
    config::MultiTenant,
    frontend::{
        router::parser::{Table, WhereClause},
        SearchPath,
    },
    net::Parameters,
};

pub struct MultiTenantCheck<'a> {
    user: &'a str,
    config: &'a MultiTenant,
    schema: Schema,
    ast: &'a ParseResult,
    parameters: &'a Parameters,
}

impl<'a> MultiTenantCheck<'a> {
    pub fn new(
        user: &'a str,
        config: &'a MultiTenant,
        schema: Schema,
        ast: &'a ParseResult,
        parameters: &'a Parameters,
    ) -> Self {
        Self {
            config,
            schema,
            ast,
            parameters,
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
                let where_clause = WhereClause::new(table.map(|t| t.name), &stmt.where_clause);
                if let Some(table) = table {
                    self.check(table, where_clause)?;
                }
            }
            Some(NodeEnum::SelectStmt(stmt)) => {
                let table = Table::try_from(&stmt.from_clause).ok();
                let where_clause = WhereClause::new(table.map(|t| t.name), &stmt.where_clause);

                if let Some(table) = table {
                    self.check(table, where_clause)?;
                }
            }
            Some(NodeEnum::DeleteStmt(stmt)) => {
                let table = stmt.relation.as_ref().map(Table::from);
                let where_clause = WhereClause::new(table.map(|t| t.name), &stmt.where_clause);

                if let Some(table) = table {
                    self.check(table, where_clause)?;
                }
            }

            _ => (),
        }
        Ok(())
    }

    fn check(&self, table: Table, where_clause: Option<WhereClause>) -> Result<(), Error> {
        let search_path = SearchPath::new(self.user, self.parameters, &self.schema);
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
