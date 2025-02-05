//! Handle INSERT statements.
use pg_query::{protobuf::*, NodeEnum};

use super::{Column, Table, Tuple};

/// Parse an `INSERT` statement.
#[derive(Debug)]
pub struct Insert<'a> {
    stmt: &'a InsertStmt,
}

impl<'a> Insert<'a> {
    /// Parse an `INSERT` statement.
    pub fn new(stmt: &'a InsertStmt) -> Self {
        Self { stmt }
    }

    /// Get columns, if any are specified.
    pub fn columns(&'a self) -> Vec<Column<'a>> {
        self.stmt
            .cols
            .iter()
            .map(|column| Column::try_from(column))
            .collect::<Result<Vec<Column<'a>>, ()>>()
            .ok()
            .unwrap_or(vec![])
    }

    /// Get table name, if specified (should always be).
    pub fn table(&self) -> Option<Table> {
        self.stmt.relation.as_ref().map(|table| Table::from(table))
    }

    /// Get rows from the statement.
    pub fn tuples(&'a self) -> Vec<Tuple<'a>> {
        if let Some(select) = &self.stmt.select_stmt {
            match &select.node {
                Some(NodeEnum::SelectStmt(stmt)) => {
                    let tuples = stmt
                        .values_lists
                        .iter()
                        .map(|values| Tuple::try_from(values))
                        .collect::<Result<Vec<Tuple<'a>>, ()>>();
                    return tuples.unwrap();
                }

                _ => (),
            }
        }

        vec![]
    }
}

#[cfg(test)]
mod test {
    use pg_query::{parse, NodeEnum};

    use super::*;

    #[test]
    fn test_insert() {
        let query = parse("INSERT INTO my_table (id, email) VALUES (1, 'test@test.com')").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                assert_eq!(
                    insert.table(),
                    Some(Table {
                        name: "my_table",
                        schema: None
                    })
                );
                assert_eq!(
                    insert.columns(),
                    vec![Column { name: "id" }, Column { name: "email" }]
                );
            }

            _ => panic!("not an insert"),
        }
    }
}
