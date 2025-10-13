//! Handle INSERT statements.
use pg_query::{protobuf::*, NodeEnum};
use tracing::debug;

use crate::{
    backend::ShardingSchema,
    frontend::router::{
        round_robin,
        sharding::{ContextBuilder, Tables, Value as ShardingValue},
    },
    net::Bind,
};

use super::{Column, Error, Shard, Table, Tuple, Value};

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
            .map(Column::try_from)
            .collect::<Result<Vec<Column<'a>>, ()>>()
            .ok()
            .unwrap_or(vec![])
    }

    /// Get table name, if specified (should always be).
    pub fn table(&'a self) -> Option<Table<'a>> {
        self.stmt.relation.as_ref().map(Table::from)
    }

    /// Get rows from the statement.
    pub fn tuples(&'a self) -> Vec<Tuple<'a>> {
        if let Some(select) = &self.stmt.select_stmt {
            if let Some(NodeEnum::SelectStmt(stmt)) = &select.node {
                let tuples = stmt
                    .values_lists
                    .iter()
                    .map(Tuple::try_from)
                    .collect::<Result<Vec<Tuple<'a>>, ()>>();
                return tuples.unwrap_or(vec![]);
            }
        }

        vec![]
    }

    /// Get the sharding key for the statement.
    pub fn shard(
        &'a self,
        schema: &'a ShardingSchema,
        bind: Option<&Bind>,
    ) -> Result<Shard, Error> {
        let tables = Tables::new(schema);
        let columns = self.columns();
        let table = self.table();
        let key = table.and_then(|table| tables.key(table, &columns));

        if let Some(key) = key {
            if let Some(bind) = bind {
                if let Ok(Some(param)) = bind.parameter(key.position) {
                    // Arrays not supported as sharding keys at the moment.
                    let value = ShardingValue::from_param(&param, key.table.data_type)?;
                    let ctx = ContextBuilder::new(key.table)
                        .value(value)
                        .shards(schema.shards)
                        .build()?;
                    return Ok(ctx.apply()?);
                }
            }

            let tuples = self.tuples();

            // TODO: support rewriting INSERTs to run against multiple shards.
            if tuples.len() != 1 {
                debug!("multiple tuples in an INSERT statement");
                return Ok(Shard::All);
            }

            if let Some(value) = tuples.first().and_then(|tuple| tuple.get(key.position)) {
                match value {
                    Value::Integer(int) => {
                        let ctx = ContextBuilder::new(key.table)
                            .data(*int)
                            .shards(schema.shards)
                            .build()?;
                        return Ok(ctx.apply()?);
                    }

                    Value::String(str) => {
                        let ctx = ContextBuilder::new(key.table)
                            .data(*str)
                            .shards(schema.shards)
                            .build()?;
                        return Ok(ctx.apply()?);
                    }

                    _ => (),
                }
            }
        } else if let Some(table) = table {
            // If this table is sharded, but the sharding key isn't in the query,
            // choose a shard at random.
            if tables.sharded(table).is_some() {
                return Ok(Shard::Direct(round_robin::next() % schema.shards));
            }
        }

        Ok(Shard::All)
    }
}

#[cfg(test)]
mod test {
    use pg_query::{parse, NodeEnum};

    use crate::backend::ShardedTables;
    use crate::config::ShardedTable;
    use crate::net::bind::Parameter;
    use crate::net::Format;
    use bytes::Bytes;

    use super::super::Value;
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
                        schema: None,
                        alias: None,
                    })
                );
                assert_eq!(
                    insert.columns(),
                    vec![
                        Column {
                            name: "id",
                            ..Default::default()
                        },
                        Column {
                            name: "email",
                            ..Default::default()
                        }
                    ]
                );
            }

            _ => panic!("not an insert"),
        }
    }

    #[test]
    fn test_insert_params() {
        let query = parse("INSERT INTO my_table (id, email) VALUES ($1, $2)").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                assert_eq!(
                    insert.tuples(),
                    vec![Tuple {
                        values: vec![Value::Placeholder(1), Value::Placeholder(2),]
                    }]
                )
            }

            _ => panic!("not an insert"),
        }
    }

    #[test]
    fn test_insert_typecasts() {
        let query =
            parse("INSERT INTO sharded (id, value) VALUES ($1::INTEGER, $2::VARCHAR)").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                assert_eq!(
                    insert.tuples(),
                    vec![Tuple {
                        values: vec![Value::Placeholder(1), Value::Placeholder(2),]
                    }]
                )
            }

            _ => panic!("not an insert"),
        }
    }

    #[test]
    fn test_shard_insert() {
        let query = parse("INSERT INTO sharded (id, value) VALUES (1, 'test')").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let schema = ShardingSchema {
            shards: 3,
            tables: ShardedTables::new(
                vec![
                    ShardedTable {
                        name: Some("sharded".into()),
                        column: "id".into(),
                        ..Default::default()
                    },
                    ShardedTable {
                        name: None,
                        column: "user_id".into(),
                        ..Default::default()
                    },
                ],
                vec![],
            ),
        };

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let shard = insert.shard(&schema, None).unwrap();
                assert!(matches!(shard, Shard::Direct(2)));

                let bind = Bind::new_params(
                    "",
                    &[Parameter {
                        len: 1,
                        data: "3".as_bytes().into(),
                    }],
                );

                let shard = insert.shard(&schema, Some(&bind)).unwrap();
                assert!(matches!(shard, Shard::Direct(1)));

                let bind = Bind::new_params_codes(
                    "",
                    &[Parameter {
                        len: 8,
                        data: Bytes::copy_from_slice(&234_i64.to_be_bytes()),
                    }],
                    &[Format::Binary],
                );

                let shard = insert.shard(&schema, Some(&bind)).unwrap();
                assert!(matches!(shard, Shard::Direct(0)));
            }

            _ => panic!("not an insert"),
        }

        let query = parse("INSERT INTO orders (user_id, value) VALUES (1, 'test')").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let shard = insert.shard(&schema, None).unwrap();
                assert!(matches!(shard, Shard::Direct(2)));
            }

            _ => panic!("not a select"),
        }

        let query = parse("INSERT INTO random_table (users_id, value) VALUES (1, 'test')").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let shard = insert.shard(&schema, None).unwrap();
                assert!(matches!(shard, Shard::All));
            }

            _ => panic!("not a select"),
        }

        // Round robin test.
        let query = parse("INSERT INTO sharded (value) VALUES ('test')").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let shard = insert.shard(&schema, None).unwrap();
                assert!(matches!(shard, Shard::Direct(_)));
            }

            _ => panic!("not a select"),
        }
    }
}
