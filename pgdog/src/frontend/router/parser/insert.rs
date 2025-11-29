//! Handle INSERT statements.
use pg_query::{protobuf::*, NodeEnum};
use std::{collections::BTreeSet, string::String as StdString};
use tracing::{debug, trace};

use crate::frontend::router::parser::rewrite::{InsertSplitPlan, InsertSplitRow};
use crate::frontend::router::parser::table::OwnedTable;
use crate::net::messages::bind::Format;
use crate::util::escape_identifier;
use crate::{
    backend::ShardingSchema,
    config::RewriteMode,
    frontend::router::{
        round_robin,
        sharding::{self, ContextBuilder, Tables, Value as ShardingValue},
    },
    net::Bind,
};

use super::{Column, Error, Route, Shard, Table, Tuple, Value};

#[derive(Debug, Clone)]
pub enum InsertRouting {
    Routed(Shard),
    Split(Box<InsertSplitPlan>),
}

impl InsertRouting {
    pub fn shard(&self) -> &Shard {
        match self {
            InsertRouting::Routed(shard) => shard,
            InsertRouting::Split(plan) => plan.route().shard(),
        }
    }
}

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

    pub fn stmt(&'a self) -> &'a InsertStmt {
        &self.stmt
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
        rewrite_enabled: bool,
        split_mode: RewriteMode,
    ) -> Result<InsertRouting, Error> {
        let tables = Tables::new(schema);
        let columns = self.columns();
        let table = self.table();
        let tuples = self.tuples();

        let key = table.and_then(|table| tables.key(table, &columns));

        if let Some(table) = table {
            // Schema-based routing.
            if let Some(schema) = schema.schemas.get(table.schema()) {
                return Ok(InsertRouting::Routed(schema.shard().into()));
            }

            if key.is_some() && tuples.len() > 1 {
                if rewrite_enabled && split_mode == RewriteMode::Rewrite {
                    let plan =
                        self.build_split_plan(&tables, schema, bind, table, &columns, &tuples);
                    trace!("rewrite plan: {:#?}", plan);
                    return plan;
                }

                if split_mode == RewriteMode::Error {
                    return Err(Error::ShardedMultiRowInsert {
                        table: table.name.to_owned(),
                        mode: split_mode,
                    });
                }
            }
        }

        if let Some(key) = key {
            if let Some(bind) = bind {
                if let Ok(Some(param)) = bind.parameter(key.position) {
                    if param.is_null() {
                        return Ok(InsertRouting::Routed(Shard::All));
                    } else {
                        // Arrays not supported as sharding keys at the moment.
                        let value = ShardingValue::from_param(&param, key.table.data_type)?;
                        let ctx = ContextBuilder::new(key.table)
                            .value(value)
                            .shards(schema.shards)
                            .build()?;
                        return Ok(InsertRouting::Routed(ctx.apply()?));
                    }
                }
            }

            // TODO: support rewriting INSERTs to run against multiple shards.
            if tuples.len() != 1 {
                debug!("multiple tuples in an INSERT statement");
                return Ok(InsertRouting::Routed(Shard::All));
            }

            if let Some(value) = tuples.first().and_then(|tuple| tuple.get(key.position)) {
                match value {
                    Value::Integer(int) => {
                        let ctx = ContextBuilder::new(key.table)
                            .data(*int)
                            .shards(schema.shards)
                            .build()?;
                        return Ok(InsertRouting::Routed(ctx.apply()?));
                    }

                    Value::String(str) => {
                        let ctx = ContextBuilder::new(key.table)
                            .data(*str)
                            .shards(schema.shards)
                            .build()?;
                        return Ok(InsertRouting::Routed(ctx.apply()?));
                    }

                    _ => (),
                }
            }
        } else if let Some(table) = table {
            // If this table is sharded, but the sharding key isn't in the query,
            // choose a shard at random.
            if tables.sharded(table).is_some() {
                return Ok(InsertRouting::Routed(Shard::Direct(
                    round_robin::next() % schema.shards,
                )));
            }
        }

        Ok(InsertRouting::Routed(Shard::All))
    }

    fn build_split_plan(
        &'a self,
        tables: &Tables<'a>,
        schema: &'a ShardingSchema,
        bind: Option<&Bind>,
        table: Table<'a>,
        columns: &[Column<'a>],
        tuples: &[Tuple<'a>],
    ) -> Result<InsertRouting, Error> {
        let key = tables
            .key(table, columns)
            .ok_or_else(|| Error::SplitInsertNotSupported {
                table: table.name.to_owned(),
                reason: "unable to determine sharding key for INSERT".into(),
            })?;

        let mut rows = Vec::with_capacity(tuples.len());
        let mut shard_indexes = Vec::with_capacity(tuples.len());

        for (tuple_index, tuple) in tuples.iter().enumerate() {
            let shard = self.compute_tuple_shard(schema, bind, &key, tuple, tuple_index, table)?;
            let values = self.tuple_values_sql(bind, tuple, tuple_index, table)?;
            shard_indexes.push(shard);
            rows.push(InsertSplitRow::new(shard, values));
        }

        let unique: BTreeSet<usize> = shard_indexes.iter().copied().collect();
        if unique.len() == 1 {
            let shard = *unique.iter().next().expect("expected shard value");
            return Ok(InsertRouting::Routed(Shard::Direct(shard)));
        }

        let columns_sql = columns
            .iter()
            .map(|column| format!("\"{}\"", escape_identifier(column.name)))
            .collect::<Vec<_>>();

        let shard_vec = unique.iter().copied().collect::<Vec<_>>();
        let route = Route::write(Shard::Multi(shard_vec));
        let plan = InsertSplitPlan::new(route, OwnedTable::from(table), columns_sql, rows);
        Ok(InsertRouting::Split(Box::new(plan)))
    }

    fn compute_tuple_shard(
        &'a self,
        schema: &'a ShardingSchema,
        bind: Option<&Bind>,
        key: &sharding::tables::Key<'a>,
        tuple: &Tuple<'a>,
        tuple_index: usize,
        table: Table<'a>,
    ) -> Result<usize, Error> {
        let value = tuple
            .get(key.position)
            .ok_or_else(|| Error::SplitInsertNotSupported {
                table: table.name.to_owned(),
                reason: format!(
                    "row {} is missing a value for sharding column \"{}\"",
                    tuple_index + 1,
                    key.table.column
                ),
            })?;

        let shard = match value {
            Value::Integer(int) => ContextBuilder::new(key.table)
                .data(*int)
                .shards(schema.shards)
                .build()?
                .apply()?,
            Value::String(str) => ContextBuilder::new(key.table)
                .data(*str)
                .shards(schema.shards)
                .build()?
                .apply()?,
            Value::Float(_) => {
                return Err(Error::SplitInsertNotSupported {
                    table: table.name.to_owned(),
                    reason: format!(
                        "row {} uses a float/decimal value for sharding column \"{}\", which is not supported as a sharding key",
                        tuple_index + 1,
                        key.table.column
                    ),
                })
            }
            Value::Null => {
                return Err(Error::SplitInsertNotSupported {
                    table: table.name.to_owned(),
                    reason: format!(
                        "row {} provides NULL for sharding column \"{}\"",
                        tuple_index + 1,
                        key.table.column
                    ),
                })
            }
            Value::Placeholder(index) => {
                let bind = bind.ok_or_else(|| Error::SplitInsertNotSupported {
                    table: table.name.to_owned(),
                    reason: format!(
                        "row {} references parameter ${}, but no Bind message was supplied",
                        tuple_index + 1,
                        index
                    ),
                })?;

                let parameter_index = (*index as usize).checked_sub(1).ok_or_else(|| {
                    Error::SplitInsertNotSupported {
                        table: table.name.to_owned(),
                        reason: format!(
                            "row {} references invalid parameter index ${}",
                            tuple_index + 1,
                            index
                        ),
                    }
                })?;

                let parameter = bind.parameter(parameter_index)?.ok_or_else(|| {
                    Error::SplitInsertNotSupported {
                        table: table.name.to_owned(),
                        reason: format!(
                            "row {} references parameter ${}, but no value was provided",
                            tuple_index + 1,
                            index
                        ),
                    }
                })?;

                if parameter.is_null() {
                    return Err(Error::SplitInsertNotSupported {
                        table: table.name.to_owned(),
                        reason: format!(
                            "row {} parameter ${} evaluates to NULL for the sharding key",
                            tuple_index + 1,
                            index
                        ),
                    });
                }

                match parameter.format() {
                    Format::Binary => {
                        return Err(Error::SplitInsertNotSupported {
                            table: table.name.to_owned(),
                            reason: format!(
                                "row {} parameter ${} uses binary format, which is not supported for split inserts",
                                tuple_index + 1,
                                index
                            ),
                        })
                    }
                    Format::Text => {
                        sharding::shard_param(&parameter, key.table, schema.shards)
                    }
                }
            }
            _ => {
                return Err(Error::SplitInsertNotSupported {
                    table: table.name.to_owned(),
                    reason: format!(
                        "row {} uses an expression that cannot be rewritten for the sharding key",
                        tuple_index + 1
                    ),
                })
            }
        };

        match shard {
            Shard::Direct(value) => Ok(value),
            Shard::Multi(_) | Shard::All => Err(Error::SplitInsertNotSupported {
                table: table.name.to_owned(),
                reason: format!(
                    "row {} produced an ambiguous shard for column \"{}\"",
                    tuple_index + 1,
                    key.table.column
                ),
            }),
        }
    }

    fn tuple_values_sql(
        &'a self,
        bind: Option<&Bind>,
        tuple: &Tuple<'a>,
        tuple_index: usize,
        table: Table<'a>,
    ) -> Result<Vec<StdString>, Error> {
        tuple
            .values
            .iter()
            .enumerate()
            .map(|(value_index, value)| {
                self.value_sql(bind, value, tuple_index, value_index, table)
            })
            .collect()
    }

    fn value_sql(
        &'a self,
        bind: Option<&Bind>,
        value: &Value<'a>,
        tuple_index: usize,
        value_index: usize,
        table: Table<'a>,
    ) -> Result<StdString, Error> {
        match value {
            Value::Integer(int) => Ok(int.to_string()),
            Value::Float(float) => Ok((*float).to_string()),
            Value::String(string) => Ok(format_literal(string)),
            Value::Boolean(boolean) => Ok(if *boolean {
                StdString::from("TRUE")
            } else {
                StdString::from("FALSE")
            }),
            Value::Null => Ok(StdString::from("NULL")),
            Value::Placeholder(index) => {
                let bind = bind.ok_or_else(|| Error::SplitInsertNotSupported {
                    table: table.name.to_owned(),
                    reason: format!(
                        "row {} references parameter ${}, but no Bind message was supplied",
                        tuple_index + 1,
                        index
                    ),
                })?;

                let parameter_index = (*index as usize).checked_sub(1).ok_or_else(|| {
                    Error::SplitInsertNotSupported {
                        table: table.name.to_owned(),
                        reason: format!(
                            "row {} references invalid parameter index ${}",
                            tuple_index + 1,
                            index
                        ),
                    }
                })?;

                let parameter = bind.parameter(parameter_index)?.ok_or_else(|| {
                    Error::SplitInsertNotSupported {
                        table: table.name.to_owned(),
                        reason: format!(
                            "row {} references parameter ${}, but no value was provided",
                            tuple_index + 1,
                            index
                        ),
                    }
                })?;

                if parameter.is_null() {
                    return Ok(StdString::from("NULL"));
                }

                match parameter.format() {
                    Format::Binary => Err(Error::SplitInsertNotSupported {
                        table: table.name.to_owned(),
                        reason: format!(
                            "row {} parameter ${} uses binary format, which is not supported for split inserts",
                            tuple_index + 1,
                            index
                        ),
                    }),
                    Format::Text => {
                        let text = parameter.text().ok_or_else(|| Error::SplitInsertNotSupported {
                            table: table.name.to_owned(),
                            reason: format!(
                                "row {} parameter ${} could not be decoded as UTF-8",
                                tuple_index + 1,
                                index
                            ),
                        })?;
                        Ok(format_literal(text))
                    }
                }
            }
            _ => Err(Error::SplitInsertNotSupported {
                table: table.name.to_owned(),
                reason: format!(
                    "row {} column {} uses an unsupported expression for split inserts",
                    tuple_index + 1,
                    value_index + 1
                ),
            }),
        }
    }
}

fn format_literal(value: &str) -> StdString {
    let escaped = value.replace('\'', "''");
    format!("'{}'", escaped)
}

#[cfg(test)]
mod test {
    use pg_query::{parse, NodeEnum};

    use crate::backend::ShardedTables;
    use crate::config::{RewriteMode, ShardedTable};
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
            ..Default::default()
        };

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let routing = insert
                    .shard(&schema, None, false, RewriteMode::Error)
                    .unwrap();
                assert!(matches!(routing.shard(), Shard::Direct(2)));

                let bind = Bind::new_params(
                    "",
                    &[Parameter {
                        len: 1,
                        data: "3".as_bytes().into(),
                    }],
                );

                let routing = insert
                    .shard(&schema, Some(&bind), false, RewriteMode::Error)
                    .unwrap();
                assert!(matches!(routing.shard(), Shard::Direct(1)));

                let bind = Bind::new_params_codes(
                    "",
                    &[Parameter {
                        len: 8,
                        data: Bytes::copy_from_slice(&234_i64.to_be_bytes()),
                    }],
                    &[Format::Binary],
                );

                let routing = insert
                    .shard(&schema, Some(&bind), false, RewriteMode::Error)
                    .unwrap();
                assert!(matches!(routing.shard(), Shard::Direct(0)));
            }

            _ => panic!("not an insert"),
        }

        let query = parse("INSERT INTO orders (user_id, value) VALUES (1, 'test')").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let routing = insert
                    .shard(&schema, None, false, RewriteMode::Error)
                    .unwrap();
                assert!(matches!(routing.shard(), Shard::Direct(2)));
            }

            _ => panic!("not a select"),
        }

        let query = parse("INSERT INTO random_table (users_id, value) VALUES (1, 'test')").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let routing = insert
                    .shard(&schema, None, false, RewriteMode::Error)
                    .unwrap();
                assert!(matches!(routing.shard(), Shard::All));
            }

            _ => panic!("not a select"),
        }

        // Round robin test.
        let query = parse("INSERT INTO sharded (value) VALUES ('test')").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let routing = insert
                    .shard(&schema, None, false, RewriteMode::Error)
                    .unwrap();
                assert!(matches!(routing.shard(), Shard::Direct(_)));
            }

            _ => panic!("not a select"),
        }
    }

    #[test]
    fn test_split_plan_multiple_shards() {
        let query = parse("INSERT INTO sharded (id, value) VALUES (1, 'a'), (11, 'b')").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let schema = ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    name: Some("sharded".into()),
                    column: "id".into(),
                    ..Default::default()
                }],
                vec![],
            ),
            ..Default::default()
        };

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let routing = insert
                    .shard(&schema, None, true, RewriteMode::Rewrite)
                    .unwrap();
                match routing {
                    InsertRouting::Split(plan) => {
                        assert_eq!(plan.rows().len(), 2);
                        assert!(plan.shard_list().len() > 1);
                    }
                    InsertRouting::Routed(_) => panic!("expected split plan"),
                }
            }
            _ => panic!("not an insert"),
        }
    }

    #[test]
    fn test_split_plan_error_mode_rejected() {
        let query = parse("INSERT INTO sharded (id, value) VALUES (1, 'a'), (11, 'b')").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let schema = ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    name: Some("sharded".into()),
                    column: "id".into(),
                    ..Default::default()
                }],
                vec![],
            ),
            ..Default::default()
        };

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let result = insert.shard(&schema, None, true, RewriteMode::Error);
                match result {
                    Err(Error::ShardedMultiRowInsert { table, mode }) => {
                        assert_eq!(table, "sharded");
                        assert_eq!(mode, RewriteMode::Error);
                    }
                    other => panic!("expected sharded multi-row error, got {:?}", other),
                }
            }
            _ => panic!("not an insert"),
        }
    }

    #[test]
    fn test_split_plan_ignore_mode_falls_back() {
        let query = parse("INSERT INTO sharded (id, value) VALUES (1, 'a'), (11, 'b')").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let schema = ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    name: Some("sharded".into()),
                    column: "id".into(),
                    ..Default::default()
                }],
                vec![],
            ),
            ..Default::default()
        };

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let routing = insert
                    .shard(&schema, None, true, RewriteMode::Ignore)
                    .expect("ignore mode should not error");
                match routing {
                    InsertRouting::Routed(shard) => {
                        assert!(matches!(shard, Shard::All));
                    }
                    InsertRouting::Split(_) => panic!("ignore mode should not split"),
                }
            }
            _ => panic!("not an insert"),
        }
    }

    #[test]
    fn split_insert_requires_bind_for_placeholders() {
        let query =
            parse("INSERT INTO sharded (id, value) VALUES ($1, 'one'), ($2, 'two')").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let schema = ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    name: Some("sharded".into()),
                    column: "id".into(),
                    ..Default::default()
                }],
                vec![],
            ),
            ..Default::default()
        };

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let result = insert.shard(&schema, None, true, RewriteMode::Rewrite);
                assert!(matches!(
                    result,
                    Err(Error::SplitInsertNotSupported { table, reason })
                        if table == "sharded" && reason.contains("no Bind message")
                ));
            }
            _ => panic!("not an insert"),
        }
    }

    #[test]
    fn split_insert_rejects_null_parameters() {
        let query =
            parse("INSERT INTO sharded (id, value) VALUES ($1, 'one'), ($2, 'two')").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let schema = ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    name: Some("sharded".into()),
                    column: "id".into(),
                    ..Default::default()
                }],
                vec![],
            ),
            ..Default::default()
        };

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let bind = Bind::new_params("", &[Parameter::new_null(), Parameter::new(b"11")]);
                let result = insert.shard(&schema, Some(&bind), true, RewriteMode::Rewrite);
                assert!(matches!(
                    result,
                    Err(Error::SplitInsertNotSupported { table, reason })
                        if table == "sharded" && reason.contains("evaluates to NULL")
                ));
            }
            _ => panic!("not an insert"),
        }
    }

    #[test]
    fn split_insert_rejects_binary_parameters() {
        let query =
            parse("INSERT INTO sharded (id, value) VALUES ($1, 'one'), ($2, 'two')").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let schema = ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    name: Some("sharded".into()),
                    column: "id".into(),
                    ..Default::default()
                }],
                vec![],
            ),
            ..Default::default()
        };

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let bind = Bind::new_params_codes(
                    "",
                    &[Parameter::new(&1_i64.to_be_bytes()), Parameter::new(b"11")],
                    &[Format::Binary, Format::Text],
                );
                let result = insert.shard(&schema, Some(&bind), true, RewriteMode::Rewrite);
                assert!(matches!(
                    result,
                    Err(Error::SplitInsertNotSupported { table, reason })
                        if table == "sharded" && reason.contains("binary format")
                ));
            }
            _ => panic!("not an insert"),
        }
    }

    #[test]
    fn test_null_sharding_key_routes_to_all() {
        let query = parse("INSERT INTO sharded (id, value) VALUES ($1, 'test')").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let schema = ShardingSchema {
            shards: 3,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    name: Some("sharded".into()),
                    column: "id".into(),
                    ..Default::default()
                }],
                vec![],
            ),
            ..Default::default()
        };

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let bind = Bind::new_params("", &[Parameter::new_null()]);
                let routing = insert
                    .shard(&schema, Some(&bind), false, RewriteMode::Error)
                    .unwrap();
                assert!(matches!(routing.shard(), Shard::All));
            }
            _ => panic!("not an insert"),
        }
    }

    #[test]
    fn split_insert_preserves_decimal_values() {
        let query = parse(
            "INSERT INTO transactions (txn_id, user_id, amount, status) VALUES \
             (1001, 1, 50.00, 'completed'), \
             (1002, 2, 20.00, 'failed'), \
             (1003, 3, 25.75, 'pending')",
        )
        .unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let schema = ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    name: Some("transactions".into()),
                    column: "user_id".into(),
                    ..Default::default()
                }],
                vec![],
            ),
            ..Default::default()
        };

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let routing = insert
                    .shard(&schema, None, true, RewriteMode::Rewrite)
                    .unwrap();

                match routing {
                    InsertRouting::Split(plan) => {
                        let rows = plan.rows();
                        // Check that decimal values are preserved without quotes
                        assert_eq!(rows[0].values()[2], "50.00");
                        assert_eq!(rows[1].values()[2], "20.00");
                        assert_eq!(rows[2].values()[2], "25.75");

                        // Verify strings are quoted
                        assert_eq!(rows[0].values()[3], "'completed'");
                        assert_eq!(rows[1].values()[3], "'failed'");
                        assert_eq!(rows[2].values()[3], "'pending'");
                    }
                    InsertRouting::Routed(_) => panic!("expected split plan"),
                }
            }
            _ => panic!("not an insert"),
        }
    }

    #[test]
    fn split_insert_with_quoted_decimal_values() {
        let query = parse(
            "INSERT INTO transactions (txn_id, user_id, amount, status) VALUES \
             (1001, 1, '50.00', 'completed'), \
             (1002, 2, '20.00', 'failed'), \
             (1003, 3, '25.75', 'pending')",
        )
        .unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let schema = ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    name: Some("transactions".into()),
                    column: "user_id".into(),
                    ..Default::default()
                }],
                vec![],
            ),
            ..Default::default()
        };

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let routing = insert
                    .shard(&schema, None, true, RewriteMode::Rewrite)
                    .unwrap();

                match routing {
                    InsertRouting::Split(plan) => {
                        let rows = plan.rows();
                        // Quoted decimals should be preserved as strings
                        assert_eq!(rows[0].values()[2], "'50.00'");
                        assert_eq!(rows[1].values()[2], "'20.00'");
                        assert_eq!(rows[2].values()[2], "'25.75'");

                        // Verify strings are quoted
                        assert_eq!(rows[0].values()[3], "'completed'");
                        assert_eq!(rows[1].values()[3], "'failed'");
                        assert_eq!(rows[2].values()[3], "'pending'");
                    }
                    InsertRouting::Routed(_) => panic!("expected split plan"),
                }
            }
            _ => panic!("not an insert"),
        }
    }

    #[test]
    fn debug_decimal_parsing() {
        let query = parse(
            "INSERT INTO transactions (txn_id, user_id, amount, status) VALUES \
             (1001, 101, 50.00, 'completed')",
        )
        .unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let tuples = insert.tuples();
                println!("Tuples: {:?}", tuples);
                assert_eq!(tuples.len(), 1);
                println!("Values: {:?}", tuples[0].values);
            }
            _ => panic!("not an insert"),
        }
    }

    #[test]
    fn reproduce_decimal_null_issue() {
        let query = parse(
            "INSERT INTO transactions (txn_id, user_id, amount, status) VALUES \
             (1001, 101, 50.00, 'completed')",
        )
        .unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let tuples = insert.tuples();
                println!("Values: {:?}", tuples[0].values);

                // After fix, this should be Float("50.00"), not Null
                assert_eq!(tuples[0].values[2], Value::Float("50.00"));
            }
            _ => panic!("not an insert"),
        }
    }

    #[test]
    fn split_insert_multi_tuple_without_table_name_in_schema() {
        // Test that we detect the sharding key in a multi-tuple insert
        // when the sharded table config has name: None (applies to any table)
        let query =
            parse("INSERT INTO orders (user_id, value) VALUES (1, 'a'), (11, 'b')").unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let schema = ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    name: None, // No table name specified - applies to any table
                    column: "user_id".into(),
                    ..Default::default()
                }],
                vec![],
            ),
            ..Default::default()
        };

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let routing = insert
                    .shard(&schema, None, true, RewriteMode::Rewrite)
                    .unwrap();
                match routing {
                    InsertRouting::Split(plan) => {
                        assert_eq!(plan.rows().len(), 2);
                        // user_id=1 and user_id=11 should hash to different shards with 2 shards
                        assert!(plan.shard_list().len() > 1);
                    }
                    InsertRouting::Routed(_) => panic!("expected split plan"),
                }
            }
            _ => panic!("not an insert"),
        }
    }

    #[test]
    fn split_insert_rejects_float_sharding_key() {
        let query = parse(
            "INSERT INTO transactions (txn_id, amount, status) VALUES \
             (1001, 50.00, 'completed'), \
             (1002, 20.00, 'failed')",
        )
        .unwrap();
        let select = query.protobuf.stmts.first().unwrap().stmt.as_ref().unwrap();
        let schema = ShardingSchema {
            shards: 2,
            tables: ShardedTables::new(
                vec![ShardedTable {
                    name: Some("transactions".into()),
                    column: "amount".into(),
                    ..Default::default()
                }],
                vec![],
            ),
            ..Default::default()
        };

        match &select.node {
            Some(NodeEnum::InsertStmt(stmt)) => {
                let insert = Insert::new(stmt);
                let result = insert.shard(&schema, None, true, RewriteMode::Rewrite);

                match result {
                    Err(Error::SplitInsertNotSupported { table, reason }) => {
                        assert_eq!(table, "transactions");
                        assert!(reason.contains("float/decimal"));
                        assert!(reason.contains("not supported as a sharding key"));
                    }
                    other => panic!("expected error for float sharding key, got {:?}", other),
                }
            }
            _ => panic!("not an insert"),
        }
    }
}
