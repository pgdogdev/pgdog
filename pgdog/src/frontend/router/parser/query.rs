//! Route queries to correct shards.
use std::{
    collections::{BTreeSet, HashSet},
    sync::Arc,
};

use crate::{
    backend::{databases::databases, replication::ShardedColumn, Cluster, ShardingSchema},
    frontend::{
        buffer::BufferedQuery,
        router::{
            parser::{OrderBy, Shard},
            round_robin,
            sharding::{shard_param, shard_value, Centroids},
            CopyRow,
        },
        Buffer,
    },
    net::messages::{Bind, CopyData, Vector},
};

use super::{Aggregate, Cache, Column, CopyParser, Error, Insert, Key, Route, Value, WhereClause};

use once_cell::sync::Lazy;
use pg_query::{
    fingerprint, parse,
    protobuf::{a_const::Val, *},
    NodeEnum,
};
use regex::Regex;
use tracing::{debug, trace};

static REPLICATION_REGEX: Lazy<Regex> = Lazy::new(|| {
    Regex::new(
        "(CREATE_REPLICATION_SLOT|IDENTIFY_SYSTEM|DROP_REPLICATION_SLOT|READ_REPLICATION_SLOT|ALTER_REPLICATION_SLOT|TIMELINE_HISTORY).*",
    )
    .unwrap()
});

/// Command determined by the query parser.
#[derive(Debug, Clone)]
pub enum Command {
    Query(Route),
    Copy(Box<CopyParser>),
    StartTransaction(BufferedQuery),
    CommitTransaction,
    RollbackTransaction,
    StartReplication,
    ReplicationMeta,
    Set { name: String, value: String },
}

#[derive(Debug)]
pub struct QueryParser {
    command: Command,
    replication_mode: bool,
}

impl Default for QueryParser {
    fn default() -> Self {
        Self {
            command: Command::Query(Route::default()),
            replication_mode: false,
        }
    }
}

impl QueryParser {
    /// Set parser to handle replication commands.
    pub fn replication_mode(&mut self) {
        self.replication_mode = true;
    }

    pub fn parse(&mut self, buffer: &Buffer, cluster: &Cluster) -> Result<&Command, Error> {
        if let Some(query) = buffer.query()? {
            self.command = self.query(&query, cluster, buffer.parameters()?)?;
        }
        Ok(&self.command)
    }

    /// Shard copy data.
    pub fn copy_data(&mut self, rows: Vec<CopyData>) -> Result<Vec<CopyRow>, Error> {
        match &mut self.command {
            Command::Copy(copy) => copy.shard(rows),
            _ => Ok(vec![]),
        }
    }

    /// Get the route currently determined by the parser.
    pub fn route(&self) -> Route {
        match self.command {
            Command::Query(ref route) => route.clone(),
            _ => Route::write(None),
        }
    }

    fn query(
        &self,
        query: &BufferedQuery,
        cluster: &Cluster,
        params: Option<Bind>,
    ) -> Result<Command, Error> {
        // Replication protocol commands
        // don't have a node in pg_query,
        // so we have to parse them using a regex.
        if self.replication_mode {
            if query.starts_with("START_REPLICATION") {
                return Ok(Command::StartReplication);
            }

            if REPLICATION_REGEX.is_match(query) {
                return Ok(Command::ReplicationMeta);
            }
        }

        // Don't use the parser if the cluster has only one shard
        // and only one kind of database (either primary or just replicas).
        //
        // We know what the routing decision is in this case and we don't
        // need to invoke the parser.
        if cluster.shards().len() == 1 {
            if cluster.read_only() {
                return Ok(Command::Query(Route::read(Some(0))));
            }
            if cluster.write_only() {
                return Ok(Command::Query(Route::write(Some(0))));
            }
        }

        let sharding_schema = cluster.sharding_schema();

        // Parse hardcoded shard from a query comment.
        let shard = super::comment::shard(query, &sharding_schema).map_err(Error::PgQuery)?;

        // Cluster is read only or write only, traffic split isn't needed,
        // so don't parse the query further.
        if let Shard::Direct(_) = shard {
            if cluster.read_only() {
                return Ok(Command::Query(Route::read(shard)));
            }

            if cluster.write_only() {
                return Ok(Command::Query(Route::write(shard)));
            }
        }

        // Get the AST from cache or parse the statement live.
        let ast = match query {
            // Only prepared statements (or just extended) are cached.
            BufferedQuery::Prepared(query) => {
                Cache::get().parse(query.query()).map_err(Error::PgQuery)?
            }
            // Don't cache simple queries.
            //
            // They contain parameter values, which makes the cache
            // too large to be practical.
            //
            // Make your clients use prepared statements
            // or at least send statements with placeholders using the
            // extended protocol.
            BufferedQuery::Query(query) => Arc::new(parse(query.query()).map_err(Error::PgQuery)?),
        };

        debug!("{}", query.query());
        trace!("{:#?}", ast);

        //
        // Get the root AST node.
        //
        // We don't expect clients to send multiple queries. If they do
        // only the first one is used for routing.
        //
        let root = ast
            .protobuf
            .stmts
            .first()
            .ok_or(Error::EmptyQuery)?
            .stmt
            .as_ref()
            .ok_or(Error::EmptyQuery)?;

        let mut command = match root.node {
            // SELECT statements.
            Some(NodeEnum::SelectStmt(ref stmt)) => {
                if matches!(shard, Shard::Direct(_)) {
                    return Ok(Command::Query(Route::read(shard)));
                }
                // `SELECT NOW()`, `SELECT 1`, etc.
                else if ast.tables().is_empty() {
                    return Ok(Command::Query(Route::read(Some(
                        round_robin::next() % cluster.shards().len(),
                    ))));
                } else {
                    Self::select(stmt, &sharding_schema, params)
                }
            }
            // SET statements.
            Some(NodeEnum::VariableSetStmt(ref stmt)) => Self::set(stmt),
            // COPY statements.
            Some(NodeEnum::CopyStmt(ref stmt)) => Self::copy(stmt, cluster),
            // INSERT statements.
            Some(NodeEnum::InsertStmt(ref stmt)) => Self::insert(stmt, &sharding_schema, &params),
            // UPDATE statements.
            Some(NodeEnum::UpdateStmt(ref stmt)) => Self::update(stmt),
            // DELETE statements.
            Some(NodeEnum::DeleteStmt(ref stmt)) => Self::delete(stmt),
            // Transaction control statements,
            // e.g. BEGIN, COMMIT, etc.
            Some(NodeEnum::TransactionStmt(ref stmt)) => match stmt.kind() {
                TransactionStmtKind::TransStmtCommit => return Ok(Command::CommitTransaction),
                TransactionStmtKind::TransStmtRollback => return Ok(Command::RollbackTransaction),
                TransactionStmtKind::TransStmtBegin | TransactionStmtKind::TransStmtStart => {
                    return Ok(Command::StartTransaction(query.clone()))
                }
                _ => Ok(Command::Query(Route::write(None))),
            },
            // All others are not handled.
            // They are sent to all shards concurrently.
            _ => Ok(Command::Query(Route::write(None))),
        }?;

        // Overwrite shard using shard we got from a comment, if any.
        if let Shard::Direct(shard) = shard {
            if let Command::Query(ref mut route) = command {
                route.set_shard(shard);
            }
        }

        // If we only have one shard, set it.
        //
        // If the query parser couldn't figure it out,
        // there is no point of doing a multi-shard query with only one shard
        // in the set.
        //
        if cluster.shards().len() == 1 {
            if let Command::Query(ref mut route) = command {
                route.set_shard(0);
            }
        }

        // Last ditch attempt to route a query to a specific shard.
        //
        // Looking through manual queries to see if we have any
        // with the fingerprint.
        //
        if let Command::Query(ref mut route) = command {
            if route.shard().all() {
                let databases = databases();
                // Only fingerprint the query if some manual queries are configured.
                // Otherwise, we're wasting time parsing SQL.
                if !databases.manual_queries().is_empty() {
                    let fingerprint = fingerprint(query).map_err(Error::PgQuery)?;
                    let manual_route = databases.manual_query(&fingerprint.hex).cloned();

                    // TODO: check routing logic required by config.
                    if manual_route.is_some() {
                        route.set_shard(round_robin::next() % cluster.shards().len());
                    }
                }
            }
        }

        trace!("{:#?}", command);

        Ok(command)
    }

    fn set(_stmt: &VariableSetStmt) -> Result<Command, Error> {
        Ok(Command::Query(Route::write(Shard::All)))
    }

    fn select(
        stmt: &SelectStmt,
        sharding_schema: &ShardingSchema,
        params: Option<Bind>,
    ) -> Result<Command, Error> {
        let order_by = Self::select_sort(&stmt.sort_clause, &params);
        let mut shards = HashSet::new();
        let table_name = stmt
            .from_clause
            .first()
            .and_then(|node| {
                node.node.as_ref().map(|node| match node {
                    NodeEnum::RangeVar(var) => Some(if let Some(ref alias) = var.alias {
                        alias.aliasname.as_str()
                    } else {
                        var.relname.as_str()
                    }),
                    _ => None,
                })
            })
            .flatten();
        if let Some(where_clause) = WhereClause::new(table_name, &stmt.where_clause) {
            // Complexity: O(number of sharded tables * number of columns in the query)
            for table in sharding_schema.tables.tables() {
                let table_name = table.name.as_deref();
                let keys = where_clause.keys(table_name, &table.column);
                for key in keys {
                    match key {
                        Key::Constant(value) => {
                            shards.insert(shard_value(
                                &value,
                                &table.data_type,
                                sharding_schema.shards,
                                &table.centroids,
                                table.centroid_probes,
                            ));
                        }

                        Key::Parameter(param) => {
                            if let Some(ref params) = params {
                                if let Some(param) = params.parameter(param)? {
                                    shards.insert(shard_param(
                                        &param,
                                        table,
                                        sharding_schema.shards,
                                    ));
                                }
                            }
                        }
                    }
                }
            }
        }
        // Shard by vector in ORDER BY clause.
        for order in &order_by {
            if let Some((vector, column_name)) = order.vector() {
                for table in sharding_schema.tables.tables() {
                    if &table.column == column_name
                        && (table.name.is_none() || table.name.as_deref() == table_name)
                    {
                        let centroids = Centroids::from(&table.centroids);
                        shards.insert(centroids.shard(
                            vector,
                            sharding_schema.shards,
                            table.centroid_probes,
                        ));
                    }
                }
            }
        }

        let shard = if shards.len() == 1 {
            shards.iter().next().cloned().unwrap()
        } else {
            let mut multi = vec![];
            let mut all = false;
            for shard in &shards {
                match shard {
                    Shard::All => {
                        all = true;
                        break;
                    }
                    Shard::Direct(v) => multi.push(*v),
                    Shard::Multi(m) => multi.extend(m),
                };
            }
            if all || shards.is_empty() {
                Shard::All
            } else {
                Shard::Multi(multi)
            }
        };

        let aggregates = Aggregate::parse(stmt)?;

        Ok(Command::Query(Route::select(shard, &order_by, &aggregates)))
    }

    /// Parse the `ORDER BY` clause of a `SELECT` statement.
    fn select_sort(nodes: &[Node], params: &Option<Bind>) -> Vec<OrderBy> {
        let mut order_by = vec![];
        for clause in nodes {
            if let Some(NodeEnum::SortBy(ref sort_by)) = clause.node {
                let asc = matches!(sort_by.sortby_dir, 0..=2);
                let Some(ref node) = sort_by.node else {
                    continue;
                };
                let Some(ref node) = node.node else {
                    continue;
                };

                match node {
                    NodeEnum::AConst(aconst) => {
                        if let Some(Val::Ival(ref integer)) = aconst.val {
                            order_by.push(if asc {
                                OrderBy::Asc(integer.ival as usize)
                            } else {
                                OrderBy::Desc(integer.ival as usize)
                            });
                        }
                    }

                    NodeEnum::ColumnRef(column_ref) => {
                        let Some(field) = column_ref.fields.first() else {
                            continue;
                        };
                        if let Some(NodeEnum::String(ref string)) = field.node {
                            order_by.push(if asc {
                                OrderBy::AscColumn(string.sval.clone())
                            } else {
                                OrderBy::DescColumn(string.sval.clone())
                            });
                        }
                    }

                    NodeEnum::AExpr(expr) => {
                        if expr.kind() == AExprKind::AexprOp {
                            if let Some(node) = expr.name.first() {
                                if let Some(NodeEnum::String(String { sval })) = &node.node {
                                    match sval.as_str() {
                                        "<->" => {
                                            let mut vector: Option<Vector> = None;
                                            let mut column: Option<std::string::String> = None;

                                            for e in
                                                [&expr.lexpr, &expr.rexpr].iter().copied().flatten()
                                            {
                                                if let Ok(vec) = Value::try_from(&e.node) {
                                                    match vec {
                                                        Value::Placeholder(p) => {
                                                            if let Some(bind) = params {
                                                                if let Ok(Some(param)) =
                                                                    bind.parameter((p - 1) as usize)
                                                                {
                                                                    vector = param.vector();
                                                                }
                                                            }
                                                        }
                                                        Value::Vector(vec) => vector = Some(vec),
                                                        _ => (),
                                                    }
                                                };

                                                if let Ok(col) = Column::try_from(&e.node) {
                                                    column = Some(col.name.to_owned());
                                                }
                                            }

                                            if let Some(vector) = vector {
                                                if let Some(column) = column {
                                                    order_by.push(OrderBy::AscVectorL2Column(
                                                        column, vector,
                                                    ));
                                                }
                                            }
                                        }
                                        _ => continue,
                                    }
                                }
                            }
                        }
                    }

                    _ => continue,
                }
            }
        }

        order_by
    }

    fn copy(stmt: &CopyStmt, cluster: &Cluster) -> Result<Command, Error> {
        let parser = CopyParser::new(stmt, cluster)?;
        if let Some(parser) = parser {
            Ok(Command::Copy(Box::new(parser)))
        } else {
            Ok(Command::Query(Route::write(None)))
        }
    }

    fn insert(
        stmt: &InsertStmt,
        sharding_schema: &ShardingSchema,
        params: &Option<Bind>,
    ) -> Result<Command, Error> {
        let insert = Insert::new(stmt);
        let columns = insert
            .columns()
            .into_iter()
            .map(|column| column.name)
            .collect::<Vec<_>>();
        let mut shards = BTreeSet::new();
        let table = insert.table().unwrap().name;
        if let Some(sharded_table) = sharding_schema.tables.table(table) {
            if let Some(column) = ShardedColumn::from_sharded_table(sharded_table, &columns) {
                for tuple in insert.tuples() {
                    if let Some(value) = tuple.get(column.position) {
                        shards.insert(if let Some(bind) = params {
                            value.shard_placeholder(bind, sharding_schema, &column)
                        } else {
                            value.shard(sharding_schema, &column)
                        });
                    }
                }
            }
            match shards.len() {
                0 => Ok(Command::Query(Route::write(Some(
                    round_robin::next() % sharding_schema.shards,
                )))),
                1 => Ok(Command::Query(Route::write(shards.pop_last().unwrap()))),
                // TODO: support sending inserts to multiple shards.
                _ => Ok(Command::Query(Route::write(None))),
            }
        } else {
            Ok(Command::Query(Route::write(None)))
        }
    }

    fn update(_stmt: &UpdateStmt) -> Result<Command, Error> {
        Ok(Command::Query(Route::write(None)))
    }

    fn delete(_stmt: &DeleteStmt) -> Result<Command, Error> {
        Ok(Command::Query(Route::write(None)))
    }
}

#[cfg(test)]
mod test {
    use crate::net::messages::{parse::Parse, Parameter, Protocol};

    use super::{super::Shard, *};
    use crate::net::messages::Query;

    #[test]
    fn test_start_replication() {
        let query = Query::new(
            r#"START_REPLICATION SLOT "sharded" LOGICAL 0/1E2C3B0 (proto_version '4', origin 'any', publication_names '"sharded"')"#,
        );
        let mut buffer = Buffer::new();
        buffer.push(query.message().unwrap().into());

        let mut query_parser = QueryParser::default();
        query_parser.replication_mode();

        let cluster = Cluster::default();

        let command = query_parser.parse(&buffer, &cluster).unwrap();
        assert!(matches!(command, &Command::StartReplication));
    }

    #[test]
    fn test_replication_meta() {
        let query = Query::new(r#"IDENTIFY_SYSTEM"#);
        let mut buffer = Buffer::new();
        buffer.push(query.message().unwrap().into());

        let mut query_parser = QueryParser::default();
        query_parser.replication_mode();

        let cluster = Cluster::default();

        let command = query_parser.parse(&buffer, &cluster).unwrap();
        assert!(matches!(command, &Command::ReplicationMeta));
    }

    #[test]
    fn test_insert() {
        let query = Parse::new_anonymous("INSERT INTO sharded (id, email) VALUES ($1, $2)");
        let params = Bind {
            portal: "".into(),
            statement: "".into(),
            codes: vec![],
            params: vec![
                Parameter {
                    len: 2,
                    data: "11".as_bytes().to_vec(),
                },
                Parameter {
                    len: "test@test.com".len() as i32,
                    data: "test@test.com".as_bytes().to_vec(),
                },
            ],
            results: vec![],
        };
        let mut buffer = Buffer::new();
        buffer.push(query.into());
        buffer.push(params.into());

        let mut parser = QueryParser::default();
        let cluster = Cluster::new_test();
        let command = parser.parse(&buffer, &cluster).unwrap();
        if let Command::Query(route) = command {
            assert_eq!(route.shard(), &Shard::direct(1));
        } else {
            panic!("not a route");
        }
    }

    #[test]
    fn test_order_by_vector() {
        let query = Query::new("SELECT * FROM embeddings ORDER BY embedding <-> '[1,2,3]'");
        let buffer = Buffer::from(vec![query.into()]);
        let route = QueryParser::default()
            .parse(&buffer, &Cluster::default())
            .unwrap()
            .clone();
        if let Command::Query(route) = route {
            let order_by = route.order_by().first().unwrap();
            assert!(order_by.asc());
            assert_eq!(
                order_by.vector().unwrap(),
                (
                    &Vector::from(&[1.0, 2.0, 3.0][..]),
                    &std::string::String::from("embedding")
                ),
            );
        } else {
            panic!("not a route");
        }

        let query = Parse::new_anonymous("SELECT * FROM embeddings ORDER BY embedding  <-> $1");
        let bind = Bind {
            portal: "".into(),
            statement: "".into(),
            codes: vec![],
            params: vec![Parameter {
                len: 7,
                data: "[4,5,6]".as_bytes().to_vec(),
            }],
            results: vec![],
        };
        let buffer = Buffer::from(vec![query.into(), bind.into()]);
        let route = QueryParser::default()
            .parse(&buffer, &Cluster::default())
            .unwrap()
            .clone();
        if let Command::Query(query) = route {
            let order_by = query.order_by().first().unwrap();
            assert!(order_by.asc());
            assert_eq!(
                order_by.vector().unwrap(),
                (
                    &Vector::from(&[4.0, 5.0, 6.0][..]),
                    &std::string::String::from("embedding")
                )
            );
        } else {
            panic!("not a route");
        }
    }
}
