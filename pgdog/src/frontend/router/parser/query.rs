//! Route queries to correct shards.
use std::collections::{BTreeSet, HashSet};

use crate::{
    backend::{databases::databases, Cluster},
    frontend::{
        router::{parser::OrderBy, round_robin, sharding::shard_str, CopyRow},
        Buffer,
    },
    net::messages::{Bind, CopyData},
};

use super::{CopyParser, Error, Insert, Key, Route, WhereClause};

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
    Copy(CopyParser),
    StartTransaction(std::string::String),
    CommitTransaction,
    RollbackTransaction,
    StartReplication,
    ReplicationMeta,
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
            _ => Err(Error::CopyOutOfSync),
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
        query: &str,
        cluster: &Cluster,
        params: Option<Bind>,
    ) -> Result<Command, Error> {
        if self.replication_mode {
            if query.starts_with("START_REPLICATION") {
                return Ok(Command::StartReplication);
            }

            if REPLICATION_REGEX.is_match(query) {
                return Ok(Command::ReplicationMeta);
            }
        }

        // Shortcut single shard clusters that don't require read/write separation.
        if cluster.shards().len() == 1 {
            if cluster.read_only() {
                return Ok(Command::Query(Route::read(Some(0))));
            }
            if cluster.write_only() {
                return Ok(Command::Query(Route::write(Some(0))));
            }
        }

        // Hardcoded shard from a comment.
        let shard = super::comment::shard(query, cluster.shards().len()).map_err(Error::PgQuery)?;

        // Cluster is read only or write only, traffic split isn't needed,
        // so don't parse the query further.
        if let Some(shard) = shard {
            if cluster.read_only() {
                return Ok(Command::Query(Route::read(Some(shard))));
            }

            if cluster.write_only() {
                return Ok(Command::Query(Route::write(Some(shard))));
            }
        }

        let ast = parse(query).map_err(Error::PgQuery)?;

        debug!("{}", query);
        trace!("{:#?}", ast);

        let stmt = ast.protobuf.stmts.first().ok_or(Error::EmptyQuery)?;
        let root = stmt.stmt.as_ref().ok_or(Error::EmptyQuery)?;

        let mut command = match root.node {
            Some(NodeEnum::SelectStmt(ref stmt)) => {
                if shard.is_some() {
                    return Ok(Command::Query(Route::read(shard)));
                }
                // `SELECT NOW()`, `SELECT 1`, etc.
                else if ast.tables().is_empty() {
                    return Ok(Command::Query(Route::read(Some(
                        round_robin::next() % cluster.shards().len(),
                    ))));
                } else {
                    Self::select(stmt, cluster, params)
                }
            }
            Some(NodeEnum::CopyStmt(ref stmt)) => Self::copy(stmt, cluster),
            Some(NodeEnum::InsertStmt(ref stmt)) => Self::insert(stmt, cluster, &params),
            Some(NodeEnum::UpdateStmt(ref stmt)) => Self::update(stmt),
            Some(NodeEnum::DeleteStmt(ref stmt)) => Self::delete(stmt),
            Some(NodeEnum::TransactionStmt(ref stmt)) => match stmt.kind() {
                TransactionStmtKind::TransStmtCommit => return Ok(Command::CommitTransaction),
                TransactionStmtKind::TransStmtRollback => return Ok(Command::RollbackTransaction),
                TransactionStmtKind::TransStmtBegin | TransactionStmtKind::TransStmtStart => {
                    return Ok(Command::StartTransaction(query.to_string()))
                }
                _ => Ok(Command::Query(Route::write(None))),
            },
            _ => Ok(Command::Query(Route::write(None))),
        }?;

        if let Some(shard) = shard {
            if let Command::Query(ref mut route) = command {
                route.set_shard(shard);
            }
        }

        if cluster.shards().len() == 1 {
            if let Command::Query(ref mut route) = command {
                route.set_shard(0);
            }
        }

        if let Command::Query(ref mut route) = command {
            if route.shard().is_none() {
                let fingerprint = fingerprint(query).map_err(Error::PgQuery)?;
                let manual_route = databases().manual_query(&fingerprint.hex).cloned();

                // TODO: check routing logic required by config.
                if manual_route.is_some() {
                    route.set_shard(round_robin::next() % cluster.shards().len());
                }
            }
        }

        trace!("{:#?}", command);

        Ok(command)
    }

    fn select(
        stmt: &SelectStmt,
        cluster: &Cluster,
        params: Option<Bind>,
    ) -> Result<Command, Error> {
        let order_by = Self::select_sort(&stmt.sort_clause);
        let sharded_tables = cluster.sharded_tables();
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
            for table in sharded_tables {
                let table_name = table.name.as_deref();
                let keys = where_clause.keys(table_name, &table.column);
                for key in keys {
                    match key {
                        Key::Constant(value) => {
                            if let Some(shard) = shard_str(&value, cluster.shards().len()) {
                                shards.insert(shard);
                            }
                        }

                        Key::Parameter(param) => {
                            if let Some(ref params) = params {
                                if let Some(param) = params.parameter(param)? {
                                    // TODO: Handle binary encoding.
                                    if let Some(text) = param.text() {
                                        if let Some(shard) = shard_str(text, cluster.shards().len())
                                        {
                                            shards.insert(shard);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        let shard = if shards.len() == 1 {
            shards.iter().next().cloned()
        } else {
            None
        };

        Ok(Command::Query(Route::select(shard, &order_by)))
    }

    /// Parse the `ORDER BY` clause of a `SELECT` statement.
    fn select_sort(nodes: &[Node]) -> Vec<OrderBy> {
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

                    _ => continue,
                }
            }
        }

        order_by
    }

    fn copy(stmt: &CopyStmt, cluster: &Cluster) -> Result<Command, Error> {
        let parser = CopyParser::new(stmt, cluster)?;
        if let Some(parser) = parser {
            Ok(Command::Copy(parser))
        } else {
            Ok(Command::Query(Route::write(None)))
        }
    }

    fn insert(
        stmt: &InsertStmt,
        cluster: &Cluster,
        params: &Option<Bind>,
    ) -> Result<Command, Error> {
        let insert = Insert::new(stmt);
        let columns = insert
            .columns()
            .into_iter()
            .map(|column| column.name)
            .collect::<Vec<_>>();
        let table = insert.table().unwrap().name;
        let num_shards = cluster.shards().len();

        let sharding_column = cluster.sharded_column(table, &columns);
        let mut shards = BTreeSet::new();
        if let Some(column) = sharding_column {
            for tuple in insert.tuples() {
                if let Some(value) = tuple.get(column) {
                    shards.insert(if let Some(bind) = params {
                        value.shard_placeholder(bind, num_shards)
                    } else {
                        value.shard(num_shards)
                    });
                }
            }
        }

        println!("shards: {:?}", shards);

        // TODO: support sending inserts to multiple shards.
        if shards.len() == 1 {
            Ok(Command::Query(Route::write(shards.pop_last().unwrap())))
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

    use super::*;
    use crate::net::messages::Query;

    #[test]
    fn test_start_replication() {
        let query = Query::new(
            r#"START_REPLICATION SLOT "sharded" LOGICAL 0/1E2C3B0 (proto_version '4', origin 'any', publication_names '"sharded"')"#,
        );
        let mut buffer = Buffer::new();
        buffer.push(query.message().unwrap());

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
        buffer.push(query.message().unwrap());

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
                    len: "test@test.com".as_bytes().len() as i32,
                    data: "test@test.com".as_bytes().to_vec(),
                },
            ],
            results: vec![],
        };
        let mut buffer = Buffer::new();
        buffer.push(query.message().unwrap());
        buffer.push(params.message().unwrap());

        let mut parser = QueryParser::default();
        let cluster = Cluster::new_test();
        let command = parser.parse(&buffer, &cluster).unwrap();
        if let Command::Query(route) = command {
            assert_eq!(route.shard(), Some(1));
        } else {
            panic!("not a route");
        }
    }
}
