//! Route queries to correct shards.
use std::collections::HashSet;

use crate::{
    backend::Cluster,
    frontend::{
        router::{parser::OrderBy, round_robin, sharding::shard_str, CopyRow},
        Buffer,
    },
    net::messages::{Bind, CopyData},
};

use super::{
    copy::CopyParser,
    where_clause::{Key, WhereClause},
    Error, Route,
};

use pg_query::{
    parse,
    protobuf::{a_const::Val, *},
    NodeEnum,
};
use tracing::trace;

/// Command determined by the query parser.
#[derive(Debug, Clone)]
pub enum Command {
    Query(Route),
    Copy(CopyParser),
    StartTransaction,
    CommitTransaction,
    RollbackTransaction,
}

impl Command {
    /// This is a BEGIN TRANSACTION command.
    pub fn begin(&self) -> bool {
        matches!(self, Command::StartTransaction)
    }

    /// This is a ROLLBACK command.
    pub fn rollback(&self) -> bool {
        matches!(self, Command::RollbackTransaction)
    }

    pub fn commit(&self) -> bool {
        matches!(self, Command::CommitTransaction)
    }
}

#[derive(Debug)]
pub struct QueryParser {
    command: Command,
}

impl Default for QueryParser {
    fn default() -> Self {
        Self {
            command: Command::Query(Route::default()),
        }
    }
}

impl QueryParser {
    pub fn parse(&mut self, buffer: &Buffer, cluster: &Cluster) -> Result<&Command, Error> {
        if let Some(query) = buffer.query()? {
            self.command = Self::query(&query, cluster, buffer.parameters()?)?;
            Ok(&self.command)
        } else {
            Err(Error::NotInSync)
        }
    }

    /// Shard copy data.
    pub fn copy_data(&mut self, rows: Vec<CopyData>) -> Result<Vec<CopyRow>, Error> {
        match &mut self.command {
            Command::Copy(copy) => copy.shard(rows),
            _ => Err(Error::NotInSync),
        }
    }

    pub fn route(&self) -> Route {
        match self.command {
            Command::Query(ref route) => route.clone(),
            Command::Copy(_) => Route::write(None),
            Command::CommitTransaction
            | Command::RollbackTransaction
            | Command::StartTransaction => Route::write(None),
        }
    }

    fn query(query: &str, cluster: &Cluster, params: Option<Bind>) -> Result<Command, Error> {
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

        let ast = parse(query).map_err(Error::PgQuery)?;

        trace!("{:#?}", ast);

        let stmt = ast.protobuf.stmts.first().ok_or(Error::EmptyQuery)?;
        let root = stmt.stmt.as_ref().ok_or(Error::EmptyQuery)?;

        let mut command = match root.node {
            Some(NodeEnum::SelectStmt(ref stmt)) => {
                // `SELECT NOW()`, `SELECT 1`, etc.
                if ast.tables().is_empty() && shard.is_none() {
                    return Ok(Command::Query(Route::read(Some(
                        round_robin::next() % cluster.shards().len(),
                    ))));
                } else {
                    Self::select(stmt, cluster, params)
                }
            }
            Some(NodeEnum::CopyStmt(ref stmt)) => Self::copy(stmt, cluster),
            Some(NodeEnum::InsertStmt(ref stmt)) => Self::insert(stmt),
            Some(NodeEnum::UpdateStmt(ref stmt)) => Self::update(stmt),
            Some(NodeEnum::DeleteStmt(ref stmt)) => Self::delete(stmt),
            Some(NodeEnum::TransactionStmt(ref stmt)) => match stmt.kind() {
                TransactionStmtKind::TransStmtCommit => return Ok(Command::CommitTransaction),
                TransactionStmtKind::TransStmtRollback => return Ok(Command::RollbackTransaction),
                TransactionStmtKind::TransStmtBegin | TransactionStmtKind::TransStmtStart => {
                    return Ok(Command::StartTransaction)
                }
                _ => Ok(Command::Query(Route::write(None))),
            },
            _ => Ok(Command::Query(Route::write(None))),
        }?;

        if let Some(shard) = shard {
            if let Command::Query(ref mut route) = command {
                route.overwrite_shard(shard);
            }
        }

        if cluster.shards().len() == 1 {
            if let Command::Query(ref mut route) = command {
                route.overwrite_shard(0);
            }
        }

        Ok(command)
    }

    fn select(
        stmt: &SelectStmt,
        cluster: &Cluster,
        params: Option<Bind>,
    ) -> Result<Command, Error> {
        let order_by = Self::select_sort(&stmt.sort_clause);
        let sharded_tables = cluster.shaded_tables();
        let mut shards = HashSet::new();
        if let Some(where_clause) = WhereClause::new(&stmt.where_clause) {
            // Complexity: O(number of sharded tables * number of columns in the query)
            for table in sharded_tables {
                let table_name = table.name.as_ref().map(|s| s.as_str());
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

    fn insert(_stmt: &InsertStmt) -> Result<Command, Error> {
        Ok(Command::Query(Route::write(None)))
    }

    fn update(_stmt: &UpdateStmt) -> Result<Command, Error> {
        Ok(Command::Query(Route::write(None)))
    }

    fn delete(_stmt: &DeleteStmt) -> Result<Command, Error> {
        Ok(Command::Query(Route::write(None)))
    }
}
