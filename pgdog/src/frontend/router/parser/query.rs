use crate::{
    backend::Cluster,
    frontend::{
        router::{parser::OrderBy, round_robin, CopyRow},
        Buffer,
    },
    net::messages::CopyData,
};

use super::{copy::CopyParser, Error, Route};

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
            self.command = Self::query(&query, cluster)?;
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
        }
    }

    fn query(query: &str, cluster: &Cluster) -> Result<Command, Error> {
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

        // `SELECT NOW()`, `SELECT 1`, etc.
        if ast.tables().is_empty() && shard.is_none() {
            return Ok(Command::Query(Route::read(Some(
                round_robin::next() % cluster.shards().len(),
            ))));
        }

        let stmt = ast.protobuf.stmts.first().ok_or(Error::EmptyQuery)?;
        let root = stmt.stmt.as_ref().ok_or(Error::EmptyQuery)?;

        let mut command = match root.node {
            Some(NodeEnum::SelectStmt(ref stmt)) => Self::select(stmt),
            Some(NodeEnum::CopyStmt(ref stmt)) => Self::copy(stmt, cluster),
            Some(NodeEnum::InsertStmt(ref stmt)) => Self::insert(stmt),
            Some(NodeEnum::UpdateStmt(ref stmt)) => Self::update(stmt),
            Some(NodeEnum::DeleteStmt(ref stmt)) => Self::delete(stmt),
            _ => todo!(),
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

    fn select(stmt: &SelectStmt) -> Result<Command, Error> {
        let order_by = Self::select_sort(&stmt.sort_clause);
        Ok(Command::Query(Route::select(None, &order_by)))
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
