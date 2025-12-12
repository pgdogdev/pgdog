use pg_query::{parse, protobuf::ObjectType, NodeEnum, NodeRef, ParseResult};
use std::{collections::HashSet, ops::Deref};

use parking_lot::Mutex;
use std::sync::Arc;

use super::super::{comment::comment, Error, Route, Shard, Table};
use super::Stats;
use crate::{backend::ShardingSchema, config::Role};

/// Abstract syntax tree (query) cache entry,
/// with statistics.
#[derive(Debug, Clone)]
pub struct Ast {
    /// Was this entry cached?
    pub cached: bool,
    /// Inner sync.
    inner: Arc<AstInner>,
}

#[derive(Debug)]
pub struct AstInner {
    /// Cached AST.
    pub ast: ParseResult,
    /// AST stats.
    pub stats: Mutex<Stats>,
    /// Shard.
    pub comment_shard: Shard,
    /// Role.
    pub comment_role: Option<Role>,
}

impl Deref for Ast {
    type Target = AstInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Ast {
    /// Create new cache entry from pg_query's AST.
    pub fn new(query: &str, schema: &ShardingSchema) -> Result<Self, Error> {
        let ast = parse(query).map_err(Error::PgQuery)?;
        let (comment_shard, comment_role) = comment(query, schema)?;

        Ok(Self {
            cached: true,
            inner: Arc::new(AstInner {
                stats: Mutex::new(Stats {
                    hits: 1,
                    ..Default::default()
                }),
                comment_shard,
                comment_role,
                ast,
            }),
        })
    }

    /// Get the reference to the AST.
    pub fn ast(&self) -> &ParseResult {
        &self.ast
    }

    /// Get a list of tables referenced by the query.
    ///
    /// This is better than pg_query's version because we
    /// also handle `NodeRef::CreateStmt` and we handle identifiers correctly.
    ///
    pub fn tables<'a>(&'a self) -> Vec<Table<'a>> {
        let mut tables = HashSet::new();

        for node in self.ast.protobuf.nodes() {
            match node.0 {
                NodeRef::RangeVar(table) => {
                    let table = Table::from(table);
                    tables.insert(table);
                }

                NodeRef::CreateStmt(stmt) => {
                    if let Some(ref stmt) = stmt.relation {
                        tables.insert(Table::from(stmt));
                    }
                }

                NodeRef::DropStmt(stmt) => {
                    if stmt.remove_type() == ObjectType::ObjectTable {
                        for object in &stmt.objects {
                            if let Some(NodeEnum::List(ref list)) = object.node {
                                if let Ok(table) = Table::try_from(list) {
                                    tables.insert(table);
                                }
                            }
                        }
                    }
                }

                _ => (),
            }
        }

        tables.into_iter().collect()
    }

    /// Update stats for this statement, given the route
    /// calculated by the query parser.
    pub fn update_stats(&self, route: &Route) {
        let mut guard = self.stats.lock();

        if route.is_cross_shard() {
            guard.multi += 1;
        } else {
            guard.direct += 1;
        }
    }
}
