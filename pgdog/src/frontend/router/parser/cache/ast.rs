use pg_query::{parse, protobuf::ObjectType, NodeEnum, NodeRef, ParseResult};
use std::fmt::Debug;
use std::{collections::HashSet, ops::Deref};

use parking_lot::Mutex;
use std::sync::Arc;

use super::super::{
    comment::comment, Error, Route, Shard, StatementRewrite, StatementRewriteContext, Table,
};
use super::{Fingerprint, Stats};
use crate::frontend::router::parser::rewrite::statement::RewritePlan;
use crate::frontend::{BufferedQuery, PreparedStatements};
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
    pub comment_shard: Option<Shard>,
    /// Role.
    pub comment_role: Option<Role>,
    /// Rewrite plan.
    pub rewrite_plan: RewritePlan,
    /// Fingerprint.
    pub fingerprint: Fingerprint,
}

impl AstInner {
    /// Create new AST record, with no rewrite or comment routing.
    pub fn new(ast: ParseResult) -> Self {
        Self {
            ast,
            stats: Mutex::new(Stats::new()),
            comment_role: None,
            comment_shard: None,
            rewrite_plan: RewritePlan::default(),
            fingerprint: Fingerprint::default(),
        }
    }
}

impl Deref for Ast {
    type Target = AstInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl Ast {
    /// Parse statement and run the rewrite engine, if necessary.
    pub fn new(
        query: &BufferedQuery,
        schema: &ShardingSchema,
        prepared_statements: &mut PreparedStatements,
    ) -> Result<Self, Error> {
        let mut ast = parse(query).map_err(Error::PgQuery)?;
        let (comment_shard, comment_role) = comment(query, schema)?;
        let fingerprint = Fingerprint::new(query).map_err(Error::PgQuery)?;

        // Don't rewrite statements that will be
        // sent to a direct shard.
        let rewrite_plan = if comment_shard.is_none() {
            StatementRewrite::new(StatementRewriteContext {
                stmt: &mut ast.protobuf,
                extended: query.extended(),
                prepared: query.prepared(),
                prepared_statements,
                schema,
            })
            .maybe_rewrite()?
        } else {
            RewritePlan::default()
        };

        Ok(Self {
            cached: true,
            inner: Arc::new(AstInner {
                stats: Mutex::new(Stats::new()),
                comment_shard,
                comment_role,
                ast,
                rewrite_plan,
                fingerprint,
            }),
        })
    }

    /// Record new AST entry, without rewriting or comment-routing.
    pub fn new_record(query: &str) -> Result<Self, Error> {
        let ast = parse(query).map_err(Error::PgQuery)?;

        Ok(Self {
            cached: true,
            inner: Arc::new(AstInner::new(ast)),
        })
    }

    /// Create new AST from a parse result.
    pub fn from_parse_result(parse_result: ParseResult) -> Self {
        Self {
            cached: true,
            inner: Arc::new(AstInner::new(parse_result)),
        }
    }

    /// Get the reference to the AST.
    pub fn parse_result(&self) -> &ParseResult {
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
