use pg_raw_parse::{Node, Owned, StmtList, make};
use pgdog_config::QueryParserEngine;
use std::fmt::Debug;
use std::ops::Deref;
use std::time::Instant;

use parking_lot::Mutex;
use std::sync::Arc;
use tracing::warn;

use super::super::{Error, Route, StatementRewrite, StatementRewriteContext};
use super::Stats;
use crate::backend::schema::Schema;
use crate::frontend::PreparedStatements;
use crate::frontend::router::parser::cache::AstQuery;
use crate::frontend::router::parser::rewrite::statement::RewritePlan;
use crate::frontend::router::sharding::ShardOrLookup;
use crate::net::parameter::ParameterValue;
use crate::{backend::ShardingSchema, config::Role};

/// Abstract syntax tree (query) cache entry,
/// with statistics.
#[derive(Debug, Clone)]
pub struct Ast {
    /// Was this entry cached?
    pub cached: bool,
    /// Shard.
    pub comment_shard: Option<ShardOrLookup>,
    /// Role.
    pub comment_role: Option<Role>,
    /// Parser query engine used.
    pub query_parser_engine: QueryParserEngine,
    /// Sharding Key.
    pub comment_sharding_key: Option<String>,
    /// Inner sync.
    inner: Arc<AstInner>,
}

#[derive(Debug)]
pub struct AstInner {
    /// Cached AST.
    pub(crate) ast: Owned<StmtList>,
    /// AST stats.
    pub stats: Mutex<Stats>,
    /// Rewrite plan.
    pub rewrite_plan: RewritePlan,
    /// Original query.
    pub query_without_comment: Arc<str>,
}

impl AstInner {
    /// Create new AST record, with no rewrite or comment routing.
    pub(crate) fn new(ast: Owned<StmtList>) -> Self {
        Self {
            ast,
            stats: Mutex::new(Stats::new()),
            rewrite_plan: RewritePlan::default(),
            query_without_comment: "".into(),
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
    pub(super) fn new(
        query: &AstQuery,
        schema: &ShardingSchema,
        db_schema: &Schema,
        prepared_statements: &mut PreparedStatements,
        user: &str,
        search_path: Option<&ParameterValue>,
    ) -> Result<Self, Error> {
        let now = Instant::now();

        // Run the rewrite unconditionally. Even when a shard comment will
        // route the query to a specific shard, we need to know whether the
        // same query body (without the comment) would require a rewrite, so
        // `Cache::query` can decide whether this entry is safe to cache.
        let mut rewriter = StatementRewrite::new(StatementRewriteContext {
            extended: query.original_query.extended(),
            prepared: query.original_query.prepared(),
            prepared_statements,
            schema,
            db_schema,
            user,
            search_path,
        });
        let mut rewrite_plan = Default::default();
        let ast = make::try_owned(|mem| {
            let mut ast = mem.parse(query.query_without_comment)?;
            // Only rewrite if there is one statement in the request.
            // Otherwise, the parser will refuse to run it and tell the query
            // engine to re-send statements separately.
            let only_one = ast.stmts().count() == 1;

            if only_one && let Some(stmt) = ast.as_mut().into_iter().next() {
                rewrite_plan = rewriter.maybe_rewrite(stmt, mem)?;
            }

            Ok::<_, Error>(ast)
        })?;

        let elapsed = now.elapsed();
        let mut stats = Stats::new();
        stats.parse_time += elapsed;

        if let Some(threshold) = schema.log_min_duration_parse
            && elapsed >= threshold
        {
            warn!(
                "[slow_query_parse] parse_time_in_ms={}ms truncated_query=\"{}\"",
                elapsed.as_millis(),
                query.truncated_query(schema.log_query_sample_length),
            );
        }

        Ok(Self {
            cached: true,
            comment_shard: None,
            comment_role: None,
            query_parser_engine: schema.query_parser_engine,
            comment_sharding_key: None,
            inner: Arc::new(AstInner {
                stats: Mutex::new(stats),
                ast,
                rewrite_plan,
                query_without_comment: query.query_without_comment.into(),
            }),
        })
    }

    /// Parse statement using AstContext for schema and user information.
    pub(super) fn with_context(
        query: &AstQuery,
        ctx: &super::AstContext<'_>,
        prepared_statements: &mut PreparedStatements,
    ) -> Result<Self, Error> {
        Self::new(
            query,
            &ctx.sharding_schema,
            &ctx.db_schema,
            prepared_statements,
            ctx.user,
            ctx.search_path,
        )
    }

    /// Record new AST entry, without rewriting or comment-routing.
    pub(crate) fn new_record(query: &str) -> Result<Self, Error> {
        let ast = pg_raw_parse::parse(query)?;

        Ok(Self {
            cached: true,
            comment_role: None,
            comment_shard: None,
            comment_sharding_key: None,
            query_parser_engine: QueryParserEngine::default(),
            inner: Arc::new(AstInner::new(ast.into_inner())),
        })
    }

    /// Create new AST from a parse result.
    pub fn from_raw_stmts(stmts: Owned<StmtList>) -> Self {
        Self {
            cached: true,
            comment_role: None,
            comment_shard: None,
            query_parser_engine: QueryParserEngine::default(),
            comment_sharding_key: None,
            inner: Arc::new(AstInner::new(stmts)),
        }
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

    /// Get statement type.
    pub(crate) fn statement_type(&self) -> StatementType {
        let root = self.ast.stmts().next();

        match root {
            Some(Node::SelectStmt(_))
            | Some(Node::InsertStmt(_))
            | Some(Node::UpdateStmt(_))
            | Some(Node::DeleteStmt(_))
            | Some(Node::CopyStmt(_))
            | Some(Node::ExplainStmt(_))
            | Some(Node::TransactionStmt(_)) => StatementType::Dml,

            Some(Node::VariableSetStmt(_))
            | Some(Node::VariableShowStmt(_))
            | Some(Node::DeallocateStmt(_))
            | Some(Node::ListenStmt(_))
            | Some(Node::NotifyStmt(_))
            | Some(Node::UnlistenStmt(_))
            | Some(Node::DiscardStmt(_)) => StatementType::Session,

            _ => StatementType::Ddl,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StatementType {
    Ddl,
    Dml,
    Session,
}
