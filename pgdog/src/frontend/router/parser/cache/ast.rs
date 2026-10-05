use itertools::Itertools;
use pg_raw_parse::{Error as ParseError, Node, Owned, StmtList, make};
use std::fmt::Debug;
use std::time::Instant;

use once_cell::sync::OnceCell;
use parking_lot::Mutex;
use std::sync::Arc;
use tracing::warn;

use super::super::{Error, Route, StatementRewrite, StatementRewriteContext};
use super::Stats;
use crate::frontend::router::parser::cache::AstQuery;
use crate::frontend::router::parser::rewrite::statement::RewritePlan;
use crate::frontend::router::parser::rewrite::statement::projection::PostRouteRewrite;
use crate::frontend::{PreparedStatements, RoutingComment};

/// A parsed query from a client, potentially containing multiple statements.
#[derive(Debug, Clone)]
pub(crate) struct ClientQuery {
    pub(crate) cached: bool,
    pub(in crate::frontend) comment: Arc<RoutingComment>,
    pub(crate) ast: Arc<Ast>,
}

#[derive(Debug)]
/// A parsed and rewritten query
pub(crate) struct Ast {
    /// Cached AST.
    pub(crate) ast: Owned<StmtList>,
    /// AST stats.
    pub(crate) stats: Mutex<Stats>,
    /// Rewrite plan.
    pub(crate) rewrite_plan: RewritePlan,
    /// Lazily generated cross-shard SQL and response metadata. This is derived
    /// only from the AST so Bind values cannot permanently change a cache entry.
    pub(crate) post_route_rewrite: OnceCell<Option<PostRouteRewrite>>,
    /// Original query.
    pub(crate) query_without_comment: Arc<str>,
}

impl Ast {
    /// Create new AST record, with no rewrite or comment routing.
    pub(crate) fn new(ast: Owned<StmtList>) -> Self {
        let mut stats = Stats::new();
        stats.memory_allocated = ast.memory_allocated();
        Self {
            ast,
            stats: Mutex::new(stats),
            rewrite_plan: RewritePlan::default(),
            post_route_rewrite: OnceCell::new(),
            query_without_comment: "".into(),
        }
    }

    /// Parse statement and run the rewrite engine, if necessary.
    pub(super) fn parse_and_rewrite(
        query: &AstQuery,
        ctx: &super::AstContext<'_>,
        prepared_statements: &mut PreparedStatements,
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
            schema: &ctx.sharding_schema,
            db_schema: &ctx.db_schema,
            user: ctx.user,
            search_path: ctx.search_path,
            timezone: ctx.timezone,
            query_timestamps: ctx.query_timestamps,
        });
        let mut rewrite_plan = Default::default();
        let ast = make::try_owned(|mem| {
            let mut ast = mem.parse(query.query_without_comment)?;
            // Parser should not receive multi-query requests.
            if let Ok(stmt) = ast.as_mut().into_iter().exactly_one() {
                rewrite_plan = rewriter.maybe_rewrite(stmt, mem)?;
            }
            Ok::<_, Error>(ast)
        })?;

        let elapsed = now.elapsed();
        let mut stats = Stats::new();
        stats.parse_time += elapsed;
        stats.memory_allocated = ast.memory_allocated();

        if let Some(threshold) = ctx.sharding_schema.log_min_duration_parse
            && elapsed >= threshold
        {
            warn!(
                "[slow_query_parse] parse_time_in_ms={}ms truncated_query=\"{}\"",
                elapsed.as_millis(),
                query.truncated_query(ctx.sharding_schema.log_query_sample_length),
            );
        }

        Ok(Self {
            stats: Mutex::new(stats),
            ast,
            rewrite_plan,
            post_route_rewrite: OnceCell::new(),
            query_without_comment: query.query_without_comment.into(),
        })
    }

    /// Parse the query without rewriting it
    pub(crate) fn parse(query: &str) -> Result<Self, ParseError> {
        Ok(Self::new(pg_raw_parse::parse(query)?.into_inner()))
    }

    /// Update stats for this statement, given the route
    /// calculated by the query parser.
    pub(crate) fn update_stats(&self, route: &Route) {
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
pub(crate) enum StatementType {
    Ddl,
    Dml,
    Session,
}
