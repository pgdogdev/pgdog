#[cfg(not(feature = "new_parser"))]
use pg_query::{NodeEnum, ParseResult, parse, parse_raw};
#[cfg(feature = "new_parser")]
use pg_raw_parse::{Node, Owned, StmtList, make};
use pgdog_config::QueryParserEngine;
use std::fmt::Debug;
use std::ops::Deref;
use std::time::Instant;

use parking_lot::Mutex;
use std::sync::Arc;
use tracing::warn;

use super::super::{Error, Route, Shard, StatementRewrite, StatementRewriteContext};
use super::Stats;
use crate::backend::schema::Schema;
use crate::frontend::PreparedStatements;
use crate::frontend::router::parser::cache::AstQuery;
use crate::frontend::router::parser::rewrite::statement::RewritePlan;
use crate::net::parameter::ParameterValue;
use crate::{backend::ShardingSchema, config::Role};

/// Abstract syntax tree (query) cache entry,
/// with statistics.
#[derive(Debug, Clone)]
pub struct Ast {
    /// Was this entry cached?
    pub cached: bool,
    /// Shard.
    pub comment_shard: Option<Shard>,
    /// Role.
    pub comment_role: Option<Role>,
    /// Parser query engine used.
    pub query_parser_engine: QueryParserEngine,
    /// Inner sync.
    inner: Arc<AstInner>,
}

#[derive(Debug)]
pub struct AstInner {
    /// Cached AST.
    // FIXME(sage): Rename to ast when parser port is done
    #[cfg(feature = "new_parser")]
    pub(crate) ast: Owned<StmtList>,
    #[cfg(not(feature = "new_parser"))]
    pub(crate) ast: ParseResult,
    /// AST stats.
    pub stats: Mutex<Stats>,
    /// Rewrite plan.
    pub rewrite_plan: RewritePlan,
    /// Original query.
    pub query_without_comment: Arc<str>,
}

impl AstInner {
    /// Create new AST record, with no rewrite or comment routing.
    #[cfg(feature = "new_parser")]
    pub(crate) fn new(ast: Owned<StmtList>) -> Self {
        Self {
            ast,
            stats: Mutex::new(Stats::new()),
            rewrite_plan: RewritePlan::default(),
            query_without_comment: "".into(),
        }
    }

    #[cfg(not(feature = "new_parser"))]
    pub(crate) fn old(ast: ParseResult) -> Self {
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
        #[cfg(not(feature = "new_parser"))]
        let mut ast = match schema.query_parser_engine {
            QueryParserEngine::PgQueryProtobuf => parse(query.query_without_comment),
            QueryParserEngine::PgQueryRaw => parse_raw(query.query_without_comment),
        }
        .map_err(Error::PgQuery)?;
        #[cfg(feature = "new_parser")]
        let ast = pg_raw_parse::parse(query.query_without_comment).map_err(Error::Parse)?;

        // Run the rewrite unconditionally. Even when a shard comment will
        // route the query to a specific shard, we need to know whether the
        // same query body (without the comment) would require a rewrite, so
        // `Cache::query` can decide whether this entry is safe to cache.
        let mut rewriter = StatementRewrite::new(StatementRewriteContext {
            #[cfg(not(feature = "new_parser"))]
            stmt: &mut ast.protobuf,
            extended: query.original_query.extended(),
            prepared: query.original_query.prepared(),
            prepared_statements,
            schema,
            db_schema,
            user,
            search_path,
        });
        #[cfg(feature = "new_parser")]
        let mut rewrite_plan = Default::default();
        #[cfg(feature = "new_parser")]
        let ast = make::try_owned(|mem| {
            // FIXME(sage): We should have a parse function on mem so we don't
            // need to parse and then copy the parsed tree just to throw the
            // original away
            let mut copy = mem.make_unique(&*ast.into_inner());
            if let Some(stmt) = copy.as_mut().into_iter().next() {
                rewrite_plan = rewriter.maybe_rewrite(stmt, mem)?;
            }
            Ok::<_, Error>(copy)
        })?;

        #[cfg(not(feature = "new_parser"))]
        let rewrite_plan = rewriter.maybe_rewrite()?;

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
            inner: Arc::new(AstInner {
                stats: Mutex::new(stats),
                #[cfg(feature = "new_parser")]
                ast,
                #[cfg(not(feature = "new_parser"))]
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
    #[cfg(feature = "new_parser")]
    pub(crate) fn new_record(
        query: &str,
        query_parser_engine: QueryParserEngine,
    ) -> Result<Self, Error> {
        let ast = pg_raw_parse::parse(query)?;

        Ok(Self {
            cached: true,
            comment_role: None,
            comment_shard: None,
            query_parser_engine,
            inner: Arc::new(AstInner::new(ast.into_inner())),
        })
    }

    cfg_select! {
        not(feature = "new_parser") => {
            pub fn new_record(query: &str, query_parser_engine: QueryParserEngine) -> Result<Self, Error> {
                let ast = match query_parser_engine {
                    QueryParserEngine::PgQueryProtobuf => parse(query),
                    QueryParserEngine::PgQueryRaw => parse_raw(query),
                }
                .map_err(Error::PgQuery)?;

                Ok(Self {
                    cached: true,
                    comment_role: None,
                    comment_shard: None,
                    query_parser_engine,
                    inner: Arc::new(AstInner::old(ast)),
                })
            }
        }
        _ => {}
    }

    /// Create new AST from a parse result.
    #[cfg(feature = "new_parser")]
    pub fn from_raw_stmts(stmts: Owned<StmtList>) -> Self {
        Self {
            cached: true,
            comment_role: None,
            comment_shard: None,
            query_parser_engine: QueryParserEngine::default(),
            inner: Arc::new(AstInner::new(stmts)),
        }
    }

    /// Create new AST from a parse result.
    #[cfg(not(feature = "new_parser"))]
    pub(crate) fn from_parse_result(parse_result: ParseResult) -> Self {
        Self {
            cached: true,
            comment_role: None,
            comment_shard: None,
            query_parser_engine: QueryParserEngine::default(),
            inner: Arc::new(AstInner::old(parse_result)),
        }
    }

    /// Get the reference to the AST.
    #[cfg(not(feature = "new_parser"))]
    pub(crate) fn parse_result(&self) -> &ParseResult {
        &self.ast
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
    #[cfg(feature = "new_parser")]
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

    cfg_select! {
        not(feature = "new_parser") => {
            pub(crate) fn statement_type(&self) -> StatementType {
                let root = self
                    .ast
                    .protobuf
                    .stmts
                    .first()
                    .and_then(|s| s.stmt.as_ref())
                    .and_then(|s| s.node.as_ref());

                match root {
                    Some(NodeEnum::SelectStmt(_))
                    | Some(NodeEnum::InsertStmt(_))
                    | Some(NodeEnum::UpdateStmt(_))
                    | Some(NodeEnum::DeleteStmt(_))
                    | Some(NodeEnum::CopyStmt(_))
                    | Some(NodeEnum::ExplainStmt(_))
                    | Some(NodeEnum::TransactionStmt(_)) => StatementType::Dml,

                    Some(NodeEnum::VariableSetStmt(_))
                    | Some(NodeEnum::VariableShowStmt(_))
                    | Some(NodeEnum::DeallocateStmt(_))
                    | Some(NodeEnum::ListenStmt(_))
                    | Some(NodeEnum::NotifyStmt(_))
                    | Some(NodeEnum::UnlistenStmt(_))
                    | Some(NodeEnum::DiscardStmt(_)) => StatementType::Session,

                    _ => StatementType::Ddl,
                }
            }
        }
        _ => {}
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum StatementType {
    Ddl,
    Dml,
    Session,
}
